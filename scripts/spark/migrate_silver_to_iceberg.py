"""migrate_silver_to_iceberg.py — Migration one-shot Parquet gi-poc-silver → Iceberg gi-silver.

Job Spark exécuté UNE SEULE FOIS pour backfiller l'historique complet dans les tables Iceberg.
Les tables Iceberg doivent être créées vides via Trino DDL AVANT d'exécuter ce job.

Mode DRY_RUN=true : lecture + comptage + schema, ZÉRO écriture Iceberg.
Mode exécution réelle (défaut) : APPEND dans chaque table Iceberg.

APPEND (jamais overwrite) : les tables Iceberg peuvent déjà contenir des données récentes
issues des nouveaux jobs Spark Silver — l'historique s'ajoute sans écraser l'incrémental.

Resilience : un échec sur une table ne stoppe pas le job — les autres tables continuent.

Dimensionnement recommandé : 4×m3.large OVH Data Processing.

Variables d'environnement requises :
  OVH_S3_ACCESS_KEY, OVH_S3_SECRET_KEY, OVH_S3_ENDPOINT
  OVH_ICEBERG_URI
  BUCKET_SILVER_SOURCE  (optionnel, défaut : gi-poc-silver — bucket Parquet source)
  DRY_RUN               (optionnel, défaut : false — mettre "true" pour validation préalable)

Validation post-append :
  Si count_iceberg < count_parquet_source × 0.99 → RuntimeError (perte de données détectée).
"""

import json
import os
import time
from datetime import datetime, timezone
from typing import NamedTuple

from pyspark.sql import SparkSession

JOB_NAME = "migrate_silver_to_iceberg"

# Seuil d'intégrité : 1% de perte maximum tolérée (doublons dédupliqués par Iceberg acceptables)
_INTEGRITY_THRESHOLD = 0.99


# ── Mapping complet Parquet source → Iceberg cible ───────────────────────────
# Source : section 5 du plan_migration_silver_iceberg.md (21 tables)
# Note : le plan mentionne "17 tables" dans son titre mais en liste 21 en section 5
#        (fin_mission, contrats_paie, contacts, encours_credit comptabilisés dans la liste finale)

class _Table(NamedTuple):
    src_path: str        # chemin relatif dans gi-poc-silver (sans bucket ni **/*.parquet)
    iceberg_table: str   # identifiant complet catalog.schema.table


TABLES: list[_Table] = [
    # ── missions ──────────────────────────────────────────────────────────────
    _Table("slv_missions/missions",                 "silver.missions.missions"),
    _Table("slv_missions/contrats",                 "silver.missions.contrats"),
    _Table("slv_missions/commandes",                "silver.missions.commandes"),
    _Table("slv_missions/placements",               "silver.missions.placements"),
    _Table("slv_missions/fin_mission",              "silver.missions.fin_mission"),
    _Table("slv_missions/contrats_paie",            "silver.missions.contrats_paie"),
    # ── temps ─────────────────────────────────────────────────────────────────
    _Table("slv_temps/releves_heures",              "silver.temps.releves_heures"),
    _Table("slv_temps/heures_detail",               "silver.temps.heures_detail"),
    # ── facturation ───────────────────────────────────────────────────────────
    _Table("slv_facturation/factures",              "silver.facturation.factures"),
    _Table("slv_facturation/lignes_factures",       "silver.facturation.lignes_factures"),
    # ── interimaires ──────────────────────────────────────────────────────────
    _Table("slv_interimaires/dim_interimaires",     "silver.interimaires.dim_interimaires"),
    _Table("slv_interimaires/evaluations",          "silver.interimaires.evaluations"),
    _Table("slv_interimaires/coordonnees",          "silver.interimaires.coordonnees"),
    _Table("slv_interimaires/portefeuille_agences", "silver.interimaires.portefeuille_agences"),
    _Table("slv_interimaires/competences",          "silver.interimaires.competences"),
    # ── clients ───────────────────────────────────────────────────────────────
    _Table("slv_clients/dim_clients",               "silver.clients.dim_clients"),
    _Table("slv_clients/sites_mission",             "silver.clients.sites_mission"),
    _Table("slv_clients/contacts",                  "silver.clients.contacts"),
    _Table("slv_clients/encours_credit",            "silver.clients.encours_credit"),
    # ── agences ───────────────────────────────────────────────────────────────
    _Table("slv_agences/dim_agences",               "silver.agences.dim_agences"),
    _Table("slv_agences/hierarchie_territoriale",   "silver.agences.hierarchie_territoriale"),
]


# ── Configuration ─────────────────────────────────────────────────────────────

class Cfg:
    """Paramètres lus depuis os.environ — pas de shared.py en environnement Spark OVH."""

    def __init__(self) -> None:
        self.access_key          = os.environ["OVH_S3_ACCESS_KEY"]
        self.secret_key          = os.environ["OVH_S3_SECRET_KEY"]
        self.endpoint            = os.environ["OVH_S3_ENDPOINT"]
        self.iceberg_uri         = os.environ["OVH_ICEBERG_URI"]
        self.bucket_silver_src   = os.environ.get("BUCKET_SILVER_SOURCE", "gi-poc-silver")
        self.dry_run             = os.environ.get("DRY_RUN", "false").lower() == "true"

    def parquet_path(self, src_path: str) -> str:
        """Chemin s3a:// complet vers les Parquet source (wildcard récursif)."""
        return f"s3a://{self.bucket_silver_src}/{src_path}/**/*.parquet"


# ── SparkSession ──────────────────────────────────────────────────────────────

def build_spark(cfg: Cfg) -> SparkSession:
    """SparkSession avec Iceberg REST Catalog 'silver' et S3A pour la lecture Parquet source.

    spark.network.timeout = 1800s (30 min) : couvre les tables volumineuses
    (heures_detail 760K+/j, lignes_factures 26M total) sans timeout Spark prématuré.
    """
    s3a_host = cfg.endpoint.replace("https://", "").replace("http://", "").rstrip("/")

    return (
        SparkSession.builder
        .appName(JOB_NAME)
        # Extensions Iceberg
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        # Catalog "silver" → REST Catalog OVH
        .config("spark.sql.catalog.silver",
                "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.silver.type", "rest")
        .config("spark.sql.catalog.silver.uri", cfg.iceberg_uri)
        # FileIO S3 pour les data-files Iceberg destination (path-style OVH)
        .config("spark.sql.catalog.silver.io-impl",
                "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.silver.s3.endpoint", cfg.endpoint)
        .config("spark.sql.catalog.silver.s3.access-key-id", cfg.access_key)
        .config("spark.sql.catalog.silver.s3.secret-access-key", cfg.secret_key)
        .config("spark.sql.catalog.silver.s3.path-style-access", "true")
        # Hadoop S3A — lecture Parquet source (gi-poc-silver) + data-files Iceberg
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint", s3a_host)
        .config("spark.hadoop.fs.s3a.access.key", cfg.access_key)
        .config("spark.hadoop.fs.s3a.secret.key", cfg.secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        # Timeout réseau 30 min — tables volumineuses (heures_detail, lignes_factures)
        .config("spark.network.timeout", "1800s")
        .config("spark.executor.heartbeatInterval", "300s")
        .config("spark.sql.broadcastTimeout", "1800")
        .getOrCreate()
    )


# ── Logging ───────────────────────────────────────────────────────────────────

def _log(payload: dict) -> None:
    """Log structuré JSON sur stdout — capturé par OVH Data Processing."""
    print(json.dumps({"job": JOB_NAME, **payload}, default=str), flush=True)


# ── Migration par table ───────────────────────────────────────────────────────

def migrate_table(
    spark: SparkSession,
    cfg: Cfg,
    table: _Table,
) -> dict:
    """Migre une table Parquet → Iceberg. Retourne un dict de résultat.

    DRY_RUN : lit + compte + loggue le schéma, ZÉRO écriture.
    Mode réel :
      1. Lire tous les Parquet source (s3a://gi-poc-silver/{src_path}/**/*.parquet)
      2. Logger src_count + schema
      3. df.writeTo(iceberg_table).option("merge-schema","true").append()
      4. Valider count Iceberg post-append ≥ src_count × 0.99
      5. Logger résultat
    """
    t0 = time.time()
    parquet_path = cfg.parquet_path(table.src_path)

    df = (
        spark.read
        .option("mergeSchema", "true")
        .parquet(parquet_path)
    )

    src_count  = df.count()
    schema_cols = df.schema.names

    _log({
        "table":      table.iceberg_table,
        "src_path":   parquet_path,
        "src_count":  src_count,
        "schema":     schema_cols,
        "dry_run":    cfg.dry_run,
    })

    if cfg.dry_run:
        return {
            "table":    table.iceberg_table,
            "src":      table.src_path,
            "count":    src_count,
            "schema":   schema_cols,
            "status":   "dry_run",
        }

    # ── Écriture Iceberg APPEND ───────────────────────────────────────────────
    # APPEND (jamais overwrite) : préserve les données incrémentales déjà présentes
    # merge-schema : tolère les évolutions de schéma entre Parquet historique et DDL Iceberg
    (df.writeTo(table.iceberg_table)
       .option("merge-schema", "true")
       .append())

    # ── Validation post-append ────────────────────────────────────────────────
    # Compter via le catalog Iceberg (lit les métadonnées Iceberg, pas le S3 brut)
    iceberg_count = spark.sql(
        f"SELECT COUNT(*) AS n FROM {table.iceberg_table}"
    ).collect()[0]["n"]

    if src_count > 0 and iceberg_count < src_count * _INTEGRITY_THRESHOLD:
        raise RuntimeError(
            f"Perte de données détectée sur {table.iceberg_table} : "
            f"src_count={src_count}, iceberg_count={iceberg_count} "
            f"(< {_INTEGRITY_THRESHOLD * 100:.0f}% du source). "
            "Vérifier le schéma Iceberg et les erreurs d'écriture."
        )

    duration_s = round(time.time() - t0, 2)
    _log({
        "table":         table.iceberg_table,
        "src_count":     src_count,
        "iceberg_count": iceberg_count,
        "duration_s":    duration_s,
        "status":        "OK",
    })

    return {
        "table":         table.iceberg_table,
        "src_count":     src_count,
        "iceberg_count": iceberg_count,
        "duration_s":    duration_s,
        "status":        "OK",
    }


# ── main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    cfg   = Cfg()
    spark = build_spark(cfg)

    mode_label = "dry_run" if cfg.dry_run else "live"
    _log({
        "phase":               "start",
        "mode":                mode_label,
        "tables_to_migrate":   len(TABLES),
        "bucket_silver_src":   cfg.bucket_silver_src,
        "started_at":          datetime.now(timezone.utc).isoformat(),
    })

    tables_ok:  list[dict] = []
    tables_ko:  list[dict] = []
    total_rows: int = 0

    for table in TABLES:
        try:
            result = migrate_table(spark, cfg, table)
            tables_ok.append(result)
            total_rows += result.get("src_count", 0)
        except Exception as exc:  # noqa: BLE001
            error_entry = {
                "table":  table.iceberg_table,
                "src":    table.src_path,
                "error":  str(exc),
                "status": "KO",
            }
            tables_ko.append(error_entry)
            _log({
                "table":  table.iceberg_table,
                "error":  str(exc),
                "status": "KO",
            })

    # ── Rapport final ─────────────────────────────────────────────────────────
    report: dict = {
        "mode":       mode_label,
        "migrated":   len(tables_ok),
        "failed":     len(tables_ko),
        "total_rows": total_rows,
        "tables_ok":  [t["table"] for t in tables_ok],
        "tables_ko":  [t["table"] for t in tables_ko],
        "finished_at": datetime.now(timezone.utc).isoformat(),
    }
    if cfg.dry_run:
        # Rapport dry_run enrichi avec les comptages pour validation humaine
        report["tables"] = [
            {"src": t["src"], "table": t["table"], "count": t["count"]}
            for t in tables_ok
        ]

    print(json.dumps(report, default=str), flush=True)

    if tables_ko:
        # Sortie non-zéro pour signaler les échecs à OVH Data Processing / Airflow
        raise SystemExit(
            f"{len(tables_ko)} table(s) en échec : "
            + ", ".join(t["table"] for t in tables_ko)
        )

    spark.stop()


if __name__ == "__main__":
    main()
