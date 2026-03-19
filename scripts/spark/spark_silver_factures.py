"""spark_silver_factures.py — Silver · Facturation (Spark/Iceberg).

Remplace silver_factures.py (DuckDB → Parquet) — sink unique : Iceberg REST Catalog OVH.
Job soumis sur OVH Data Processing (Spark serverless). Pas d'import shared.py.

Tables produites :
  silver.facturation.factures         ← WTEFAC           (~PK : EFAC_NUM)
  silver.facturation.lignes_factures  ← WTLFAC × WTEFAC  (~PK : FAC_NUM + LFAC_ORD)

Corrections DDL probe 2026-03-05 :
  WTEFAC : EFAC_TYPE→EFAC_TYPF, EFAC_DATE→EFAC_DTEEDI, EFAC_ECHEANCE→EFAC_DTEECH
           EFAC_MONTANTHT/TTC absent DDL → NULL ; PRH_BTS absent DDL → NULL
  WTLFAC : LFAC_LIBELLE→LFAC_LIB, LFAC_RUBRIQUE absent DDL → NULL

Contrainte critique WTLFAC (26M lignes total, documentée bronze_missions.py 2026-03-14) :
  WTLFAC est en TABLES_FULL Bronze → full-dump dans raw_wtlfac/{date_partition}/*.json.
  Le filtre EFAC_DTEEDI >= 2024-01-01 est appliqué via JOIN WTLFAC × WTEFAC(historique).
  Sans ce filtre, 26M lignes satureraient le cluster. Ne JAMAIS l'omettre.
  WTEFAC historique : raw_wtefac/**/*.json (scan complet pour couvrir toutes les factures).

Variables d'environnement requises :
  OVH_S3_ACCESS_KEY, OVH_S3_SECRET_KEY, OVH_S3_ENDPOINT
  OVH_ICEBERG_URI, BUCKET_BRONZE
  SILVER_DATE_PARTITION  (optionnel, défaut : date du jour UTC YYYY/MM/DD)

lignes_factures :
  spark.sql.shuffle.partitions = 200  (réglé dans process_lignes_factures)
  coalesce(10) avant writeTo pour limiter la fragmentation des data-files Iceberg.
  Partition Iceberg : month(efac_dteedi) — colonne incluse dans le DataFrame.
  Validation post-écriture : WARNING si count < 1 000 000 (filtre 2024 suspect).
"""

import json
import os
import time
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import (
    DateType,
    DecimalType,
    IntegerType,
    StringType,
    TimestampType,
)
from pyspark.sql.window import Window

JOB_NAME = "spark_silver_factures"

# Horizon fixe WTLFAC — documenté bronze_missions.py 2026-03-14 (non avançant).
_WTLFAC_HORIZON = "2024-01-01"

# Seuil de validation post-écriture lignes_factures.
# En dessous de ce seuil, le filtre 2024 est probablement cassé.
_LIGNES_MIN_COUNT = 1_000_000


# ── Configuration ─────────────────────────────────────────────────────────────

class Cfg:
    """Paramètres lus depuis os.environ — pas de shared.py en environnement Spark OVH."""

    def __init__(self) -> None:
        self.access_key     = os.environ["OVH_S3_ACCESS_KEY"]
        self.secret_key     = os.environ["OVH_S3_SECRET_KEY"]
        self.endpoint       = os.environ["OVH_S3_ENDPOINT"]          # https://s3.gra.io.cloud.ovh.net
        self.iceberg_uri    = os.environ["OVH_ICEBERG_URI"]
        self.bucket_bronze  = os.environ.get("BUCKET_BRONZE", "gi-poc-bronze")
        self.date_partition = os.environ.get(
            "SILVER_DATE_PARTITION",
            datetime.now(timezone.utc).strftime("%Y/%m/%d"),
        )

    def bronze_path(self, table: str) -> str:
        """Chemin s3a:// partitionné par date — pour les tables delta (WTEFAC)."""
        return f"s3a://{self.bucket_bronze}/raw_{table}/{self.date_partition}/*.json"

    def bronze_path_full(self, table: str) -> str:
        """Chemin s3a:// TABLES_FULL — dump complet sans partitionnement date (WTLFAC)."""
        return f"s3a://{self.bucket_bronze}/raw_{table}/{self.date_partition}/*.json"

    def bronze_path_history(self, table: str) -> str:
        """Chemin s3a:// scan historique complet — pour JOIN inter-jours (WTEFAC dans lignes)."""
        return f"s3a://{self.bucket_bronze}/raw_{table}/**/*.json"


# ── SparkSession ──────────────────────────────────────────────────────────────

def build_spark(cfg: Cfg) -> SparkSession:
    """SparkSession avec Iceberg REST Catalog 'silver' et S3A pour les lectures Bronze."""
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
        # FileIO S3 pour les data-files Iceberg (path-style obligatoire OVH)
        .config("spark.sql.catalog.silver.io-impl",
                "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.silver.s3.endpoint", cfg.endpoint)
        .config("spark.sql.catalog.silver.s3.access-key-id", cfg.access_key)
        .config("spark.sql.catalog.silver.s3.secret-access-key", cfg.secret_key)
        .config("spark.sql.catalog.silver.s3.path-style-access", "true")
        # Hadoop S3A — lectures Bronze JSON
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint", s3a_host)
        .config("spark.hadoop.fs.s3a.access.key", cfg.access_key)
        .config("spark.hadoop.fs.s3a.secret.key", cfg.secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .getOrCreate()
    )


# ── Helpers ───────────────────────────────────────────────────────────────────

def _log(table: str, rows: int, duration_s: float) -> None:
    """Log structuré JSON sur stdout — capturé par OVH Data Processing."""
    print(json.dumps({
        "job": JOB_NAME,
        "table": table,
        "rows": rows,
        "duration_s": round(duration_s, 2),
    }), flush=True)


def _log_warning(table: str, message: str) -> None:
    print(json.dumps({
        "job": JOB_NAME,
        "table": table,
        "level": "WARNING",
        "message": message,
    }), flush=True)


def _log_error(table: str, error: str, duration_s: float) -> None:
    print(json.dumps({
        "job": JOB_NAME,
        "table": table,
        "error": error,
        "duration_s": round(duration_s, 2),
    }), flush=True)


def _write_iceberg(df: DataFrame, table_id: str) -> None:
    """Écriture Iceberg unique — overwritePartitions() remplace les partitions existantes."""
    (df.writeTo(table_id)
       .option("merge-schema", "true")
       .overwritePartitions())


def _read_bronze(spark: SparkSession, path: str) -> DataFrame:
    """Lecture JSON Bronze avec fusion de schéma (équivalent union_by_name DuckDB)."""
    return spark.read.option("mergeSchema", "true").json(path)


# ── Tables ────────────────────────────────────────────────────────────────────

def process_factures(spark: SparkSession, cfg: Cfg) -> DataFrame:
    """silver.facturation.factures ← WTEFAC (delta quotidien).

    Dédup : QUALIFY ROW_NUMBER() OVER (PARTITION BY EFAC_NUM ORDER BY _loaded_at DESC) = 1
    Corrections DDL probe 2026-03-05 :
      EFAC_TYPE    → EFAC_TYPF
      EFAC_DATE    → EFAC_DTEEDI
      EFAC_ECHEANCE → EFAC_DTEECH
      EFAC_MONTANTHT / EFAC_MONTANTTC absent DDL → NULL::DECIMAL(18,2)
      PRH_BTS absent DDL → NULL::INTEGER
    """
    w = Window.partitionBy("EFAC_NUM").orderBy(F.col("_loaded_at").desc())
    return (
        _read_bronze(spark, cfg.bronze_path("wtefac"))
        .filter(F.col("EFAC_NUM").isNotNull())
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .select(
            F.trim(F.col("EFAC_NUM")).alias("efac_num"),
            F.col("RGPCNT_ID").cast(IntegerType()).alias("rgpcnt_id"),
            F.col("TIE_ID").cast(IntegerType()).alias("tie_id"),
            F.col("TIES_SERV").cast(IntegerType()).alias("ties_serv"),
            F.trim(F.col("EFAC_TYPF")).alias("type_facture"),
            F.col("EFAC_DTEEDI").cast(DateType()).alias("date_facture"),
            F.col("EFAC_DTEECH").cast(DateType()).alias("date_echeance"),
            F.lit(None).cast(DecimalType(18, 2)).alias("montant_ht"),   # absent DDL
            F.lit(None).cast(DecimalType(18, 2)).alias("montant_ttc"),  # absent DDL
            F.col("EFAC_TAUXTVA").cast(DecimalType(6, 4)).alias("taux_tva"),
            F.lit(None).cast(IntegerType()).alias("prh_bts"),            # absent DDL
            F.col("_batch_id"),
            F.col("_loaded_at").cast(TimestampType()).alias("_loaded_at"),
        )
    )


def process_lignes_factures(spark: SparkSession, cfg: Cfg) -> DataFrame:
    """silver.facturation.lignes_factures ← WTLFAC JOIN WTEFAC.

    WTLFAC est en TABLES_FULL Bronze (26M lignes total, full-dump quotidien).
    Le filtre EFAC_DTEEDI >= 2024-01-01 via JOIN WTEFAC est OBLIGATOIRE :
    sans lui, 26M lignes satureraient le cluster (documenté bronze_missions.py 2026-03-14).

    Stratégie JOIN :
      WTLFAC  : raw_wtlfac/{date_partition}/*.json  (dump complet filtré côté Bronze)
      WTEFAC  : raw_wtefac/**/*.json                (scan historique complet pour la jointure)
      Le filtre date est appliqué SUR WTEFAC avant le join → predicate pushdown efficace.

    efac_dteedi : colonne de WTEFAC incluse dans l'output pour le partitionnement
    Iceberg month(efac_dteedi). Absente du SELECT DuckDB original.

    Dédup WTLFAC : QUALIFY ROW_NUMBER() OVER (PARTITION BY FAC_NUM, LFAC_ORD
                                               ORDER BY _loaded_at DESC) = 1
    Corrections DDL probe 2026-03-05 :
      LFAC_LIBELLE → LFAC_LIB
      LFAC_RUBRIQUE absent DDL → NULL::VARCHAR
    """
    # shuffle.partitions = 200 pour absorber le shuffle du JOIN 26M lignes
    spark.conf.set("spark.sql.shuffle.partitions", "200")

    # ── WTEFAC historique : filtre date appliqué avant JOIN (predicate pushdown) ─
    # Scan complet **/*.json pour couvrir toutes les factures historiques
    # que WTLFAC peut référencer (WTEFAC delta quotidien ne couvre pas tout l'historique)
    wtefac = (
        _read_bronze(spark, cfg.bronze_path_history("wtefac"))
        .select(
            F.trim(F.col("EFAC_NUM")).alias("_efac_num"),
            F.col("EFAC_DTEEDI").cast(DateType()).alias("efac_dteedi"),
        )
        .filter(F.col("efac_dteedi") >= F.lit(_WTLFAC_HORIZON).cast(DateType()))
        # Dédup WTEFAC sur EFAC_NUM pour éviter la multiplication des lignes WTLFAC
        .dropDuplicates(["_efac_num"])
    )

    # ── WTLFAC full-dump : dédup par (FAC_NUM, LFAC_ORD) ────────────────────
    w = Window.partitionBy("FAC_NUM", "LFAC_ORD").orderBy(F.col("_loaded_at").desc())
    wtlfac = (
        _read_bronze(spark, cfg.bronze_path_full("wtlfac"))
        .filter(F.col("FAC_NUM").isNotNull() & F.col("LFAC_ORD").isNotNull())
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    # ── JOIN INNER : seules les lignes liées à des factures >= 2024-01-01 ───
    # INNER join : ligne WTLFAC sans WTEFAC correspondant = orpheline → exclue
    df = (
        wtlfac
        .join(wtefac, F.trim(F.col("FAC_NUM")) == F.col("_efac_num"), "inner")
        .select(
            F.trim(F.col("FAC_NUM")).alias("fac_num"),
            F.col("LFAC_ORD").cast(IntegerType()).alias("lfac_ord"),
            F.trim(F.col("LFAC_LIB")).alias("libelle"),
            F.col("LFAC_BASE").cast(DecimalType(10, 2)).alias("base"),
            F.col("LFAC_TAUX").cast(DecimalType(10, 4)).alias("taux"),
            F.col("LFAC_MNT").cast(DecimalType(18, 2)).alias("montant"),
            F.lit(None).cast(StringType()).alias("rubrique"),  # absent DDL
            # Colonne de partition Iceberg month(efac_dteedi) — issue du JOIN WTEFAC
            F.col("efac_dteedi"),
            F.col("_batch_id"),
            F.col("_loaded_at").cast(TimestampType()).alias("_loaded_at"),
        )
    )

    # coalesce(10) : réduit la fragmentation des data-files Iceberg sur le volume WTLFAC
    return df.coalesce(10)


# ── main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    cfg = Cfg()
    spark = build_spark(cfg)

    tables = [
        (process_factures,         "silver.facturation.factures"),
        (process_lignes_factures,  "silver.facturation.lignes_factures"),
    ]

    for process_fn, table_id in tables:
        table_name = table_id.split(".")[-1]
        t0 = time.time()
        try:
            df = process_fn(spark, cfg).cache()
            rows = df.count()
            _write_iceberg(df, table_id)
            df.unpersist()

            # Validation post-écriture lignes_factures :
            # un count < 1M signale que le filtre horizon 2024 est probablement cassé
            if table_id == "silver.facturation.lignes_factures" and rows < _LIGNES_MIN_COUNT:
                _log_warning(
                    table_name,
                    f"count suspect post-filtre {_WTLFAC_HORIZON} : {rows} "
                    f"(attendu >= {_LIGNES_MIN_COUNT}). "
                    "Vérifier le JOIN WTLFAC × WTEFAC et le dump Bronze raw_wtlfac.",
                )

            _log(table_name, rows, time.time() - t0)
        except Exception as exc:  # noqa: BLE001
            _log_error(table_name, str(exc), time.time() - t0)

    spark.stop()


if __name__ == "__main__":
    main()
