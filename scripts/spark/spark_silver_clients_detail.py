"""spark_silver_clients_detail.py — Silver · Clients détail (Spark/Iceberg).

Remplace silver_clients_detail.py (DuckDB → Parquet) — sink unique : Iceberg REST Catalog OVH.
Job soumis sur OVH Data Processing (Spark serverless). Pas d'import shared.py.

Tables produites :
  silver.clients.sites_mission    ← WTTIESERV     (~PK : TIE_ID + TIES_SERV)
  silver.clients.contacts         ← WTTIEINT      (~PK : TIE_ID + TIEI_ORDRE)
  silver.clients.encours_credit   ← WTENCOURSG    (~PK : ENCGRP_ID)

RGPD : contacts (email/tel) Silver-only — jamais exposé en Gold.

Corrections DDL probe 2026-03-05 :
  WTTIESERV : TIES_RS→TIES_DESIGNATION, TIES_SIRET absent DDL→NULL, TIES_CODPOS→TIES_CODEP
              +TIES_SIREN, +TIES_NIC, +TIES_EMAIL, +PAYS_CODE, +CLOT_DAT (is_active corrigé)
              RGPCNT_ID absent → NULL dans DDL d'origine, maintenant disponible Bronze v2
  WTTIEINT  : TIEINT_NOM→TIEI_NOM, TIEINT_PRENOM→TIEI_PRENOM,
              TIEINT_EMAIL→TIEI_EMAIL, TIEINT_TEL→TIEI_BUREAU,
              TIEINT_FONCTION→FCTI_CODE, ORDRE→TIEI_ORDRE
  WTENCOURSG : SIREN→ENC_SIREN, ENCGRP_MONTANT/LIMITE/DATE/STATUT absent DDL → NULL
               ENCG_DECISIONLIB disponible. ignore_errors→PERMISSIVE mode.

Variables d'environnement requises :
  OVH_S3_ACCESS_KEY, OVH_S3_SECRET_KEY, OVH_S3_ENDPOINT
  OVH_ICEBERG_URI, BUCKET_BRONZE
  SILVER_DATE_PARTITION  (optionnel, défaut : date du jour UTC YYYY/MM/DD)
"""

import json
import os
import time
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DecimalType,
    IntegerType,
    StringType,
    TimestampType,
)
from pyspark.sql.window import Window

JOB_NAME = "spark_silver_clients_detail"


# ── Configuration ─────────────────────────────────────────────────────────────

class Cfg:
    """Paramètres lus depuis os.environ — pas de shared.py en environnement Spark OVH."""

    def __init__(self) -> None:
        self.access_key     = os.environ["OVH_S3_ACCESS_KEY"]
        self.secret_key     = os.environ["OVH_S3_SECRET_KEY"]
        self.endpoint       = os.environ["OVH_S3_ENDPOINT"]
        self.iceberg_uri    = os.environ["OVH_ICEBERG_URI"]
        self.bucket_bronze  = os.environ.get("BUCKET_BRONZE", "gi-poc-bronze")
        self.date_partition = os.environ.get(
            "SILVER_DATE_PARTITION",
            datetime.now(timezone.utc).strftime("%Y/%m/%d"),
        )

    def bronze_path(self, table: str) -> str:
        """Chemin s3a:// partitionné par date."""
        return f"s3a://{self.bucket_bronze}/raw_{table}/{self.date_partition}/*.json"


# ── SparkSession ──────────────────────────────────────────────────────────────

def build_spark(cfg: Cfg) -> SparkSession:
    """SparkSession avec Iceberg REST Catalog 'silver' et S3A pour les lectures Bronze."""
    s3a_host = cfg.endpoint.replace("https://", "").replace("http://", "").rstrip("/")

    return (
        SparkSession.builder
        .appName(JOB_NAME)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.silver",
                "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.silver.type", "rest")
        .config("spark.sql.catalog.silver.uri", cfg.iceberg_uri)
        .config("spark.sql.catalog.silver.io-impl",
                "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.silver.s3.endpoint", cfg.endpoint)
        .config("spark.sql.catalog.silver.s3.access-key-id", cfg.access_key)
        .config("spark.sql.catalog.silver.s3.secret-access-key", cfg.secret_key)
        .config("spark.sql.catalog.silver.s3.path-style-access", "true")
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
    print(json.dumps({
        "job": JOB_NAME,
        "table": table,
        "rows": rows,
        "duration_s": round(duration_s, 2),
    }), flush=True)


def _log_error(table: str, error: str, duration_s: float) -> None:
    print(json.dumps({
        "job": JOB_NAME,
        "table": table,
        "error": error,
        "duration_s": round(duration_s, 2),
    }), flush=True)


def _write_iceberg(df: DataFrame, table_id: str) -> None:
    """Écriture Iceberg — overwritePartitions() remplace les partitions existantes."""
    (df.writeTo(table_id)
       .option("merge-schema", "true")
       .overwritePartitions())


def _read_bronze(spark: SparkSession, path: str) -> DataFrame:
    """Lecture JSON Bronze avec fusion de schéma."""
    return spark.read.option("mergeSchema", "true").json(path)


def _nic_or_null(col_expr) -> "Column":
    """CASE WHEN LEN(TRIM(NIC)) = 5 THEN TRIM(NIC) ELSE NULL END."""
    trimmed = F.trim(col_expr.cast(StringType()))
    return F.when(F.length(trimmed) == 5, trimmed).otherwise(F.lit(None).cast(StringType()))


# ── Tables ────────────────────────────────────────────────────────────────────

def process_sites_mission(spark: SparkSession, cfg: Cfg) -> DataFrame:
    """silver.clients.sites_mission ← WTTIESERV.

    Dédup : ROW_NUMBER() OVER (PARTITION BY TIE_ID, TIES_SERV ORDER BY _loaded_at DESC) = 1
    is_active : CLOT_DAT IS NULL
    row_hash  : MD5 sur siren + nic + adresse + email (détection changements)
    Corrections DDL 2026-03-05/2026-03-11 :
      TIES_DESIGNATION (ex TIES_RS), TIES_CODEP (ex TIES_CODPOS),
      +TIES_SIREN, +TIES_NIC, +TIES_EMAIL, +PAYS_CODE, +CLOT_DAT
    """
    w = Window.partitionBy("TIE_ID", "TIES_SERV").orderBy(F.col("_loaded_at").desc())
    return (
        _read_bronze(spark, cfg.bronze_path("wttieserv"))
        .filter(F.col("TIE_ID").isNotNull())
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .select(
            F.col("TIES_SERV").cast(IntegerType()).alias("site_id"),
            F.col("TIE_ID").cast(IntegerType()).alias("tie_id"),
            F.trim(F.col("TIES_DESIGNATION")).alias("nom_site"),
            F.trim(F.coalesce(F.col("TIES_SIREN"), F.lit(""))).alias("siren"),
            _nic_or_null(F.col("TIES_NIC")).alias("nic"),
            F.concat_ws(
                " ",
                F.col("TIES_ADR1"), F.col("TIES_ADR2"),
                F.col("TIES_CODEP"), F.col("TIES_VILLE"),
            ).alias("adresse"),
            F.trim(F.col("TIES_VILLE")).alias("ville"),
            F.trim(F.col("TIES_CODEP")).alias("code_postal"),
            F.upper(F.trim(F.coalesce(F.col("PAYS_CODE"), F.lit("FR")))).alias("pays_code"),
            F.lit(None).cast(StringType()).alias("siret_site"),  # absent DDL
            F.lower(F.trim(F.col("TIES_EMAIL"))).alias("email"),
            F.col("RGPCNT_ID").cast(IntegerType()).alias("agence_id"),
            F.col("CLOT_DAT").isNull().cast(BooleanType()).alias("is_active"),
            F.col("CLOT_DAT").cast(DateType()).alias("clot_at"),
            # row_hash : détection des changements clés site
            F.md5(F.concat_ws(
                "|",
                F.trim(F.coalesce(F.col("TIES_SIREN"), F.lit(""))),
                F.when(F.length(F.trim(F.coalesce(F.col("TIES_NIC"), F.lit("")))) == 5,
                       F.trim(F.col("TIES_NIC"))).otherwise(F.lit("")),
                F.trim(F.coalesce(F.col("TIES_ADR1"), F.lit(""))),
                F.trim(F.coalesce(F.col("TIES_CODEP"), F.lit(""))),
                F.trim(F.coalesce(F.col("TIES_VILLE"), F.lit(""))),
                F.lower(F.trim(F.coalesce(F.col("TIES_EMAIL"), F.lit("")))),
            )).alias("row_hash"),
            F.current_timestamp().alias("_loaded_at"),
        )
    )


def process_contacts(spark: SparkSession, cfg: Cfg) -> DataFrame:
    """silver.clients.contacts ← WTTIEINT.

    RGPD : email/tel Silver-only — jamais exposé en Gold.
    Dédup : ROW_NUMBER() OVER (PARTITION BY TIE_ID, TIEI_ORDRE ORDER BY _loaded_at DESC) = 1
    contact_id : MD5(concat(tie_id, '|', tiei_ordre))
    Corrections DDL probe 2026-03-05 :
      TIEINT_NOM     → TIEI_NOM
      TIEINT_PRENOM  → TIEI_PRENOM
      TIEINT_EMAIL   → TIEI_EMAIL
      TIEINT_TEL     → TIEI_BUREAU
      TIEINT_FONCTION → FCTI_CODE
      ORDRE          → TIEI_ORDRE
    """
    w = Window.partitionBy("TIE_ID", "TIEI_ORDRE").orderBy(F.col("_loaded_at").desc())
    return (
        _read_bronze(spark, cfg.bronze_path("wttieint"))
        .filter(F.col("TIE_ID").isNotNull())
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .select(
            F.md5(F.concat(
                F.col("TIE_ID").cast(StringType()),
                F.lit("|"),
                F.col("TIEI_ORDRE").cast(StringType()),
            )).alias("contact_id"),
            F.col("TIE_ID").cast(IntegerType()).alias("tie_id"),
            F.trim(F.col("TIEI_NOM")).alias("nom"),
            F.trim(F.col("TIEI_PRENOM")).alias("prenom"),
            F.trim(F.col("TIEI_EMAIL")).alias("email"),
            F.trim(F.col("TIEI_BUREAU")).alias("telephone"),
            F.trim(F.col("FCTI_CODE")).alias("fonction_code"),
            F.current_timestamp().alias("_loaded_at"),
        )
    )


def process_encours_credit(spark: SparkSession, cfg: Cfg) -> DataFrame:
    """silver.clients.encours_credit ← WTENCOURSG.

    Dédup : ROW_NUMBER() OVER (PARTITION BY ENCGRP_ID ORDER BY _loaded_at DESC) = 1
    Colonnes DDL absentes → NULL : montant_encours, limite_credit, date_decision.
    PERMISSIVE mode (équivalent ignore_errors=true DuckDB) — lecteur JSON tolère les erreurs.
    Corrections DDL probe 2026-03-05 :
      SIREN       → ENC_SIREN
      ENCG_DECISIONLIB disponible (libellé décision)
    """
    # PERMISSIVE : lignes corrompues → NULL (équivalent DuckDB ignore_errors=true)
    raw = (
        spark.read
        .option("mergeSchema", "true")
        .option("mode", "PERMISSIVE")
        .json(cfg.bronze_path("wtencoursg"))
    )

    w = Window.partitionBy("ENCGRP_ID").orderBy(F.col("_loaded_at").desc())
    return (
        raw
        .filter(F.col("ENCGRP_ID").isNotNull())
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .select(
            F.col("ENCGRP_ID").cast(IntegerType()).alias("encours_id"),
            F.trim(F.col("ENC_SIREN")).alias("siren"),
            F.lit(None).cast(DecimalType(18, 2)).alias("montant_encours"),  # absent DDL
            F.lit(None).cast(DecimalType(18, 2)).alias("limite_credit"),    # absent DDL
            F.lit(None).cast(DateType()).alias("date_decision"),             # absent DDL
            F.trim(F.coalesce(
                F.col("ENCG_DECISIONLIB").cast(StringType()), F.lit("")
            )).alias("decision_libelle"),
            F.current_timestamp().alias("_loaded_at"),
        )
    )


# ── main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    cfg = Cfg()
    spark = build_spark(cfg)

    tables = [
        (process_sites_mission,  "silver.clients.sites_mission"),
        (process_contacts,       "silver.clients.contacts"),
        (process_encours_credit, "silver.clients.encours_credit"),
    ]

    for process_fn, table_id in tables:
        table_name = table_id.split(".")[-1]
        t0 = time.time()
        try:
            df = process_fn(spark, cfg).cache()
            rows = df.count()
            _write_iceberg(df, table_id)
            df.unpersist()
            _log(table_name, rows, time.time() - t0)
        except Exception as exc:  # noqa: BLE001
            _log_error(table_name, str(exc), time.time() - t0)

    spark.stop()


if __name__ == "__main__":
    main()
