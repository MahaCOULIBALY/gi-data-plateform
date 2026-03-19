"""spark_silver_clients.py — Silver · dim_clients SCD Type 2 (Spark/Iceberg).

Remplace silver_clients.py (DuckDB → Parquet) — sink unique : Iceberg REST Catalog OVH.
Job soumis sur OVH Data Processing (Spark serverless). Pas d'import shared.py.

Table produite :
  silver.clients.dim_clients  ← WTTIESERV × WTCLPT  (~PK : tie_id)

SCD Type 2 : existing=[] (pas de lecture Iceberg) → tous les enregistrements staging
sont insérés comme nouvelles versions is_current=True. Comportement identique à la
version DuckDB où existing est toujours une liste vide (pas de read-back Iceberg).

SCD2_TRACKED : raison_sociale, siren, nic, naf_code, adresse_complete, ville,
               code_postal, statut_client, ca_potentiel.

Corrections DDL probe 2026-03-05 :
  WTTIESERV : TIES_RS→TIES_DESIGNATION, TIES_CODPOS→TIES_CODEP, TIES_SIRET absent DDL
              +TIES_SIREN (via TIES_SIREN), naf via NAF/NAF2008
              Adresse : TIES_ADR1/ADR2/CODEP/VILLE confirmés
  WTCLPT    : CLPT_PROSPEC→CLPT_PROS, CLPT_CAESTIME→CLPT_CAPT, CLPT_DATCREA→CLPT_DCREA
Corrections DDL 2026-03-11 :
  NIC ajouté au tracking SCD2 (TIES_NIC validé len=5)

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

JOB_NAME = "spark_silver_clients"

# Colonnes trackées par SCD2 — doit correspondre à SCD2_TRACKED_COLS de silver_clients.py
SCD2_TRACKED = (
    "raison_sociale", "siren", "nic", "naf_code",
    "adresse_complete", "ville", "code_postal",
    "statut_client", "ca_potentiel",
)


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


def _read_bronze(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.option("mergeSchema", "true").json(path)


def _nic_or_null(col_expr) -> "Column":
    """CASE WHEN LEN(TRIM(NIC)) = 5 THEN TRIM(NIC) ELSE NULL END."""
    trimmed = F.trim(col_expr.cast(StringType()))
    return F.when(F.length(trimmed) == 5, trimmed).otherwise(F.lit(None).cast(StringType()))


# ── Staging ────────────────────────────────────────────────────────────────────

def build_staging(spark: SparkSession, cfg: Cfg) -> DataFrame:
    """Staging WTTIESERV × WTCLPT (dédup WTTIESERV sur TIE_ID + TIES_SERV).

    Corrections DDL probe 2026-03-05/2026-03-11 (voir module docstring).
    """
    # ── WTTIESERV : dédup sur (TIE_ID, TIES_SERV) ──
    w_ties = Window.partitionBy("TIE_ID", "TIES_SERV").orderBy(F.col("_loaded_at").desc())
    raw_ties = (
        _read_bronze(spark, cfg.bronze_path("wttieserv"))
        .filter(F.col("TIE_ID").isNotNull())
        .withColumn("_rn", F.row_number().over(w_ties))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    # ── WTCLPT : pas de dédup — jointure directe sur TIE_ID ──
    raw_clpt = _read_bronze(spark, cfg.bronze_path("wtclpt"))

    return (
        raw_ties
        .join(raw_clpt.select(
            F.col("TIE_ID").cast(IntegerType()).alias("_clpt_tie_id"),
            F.trim(F.coalesce(F.col("CLPT_PROS").cast(StringType()), F.lit("INCONNU"))).alias("statut_client"),
            F.col("CLPT_CAPT").cast(DecimalType(18, 2)).alias("ca_potentiel"),
            F.col("CLPT_DCREA").cast(DateType()).alias("date_creation_fiche"),
        ), raw_ties["TIE_ID"].cast(IntegerType()) == F.col("_clpt_tie_id"), "left")
        .select(
            F.col("TIE_ID").cast(IntegerType()).alias("tie_id"),
            F.trim(F.col("TIES_DESIGNATION")).alias("raison_sociale"),
            F.trim(F.coalesce(F.col("TIES_SIREN"), F.lit(""))).alias("siren"),
            _nic_or_null(F.col("TIES_NIC")).alias("nic"),
            F.trim(F.coalesce(
                F.col("NAF"),
                F.coalesce(F.col("NAF2008"), F.lit("")),
            )).alias("naf_code"),
            F.concat_ws(
                " ",
                F.col("TIES_ADR1"), F.col("TIES_ADR2"),
                F.col("TIES_CODEP"), F.col("TIES_VILLE"),
            ).alias("adresse_complete"),
            F.trim(F.col("TIES_VILLE")).alias("ville"),
            F.trim(F.col("TIES_CODEP")).alias("code_postal"),
            F.coalesce(F.col("statut_client"), F.lit("INCONNU")).alias("statut_client"),
            F.col("ca_potentiel"),
            F.col("date_creation_fiche"),
            F.col("_batch_id").alias("_source_raw_id"),
        )
    )


# ── SCD2 transform ────────────────────────────────────────────────────────────

def apply_scd2(staging: DataFrame, now_iso: str) -> DataFrame:
    """Applique les colonnes SCD2 sur le staging.

    existing=[] dans la version DuckDB → pas de read-back Iceberg, pas de fermeture
    de versions précédentes. Tous les enregistrements staging deviennent de nouvelles
    versions is_current=True.

    change_hash : MD5 sur la concaténation pipe-séparé des colonnes SCD2_TRACKED.
    client_sk   : MD5(concat(tie_id, '|', now_iso)) — clé technique surrogate.
    """
    # change_hash : reproduit hashlib.md5("|".join(str(row[c]) for c in SCD2_TRACKED))
    hash_parts = F.concat_ws(
        "|",
        *[F.coalesce(F.col(c).cast(StringType()), F.lit("")) for c in SCD2_TRACKED],
    )

    return staging.select(
        # Surrogate key
        F.md5(F.concat_ws("|", F.col("tie_id").cast(StringType()), F.lit(now_iso))).alias("client_sk"),
        F.col("tie_id"),
        F.md5(hash_parts).alias("change_hash"),
        F.col("raison_sociale"),
        F.when(F.col("siren") != "", F.col("siren")).otherwise(F.lit(None).cast(StringType())).alias("siren"),
        F.col("nic"),
        F.when(F.col("naf_code") != "", F.col("naf_code")).otherwise(F.lit(None).cast(StringType())).alias("naf_code"),
        F.col("adresse_complete"),
        F.col("ville"),
        F.col("code_postal"),
        F.col("statut_client"),
        F.col("ca_potentiel"),
        F.col("date_creation_fiche"),
        F.lit(True).cast(BooleanType()).alias("is_current"),
        F.lit(now_iso).cast(TimestampType()).alias("valid_from"),
        F.lit(None).cast(TimestampType()).alias("valid_to"),
        F.col("_source_raw_id"),
        F.lit(now_iso).cast(TimestampType()).alias("_loaded_at"),
    )


# ── main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    cfg = Cfg()
    spark = build_spark(cfg)

    now_iso = datetime.now(timezone.utc).isoformat()
    table_id = "silver.clients.dim_clients"
    table_name = "dim_clients"
    t0 = time.time()

    try:
        staging = build_staging(spark, cfg)
        df = apply_scd2(staging, now_iso).cache()
        rows = df.count()
        (df.writeTo(table_id)
           .option("merge-schema", "true")
           .overwritePartitions())
        df.unpersist()
        _log(table_name, rows, time.time() - t0)
    except Exception as exc:  # noqa: BLE001
        _log_error(table_name, str(exc), time.time() - t0)

    spark.stop()


if __name__ == "__main__":
    main()
