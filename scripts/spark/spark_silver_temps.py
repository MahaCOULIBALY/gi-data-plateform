"""spark_silver_temps.py — Silver · Temps & Relevés d'heures (Spark/Iceberg).

Remplace silver_temps.py (DuckDB → Parquet) — sink unique : Iceberg REST Catalog OVH.
Job soumis sur OVH Data Processing (Spark serverless). Pas d'import shared.py.

Tables produites :
  silver.temps.releves_heures   ← WTPRH         (~PK : PRH_BTS)
  silver.temps.heures_detail    ← WTRHDON        (~PK : PRH_BTS + RHD_LIGNE, 760K+/j)

Corrections DDL probe 2026-03-05 :
  WTRHDON : RHD_RUBRIQUE→RHD_ORIRUB, RHD_BASEPAYE→RHD_BASEP, RHD_TAUXPAYE→RHD_TAUXP
            RHD_BASEFACT→RHD_BASEF, RHD_TAUXFACT→RHD_TAUXF, RHD_LIBELLE→RHD_LIBRUB
  WTPRH   : PRH_DATEMODIF→PRH_MODIFDATE, PRH_VALIDE→PRH_FLAG_RH
            PRH_PERIODE/RGPCNT_ID→NULL (absent DDL, confirmé probe)

RGPD : PER_ID absent de heures_detail — jointure via PRH_BTS uniquement. Aucun NIR.

Variables d'environnement requises :
  OVH_S3_ACCESS_KEY, OVH_S3_SECRET_KEY, OVH_S3_ENDPOINT
  OVH_ICEBERG_URI, BUCKET_BRONZE
  SILVER_DATE_PARTITION  (optionnel, défaut : date du jour UTC YYYY/MM/DD)
  SPARK_WORKERS          (optionnel, défaut : 2 — adapte spark.executor.instances)

heures_detail : coalesce(20) avant writeTo pour limiter la fragmentation Iceberg.
                Partition Iceberg : month(rhd_dated) — colonne incluse dans le DataFrame.
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

JOB_NAME = "spark_silver_temps"


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
        # Nombre de workers Spark — adapte executor.instances pour les gros volumes
        self.spark_workers  = int(os.environ.get("SPARK_WORKERS", "2"))

    def bronze_path(self, table: str) -> str:
        """Chemin s3a:// vers les fichiers JSON Bronze d'une table pour la partition courante."""
        return f"s3a://{self.bucket_bronze}/raw_{table}/{self.date_partition}/*.json"


# ── SparkSession ──────────────────────────────────────────────────────────────

def build_spark(cfg: Cfg) -> SparkSession:
    """SparkSession avec Iceberg REST Catalog 'silver' et S3A pour les lectures Bronze.

    spark.executor.instances est calé sur SPARK_WORKERS — utile pour adapter
    la parallélisation au volume WTRHDON (760K+/j) sans sur-allouer en mode light.
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
        # Dimensionnement workers (WTRHDON 760K+/j)
        .config("spark.executor.instances", str(cfg.spark_workers))
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


def _read_bronze(spark: SparkSession, cfg: Cfg, table: str) -> DataFrame:
    """Lecture JSON Bronze avec fusion de schéma (équivalent union_by_name DuckDB)."""
    return spark.read.option("mergeSchema", "true").json(cfg.bronze_path(table))


# ── Tables ────────────────────────────────────────────────────────────────────

def process_releves_heures(spark: SparkSession, cfg: Cfg) -> DataFrame:
    """silver.temps.releves_heures ← WTPRH.

    Dédup : QUALIFY ROW_NUMBER() OVER (PARTITION BY PRH_BTS ORDER BY _loaded_at DESC) = 1
    Corrections DDL probe 2026-03-05 :
      PRH_DATEMODIF → PRH_MODIFDATE
      PRH_VALIDE    → PRH_FLAG_RH
      PRH_PERIODE / RGPCNT_ID → NULL (absent DDL confirmé)
    """
    w = Window.partitionBy("PRH_BTS").orderBy(F.col("_loaded_at").desc())
    return (
        _read_bronze(spark, cfg, "wtprh")
        .filter(F.col("PRH_BTS").isNotNull())
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .select(
            F.col("PRH_BTS").cast(IntegerType()).alias("prh_bts"),
            F.col("PER_ID").cast(IntegerType()).alias("per_id"),
            F.col("CNT_ID").cast(IntegerType()).alias("cnt_id"),
            F.col("TIE_ID").cast(IntegerType()).alias("tie_id"),
            F.lit(None).cast(IntegerType()).alias("rgpcnt_id"),  # absent DDL
            F.lit(None).cast(StringType()).alias("periode"),     # absent DDL
            F.col("PRH_MODIFDATE").cast(TimestampType()).alias("date_modif"),
            F.col("PRH_FLAG_RH").cast(BooleanType()).alias("valide"),
            F.col("_batch_id"),
            F.col("_loaded_at").cast(TimestampType()).alias("_loaded_at"),
        )
    )


def process_heures_detail(spark: SparkSession, cfg: Cfg) -> DataFrame:
    """silver.temps.heures_detail ← WTRHDON (760K+/j — cas critique volume).

    Dédup : QUALIFY ROW_NUMBER() OVER (PARTITION BY PRH_BTS, RHD_LIGNE
                                        ORDER BY _loaded_at DESC) = 1
    Corrections DDL probe 2026-03-05 :
      RHD_RUBRIQUE → RHD_ORIRUB
      RHD_BASEPAYE → RHD_BASEP,  RHD_TAUXPAYE → RHD_TAUXP
      RHD_BASEFACT → RHD_BASEF,  RHD_TAUXFACT → RHD_TAUXF
      RHD_LIBELLE  → RHD_LIBRUB

    rhd_dated : colonne de date incluse pour le partitionnement Iceberg month(rhd_dated).
    coalesce(20) appliqué avant writeTo pour limiter la fragmentation des data-files.

    RGPD : PER_ID absent — jointure Gold via PRH_BTS ↔ silver.temps.releves_heures.
    """
    w = Window.partitionBy("PRH_BTS", "RHD_LIGNE").orderBy(F.col("_loaded_at").desc())
    df = (
        _read_bronze(spark, cfg, "wtrhdon")
        .filter(F.col("PRH_BTS").isNotNull() & F.col("RHD_LIGNE").isNotNull())
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .select(
            F.col("PRH_BTS").cast(IntegerType()).alias("prh_bts"),
            F.col("RHD_LIGNE").cast(IntegerType()).alias("rhd_ligne"),
            F.trim(F.col("RHD_ORIRUB").cast(StringType())).alias("rubrique"),
            F.col("RHD_BASEP").cast(DecimalType(10, 2)).alias("base_paye"),
            F.col("RHD_TAUXP").cast(DecimalType(10, 4)).alias("taux_paye"),
            F.col("RHD_BASEF").cast(DecimalType(10, 2)).alias("base_fact"),
            F.col("RHD_TAUXF").cast(DecimalType(10, 4)).alias("taux_fact"),
            F.trim(F.col("RHD_LIBRUB")).alias("libelle"),
            # Colonne de partition Iceberg month(rhd_dated)
            # Absente du SELECT original DuckDB — ajoutée pour le partitionnement Spark/Iceberg
            F.col("RHD_DATED").cast(DateType()).alias("rhd_dated"),
            F.col("_batch_id"),
            F.col("_loaded_at").cast(TimestampType()).alias("_loaded_at"),
        )
    )
    # coalesce(20) : réduit le nombre de data-files Iceberg sur 760K+ lignes/jour
    # sans repartition() coûteux — acceptable car les partitions sont déjà triées
    return df.coalesce(20)


# ── main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    cfg = Cfg()
    spark = build_spark(cfg)

    tables = [
        (process_releves_heures, "silver.temps.releves_heures"),
        (process_heures_detail,  "silver.temps.heures_detail"),
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
