"""spark_silver_interimaires_detail.py — Silver · Intérimaires détail (Spark/Iceberg).

Remplace silver_interimaires_detail.py (DuckDB → Parquet) — sink unique : Iceberg REST Catalog OVH.
Job soumis sur OVH Data Processing (Spark serverless). Pas d'import shared.py.

Tables produites :
  silver.interimaires.evaluations         ← WTPEVAL          (~PK : PER_ID + PEVAL_DU)
  silver.interimaires.coordonnees         ← PYCOORDONNEE     (~PK : PER_ID + TYPTEL_CODE)
  silver.interimaires.portefeuille_agences ← WTUGPINT         (~PK : PER_ID + RGPCNT_ID)
  silver.interimaires.fidelisation        ← WTPINT           (~PK : PER_ID)

Corrections DDL probe 2026-03-05 :
  WTPEVAL     : PEVAL_DATE→PEVAL_DU, PEVAL_NOTE→PEVAL_EVALUATION,
                PEVAL_EVALUATEUR/COMMENTAIRE absent DDL → NULL
  PYCOORDONNEE: TYPTEL→TYPTEL_CODE, COORD_VALEUR→PER_TEL_NTEL, COORD_PRINC→NULL (is_principal hardcodé false)
Corrections DDL 2026-03-11 :
  WTUGPINT    : UGPINT_DATEMODIF absent DDL → full-load, dédup (PER_ID, RGPCNT_ID)

RGPD : coordonnees Silver-only — jamais exposé en Gold.

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

JOB_NAME = "spark_silver_interimaires_detail"


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
        """Chemin s3a:// partitionné par date pour les tables delta."""
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


# ── Tables ────────────────────────────────────────────────────────────────────

def process_evaluations(spark: SparkSession, cfg: Cfg) -> DataFrame:
    """silver.interimaires.evaluations ← WTPEVAL.

    Dédup : ROW_NUMBER() OVER (PARTITION BY PER_ID, PEVAL_DU ORDER BY _loaded_at DESC) = 1
    eval_id : MD5(concat(per_id, '|', coalesce(peval_du, '')))
    Corrections DDL probe 2026-03-05 :
      PEVAL_DATE     → PEVAL_DU
      PEVAL_NOTE     → PEVAL_EVALUATION
      PEVAL_EVALUATEUR / COMMENTAIRE absent DDL → NULL
    """
    w = Window.partitionBy("PER_ID", "PEVAL_DU").orderBy(F.col("_loaded_at").desc())
    return (
        _read_bronze(spark, cfg.bronze_path("wtpeval"))
        .filter(F.col("PER_ID").isNotNull())
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .select(
            F.md5(F.concat(
                F.col("PER_ID").cast(StringType()),
                F.lit("|"),
                F.coalesce(F.col("PEVAL_DU").cast(StringType()), F.lit("")),
            )).alias("eval_id"),
            F.col("PER_ID").cast(IntegerType()).alias("per_id"),
            F.col("PEVAL_DU").cast(DateType()).alias("date_eval"),
            F.col("PEVAL_EVALUATION").cast(DecimalType(5, 2)).alias("note"),
            F.lit(None).cast(StringType()).alias("commentaire"),    # absent DDL
            F.col("PEVAL_UTL").cast(IntegerType()).alias("evaluateur_id"),
            F.current_timestamp().alias("_loaded_at"),
        )
    )


def process_coordonnees(spark: SparkSession, cfg: Cfg) -> DataFrame:
    """silver.interimaires.coordonnees ← PYCOORDONNEE.

    RGPD : Silver-only — jamais exposé en Gold.
    Dédup : ROW_NUMBER() OVER (PARTITION BY PER_ID, TYPTEL_CODE ORDER BY _loaded_at DESC) = 1
    coord_id : MD5(concat(per_id, '|', coalesce(typtel_code, '')))
    Corrections DDL probe 2026-03-05 :
      TYPTEL       → TYPTEL_CODE
      COORD_VALEUR → PER_TEL_NTEL
      COORD_PRINC  absent DDL → is_principal hardcodé false
    """
    w = Window.partitionBy("PER_ID", "TYPTEL_CODE").orderBy(F.col("_loaded_at").desc())
    return (
        _read_bronze(spark, cfg.bronze_path("pycoordonnee"))
        .filter(F.col("PER_ID").isNotNull())
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .select(
            F.md5(F.concat(
                F.col("PER_ID").cast(StringType()),
                F.lit("|"),
                F.coalesce(F.col("TYPTEL_CODE").cast(StringType()), F.lit("")),
            )).alias("coord_id"),
            F.col("PER_ID").cast(IntegerType()).alias("per_id"),
            F.trim(F.col("TYPTEL_CODE").cast(StringType())).alias("type_coord"),
            F.trim(F.col("PER_TEL_NTEL")).alias("valeur"),
            F.trim(F.coalesce(F.col("PER_TEL_POSTE"), F.lit(""))).alias("poste"),
            F.lit(False).cast(BooleanType()).alias("is_principal"),  # absent DDL
            F.current_timestamp().alias("_loaded_at"),
        )
    )


def process_portefeuille_agences(spark: SparkSession, cfg: Cfg) -> DataFrame:
    """silver.interimaires.portefeuille_agences ← WTUGPINT.

    UGPINT_DATEMODIF absent DDL → full-load, dédup sur (PER_ID, RGPCNT_ID).
    """
    w = Window.partitionBy("PER_ID", "RGPCNT_ID").orderBy(F.col("_loaded_at").desc())
    return (
        _read_bronze(spark, cfg.bronze_path("wtugpint"))
        .filter(F.col("PER_ID").isNotNull() & F.col("RGPCNT_ID").isNotNull())
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .select(
            F.col("PER_ID").cast(IntegerType()).alias("per_id"),
            F.col("RGPCNT_ID").cast(IntegerType()).alias("rgpcnt_id"),
            F.current_timestamp().alias("_loaded_at"),
        )
    )


def process_fidelisation(spark: SparkSession, cfg: Cfg) -> DataFrame:
    """silver.interimaires.fidelisation ← WTPINT.

    Dédup : ROW_NUMBER() OVER (PARTITION BY PER_ID ORDER BY _loaded_at DESC) = 1
    Catégories : actif_recent (≤90j), actif_annee (≤365j), inactif_long (>365j), inactif (jamais).

    Note : cette table était du code mort dans silver_interimaires_detail.py (DuckDB) —
    non appelée dans run() — mais est nécessaire pour gold_staffing.build_fidelisation_query().
    """
    w = Window.partitionBy("PER_ID").orderBy(F.col("_loaded_at").desc())
    der_vente = F.col("PINT_DERVENDTE").cast(DateType())
    creat_dte = F.col("PINT_CREATDTE").cast(DateType())
    jours_inactif = F.datediff(F.current_date(), der_vente)
    return (
        _read_bronze(spark, cfg.bronze_path("wtpint"))
        .filter(F.col("PER_ID").isNotNull())
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .select(
            F.col("PER_ID").cast(IntegerType()).alias("per_id"),
            creat_dte.alias("date_premiere_vente"),
            F.col("PINT_PREVENDTE").cast(DateType()).alias("date_avant_derniere_vente"),
            der_vente.alias("date_derniere_vente"),
            F.when(
                F.col("PINT_DERVENDTE").isNotNull() & F.col("PINT_CREATDTE").isNotNull(),
                F.datediff(der_vente, creat_dte),
            ).alias("anciennete_jours"),
            F.when(
                F.col("PINT_DERVENDTE").isNotNull(),
                jours_inactif,
            ).alias("jours_depuis_derniere_vente"),
            F.when(F.col("PINT_DERVENDTE").isNull(), F.lit("inactif"))
             .when(jours_inactif <= 90, F.lit("actif_recent"))
             .when(jours_inactif <= 365, F.lit("actif_annee"))
             .otherwise(F.lit("inactif_long")).alias("categorie_fidelisation"),
            F.current_timestamp().alias("_loaded_at"),
        )
    )


# ── main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    cfg = Cfg()
    spark = build_spark(cfg)

    tables = [
        (process_evaluations,          "silver.interimaires.evaluations"),
        (process_coordonnees,          "silver.interimaires.coordonnees"),
        (process_portefeuille_agences, "silver.interimaires.portefeuille_agences"),
        (process_fidelisation,         "silver.interimaires.fidelisation"),
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
