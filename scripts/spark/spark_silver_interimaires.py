"""spark_silver_interimaires.py — Silver · dim_interimaires SCD Type 2 + RGPD NIR (Spark/Iceberg).

Remplace silver_interimaires.py (DuckDB → Parquet) — sink unique : Iceberg REST Catalog OVH.
Job soumis sur OVH Data Processing (Spark serverless). Pas d'import shared.py.

Table produite :
  silver.interimaires.dim_interimaires  ← PYPERSONNE × PYSALARIE × WTPINT

SCD Type 2 : existing=[] (pas de lecture Iceberg) → tous les enregistrements staging
sont insérés comme nouvelles versions is_current=True. Comportement identique à la
version DuckDB où existing est toujours une liste vide (pas de read-back Iceberg).

SCD2_TRACKED : nom, prenom, adresse, ville, code_postal,
               is_actif, is_candidat, is_permanent, agence_rattachement.

RGPD NIR :
  nir_pseudo = SHA-256(TRIM(PER_NIR) || RGPD_SALT) si NIR non nul/vide.
  RGPD_SALT : variable d'environnement obligatoire.
  Le NIR brut n'est JAMAIS écrit en Silver.

Booleans DuckDB → Spark :
  TRY_CAST('1' AS BOOLEAN) = true  en DuckDB.
  cast("1", BooleanType)   = null  en Spark → helper _try_bool() requis.

Corrections DDL probe 2026-03-05 :
  PER_DATENAIS → PER_NAISSANCE
  PER_NATIONALITE → NAT_CODE
  PER_PAYS → PAYS_CODE
  PER_NUMVOIE/TYPVOIE/VOIE → PER_BISVOIE/COMPVOIE (adresse partielle DDL)
  RGPCNT_ID absent de WTPINT → agence_rattachement toujours NULL
  SAL_DATESORTIE absent DDL → date_sortie toujours NULL

Variables d'environnement requises :
  OVH_S3_ACCESS_KEY, OVH_S3_SECRET_KEY, OVH_S3_ENDPOINT
  OVH_ICEBERG_URI, BUCKET_BRONZE, RGPD_SALT
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
    IntegerType,
    StringType,
    TimestampType,
)
from pyspark.sql.window import Window

JOB_NAME = "spark_silver_interimaires"

# Colonnes trackées par SCD2 — doit correspondre à SCD2_TRACKED de silver_interimaires.py
SCD2_TRACKED = (
    "nom", "prenom", "adresse", "ville", "code_postal",
    "is_actif", "is_candidat", "is_permanent", "agence_rattachement",
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
        self.rgpd_salt      = os.environ["RGPD_SALT"]
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


def _try_bool(col_expr, default: bool = False) -> "Column":
    """Convertit '0'/'1'/'true'/'false' en BooleanType.

    Spark cast("1", BooleanType) = null — contrairement à DuckDB TRY_CAST('1' AS BOOLEAN) = true.
    Ce helper reproduit le comportement DuckDB avec COALESCE(..., default).
    """
    s = F.lower(col_expr.cast(StringType()))
    return F.coalesce(
        F.when(s.isin("1", "true", "t", "yes"), F.lit(True))
         .when(s.isin("0", "false", "f", "no"), F.lit(False)),
        F.lit(default),
    ).cast(BooleanType())


def _pseudonymize_nir(nir_col, salt: str) -> "Column":
    """RGPD : SHA-256(TRIM(NIR) || salt) si NIR non nul/vide, sinon NULL.

    Équivalent de pseudonymize_nir(PER_NIR, cfg.rgpd_salt) dans shared.py.
    """
    trimmed = F.trim(nir_col)
    return F.when(
        nir_col.isNotNull() & (trimmed != ""),
        F.sha2(F.concat(trimmed, F.lit(salt)), 256),
    ).otherwise(F.lit(None).cast(StringType()))


# ── Staging ────────────────────────────────────────────────────────────────────

def build_staging(spark: SparkSession, cfg: Cfg) -> DataFrame:
    """Staging PYPERSONNE × PYSALARIE (LEFT) × WTPINT (LEFT).

    Dédup de chaque source sur PER_ID avant les jointures.
    Corrections DDL probe 2026-03-05 (voir module docstring).
    """
    # ── raw_pypersonne : dédup sur PER_ID ──
    w_per = Window.partitionBy("PER_ID").orderBy(F.col("_loaded_at").desc())
    raw_per = (
        _read_bronze(spark, cfg.bronze_path("pypersonne"))
        .filter(F.col("PER_ID").isNotNull())
        .withColumn("_rn", F.row_number().over(w_per))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    # ── raw_pysalarie : dédup sur PER_ID ──
    w_sal = Window.partitionBy("PER_ID").orderBy(F.col("_loaded_at").desc())
    raw_sal = (
        _read_bronze(spark, cfg.bronze_path("pysalarie"))
        .withColumn("_rn", F.row_number().over(w_sal))
        .filter(F.col("_rn") == 1)
        .select(
            F.col("PER_ID").cast(IntegerType()).alias("_sal_per_id"),
            F.trim(F.col("SAL_MATRICULE")).alias("_matricule"),
            F.col("SAL_DATEENTREE").cast(DateType()).alias("_date_entree"),
            F.col("SAL_ACTIF").alias("_sal_actif"),
        )
    )

    # ── raw_wtpint : dédup sur PER_ID ──
    w_pint = Window.partitionBy("PER_ID").orderBy(F.col("_loaded_at").desc())
    raw_pint = (
        _read_bronze(spark, cfg.bronze_path("wtpint"))
        .withColumn("_rn", F.row_number().over(w_pint))
        .filter(F.col("_rn") == 1)
        .select(
            F.col("PER_ID").cast(IntegerType()).alias("_pint_per_id"),
            F.col("PINT_CANDIDAT").alias("_pint_candidat"),
            F.col("PINT_PERMANENT").alias("_pint_permanent"),
        )
    )

    return (
        raw_per
        .join(raw_sal, raw_per["PER_ID"].cast(IntegerType()) == F.col("_sal_per_id"), "left")
        .join(raw_pint, raw_per["PER_ID"].cast(IntegerType()) == F.col("_pint_per_id"), "left")
        .select(
            raw_per["PER_ID"].cast(IntegerType()).alias("per_id"),
            F.trim(F.col("SAL_MATRICULE")).alias("matricule"),
            F.trim(raw_per["PER_NOM"]).alias("nom"),
            F.trim(raw_per["PER_PRENOM"]).alias("prenom"),
            F.col("PER_NAISSANCE").cast(DateType()).alias("date_naissance"),
            # NIR brut — pseudonymisé dans apply_scd2, jamais persisté
            F.trim(raw_per["PER_NIR"]).alias("_nir_brut"),
            F.trim(F.coalesce(raw_per["NAT_CODE"], F.lit(""))).alias("nationalite"),
            F.trim(F.coalesce(raw_per["PAYS_CODE"], F.lit(""))).alias("pays"),
            # Adresse partielle DDL : PER_BISVOIE + COMPVOIE + CP + VILLE
            F.concat_ws(
                " ",
                raw_per["PER_BISVOIE"], raw_per["PER_COMPVOIE"],
                raw_per["PER_CP"], raw_per["PER_VILLE"],
            ).alias("adresse"),
            F.trim(raw_per["PER_VILLE"]).alias("ville"),
            F.trim(raw_per["PER_CP"]).alias("code_postal"),
            F.col("SAL_DATEENTREE").cast(DateType()).alias("date_entree"),
            F.lit(None).cast(DateType()).alias("date_sortie"),           # SAL_DATESORTIE absent DDL
            _try_bool(F.col("_sal_actif"), default=False).alias("is_actif"),
            _try_bool(F.col("_pint_candidat"), default=False).alias("is_candidat"),
            _try_bool(F.col("_pint_permanent"), default=False).alias("is_permanent"),
            F.lit(None).cast(IntegerType()).alias("agence_rattachement"),  # RGPCNT_ID absent WTPINT
            raw_per["_batch_id"].alias("_source_raw_id"),
        )
    )


# ── SCD2 transform ────────────────────────────────────────────────────────────

def apply_scd2(staging: DataFrame, cfg: Cfg, now_iso: str) -> DataFrame:
    """Applique les colonnes SCD2 sur le staging et pseudonymise le NIR.

    existing=[] dans la version DuckDB → pas de read-back Iceberg, pas de fermeture
    de versions précédentes. Tous les enregistrements staging sont de nouvelles versions.

    change_hash : MD5 sur concaténation pipe-séparé des colonnes SCD2_TRACKED.
    interimaire_sk : MD5(concat(per_id, '|', now_iso)) — clé surrogate technique.
    nir_pseudo : SHA-256(TRIM(PER_NIR) || RGPD_SALT) — le NIR brut est droppé.
    """
    salt = cfg.rgpd_salt

    # change_hash : reproduit hashlib.md5("|".join(str(row[c]) for c in SCD2_TRACKED))
    hash_parts = F.concat_ws(
        "|",
        *[F.coalesce(F.col(c).cast(StringType()), F.lit("")) for c in SCD2_TRACKED],
    )

    return (
        staging
        .withColumn("nir_pseudo", _pseudonymize_nir(F.col("_nir_brut"), salt))
        .drop("_nir_brut")
        .select(
            # Surrogate key SCD2
            F.md5(F.concat_ws("|", F.col("per_id").cast(StringType()), F.lit(now_iso))).alias("interimaire_sk"),
            F.col("per_id"),
            F.md5(hash_parts).alias("change_hash"),
            F.col("matricule"),
            F.col("nom"),
            F.col("prenom"),
            F.col("date_naissance"),
            F.col("nir_pseudo"),                              # NIR pseudonymisé (jamais brut)
            F.col("nationalite"),
            F.col("pays"),
            F.col("adresse"),
            F.col("ville"),
            F.col("code_postal"),
            F.col("date_entree"),
            F.col("date_sortie"),
            F.col("is_actif"),
            F.col("is_candidat"),
            F.col("is_permanent"),
            F.col("agence_rattachement"),
            F.lit(True).cast(BooleanType()).alias("is_current"),
            F.lit(now_iso).cast(TimestampType()).alias("valid_from"),
            F.lit(None).cast(TimestampType()).alias("valid_to"),
            F.col("_source_raw_id"),
            F.lit(now_iso).cast(TimestampType()).alias("_loaded_at"),
        )
    )


# ── main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    cfg = Cfg()
    spark = build_spark(cfg)

    now_iso = datetime.now(timezone.utc).isoformat()
    table_id = "silver.interimaires.dim_interimaires"
    table_name = "dim_interimaires"
    t0 = time.time()

    try:
        staging = build_staging(spark, cfg)
        df = apply_scd2(staging, cfg, now_iso).cache()
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
