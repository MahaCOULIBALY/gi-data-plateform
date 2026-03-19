"""spark_silver_competences.py — Silver · Compétences (Spark/Iceberg).

Remplace silver_competences.py (DuckDB → Parquet) — sink unique : Iceberg REST Catalog OVH.
Job soumis sur OVH Data Processing (Spark serverless). Pas d'import shared.py.

Table produite :
  silver.interimaires.competences  ← UNION ALL de 4 sources :
    WTPMET × WTMET    → métiers
    WTPHAB × WTTHAB   → habilitations (date_expiration composite)
    WTPDIP × WTTDIP   → diplômes
    WTEXP             → expériences

Toutes les sources : scan complet **/*.json (pas de date_partition — tables enrichissement).

Corrections DDL probe 2026-03-05 :
  WTPMET : ORDRE→PMET_ORDRE, PMET_NIVEAU absent DDL → NULL
  WTPHAB : PHAB_DATEDEBUT→PHAB_DELIVR, PHAB_DATEFIN→PHAB_EXPIR
  WTPDIP : PDIP_DATE confirmé (remplace PDIP_ANNEE)
  WTEXP  : ORDRE→EXP_ORDRE, EXP_POSTE absent, EXP_ENTREPRISE→EXP_NOM,
           EXP_DUREE absent, EXP_DATEDEBUT→EXP_DEBUT, EXP_DATEFIN→EXP_FIN

Enrichissement référentiels 2026-03-12 :
  WTMET  : is_active basé sur MET_DELETE (0/NULL=actif) + pcs_code (PCS_CODE_2003 INSEE)
  WTTHAB : date_expiration = COALESCE(PHAB_EXPIR, PHAB_DELIVR + THAB_NBMOIS mois)
           is_active recalculé sur date_expiration composite
  WTTDIP : niveau = TDIP_REF (catégorie/niveau diplôme)

Dédup final : ROW_NUMBER() OVER (PARTITION BY competence_id ORDER BY _loaded_at DESC) = 1

Variables d'environnement requises :
  OVH_S3_ACCESS_KEY, OVH_S3_SECRET_KEY, OVH_S3_ENDPOINT
  OVH_ICEBERG_URI, BUCKET_BRONZE
"""

import json
import os
import time

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

JOB_NAME = "spark_silver_competences"


# ── Configuration ─────────────────────────────────────────────────────────────

class Cfg:
    """Paramètres lus depuis os.environ — pas de shared.py en environnement Spark OVH."""

    def __init__(self) -> None:
        self.access_key    = os.environ["OVH_S3_ACCESS_KEY"]
        self.secret_key    = os.environ["OVH_S3_SECRET_KEY"]
        self.endpoint      = os.environ["OVH_S3_ENDPOINT"]
        self.iceberg_uri   = os.environ["OVH_ICEBERG_URI"]
        self.bucket_bronze = os.environ.get("BUCKET_BRONZE", "gi-poc-bronze")

    def bronze_full(self, table: str) -> str:
        """Chemin s3a:// scan complet — toutes partitions Bronze."""
        return f"s3a://{self.bucket_bronze}/raw_{table}/**/*.json"


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
    """Lecture JSON Bronze avec fusion de schéma."""
    return spark.read.option("mergeSchema", "true").json(path)


def _nullif_trim(col_expr) -> "Column":
    """Équivalent DuckDB NULLIF(TRIM(col), '') — retourne NULL si vide après trim."""
    trimmed = F.trim(col_expr.cast(StringType()))
    return F.when(trimmed != "", trimmed).otherwise(F.lit(None).cast(StringType()))


# ── 4 CTEs compétences ────────────────────────────────────────────────────────

def _build_metiers(spark: SparkSession, cfg: Cfg) -> DataFrame:
    """Métiers : WTPMET × WTMET (LEFT JOIN sur MET_ID).

    competence_id : MD5(concat(per_id, '|METIER|', coalesce(met_id, pmet_ordre)))
    is_active     : MET_DELETE == 0 ou NULL → actif
    pcs_code      : PCS_CODE_2003 INSEE — NULLIF(TRIM(...), '')
    """
    wtpmet = _read_bronze(spark, cfg.bronze_full("wtpmet"))
    wtmet  = (
        _read_bronze(spark, cfg.bronze_full("wtmet"))
        .select(
            F.col("MET_ID").cast(StringType()).alias("_met_id"),
            F.trim(F.col("MET_LIBELLE")).alias("_met_libelle"),
            # MET_DELETE : 1=supprimé, NULL/0=actif
            F.coalesce(F.col("MET_DELETE").cast(IntegerType()), F.lit(0)).alias("_met_delete"),
            _nullif_trim(F.col("PCS_CODE_2003")).alias("_pcs_code"),
        )
    )

    return (
        wtpmet
        .join(wtmet, wtpmet["MET_ID"].cast(StringType()) == wtmet["_met_id"], "left")
        .filter(F.col("PER_ID").isNotNull())
        .select(
            F.md5(F.concat(
                F.col("PER_ID").cast(StringType()),
                F.lit("|METIER|"),
                F.coalesce(
                    F.col("MET_ID").cast(StringType()),
                    F.col("PMET_ORDRE").cast(StringType()),
                ),
            )).alias("competence_id"),
            F.col("PER_ID").cast(IntegerType()).alias("per_id"),
            F.lit("METIER").alias("type_competence"),
            F.coalesce(F.col("MET_ID").cast(StringType()), F.lit("")).alias("code"),
            F.coalesce(F.trim(F.col("_met_libelle")), F.lit("Métier inconnu")).alias("libelle"),
            F.lit(None).cast(StringType()).alias("niveau"),
            F.lit(None).cast(DateType()).alias("date_obtention"),
            F.lit(None).cast(DateType()).alias("date_expiration"),
            (F.col("_met_delete") == 0).cast(BooleanType()).alias("is_active"),
            F.col("_pcs_code").alias("pcs_code"),
            F.lit("WTPMET").alias("_source_table"),
            F.col("_loaded_at").cast(TimestampType()).alias("_loaded_at"),
        )
    )


def _build_habilitations(spark: SparkSession, cfg: Cfg) -> DataFrame:
    """Habilitations : WTPHAB × WTTHAB (LEFT JOIN sur THAB_ID).

    date_expiration = COALESCE(PHAB_EXPIR, PHAB_DELIVR + THAB_NBMOIS mois)
    is_active = (date_expiration IS NULL) OR (date_expiration > current_date)
    Corrections DDL probe 2026-03-05 :
      PHAB_DATEDEBUT → PHAB_DELIVR
      PHAB_DATEFIN   → PHAB_EXPIR
    """
    wtphab = _read_bronze(spark, cfg.bronze_full("wtphab"))
    wtthab = (
        _read_bronze(spark, cfg.bronze_full("wtthab"))
        .select(
            F.col("THAB_ID").cast(StringType()).alias("_thab_id"),
            F.trim(F.col("THAB_LIBELLE")).alias("_thab_libelle"),
            F.col("THAB_NBMOIS").cast(IntegerType()).alias("_thab_nbmois"),
        )
    )

    joined = (
        wtphab
        .join(wtthab, wtphab["THAB_ID"].cast(StringType()) == wtthab["_thab_id"], "left")
        .filter(F.col("PER_ID").isNotNull())
    )

    # date_expiration composite : PHAB_EXPIR explicite OU PHAB_DELIVR + THAB_NBMOIS mois
    phab_expir  = F.col("PHAB_EXPIR").cast(DateType())
    phab_delivr = F.col("PHAB_DELIVR").cast(DateType())
    nbmois      = F.col("_thab_nbmois")

    date_exp = F.coalesce(
        phab_expir,
        F.when(
            nbmois.isNotNull() & phab_delivr.isNotNull(),
            F.add_months(phab_delivr, nbmois),
        ),
    )

    is_active = (date_exp.isNull() | (date_exp > F.current_date())).cast(BooleanType())

    return joined.select(
        F.md5(F.concat(
            F.col("PER_ID").cast(StringType()),
            F.lit("|HABILITATION|"),
            F.col("THAB_ID").cast(StringType()),
        )).alias("competence_id"),
        F.col("PER_ID").cast(IntegerType()).alias("per_id"),
        F.lit("HABILITATION").alias("type_competence"),
        F.coalesce(F.col("THAB_ID").cast(StringType()), F.lit("")).alias("code"),
        F.coalesce(F.trim(F.col("_thab_libelle")), F.lit("Habilitation inconnue")).alias("libelle"),
        F.lit("").alias("niveau"),
        phab_delivr.alias("date_obtention"),
        date_exp.alias("date_expiration"),
        is_active.alias("is_active"),
        F.lit(None).cast(StringType()).alias("pcs_code"),
        F.lit("WTPHAB").alias("_source_table"),
        F.col("_loaded_at").cast(TimestampType()).alias("_loaded_at"),
    )


def _build_diplomes(spark: SparkSession, cfg: Cfg) -> DataFrame:
    """Diplômes : WTPDIP × WTTDIP (LEFT JOIN sur TDIP_ID).

    niveau  : TDIP_REF (catégorie/niveau diplôme)
    Corrections DDL probe 2026-03-05 :
      PDIP_DATE confirmé (remplace PDIP_ANNEE)
    """
    wtpdip = _read_bronze(spark, cfg.bronze_full("wtpdip"))
    wttdip = (
        _read_bronze(spark, cfg.bronze_full("wttdip"))
        .select(
            F.col("TDIP_ID").cast(StringType()).alias("_tdip_id"),
            F.trim(F.col("TDIP_LIB")).alias("_tdip_lib"),
            _nullif_trim(F.col("TDIP_REF").cast(StringType())).alias("_tdip_ref"),
        )
    )

    return (
        wtpdip
        .join(wttdip, wtpdip["TDIP_ID"].cast(StringType()) == wttdip["_tdip_id"], "left")
        .filter(F.col("PER_ID").isNotNull())
        .select(
            F.md5(F.concat(
                F.col("PER_ID").cast(StringType()),
                F.lit("|DIPLOME|"),
                F.col("TDIP_ID").cast(StringType()),
            )).alias("competence_id"),
            F.col("PER_ID").cast(IntegerType()).alias("per_id"),
            F.lit("DIPLOME").alias("type_competence"),
            F.coalesce(F.col("TDIP_ID").cast(StringType()), F.lit("")).alias("code"),
            F.coalesce(F.trim(F.col("_tdip_lib")), F.lit("Diplôme inconnu")).alias("libelle"),
            F.col("_tdip_ref").alias("niveau"),
            F.col("PDIP_DATE").cast(DateType()).alias("date_obtention"),
            F.lit(None).cast(DateType()).alias("date_expiration"),
            F.lit(True).cast(BooleanType()).alias("is_active"),
            F.lit(None).cast(StringType()).alias("pcs_code"),
            F.lit("WTPDIP").alias("_source_table"),
            F.col("_loaded_at").cast(TimestampType()).alias("_loaded_at"),
        )
    )


def _build_experiences(spark: SparkSession, cfg: Cfg) -> DataFrame:
    """Expériences : WTEXP (pas de référentiel).

    Corrections DDL probe 2026-03-05 :
      ORDRE          → EXP_ORDRE
      EXP_ENTREPRISE → EXP_NOM
      EXP_DATEDEBUT  → EXP_DEBUT
      EXP_DATEFIN    → EXP_FIN
      EXP_POSTE / EXP_DUREE : absents DDL
    """
    return (
        _read_bronze(spark, cfg.bronze_full("wtexp"))
        .filter(F.col("PER_ID").isNotNull())
        .select(
            F.md5(F.concat(
                F.col("PER_ID").cast(StringType()),
                F.lit("|EXPERIENCE|"),
                F.col("EXP_ORDRE").cast(StringType()),
            )).alias("competence_id"),
            F.col("PER_ID").cast(IntegerType()).alias("per_id"),
            F.lit("EXPERIENCE").alias("type_competence"),
            F.coalesce(F.col("EXP_ORDRE").cast(StringType()), F.lit("")).alias("code"),
            F.trim(F.coalesce(F.col("EXP_NOM"), F.lit("Expérience inconnue"))).alias("libelle"),
            F.lit(None).cast(StringType()).alias("niveau"),
            F.col("EXP_DEBUT").cast(DateType()).alias("date_obtention"),
            F.col("EXP_FIN").cast(DateType()).alias("date_expiration"),
            F.lit(True).cast(BooleanType()).alias("is_active"),
            F.lit(None).cast(StringType()).alias("pcs_code"),
            F.lit("WTEXP").alias("_source_table"),
            F.col("_loaded_at").cast(TimestampType()).alias("_loaded_at"),
        )
    )


# ── Table principale ──────────────────────────────────────────────────────────

def process_competences(spark: SparkSession, cfg: Cfg) -> DataFrame:
    """silver.interimaires.competences ← UNION ALL 4 CTEs + dédup final.

    Dédup : ROW_NUMBER() OVER (PARTITION BY competence_id ORDER BY _loaded_at DESC) = 1
    """
    metiers       = _build_metiers(spark, cfg)
    habilitations = _build_habilitations(spark, cfg)
    diplomes      = _build_diplomes(spark, cfg)
    experiences   = _build_experiences(spark, cfg)

    all_comp = metiers.unionAll(habilitations).unionAll(diplomes).unionAll(experiences)

    w = Window.partitionBy("competence_id").orderBy(F.col("_loaded_at").desc())
    return (
        all_comp
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
        .filter(F.col("per_id").isNotNull())
        .withColumn("_loaded_at", F.current_timestamp())
    )


# ── main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    cfg = Cfg()
    spark = build_spark(cfg)

    table_id = "silver.interimaires.competences"
    table_name = "competences"
    t0 = time.time()
    try:
        df = process_competences(spark, cfg).cache()
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
