"""spark_silver_agences_light.py — Silver · Agences (Spark/Iceberg).

Remplace silver_agences_light.py (DuckDB → Parquet) — sink unique : Iceberg REST Catalog OVH.
Job soumis sur OVH Data Processing (Spark serverless). Pas d'import shared.py.

Tables produites :
  silver.agences.dim_agences              ← PYREGROUPECNT × [agence_gestion] (optionnel)
  silver.agences.hierarchie_territoriale  ← [agence_gestion] × [secteurs]    (optionnel)

Sources full-scan **/*.json (tables de référentiel — pas de date_partition).

Corrections DDL probe 2026-03-05 :
  UG_NOM absent DDL → RGPCNT_LIBELLE utilisé comme nom agence
  RGPCNT_VILLE/EMAIL/GPS_LAT/GPS_LON absents DDL → NULL
  DATE_CLOTURE ajouté PYREGROUPECNT Bronze → is_cloture / is_active depuis source de vérité

Corrections 2026-03-14 (DT-04 résolu) :
  marque / branche / nom_commercial / code_comm depuis raw_agence_gestion (ID_UG = RGPCNT_ID)
  hierarchie_territoriale réactivée : [Agence Gestion].NOM_UG ↔ Secteurs.[Agence de gestion]
  raw_secteurs et raw_agence_gestion : optionnels (graceful fallback si Bronze absent)

Colonnes spéciales dans raw_secteurs (noms avec espaces/accents) :
  "Agence de gestion", "Périmètre", "Zone Géographique" → lus via selectExpr + backticks.

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
    DoubleType,
    IntegerType,
    StringType,
    TimestampType,
)
from pyspark.sql.window import Window

JOB_NAME = "spark_silver_agences_light"


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
        """Chemin s3a:// scan complet — tables de référentiel sans partitionnement date."""
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


def _read_bronze(spark: SparkSession, path: str) -> DataFrame:
    """Lecture JSON Bronze avec fusion de schéma."""
    return spark.read.option("mergeSchema", "true").json(path)


def _nullif_trim(col_expr) -> "Column":
    """NULLIF(TRIM(col), '') — retourne NULL si chaîne vide après trim."""
    trimmed = F.trim(col_expr.cast(StringType()))
    return F.when(trimmed != "", trimmed).otherwise(F.lit(None).cast(StringType()))


def _try_read_optional(spark: SparkSession, path: str) -> "DataFrame | None":
    """Tente de lire une source optionnelle — retourne None si le chemin est absent."""
    try:
        df = spark.read.option("mergeSchema", "true").json(path)
        # Vérifier que le DataFrame n'est pas vide (chemin S3 absent → schéma inféré vide)
        if not df.columns:
            return None
        return df
    except Exception:  # noqa: BLE001
        return None


# ── Tables ────────────────────────────────────────────────────────────────────

def process_dim_agences(spark: SparkSession, cfg: Cfg) -> DataFrame:
    """silver.agences.dim_agences ← PYREGROUPECNT × [agence_gestion] (optionnel).

    DT-04 résolu (2026-03-14) :
      LEFT JOIN raw_agence_gestion pour marque / branche / nom_commercial / code_comm.
      Graceful fallback : si raw_agence_gestion absent → colonnes NULL, pas de crash.

    Dédup PYREGROUPECNT : ROW_NUMBER() OVER (PARTITION BY RGPCNT_ID ORDER BY _loaded_at DESC) = 1
    Dédup agence_gestion : ROW_NUMBER() OVER (PARTITION BY ID_UG ORDER BY DATE_UG_GEST DESC) = 1

    Colonnes absentes DDL Evolia : ville, email, latitude, longitude → NULL.
    """
    # ── PYREGROUPECNT (obligatoire) ──
    w_rg = Window.partitionBy("RGPCNT_ID").orderBy(F.col("_loaded_at").desc())
    pyregroupecnt = (
        _read_bronze(spark, cfg.bronze_full("pyregroupecnt"))
        .filter(F.col("RGPCNT_ID").isNotNull())
        .withColumn("_rn", F.row_number().over(w_rg))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    # ── agence_gestion (optionnel) ──
    ag_raw = _try_read_optional(spark, cfg.bronze_full("agence_gestion"))

    if ag_raw is not None:
        w_ag = Window.partitionBy("ID_UG").orderBy(F.col("DATE_UG_GEST").desc())
        agence_gest = (
            ag_raw
            .withColumn("_rn_ag", F.row_number().over(w_ag))
            .filter(F.col("_rn_ag") == 1)
            .select(
                F.col("ID_UG").cast(IntegerType()).alias("_ag_rgpcnt_id"),
                _nullif_trim(F.col("MARQUE")).alias("_ag_marque"),
                _nullif_trim(F.col("BRANCHE")).alias("_ag_branche"),
                F.trim(F.col("NOM_COMMERCIAL")).alias("_ag_nom_commercial"),
                F.trim(F.col("CODE_COMM")).alias("_ag_code_comm"),
            )
        )
        joined = pyregroupecnt.join(
            agence_gest,
            pyregroupecnt["RGPCNT_ID"].cast(IntegerType()) == agence_gest["_ag_rgpcnt_id"],
            "left",
        )
        marque         = F.col("_ag_marque")
        branche        = F.col("_ag_branche")
        nom_commercial = F.col("_ag_nom_commercial")
        code_comm      = F.col("_ag_code_comm")
    else:
        joined         = pyregroupecnt
        marque         = F.lit(None).cast(StringType())
        branche        = F.lit(None).cast(StringType())
        nom_commercial = F.lit(None).cast(StringType())
        code_comm      = F.lit(None).cast(StringType())

    return joined.select(
        F.md5(F.col("RGPCNT_ID").cast(StringType())).alias("agence_sk"),
        F.col("RGPCNT_ID").cast(IntegerType()).alias("rgpcnt_id"),
        F.trim(F.col("RGPCNT_LIBELLE")).alias("nom"),
        F.trim(F.col("RGPCNT_CODE")).alias("code"),
        marque.alias("marque"),
        branche.alias("branche"),
        nom_commercial.alias("nom_commercial"),
        code_comm.alias("code_comm"),
        F.lit(None).cast(StringType()).alias("ville"),      # absent DDL Evolia
        F.lit(None).cast(StringType()).alias("email"),      # absent DDL Evolia
        F.lit(None).cast(DoubleType()).alias("latitude"),   # absent DDL Evolia
        F.lit(None).cast(DoubleType()).alias("longitude"),  # absent DDL Evolia
        F.col("DATE_CLOTURE").isNotNull().cast(BooleanType()).alias("is_cloture"),
        F.col("DATE_CLOTURE").isNull().cast(BooleanType()).alias("is_active"),
        F.col("DATE_CLOTURE").cast(DateType()).alias("cloture_date"),
        F.current_timestamp().alias("_loaded_at"),
    )


def process_hierarchie_territoriale(spark: SparkSession, cfg: Cfg) -> DataFrame:
    """silver.agences.hierarchie_territoriale ← [agence_gestion] × [secteurs].

    Pont texte : agence_gestion.NOM_UG ↔ secteurs.[Agence de gestion] (LOWER+TRIM).
    INNER JOIN : seules les agences avec correspondance secteur confirmée sont incluses.
    Secteurs.ID_UG absent → linkage via NOM_UG assumé.

    Colonnes spéciales dans raw_secteurs (espaces/accents) lues via selectExpr + backticks :
      `Agence de gestion`, `Périmètre`, `Zone Géographique`

    Raises RuntimeError si l'une des deux sources est absente (intercepté dans main()).
    """
    ag_raw = _try_read_optional(spark, cfg.bronze_full("agence_gestion"))
    if ag_raw is None:
        raise RuntimeError("Source raw_agence_gestion absente — hierarchie_territoriale ignorée.")

    sec_raw = _try_read_optional(spark, cfg.bronze_full("secteurs"))
    if sec_raw is None:
        raise RuntimeError("Source raw_secteurs absente — hierarchie_territoriale ignorée.")

    # ── agence_gestion : dédup sur ID_UG ──
    w_ag = Window.partitionBy("ID_UG").orderBy(F.col("DATE_UG_GEST").desc())
    agence_gest = (
        ag_raw
        .withColumn("_rn_ag", F.row_number().over(w_ag))
        .filter(F.col("_rn_ag") == 1)
        .select(
            F.col("ID_UG").cast(IntegerType()).alias("rgpcnt_id"),
            F.lower(F.trim(F.col("NOM_UG"))).alias("nom_ug_norm"),
        )
        .filter(F.col("rgpcnt_id").isNotNull())
    )

    # ── secteurs : colonnes avec espaces/accents → selectExpr + backticks ──
    secteurs = sec_raw.selectExpr(
        "LOWER(TRIM(`Agence de gestion`)) AS nom_ug_norm",
        "TRIM(Secteur)                    AS secteur",
        "TRIM(`Périmètre`)                AS perimetre",
        "TRIM(`Zone Géographique`)        AS zone_geo",
    )

    # INNER JOIN : n'inclut que les agences avec secteur confirmé
    return (
        agence_gest
        .join(secteurs, "nom_ug_norm", "inner")
        .select(
            F.col("rgpcnt_id"),
            F.col("secteur"),
            F.col("perimetre"),
            F.col("zone_geo"),
            F.current_timestamp().alias("_loaded_at"),
        )
    )


# ── main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    cfg = Cfg()
    spark = build_spark(cfg)

    # ── dim_agences — obligatoire ─────────────────────────────────────────────
    t0 = time.time()
    try:
        df = process_dim_agences(spark, cfg).cache()
        rows = df.count()
        (df.writeTo("silver.agences.dim_agences")
           .option("merge-schema", "true")
           .overwritePartitions())
        df.unpersist()
        _log("dim_agences", rows, time.time() - t0)
    except Exception as exc:  # noqa: BLE001
        _log_error("dim_agences", str(exc), time.time() - t0)

    # ── hierarchie_territoriale — optionnel ───────────────────────────────────
    # raw_agence_gestion ou raw_secteurs absents → warning non-bloquant
    t0 = time.time()
    try:
        df = process_hierarchie_territoriale(spark, cfg).cache()
        rows = df.count()
        (df.writeTo("silver.agences.hierarchie_territoriale")
           .option("merge-schema", "true")
           .overwritePartitions())
        df.unpersist()
        _log("hierarchie_territoriale", rows, time.time() - t0)
    except Exception as exc:  # noqa: BLE001
        _log_warning(
            "hierarchie_territoriale",
            f"Ignorée — raw_agence_gestion ou raw_secteurs absent : {exc}",
        )

    spark.stop()


if __name__ == "__main__":
    main()
