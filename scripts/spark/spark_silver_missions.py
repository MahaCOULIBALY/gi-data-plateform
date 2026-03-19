"""spark_silver_missions.py — Silver · Missions & Contrats (Spark/Iceberg).

Remplace silver_missions.py (DuckDB → Parquet) — sink unique : Iceberg REST Catalog OVH.
Job soumis sur OVH Data Processing (Spark serverless). Pas d'import shared.py.

Tables produites :
  silver.missions.missions        ← WTMISS + WTCNTI (agg) + WTCMD (dedup)
  silver.missions.contrats        ← WTCNTI
  silver.missions.commandes       ← WTCMD
  silver.missions.placements      ← WTPLAC
  silver.missions.fin_mission     ← WTMISS + WTCNTI + WTFINMISS + PYMTFCNT
  silver.missions.contrats_paie   ← PYCONTRAT
  silver.missions.facinfo         ← WTFACINFO (full-load, FACINFO_DATEMODIF absent DDL)

Variables d'environnement requises :
  OVH_S3_ACCESS_KEY, OVH_S3_SECRET_KEY, OVH_S3_ENDPOINT
  OVH_ICEBERG_URI, BUCKET_BRONZE
  SILVER_DATE_PARTITION  (optionnel, défaut : date du jour UTC YYYY/MM/DD)

Traductions DuckDB → Spark :
  QUALIFY ROW_NUMBER() OVER (...) = 1  → withColumn(_rn) + filter(_rn == 1)
  TRY_CAST(x AS T)                     → col(x).cast(T)  [null on failure, same semantics]
  DATEDIFF('hour', start, end)         → (unix_timestamp(end) - unix_timestamp(start)) / 3600
  DATEDIFF('day',  start, end)         → F.datediff(end, start)
  NULL::VARCHAR                        → F.lit(None).cast(StringType())

RGPD : domaine missions — aucun champ NIR, aucun hash requis.
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
    LongType,
    ShortType,
    StringType,
    TimestampType,
)
from pyspark.sql.window import Window

JOB_NAME = "spark_silver_missions"


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
        """Chemin s3a:// vers les fichiers JSON Bronze d'une table pour la partition courante."""
        return f"s3a://{self.bucket_bronze}/raw_{table}/{self.date_partition}/*.json"


# ── SparkSession ──────────────────────────────────────────────────────────────

def build_spark(cfg: Cfg) -> SparkSession:
    """SparkSession avec Iceberg REST Catalog 'silver' et S3A pour les lectures Bronze."""
    # S3A exige le hostname sans protocole
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

def process_missions(spark: SparkSession, cfg: Cfg) -> DataFrame:
    """silver.missions.missions ← WTMISS + WTCNTI (agg MIN date_debut) + WTCMD (dedup).

    Enrichissements : statut_dpae, ecart_heures (DPAE vs début contrat),
    delai_placement_heures (CMD_DTE → premier CNTI_DATEFFET), categorie_delai.
    Anomalie CMD_ID stocké float dans Evolia → cast IntegerType obligatoire.
    """
    # ── wtcnti CTE : MIN(CNTI_DATEFFET) par (PER_ID, CNT_ID) ─────────────────
    # Cast avant l'agrégation pour garantir min chronologique (pas lexicographique)
    wtcnti = (
        _read_bronze(spark, cfg, "wtcnti")
        .select(
            F.col("PER_ID"),
            F.col("CNT_ID"),
            F.col("CNTI_DATEFFET").cast(TimestampType()).alias("_cnti_ts"),
        )
        .groupBy("PER_ID", "CNT_ID")
        .agg(F.min("_cnti_ts").alias("_cnti_dateffet"))
        # Colonnes après agg : PER_ID, CNT_ID, _cnti_dateffet (TimestampType)
    )

    # ── wtcmd CTE : dedup par CMD_ID ORDER BY _loaded_at DESC ────────────────
    # Anomalie : CMD_ID stocké float → cast Integer + filtre NOT NULL
    w_cmd = Window.partitionBy("CMD_ID").orderBy(F.col("_loaded_at").desc())
    wtcmd = (
        _read_bronze(spark, cfg, "wtcmd")
        .withColumn("_cmd_id_int", F.col("CMD_ID").cast(IntegerType()))
        .withColumn("_rn", F.row_number().over(w_cmd))
        .filter((F.col("_rn") == 1) & F.col("_cmd_id_int").isNotNull())
        .select(
            F.col("_cmd_id_int").alias("_wtcmd_id"),
            F.col("CMD_DTE").cast(TimestampType()).alias("_wtcmd_dte"),
        )
        # Colonnes : _wtcmd_id (IntegerType), _wtcmd_dte (TimestampType)
    )

    # ── src : raw_wtmiss dédupliqué QUALIFY par (PER_ID, CNT_ID) ─────────────
    w_miss = Window.partitionBy("PER_ID", "CNT_ID").orderBy(F.col("_loaded_at").desc())
    src = (
        _read_bronze(spark, cfg, "wtmiss")
        .filter(F.col("PER_ID").isNotNull() & F.col("CNT_ID").isNotNull())
        .withColumn("_rn", F.row_number().over(w_miss))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
        # CMD_ID float → int pour le join avec wtcmd
        .withColumn("_src_cmd_int", F.col("CMD_ID").cast(IntegerType()))
    )

    # ── Joins LEFT ────────────────────────────────────────────────────────────
    # join sur ["PER_ID", "CNT_ID"] fusionne ces deux colonnes (PySpark semantics)
    joined = (
        src
        .join(wtcnti, ["PER_ID", "CNT_ID"], "left")
        .join(wtcmd, F.col("_src_cmd_int") == F.col("_wtcmd_id"), "left")
    )

    # ── delai_placement_heures : DATEDIFF('hour', CMD_DTE, CNTI_DATEFFET) ────
    # = CNTI_DATEFFET − CMD_DTE en heures (DuckDB : datediff(part, start, end) = end − start)
    joined = joined.withColumn(
        "_delai_h",
        F.when(
            F.col("_cnti_dateffet").isNotNull() & F.col("_wtcmd_dte").isNotNull(),
            (F.unix_timestamp(F.col("_cnti_dateffet")) - F.unix_timestamp(F.col("_wtcmd_dte")))
            / 3600.0,
        ),
    )

    return joined.select(
        F.col("PER_ID").cast(IntegerType()).alias("per_id"),
        F.col("CNT_ID").cast(IntegerType()).alias("cnt_id"),
        F.col("TIE_ID").cast(IntegerType()).alias("tie_id"),
        F.col("TIES_SERV").cast(IntegerType()).alias("ties_serv"),
        F.col("RGPCNT_ID").cast(IntegerType()).alias("rgpcnt_id"),
        # date_debut = MIN(CNTI_DATEFFET) — MISS_DATEDEBUT absent DDL (probe 2026-03-05)
        F.col("_cnti_dateffet").cast(DateType()).alias("date_debut"),
        F.col("MISS_SAISIE_DTFIN").cast(DateType()).alias("date_fin"),
        F.lit(None).cast(StringType()).alias("motif"),        # MISS_DATEDEBUT absent DDL
        F.trim(F.col("FINMISS_CODE")).alias("code_fin"),
        F.lit(None).cast(IntegerType()).alias("prh_bts"),     # PRH_BTS absent DDL
        # statut_dpae : MISS_FLAGDPAE est datetime2 (date transmission), pas un booléen
        F.col("MISS_FLAGDPAE").cast(TimestampType()).alias("statut_dpae"),
        # ecart_heures : DATEDIFF('hour', CNTI_DATEFFET, MISS_FLAGDPAE) = FLAGDPAE − date_debut
        F.when(
            F.col("MISS_FLAGDPAE").isNotNull() & F.col("_cnti_dateffet").isNotNull(),
            (
                F.unix_timestamp(F.col("MISS_FLAGDPAE").cast(TimestampType()))
                - F.unix_timestamp(F.col("_cnti_dateffet"))
            ) / 3600.0,
        ).cast(LongType()).alias("ecart_heures"),
        F.col("_delai_h").cast(LongType()).alias("delai_placement_heures"),
        # categorie_delai : 'inconnu' si données manquantes (null → inconnu, pas 'long')
        F.when(F.col("_delai_h").isNull(), "inconnu")
         .when(F.col("_delai_h") <= 24,  "urgent")
         .when(F.col("_delai_h") <= 72,  "court")
         .when(F.col("_delai_h") <= 168, "standard")
         .otherwise("long")
         .alias("categorie_delai"),
        F.col("_batch_id"),
        F.col("_loaded_at").cast(TimestampType()).alias("_loaded_at"),
    )


def process_contrats(spark: SparkSession, cfg: Cfg) -> DataFrame:
    """silver.missions.contrats ← WTCNTI.

    Dédup : QUALIFY ROW_NUMBER() OVER (PARTITION BY PER_ID, CNT_ID, CNTI_ORDRE
                                        ORDER BY _loaded_at DESC) = 1
    Corrections DDL probe 2026-03-05 :
      ORDRE→CNTI_ORDRE, TPCI_CODE→PCS_CODE_2003, CNT_TAUXPAYE→CNTI_THPAYE,
      CNT_TAUXFACT→CNTI_THFACT, CNT_NBHEURE→CNTI_DURHEBDO, CNT_POSTE→CNTI_POSTE.
    """
    w = Window.partitionBy("PER_ID", "CNT_ID", "CNTI_ORDRE").orderBy(
        F.col("_loaded_at").desc()
    )
    return (
        _read_bronze(spark, cfg, "wtcnti")
        .filter(F.col("PER_ID").isNotNull() & F.col("CNT_ID").isNotNull())
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .select(
            F.col("PER_ID").cast(IntegerType()).alias("per_id"),
            F.col("CNT_ID").cast(IntegerType()).alias("cnt_id"),
            F.col("CNTI_ORDRE").cast(IntegerType()).alias("ordre"),
            F.col("MET_ID").cast(IntegerType()).alias("met_id"),
            F.col("PCS_CODE_2003").cast(StringType()).alias("tpci_code"),
            F.col("CNTI_DATEFFET").cast(DateType()).alias("date_debut"),
            F.col("CNTI_DATEFINCNTI").cast(DateType()).alias("date_fin"),
            F.col("CNTI_THPAYE").cast(DecimalType(10, 4)).alias("taux_paye"),
            F.col("CNTI_THFACT").cast(DecimalType(10, 4)).alias("taux_fact"),
            F.col("CNTI_DURHEBDO").cast(DecimalType(10, 2)).alias("nb_heures"),
            F.trim(F.col("CNTI_POSTE")).alias("poste"),
            F.col("_batch_id"),
            F.col("_loaded_at").cast(TimestampType()).alias("_loaded_at"),
        )
    )


def process_commandes(spark: SparkSession, cfg: Cfg) -> DataFrame:
    """silver.missions.commandes ← WTCMD.

    Dédup : QUALIFY ROW_NUMBER() OVER (PARTITION BY CMD_ID ORDER BY _loaded_at DESC) = 1
    Corrections DDL :
      CMD_DATE→CMD_DTE, CMD_NBSAL→CMD_NBSALS, TIE_ID/MET_ID retirés (absents DDL 2026-03-11).
      STAT_CODE/STAT_TYPE ajoutés (bronze_missions v2 confirmés).
    """
    w = Window.partitionBy("CMD_ID").orderBy(F.col("_loaded_at").desc())
    return (
        _read_bronze(spark, cfg, "wtcmd")
        .filter(F.col("CMD_ID").isNotNull())
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .select(
            F.col("CMD_ID").cast(IntegerType()).alias("cmd_id"),
            F.col("RGPCNT_ID").cast(IntegerType()).alias("rgpcnt_id"),
            F.col("CMD_DTE").cast(DateType()).alias("cmd_date"),
            F.col("CMD_NBSALS").cast(IntegerType()).alias("nb_sal"),
            # COALESCE(..., '') reproduit le comportement DuckDB : jamais NULL en sortie
            F.trim(F.coalesce(F.col("STAT_CODE").cast(StringType()), F.lit("")))
             .alias("stat_code"),
            F.trim(F.coalesce(F.col("STAT_TYPE").cast(StringType()), F.lit("")))
             .alias("stat_type"),
            F.col("_batch_id"),
            F.col("_loaded_at").cast(TimestampType()).alias("_loaded_at"),
        )
    )


def process_placements(spark: SparkSession, cfg: Cfg) -> DataFrame:
    """silver.missions.placements ← WTPLAC.

    Dédup : QUALIFY ROW_NUMBER() OVER (PARTITION BY PLAC_ID ORDER BY _loaded_at DESC) = 1
    Corrections DDL : PLAC_DATE→PLAC_DTEEDI ; PLAC_STATUT absent DDL → NULL::VARCHAR.
    """
    w = Window.partitionBy("PLAC_ID").orderBy(F.col("_loaded_at").desc())
    return (
        _read_bronze(spark, cfg, "wtplac")
        .filter(F.col("PLAC_ID").isNotNull())
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .select(
            F.col("PLAC_ID").cast(IntegerType()).alias("plac_id"),
            F.col("RGPCNT_ID").cast(IntegerType()).alias("rgpcnt_id"),
            F.col("TIE_ID").cast(IntegerType()).alias("tie_id"),
            F.col("MET_ID").cast(IntegerType()).alias("met_id"),
            F.lit(None).cast(StringType()).alias("statut"),   # PLAC_STATUT absent DDL
            F.col("PLAC_DTEEDI").cast(DateType()).alias("plac_date"),
            F.col("_batch_id"),
            F.col("_loaded_at").cast(TimestampType()).alias("_loaded_at"),
        )
    )


def process_fin_mission(spark: SparkSession, cfg: Cfg) -> DataFrame:
    """silver.missions.fin_mission ← WTMISS + WTCNTI + WTFINMISS + PYMTFCNT.

    Enrichissement rupture CTT (phase 4, probe 2026-03-15).
    statut_fin_mission logique validée :
      MISS_ANNULE=1                          → ANNULEE
      FINMISS_CODE IS NULL                   → EN_COURS
      MTFCNT_FINCNT=0                        → EN_COURS  (changement admin, mission continue)
      MTFCNT_FINCNT=1 LIKE '%fin de mission%' → TERME_NORMAL
      MTFCNT_FINCNT=1 autres                 → RUPTURE   (16 codes : démission, licenciement…)
      Fallback LIKE finmiss_libelle          → TERME_NORMAL / RUPTURE (MTFCNT_ID NULL)

    MISS_ANNULE : NULL=2 341 013 / 0=559 375 → non-annulé ; 1=12 314 → annulé.
    """
    # ── wtcnti CTE : MIN(date_debut) + MAX(date_fin) par (PER_ID, CNT_ID) ────
    wtcnti = (
        _read_bronze(spark, cfg, "wtcnti")
        .select(
            F.col("PER_ID"),
            F.col("CNT_ID"),
            F.col("CNTI_DATEFFET").cast(TimestampType()).alias("_cnti_dateffet_ts"),
            F.col("CNTI_DATEFINCNTI").cast(TimestampType()).alias("_cnti_datefincnti_ts"),
        )
        .groupBy("PER_ID", "CNT_ID")
        .agg(
            F.min("_cnti_dateffet_ts").alias("_cnti_dateffet"),       # TimestampType
            F.max("_cnti_datefincnti_ts").alias("_cnti_datefincnti"),  # TimestampType
        )
    )

    # ── wtfinmiss CTE : table de codes fin de mission ─────────────────────────
    wtfinmiss = (
        _read_bronze(spark, cfg, "wtfinmiss")
        .select(
            F.trim(F.col("FINMISS_CODE")).alias("_fm_code"),
            F.trim(F.col("FINMISS_LIBELLE")).alias("_fm_libelle"),
            F.col("MTFCNT_ID").cast(IntegerType()).alias("_fm_mtfcnt_id"),
        )
    )

    # ── pymtfcnt CTE : table motifs fin contrat paie ──────────────────────────
    # MTFCNT_FINCNT=0 → changement admin ; MTFCNT_FINCNT=1 → fin effective
    pymtfcnt = (
        _read_bronze(spark, cfg, "pymtfcnt")
        .select(
            F.col("MTFCNT_ID").cast(IntegerType()).alias("_pmt_id"),
            F.trim(F.col("MTFCNT_CODE")).alias("_pmt_code"),
            F.trim(F.col("MTFCNT_LIBELLE")).alias("_pmt_libelle"),
            F.col("MTFCNT_FINCNT").cast(ShortType()).alias("_pmt_fincnt"),
        )
    )

    # ── src : raw_wtmiss dédupliqué QUALIFY par (PER_ID, CNT_ID) ─────────────
    w_miss = Window.partitionBy("PER_ID", "CNT_ID").orderBy(F.col("_loaded_at").desc())
    src = (
        _read_bronze(spark, cfg, "wtmiss")
        .filter(F.col("PER_ID").isNotNull() & F.col("CNT_ID").isNotNull())
        .withColumn("_rn", F.row_number().over(w_miss))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    # ── Joins LEFT (4 tables) ─────────────────────────────────────────────────
    # 1. src → wtcnti  ON (PER_ID, CNT_ID)
    # 2. src → wtfinmiss ON TRIM(FINMISS_CODE) = _fm_code
    # 3. wtfinmiss → pymtfcnt ON _fm_mtfcnt_id = _pmt_id
    # FINMISS_CODE n'existe que dans src → F.col("FINMISS_CODE") est non-ambigu après joins
    joined = (
        src
        .join(wtcnti, ["PER_ID", "CNT_ID"], "left")
        .join(
            wtfinmiss,
            F.trim(F.col("FINMISS_CODE")) == F.col("_fm_code"),
            "left",
        )
        .join(
            pymtfcnt,
            F.col("_fm_mtfcnt_id") == F.col("_pmt_id"),
            "left",
        )
    )

    # ── statut_fin_mission CASE ───────────────────────────────────────────────
    # MISS_ANNULE : smallint → cast ShortType (null=non-annulé, 1=annulé, 0=non-annulé)
    statut_fin = (
        F.when(F.col("MISS_ANNULE").cast(ShortType()) == 1, "ANNULEE")
         .when(F.col("FINMISS_CODE").isNull(), "EN_COURS")
         # MTFCNT_FINCNT=0 : changement admin (établissement, taux) — mission non terminée
         .when(F.col("_pmt_fincnt") == 0, "EN_COURS")
         # MTFCNT_FINCNT=1 fin effective : seul "Fin de Mission TT" = terme normal
         .when(F.lower(F.col("_pmt_libelle")).like("%fin de mission%"), "TERME_NORMAL")
         .when(F.col("_pmt_fincnt") == 1, "RUPTURE")
         # Fallback LIKE finmiss_libelle si MTFCNT_ID NULL (codes WTFINMISS sans PYMTFCNT)
         .when(
             F.lower(F.col("_fm_libelle")).like("%terme%")
             | F.lower(F.col("_fm_libelle")).like("%normal%")
             | F.lower(F.col("_fm_libelle")).like("%échéance%"),
             "TERME_NORMAL",
         )
         .otherwise("RUPTURE")
    )

    # ── duree_reelle_jours : DATEDIFF('day', cnti_dateffet, cnti_datefincnti) ─
    # F.datediff(end, start) = end − start en jours (même sémantique que DuckDB)
    duree_jours = F.datediff(
        F.col("_cnti_datefincnti").cast(DateType()),
        F.col("_cnti_dateffet").cast(DateType()),
    )

    return joined.select(
        F.col("PER_ID").cast(IntegerType()).alias("per_id"),
        F.col("CNT_ID").cast(IntegerType()).alias("cnt_id"),
        F.col("RGPCNT_ID").cast(IntegerType()).alias("rgpcnt_id"),
        F.col("TIE_ID").cast(IntegerType()).alias("tie_id"),
        F.col("_cnti_dateffet").cast(DateType()).alias("date_debut"),
        F.col("_cnti_datefincnti").cast(DateType()).alias("date_fin_reelle"),
        F.col("MISS_SAISIE_DTFIN").cast(DateType()).alias("date_fin_saisie"),
        F.trim(F.col("FINMISS_CODE")).alias("finmiss_code"),
        F.col("_fm_libelle").alias("finmiss_libelle"),
        F.col("_pmt_code").alias("mtfcnt_code"),
        F.col("_pmt_libelle").alias("mtfcnt_libelle"),
        F.col("_pmt_fincnt").alias("mtfcnt_fincnt"),
        F.col("MISS_ANNULE").cast(ShortType()).alias("miss_annule"),
        duree_jours.alias("duree_reelle_jours"),
        statut_fin.alias("statut_fin_mission"),
        F.col("_batch_id"),
        F.col("_loaded_at").cast(TimestampType()).alias("_loaded_at"),
    )


def process_facinfo(spark: SparkSession, cfg: Cfg) -> DataFrame:
    """silver.missions.facinfo ← WTFACINFO (full-load, FACINFO_DATEMODIF absent DDL).

    Table de liaison factures ↔ missions : fac_num ↔ per_id + cnt_id + tie_id.
    Utilisée dans gold_ca_mensuel pour compter nb_missions_fac par facture.

    Pas de delta_col → full-load quotidien. Le dump du jour contient tous les enregistrements.
    Dédup : QUALIFY ROW_NUMBER() OVER (PARTITION BY CNT_ID, FAC_NUM
                                        ORDER BY _loaded_at DESC) = 1
    Colonnes DDL absentes : EFAC_NUM, RGPCNT_ID, FACINFO_DATEMODIF (probe_ddl_schema.py).
    """
    w = Window.partitionBy("CNT_ID", "FAC_NUM").orderBy(F.col("_loaded_at").desc())
    return (
        _read_bronze(spark, cfg, "wtfacinfo")
        .filter(F.col("CNT_ID").isNotNull() & F.col("FAC_NUM").isNotNull())
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .select(
            F.col("CNT_ID").cast(IntegerType()).alias("cnt_id"),
            F.trim(F.col("FAC_NUM").cast(StringType())).alias("fac_num"),
            F.col("PER_ID").cast(IntegerType()).alias("per_id"),
            F.col("TIE_ID").cast(IntegerType()).alias("tie_id"),
            F.col("_batch_id"),
            F.col("_loaded_at").cast(TimestampType()).alias("_loaded_at"),
        )
    )


def process_contrats_paie(spark: SparkSession, cfg: Cfg) -> DataFrame:
    """silver.missions.contrats_paie ← PYCONTRAT.

    Table maîtresse contrats paie (ajout 2026-03-11).
    Jointures Gold : PER_ID+CNT_ID ↔ WTMISS/WTCNTI/WTPRH ; ETA_ID ↔ PYETABLISSEMENT.
    Dédup : QUALIFY ROW_NUMBER() OVER (PARTITION BY PER_ID, CNT_ID
                                        ORDER BY _loaded_at DESC) = 1
    """
    w = Window.partitionBy("PER_ID", "CNT_ID").orderBy(F.col("_loaded_at").desc())
    return (
        _read_bronze(spark, cfg, "pycontrat")
        .filter(F.col("PER_ID").isNotNull() & F.col("CNT_ID").isNotNull())
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .select(
            F.col("PER_ID").cast(IntegerType()).alias("per_id"),
            F.col("CNT_ID").cast(IntegerType()).alias("cnt_id"),
            F.col("ETA_ID").cast(IntegerType()).alias("eta_id"),
            F.col("RGPCNT_ID").cast(IntegerType()).alias("rgpcnt_id"),
            F.col("CNT_DATEDEB").cast(DateType()).alias("date_debut"),
            F.col("CNT_DATEFIN").cast(DateType()).alias("date_fin"),
            F.col("CNT_FINPREVU").cast(DateType()).alias("date_fin_prevue"),
            F.trim(F.col("LOTPAYE_CODE")).alias("lot_paye_code"),
            F.trim(F.col("TYPCOT_CODE")).alias("typ_cotisation_code"),
            F.col("CNT_AVT_ORDRE").cast(IntegerType()).alias("avt_ordre"),
            F.col("CNT_INI_ORDRE").cast(IntegerType()).alias("ini_ordre"),
            F.col("_batch_id"),
            F.col("_loaded_at").cast(TimestampType()).alias("_loaded_at"),
        )
    )


# ── main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    cfg = Cfg()
    spark = build_spark(cfg)

    tables = [
        (process_missions,      "silver.missions.missions"),
        (process_contrats,      "silver.missions.contrats"),
        (process_commandes,     "silver.missions.commandes"),
        (process_placements,    "silver.missions.placements"),
        (process_fin_mission,   "silver.missions.fin_mission"),
        (process_contrats_paie, "silver.missions.contrats_paie"),
        (process_facinfo,       "silver.missions.facinfo"),
    ]

    for process_fn, table_id in tables:
        table_name = table_id.split(".")[-1]
        t0 = time.time()
        try:
            # cache() évite la double évaluation (count + write)
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
