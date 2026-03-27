"""silver_interimaires_detail.py — Bronze → Iceberg OVH · évaluations + coordonnées + portefeuille agences.
Phase 2 · GI Data Lakehouse · Manifeste v2.0
# CORRECTIONS DDL (probe 2026-03-05) :
# WTPEVAL : PEVAL_DATE→PEVAL_DU, PEVAL_NOTE→PEVAL_EVALUATION,
#           PEVAL_EVALUATEUR/COMMENTAIRE absent → NULL
# PYCOORDONNEE: TYPTEL→TYPTEL_CODE, COORD_VALEUR→PER_TEL_NTEL, COORD_PRINC→NULL
# CORRECTIONS DDL (2026-03-11) :
# WTUGPINT : ajout process_ugpint — UGPINT_DATEMODIF absent DDL (full-load)
#            table slv_interimaires/portefeuille_agences (rattachement intérimaire↔agence)
# Partition date appliquée sur toutes les sources Bronze (FinOps — évite full-scan S3)
# CORRECTIONS (2026-03-23) :
# Anomalie #3 : s3_delete_prefix avant COPY — purge Silver avant réécriture (toutes les tables)
# Anomalie #5 : ajout ')' fermeture CTE dans les 4 fonctions process_*
# Anomalie #7 : process_fidelisation — ajout try/except + paramètre stats: Stats
#               run() : passe stats à process_fidelisation (alignement avec les autres fonctions)
"""
import json
from shared import Config, RunMode, Stats, get_duckdb_connection, s3_has_files, s3_delete_prefix, logger


PIPELINE = "silver_interimaires_detail"


def process_evaluations(ddb, cfg: Config, stats: Stats) -> int:
    if not s3_has_files(cfg, cfg.bucket_bronze, f"raw_wtpeval/{cfg.date_partition}/"):
        logger.info(json.dumps({"table": "evaluations", "rows": 0, "status": "empty"}))
        return 0
    b = f"s3://{cfg.bucket_bronze}"
    query = f"""
WITH raw AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY PER_ID, PEVAL_DU ORDER BY _loaded_at DESC) AS rn
    FROM read_json_auto('{b}/raw_wtpeval/{cfg.date_partition}/*.json',
        union_by_name=true, hive_partitioning=false)
)
SELECT
    MD5(CONCAT(PER_ID::VARCHAR, '|', COALESCE(PEVAL_DU::VARCHAR, ''))) AS eval_id,
    PER_ID::INT AS per_id,
    TRY_CAST(PEVAL_DU AS DATE) AS date_eval,
    TRY_CAST(PEVAL_EVALUATION AS DECIMAL(5,2)) AS note,
    NULL::VARCHAR AS commentaire,
    TRY_CAST(PEVAL_UTL AS INT) AS evaluateur_id,
    CURRENT_TIMESTAMP AS _loaded_at
FROM raw WHERE rn = 1 AND PER_ID IS NOT NULL
"""
    silver_path = f"s3://{cfg.bucket_silver}/slv_interimaires/evaluations/**/*.parquet"
    silver_prefix = "slv_interimaires/evaluations/"
    if cfg.mode in (RunMode.OFFLINE, RunMode.PROBE):
        count = ddb.execute(f"SELECT COUNT(*) FROM ({query})").fetchone()[0]
        logger.info(json.dumps({"mode": cfg.mode.value,
                    "table": "evaluations", "rows": count}))
        return count
    s3_delete_prefix(cfg, cfg.bucket_silver, silver_prefix)
    ddb.execute(
        f"COPY ({query}) TO '{silver_path}' (FORMAT PARQUET, COMPRESSION ZSTD, OVERWRITE_OR_IGNORE TRUE)")
    count = ddb.execute(
        f"SELECT COUNT(*) FROM read_parquet('{silver_path}')").fetchone()[0]
    logger.info(json.dumps({"table": "evaluations", "rows": count}))
    return count


def process_coordonnees(ddb, cfg: Config, stats: Stats) -> int:
    """RGPD : Silver-only, jamais exposé en Gold."""
    if not s3_has_files(cfg, cfg.bucket_bronze, f"raw_pycoordonnee/{cfg.date_partition}/"):
        logger.info(json.dumps({"table": "coordonnees", "rows": 0, "status": "empty"}))
        return 0
    b = f"s3://{cfg.bucket_bronze}"
    query = f"""
WITH raw AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY PER_ID, TYPTEL_CODE ORDER BY _loaded_at DESC) AS rn
    FROM read_json_auto('{b}/raw_pycoordonnee/{cfg.date_partition}/*.json',
        union_by_name=true, hive_partitioning=false)
)
SELECT
    MD5(CONCAT(PER_ID::VARCHAR, '|', COALESCE(TYPTEL_CODE::VARCHAR, ''))) AS coord_id,
    PER_ID::INT AS per_id,
    TRIM(TYPTEL_CODE::VARCHAR) AS type_coord,
    TRIM(PER_TEL_NTEL) AS valeur,
    TRIM(COALESCE(PER_TEL_POSTE, '')) AS poste,
    false AS is_principal,
    CURRENT_TIMESTAMP AS _loaded_at
FROM raw WHERE rn = 1 AND PER_ID IS NOT NULL
"""
    silver_path = f"s3://{cfg.bucket_silver}/slv_interimaires/coordonnees/**/*.parquet"
    silver_prefix = "slv_interimaires/coordonnees/"
    if cfg.mode in (RunMode.OFFLINE, RunMode.PROBE):
        count = ddb.execute(f"SELECT COUNT(*) FROM ({query})").fetchone()[0]
        logger.info(json.dumps({"mode": cfg.mode.value,
                    "table": "coordonnees", "rows": count}))
        return count
    s3_delete_prefix(cfg, cfg.bucket_silver, silver_prefix)
    ddb.execute(
        f"COPY ({query}) TO '{silver_path}' (FORMAT PARQUET, COMPRESSION ZSTD, OVERWRITE_OR_IGNORE TRUE)")
    count = ddb.execute(
        f"SELECT COUNT(*) FROM read_parquet('{silver_path}')").fetchone()[0]
    logger.info(json.dumps({"table": "coordonnees", "rows": count}))
    return count


def process_ugpint(ddb, cfg: Config, stats: Stats) -> int:
    """Portefeuille agence↔intérimaire (WTUGPINT).
    UGPINT_DATEMODIF absent DDL → full-load, dédup sur (PER_ID, RGPCNT_ID).
    """
    if not s3_has_files(cfg, cfg.bucket_bronze, f"raw_wtugpint/{cfg.date_partition}/"):
        logger.info(json.dumps({"table": "portefeuille_agences", "rows": 0, "status": "empty"}))
        return 0
    b = f"s3://{cfg.bucket_bronze}"
    query = f"""
WITH raw AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY PER_ID, RGPCNT_ID
            ORDER BY _loaded_at DESC
        ) AS rn
    FROM read_json_auto('{b}/raw_wtugpint/{cfg.date_partition}/*.json',
        union_by_name=true, hive_partitioning=false)
)
SELECT
    CAST(PER_ID AS INT)     AS per_id,
    CAST(RGPCNT_ID AS INT)  AS rgpcnt_id,
    CURRENT_TIMESTAMP       AS _loaded_at
FROM raw WHERE rn = 1 AND PER_ID IS NOT NULL AND RGPCNT_ID IS NOT NULL
"""
    silver_path = f"s3://{cfg.bucket_silver}/slv_interimaires/portefeuille_agences/**/*.parquet"
    silver_prefix = "slv_interimaires/portefeuille_agences/"
    if cfg.mode in (RunMode.OFFLINE, RunMode.PROBE):
        count = ddb.execute(f"SELECT COUNT(*) FROM ({query})").fetchone()[0]
        logger.info(json.dumps(
            {"mode": cfg.mode.value, "table": "portefeuille_agences", "rows": count}))
        return count
    s3_delete_prefix(cfg, cfg.bucket_silver, silver_prefix)
    ddb.execute(
        f"COPY ({query}) TO '{silver_path}' (FORMAT PARQUET, COMPRESSION ZSTD, OVERWRITE_OR_IGNORE TRUE)")
    count = ddb.execute(
        f"SELECT COUNT(*) FROM read_parquet('{silver_path}')").fetchone()[0]
    logger.info(json.dumps({"table": "portefeuille_agences", "rows": count}))
    return count


def process_fidelisation(ddb, cfg: Config, stats: Stats) -> int:
    """Fidélisation intérimaires via PINT_DERVENDTE (proxy SAL_DATESORTIE absent DDL Evolia).
    Catégories : actif_recent (≤90j), actif_annee (≤365j), inactif_long (>365j), inactif (jamais).
    Phase 4 : nb_missions_12m + taux_fidelisation_pct depuis slv_missions/missions.
    """
    if not s3_has_files(cfg, cfg.bucket_bronze, f"raw_wtpint/{cfg.date_partition}/"):
        logger.info(json.dumps({"table": "fidelisation", "rows": 0, "status": "empty"}))
        return 0
    b = f"s3://{cfg.bucket_bronze}"
    slv = f"s3://{cfg.bucket_silver}"
    silver_path = f"{slv}/slv_interimaires/fidelisation/**/*.parquet"
    silver_prefix = "slv_interimaires/fidelisation/"
    query = f"""
WITH raw AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY PER_ID ORDER BY _loaded_at DESC) AS rn
    FROM read_json_auto('{b}/raw_wtpint/{cfg.date_partition}/*.json',
        union_by_name=true, hive_partitioning=false)
),
missions_12m AS (
    SELECT
        per_id,
        COUNT(DISTINCT cnt_id) AS nb_missions_12m
    FROM read_parquet('{slv}/slv_missions/missions/**/*.parquet')
    WHERE TRY_CAST(date_debut AS DATE) >= CURRENT_DATE - INTERVAL '12 months'
    GROUP BY per_id
),
base AS (
    SELECT
        CAST(PER_ID AS INT) AS per_id,
        TRY_CAST(PINT_CREATDTE  AS DATE) AS date_premiere_vente,
        TRY_CAST(PINT_PREVENDTE AS DATE) AS date_avant_derniere_vente,
        TRY_CAST(PINT_DERVENDTE AS DATE) AS date_derniere_vente,
        CASE WHEN PINT_DERVENDTE IS NOT NULL AND PINT_CREATDTE IS NOT NULL
            THEN DATEDIFF('day',
                TRY_CAST(PINT_CREATDTE  AS DATE),
                TRY_CAST(PINT_DERVENDTE AS DATE))
            ELSE NULL END AS anciennete_jours,
        CASE WHEN PINT_DERVENDTE IS NOT NULL
            THEN DATEDIFF('day',
                TRY_CAST(PINT_DERVENDTE AS DATE),
                CURRENT_DATE)
            ELSE NULL END AS jours_depuis_derniere_vente,
        CASE
            WHEN PINT_DERVENDTE IS NULL THEN 'inactif'
            WHEN DATEDIFF('day', TRY_CAST(PINT_DERVENDTE AS DATE), CURRENT_DATE) <= 90
                THEN 'actif_recent'
            WHEN DATEDIFF('day', TRY_CAST(PINT_DERVENDTE AS DATE), CURRENT_DATE) <= 365
                THEN 'actif_annee'
            ELSE 'inactif_long'
        END AS categorie_fidelisation,
        CURRENT_TIMESTAMP AS _loaded_at
    FROM raw
    WHERE rn = 1 AND PER_ID IS NOT NULL
)
SELECT
    b.per_id,
    b.date_premiere_vente,
    b.date_avant_derniere_vente,
    b.date_derniere_vente,
    b.anciennete_jours,
    b.jours_depuis_derniere_vente,
    b.categorie_fidelisation,
    COALESCE(m.nb_missions_12m, 0)::INT                              AS nb_missions_12m,
    ROUND(
        COALESCE(m.nb_missions_12m, 0)::DECIMAL
        / NULLIF(b.anciennete_jours / 30.0, 0),
    4)::DECIMAL(10,4)                                                AS taux_fidelisation_pct,
    b._loaded_at
FROM base b
LEFT JOIN missions_12m m ON m.per_id = b.per_id
"""
    try:
        if cfg.mode in (RunMode.OFFLINE, RunMode.PROBE):
            count = ddb.execute(
                f"SELECT COUNT(*) FROM ({query})").fetchone()[0]
            logger.info(json.dumps(
                {"mode": cfg.mode.value, "table": "fidelisation", "rows": count}))
            return count
        s3_delete_prefix(cfg, cfg.bucket_silver, silver_prefix)
        ddb.execute(
            f"COPY ({query}) TO '{silver_path}' (FORMAT PARQUET, COMPRESSION ZSTD, OVERWRITE_OR_IGNORE TRUE)")
        count = ddb.execute(
            f"SELECT COUNT(*) FROM read_parquet('{silver_path}')").fetchone()[0]
        logger.info(json.dumps({"table": "fidelisation", "rows": count}))
        return count
    except Exception as e:
        logger.exception(json.dumps(
            {"table": "fidelisation", "error": str(e)}))
        stats.errors.append({"table": "fidelisation", "error": str(e)})
        return 0


def run(cfg: Config) -> dict:
    stats = Stats()
    with get_duckdb_connection(cfg) as ddb:
        c1 = process_evaluations(ddb, cfg, stats)
        c2 = process_coordonnees(ddb, cfg, stats)
        c3 = process_ugpint(ddb, cfg, stats)
        # stats transmis — cohérence + gestion erreurs
        c4 = process_fidelisation(ddb, cfg, stats)
    stats.tables_processed = 4
    stats.rows_transformed = c1 + c2 + c3 + c4
    stats.extra = {
        "evaluations": c1,
        "coordonnees": c2,
        "portefeuille_agences": c3,
        "fidelisation": c4,
    }
    return stats.finish(cfg, PIPELINE)


if __name__ == "__main__":
    run(Config())
