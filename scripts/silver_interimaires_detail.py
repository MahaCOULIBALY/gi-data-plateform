"""silver_interimaires_detail.py — Bronze → Iceberg OVH · évaluations + coordonnées + portefeuille agences.
Phase 2 · GI Data Lakehouse · Manifeste v2.0
# CORRECTIONS DDL (probe 2026-03-05) :
#   WTPEVAL     : PEVAL_DATE→PEVAL_DU, PEVAL_NOTE→PEVAL_EVALUATION,
#                 PEVAL_EVALUATEUR/COMMENTAIRE absent → NULL
#   PYCOORDONNEE: TYPTEL→TYPTEL_CODE, COORD_VALEUR→PER_TEL_NTEL, COORD_PRINC→NULL
# CORRECTIONS DDL (2026-03-11) :
#   WTUGPINT    : ajout process_ugpint — UGPINT_DATEMODIF absent DDL (full-load)
#                 table slv_interimaires/portefeuille_agences (rattachement intérimaire↔agence)
#   Partition date appliquée sur toutes les sources Bronze (FinOps — évite full-scan S3)
"""
import json
from shared import Config, Stats, get_duckdb_connection, write_silver_iceberg, logger


def process_evaluations(ddb, cfg: Config, stats: Stats) -> int:
    b = f"s3://{cfg.bucket_bronze}"
    query = f"""
    WITH raw AS (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY PER_ID, PEVAL_DU ORDER BY _loaded_at DESC) AS rn
        FROM read_json_auto('{b}/raw_wtpeval/{cfg.date_partition}/*.json',
                            union_by_name=true, hive_partitioning=false)
    )
    SELECT
        MD5(CONCAT(PER_ID::VARCHAR, '|', COALESCE(PEVAL_DU::VARCHAR, ''))) AS eval_id,
        PER_ID::INT                                     AS per_id,
        TRY_CAST(PEVAL_DU AS DATE)                     AS date_eval,
        TRY_CAST(PEVAL_EVALUATION AS DECIMAL(5,2))     AS note,
        NULL::VARCHAR                                   AS commentaire,
        TRY_CAST(PEVAL_UTL AS INT)                     AS evaluateur_id,
        CURRENT_TIMESTAMP                               AS _loaded_at
    FROM raw WHERE rn = 1 AND PER_ID IS NOT NULL
    """
    return write_silver_iceberg(ddb, query, "silver.interimaires.evaluations", cfg, stats)


def process_coordonnees(ddb, cfg: Config, stats: Stats) -> int:
    """RGPD : Silver-only, jamais exposé en Gold."""
    b = f"s3://{cfg.bucket_bronze}"
    query = f"""
    WITH raw AS (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY PER_ID, TYPTEL_CODE ORDER BY _loaded_at DESC) AS rn
        FROM read_json_auto('{b}/raw_pycoordonnee/{cfg.date_partition}/*.json',
                            union_by_name=true, hive_partitioning=false)
    )
    SELECT
        MD5(CONCAT(PER_ID::VARCHAR, '|', COALESCE(TYPTEL_CODE::VARCHAR, ''))) AS coord_id,
        PER_ID::INT                                     AS per_id,
        TRIM(TYPTEL_CODE::VARCHAR)                      AS type_coord,
        TRIM(PER_TEL_NTEL)                              AS valeur,
        TRIM(COALESCE(PER_TEL_POSTE, ''))               AS poste,
        false                                           AS is_principal,
        CURRENT_TIMESTAMP                               AS _loaded_at
    FROM raw WHERE rn = 1 AND PER_ID IS NOT NULL
    """
    return write_silver_iceberg(ddb, query, "silver.interimaires.coordonnees", cfg, stats)


def process_ugpint(ddb, cfg: Config, stats: Stats) -> int:
    """Portefeuille agence↔intérimaire (WTUGPINT).
    UGPINT_DATEMODIF absent DDL → full-load, dédup sur (PER_ID, RGPCNT_ID).
    """
    b = f"s3://{cfg.bucket_bronze}"
    # full-load (pas de colonne DATEMODIF) — dédup clé naturelle PER_ID + RGPCNT_ID
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
        CAST(PER_ID    AS INT)  AS per_id,
        CAST(RGPCNT_ID AS INT)  AS rgpcnt_id,
        CURRENT_TIMESTAMP       AS _loaded_at
    FROM raw WHERE rn = 1 AND PER_ID IS NOT NULL AND RGPCNT_ID IS NOT NULL
    """
    return write_silver_iceberg(ddb, query, "silver.interimaires.portefeuille_agences", cfg, stats)


def process_fidelisation(ddb, cfg: Config) -> int:
    """Fidélisation intérimaires via PINT_DERVENDTE (proxy SAL_DATESORTIE absent DDL Evolia).
    Catégories : actif_recent (≤90j), actif_annee (≤365j), inactif_long (>365j), inactif (jamais).
    """
    b = f"s3://{cfg.bucket_bronze}"
    silver = f"s3://{cfg.bucket_silver}/slv_interimaires/fidelisation"
    query = f"""
    WITH raw AS (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY PER_ID ORDER BY _loaded_at DESC) AS rn
        FROM read_json_auto('{b}/raw_wtpint/{cfg.date_partition}/*.json',
                            union_by_name=true, hive_partitioning=false)
    )
    SELECT
        CAST(PER_ID AS INT)                                        AS per_id,
        TRY_CAST(PINT_CREATDTE   AS DATE)                         AS date_premiere_vente,
        TRY_CAST(PINT_PREVENDTE  AS DATE)                         AS date_avant_derniere_vente,
        TRY_CAST(PINT_DERVENDTE  AS DATE)                         AS date_derniere_vente,
        CASE WHEN PINT_DERVENDTE IS NOT NULL AND PINT_CREATDTE IS NOT NULL
             THEN DATEDIFF('day',
                  TRY_CAST(PINT_CREATDTE AS DATE),
                  TRY_CAST(PINT_DERVENDTE AS DATE))
             ELSE NULL END                                         AS anciennete_jours,
        CASE WHEN PINT_DERVENDTE IS NOT NULL
             THEN DATEDIFF('day',
                  TRY_CAST(PINT_DERVENDTE AS DATE),
                  CURRENT_DATE)
             ELSE NULL END                                         AS jours_depuis_derniere_vente,
        CASE
            WHEN PINT_DERVENDTE IS NULL THEN 'inactif'
            WHEN DATEDIFF('day', TRY_CAST(PINT_DERVENDTE AS DATE), CURRENT_DATE) <= 90
                THEN 'actif_recent'
            WHEN DATEDIFF('day', TRY_CAST(PINT_DERVENDTE AS DATE), CURRENT_DATE) <= 365
                THEN 'actif_annee'
            ELSE 'inactif_long'
        END                                                        AS categorie_fidelisation,
        CURRENT_TIMESTAMP                                          AS _loaded_at
    FROM raw
    WHERE rn = 1 AND PER_ID IS NOT NULL
    """
    if cfg.mode in (RunMode.OFFLINE, RunMode.PROBE):
        count = ddb.execute(f"SELECT COUNT(*) FROM ({query})").fetchone()[0]
        logger.info(json.dumps({"mode": cfg.mode.value,
                    "table": "fidelisation", "rows": count}))
        return count
    ddb.execute(
        f"COPY ({query}) TO '{silver}' (FORMAT PARQUET, OVERWRITE_OR_IGNORE true)")
    count = ddb.execute(
        f"SELECT COUNT(*) FROM read_parquet('{silver}/**/*.parquet')").fetchone()[0]
    logger.info(json.dumps({"table": "fidelisation", "rows": count}))
    return count


def run(cfg: Config) -> dict:
    stats = Stats()
    with get_duckdb_connection(cfg) as ddb:
        c1 = process_evaluations(ddb, cfg, stats)
        c2 = process_coordonnees(ddb, cfg, stats)
        c3 = process_ugpint(ddb, cfg, stats)
        stats.tables_processed = 3
        stats.rows_transformed = c1 + c2 + c3
        stats.extra = {"evaluations": c1, "coordonnees": c2,
                       "portefeuille_agences": c3}
    return stats.finish()


if __name__ == "__main__":
    run(Config())
