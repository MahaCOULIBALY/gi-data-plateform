"""gold_competences.py — Pool compétences disponibles → gld_staffing.fact_competences_dispo.
Phase 3 · GI Data Lakehouse · Manifeste v2.0

=== CORRECTIONS SESSION 2 (audit Silver→Gold) ===
- MISS_DATEFIN → date_fin (Silver alias canonical in missions)
- MISS_DATEDEBUT implicite OK (non référencé directement)

# MIGRÉ : iceberg_scan(cfg.iceberg_path(*)) → read_parquet(s3://gi-poc-silver/slv_*) (D01)
"""
import sys
import logging
from shared import Config, Stats, get_duckdb_connection, get_pg_connection, pg_bulk_insert, logger


def build_competences_dispo_query(cfg: Config) -> str:
    return f"""
    WITH competences AS (
        SELECT per_id, code::INT AS met_id
        FROM read_parquet('s3://{cfg.bucket_silver}/slv_interimaires/competences/**/*.parquet')
        WHERE type_competence = 'METIER' AND is_active = true AND code != ''
    ),
    dim_int AS (
        SELECT per_id, agence_rattachement AS rgpcnt_id, is_actif
        FROM read_parquet('s3://{cfg.bucket_silver}/slv_interimaires/dim_interimaires/**/*.parquet')
        WHERE is_current = true AND is_actif = true
        QUALIFY ROW_NUMBER() OVER (PARTITION BY per_id ORDER BY valid_from DESC NULLS LAST) = 1
    ),
    missions_actives AS (
        SELECT DISTINCT per_id
        FROM read_parquet('s3://{cfg.bucket_silver}/slv_missions/missions/**/*.parquet')
        WHERE TRY_CAST(date_fin AS DATE) >= CURRENT_DATE
              OR date_fin IS NULL
    ),
    base AS (
        SELECT
            c.met_id,
            d.rgpcnt_id,
            d.per_id,
            CASE WHEN ma.per_id IS NOT NULL THEN true ELSE false END AS en_mission
        FROM competences c
        INNER JOIN dim_int d ON d.per_id = c.per_id
        LEFT JOIN missions_actives ma ON ma.per_id = c.per_id
    )
    SELECT
        MD5(met_id::VARCHAR) AS metier_sk,
        MD5(rgpcnt_id::VARCHAR) AS agence_sk,
        met_id,
        rgpcnt_id,
        COUNT(DISTINCT per_id) AS nb_qualifies,
        COUNT(DISTINCT CASE WHEN NOT en_mission THEN per_id END) AS nb_disponibles,
        COUNT(DISTINCT CASE WHEN en_mission THEN per_id END) AS nb_en_mission,
        CASE WHEN COUNT(DISTINCT per_id) > 0
             THEN ROUND(COUNT(DISTINCT CASE WHEN en_mission THEN per_id END)::DECIMAL
                        / COUNT(DISTINCT per_id), 4)
             ELSE 0 END AS taux_couverture,
        CURRENT_TIMESTAMP AS _computed_at
    FROM base
    WHERE rgpcnt_id IS NOT NULL AND met_id IS NOT NULL
    GROUP BY met_id, rgpcnt_id
    """


COLUMNS = ["metier_sk", "agence_sk", "met_id", "rgpcnt_id",
           "nb_qualifies", "nb_disponibles", "nb_en_mission",
           "taux_couverture", "_computed_at"]


def run(cfg: Config) -> dict:
    stats = Stats()
    with get_duckdb_connection(cfg) as ddb:
        rows = ddb.execute(build_competences_dispo_query(cfg)).fetchall()
        logger.info(
            f"fact_competences_dispo: {len(rows)} rows (metier×agence)")

    with get_pg_connection(cfg) as pg:
        pg_bulk_insert(cfg, pg, "gld_staffing",
                       "fact_competences_dispo", COLUMNS, rows, stats)

    stats.tables_processed = 1
    stats.rows_transformed = len(rows)
    return stats.finish()


if __name__ == "__main__":
    cfg = Config()
    run(cfg)
