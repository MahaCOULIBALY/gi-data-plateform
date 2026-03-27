"""gold_competences.py — Pool compétences disponibles → gld_staffing.fact_competences_dispo.
Phase 3 · GI Data Lakehouse · Manifeste v2.0

=== CORRECTIONS SESSION 2 (audit Silver→Gold) ===
- MISS_DATEFIN → date_fin (Silver alias canonical in missions)

=== CORRECTIONS SESSION 3 (audit Gold) ===
- G-COMP-B01 : TRUNCATE avant pg_bulk_insert (idempotence PG)
- G-COMP-M01 : RunMode importé + guards OFFLINE/PROBE
- G-COMP-M02 : try/except dans run() — dégradation gracieuse
- G-COMP-M03 : agence_sk lu depuis slv_agences/dim_agences (pas recomputed MD5)
- G-COMP-m01 : imports sys/logging supprimés

# MIGRÉ : iceberg_scan → read_parquet(s3://gi-poc-silver/slv_*) (D01)
"""
import json

from shared import (
    Config, RunMode, Stats,
    get_duckdb_connection, get_pg_connection, pg_bulk_insert,
    logger,
)


PIPELINE = "gold_competences"


def build_competences_dispo_query(cfg: Config) -> str:
    slv = f"s3://{cfg.bucket_silver}"
    return f"""
    WITH competences AS (
        SELECT per_id, code::INT AS met_id
        FROM read_parquet('{slv}/slv_interimaires/competences/**/*.parquet')
        WHERE type_competence = 'METIER' AND is_active = true AND code != ''
    ),
    dim_int AS (
        SELECT per_id, agence_rattachement AS rgpcnt_id, is_actif
        FROM read_parquet('{slv}/slv_interimaires/dim_interimaires/**/*.parquet')
        WHERE is_current = true AND is_actif = true AND agence_rattachement IS NOT NULL
        QUALIFY ROW_NUMBER() OVER (PARTITION BY per_id ORDER BY valid_from DESC NULLS LAST) = 1
    ),
    -- G-COMP-M03 : agence_sk canonique depuis Silver dim_agences (pas recomputed)
    dim_agences AS (
        SELECT rgpcnt_id, agence_sk
        FROM read_parquet('{slv}/slv_agences/dim_agences/**/*.parquet')
        WHERE is_active = true
        QUALIFY ROW_NUMBER() OVER (PARTITION BY rgpcnt_id ORDER BY rgpcnt_id) = 1
    ),
    missions_actives AS (
        SELECT DISTINCT per_id
        FROM read_parquet('{slv}/slv_missions/missions/**/*.parquet')
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
        MD5(b.met_id::VARCHAR)          AS metier_sk,
        COALESCE(ag.agence_sk,
                 MD5(COALESCE(b.rgpcnt_id, 0)::VARCHAR))  AS agence_sk,   -- 0 = agence non identifiée (B1)
        b.met_id,
        b.rgpcnt_id,
        COUNT(DISTINCT b.per_id)                             AS nb_qualifies,
        COUNT(DISTINCT CASE WHEN NOT b.en_mission THEN b.per_id END) AS nb_disponibles,
        COUNT(DISTINCT CASE WHEN b.en_mission     THEN b.per_id END) AS nb_en_mission,
        CASE WHEN COUNT(DISTINCT b.per_id) > 0
             THEN ROUND(
                 COUNT(DISTINCT CASE WHEN b.en_mission THEN b.per_id END)::DECIMAL
                 / COUNT(DISTINCT b.per_id), 4)
             ELSE 0
        END                                                  AS taux_couverture,
        CURRENT_TIMESTAMP                                    AS _computed_at
    FROM base b
    LEFT JOIN dim_agences ag ON ag.rgpcnt_id = b.rgpcnt_id
    WHERE b.met_id IS NOT NULL
    GROUP BY b.met_id, b.rgpcnt_id, ag.agence_sk
    """


COLUMNS = [
    "metier_sk", "agence_sk", "met_id", "rgpcnt_id",
    "nb_qualifies", "nb_disponibles", "nb_en_mission",
    "taux_couverture", "_computed_at",
]


def run(cfg: Config) -> dict:
    stats = Stats()

    if cfg.mode in (RunMode.OFFLINE, RunMode.PROBE):  # G-COMP-M01
        logger.info(json.dumps({
            "mode": cfg.mode.name, "table": "fact_competences_dispo", "action": "skipped"
        }))
        stats.tables_processed += 1
        return stats.finish(cfg, PIPELINE)

    try:
        with get_duckdb_connection(cfg) as ddb:
            rows = ddb.execute(build_competences_dispo_query(cfg)).fetchall()
            logger.info(json.dumps({
                "table": "fact_competences_dispo",
                "rows": len(rows),
                "step": "duckdb_ok",
            }))
    except Exception as e:  # G-COMP-M02
        logger.exception(json.dumps({"step": "duckdb_build", "error": str(e)}))
        stats.errors.append({"step": "duckdb_build", "error": str(e)})
        return stats.finish(cfg, PIPELINE)

    try:
        with get_pg_connection(cfg) as pg:
            # G-COMP-B01 : idempotence PG
            with pg.cursor() as cur:
                cur.execute(
                    "TRUNCATE TABLE gld_staffing.fact_competences_dispo")
            pg.commit()

            pg_bulk_insert(cfg, pg, "gld_staffing",
                           "fact_competences_dispo", COLUMNS, rows, stats)

        stats.tables_processed = 1
        stats.rows_transformed = len(rows)
        logger.info(json.dumps({
            "table": "fact_competences_dispo", "rows": len(rows), "status": "ok"
        }))
    except Exception as e:  # G-COMP-M02
        logger.exception(json.dumps({"step": "pg_insert", "error": str(e)}))
        stats.errors.append({"step": "pg_insert", "error": str(e)})

    return stats.finish(cfg, PIPELINE)


if __name__ == "__main__":
    run(Config())
