"""gold_etp.py — Gold · ETP hebdomadaire → gld_operationnel.fact_etp_hebdo.
Phase 2 · GI Data Lakehouse · Manifeste v2.0
Source : slv_temps/releves_heures (valide=true) + slv_temps/heures_detail
ETP = SUM(base_paye) / 35 — agrégation hebdomadaire par agence.

=== CORRECTIONS SESSION 3 (audit Gold) ===
- G-ETP-B01 : TRUNCATE avant pg_bulk_insert (idempotence PG)
- G-ETP-M01 : RunMode importé + guards OFFLINE/PROBE
- G-ETP-M02 : try/except autour DuckDB + PG insert

# MIGRÉ : iceberg_scan → read_parquet(s3://gi-poc-silver/slv_*) (D01)
"""
import json

from shared import Config, RunMode, Stats, get_duckdb_connection, get_pg_connection, pg_bulk_insert, logger


PIPELINE = "gold_etp"
COLS = ["agence_id", "semaine_debut", "nb_releves",
        "nb_interimaires", "heures_totales", "etp"]


def build_etp_query(cfg: Config) -> str:
    return f"""
    WITH releves AS (
        SELECT prh_bts, rgpcnt_id, per_id, date_modif
        FROM read_parquet('s3://{cfg.bucket_silver}/slv_temps/releves_heures/**/*.parquet')
        WHERE valide = true AND rgpcnt_id IS NOT NULL AND date_modif IS NOT NULL
    ),
    heures AS (
        SELECT prh_bts, SUM(base_paye) AS heures_semaine
        FROM read_parquet('s3://{cfg.bucket_silver}/slv_temps/heures_detail/**/*.parquet')
        GROUP BY prh_bts
    )
    SELECT
        r.rgpcnt_id::INT                                    AS agence_id,
        DATE_TRUNC('week', r.date_modif::DATE)::DATE        AS semaine_debut,
        COUNT(DISTINCT r.prh_bts)                           AS nb_releves,
        COUNT(DISTINCT r.per_id)                            AS nb_interimaires,
        COALESCE(SUM(h.heures_semaine), 0)::DECIMAL(12,2)   AS heures_totales,
        ROUND(COALESCE(SUM(h.heures_semaine), 0) / 35.0,
              4)::DECIMAL(10,4)                             AS etp
    FROM releves r
    LEFT JOIN heures h ON h.prh_bts = r.prh_bts
    GROUP BY 1, 2
    ORDER BY 2 DESC, 1
    """


def run(cfg: Config) -> dict:
    stats = Stats()

    if cfg.mode in (RunMode.OFFLINE, RunMode.PROBE):  # G-ETP-M01
        logger.info(json.dumps({
            "mode": cfg.mode.name, "table": "fact_etp_hebdo", "action": "skipped"
        }))
        stats.tables_processed += 1
        return stats.finish(cfg, PIPELINE)

    rows = []
    try:
        with get_duckdb_connection(cfg) as ddb:
            rows = ddb.execute(build_etp_query(cfg)).fetchall()
            logger.info(json.dumps(
                {"table": "fact_etp_hebdo", "rows": len(rows)}))
    except Exception as e:  # G-ETP-M02
        logger.exception(json.dumps({"step": "duckdb_build", "error": str(e)}))
        stats.errors.append({"step": "duckdb_build", "error": str(e)})
        return stats.finish(cfg, PIPELINE)

    try:
        with get_pg_connection(cfg) as pg:
            # G-ETP-B01 : idempotence PG
            with pg.cursor() as cur:
                cur.execute("TRUNCATE TABLE gld_operationnel.fact_etp_hebdo")
            pg.commit()
            pg_bulk_insert(cfg, pg, "gld_operationnel",
                           "fact_etp_hebdo", COLS, rows, stats)
    except Exception as e:  # G-ETP-M02
        logger.exception(json.dumps({"step": "pg_insert", "error": str(e)}))
        stats.errors.append({"step": "pg_insert", "error": str(e)})
        return stats.finish(cfg, PIPELINE)

    stats.tables_processed = 1
    stats.rows_transformed = len(rows)
    return stats.finish(cfg, PIPELINE)


if __name__ == "__main__":
    run(Config())
