"""gold_etp.py — Gold · ETP hebdomadaire → gld_operationnel.fact_etp_hebdo.
Phase 2 · GI Data Lakehouse · Manifeste v2.0
Source : slv_temps/releves_heures (valide=true) + slv_temps/heures_detail
ETP = SUM(base_paye) / 35 — agrégation hebdomadaire par agence.

# MIGRÉ : iceberg_scan(cfg.iceberg_path(*)) → read_parquet(s3://gi-poc-silver/slv_*) (D01)
"""
import json
from shared import Config, Stats, get_duckdb_connection, get_pg_connection, pg_bulk_insert, logger


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
    cols = ["agence_id", "semaine_debut", "nb_releves",
            "nb_interimaires", "heures_totales", "etp"]

    with get_duckdb_connection(cfg) as ddb:
        rows = ddb.execute(build_etp_query(cfg)).fetchall()
        logger.info(json.dumps({"table": "fact_etp_hebdo", "rows": len(rows)}))
        stats.rows_transformed = len(rows)

    with get_pg_connection(cfg) as pg:
        pg_bulk_insert(cfg, pg, "gld_operationnel", "fact_etp_hebdo", cols, rows, stats)

    stats.tables_processed = 1
    return stats.finish()


if __name__ == "__main__":
    run(Config())
