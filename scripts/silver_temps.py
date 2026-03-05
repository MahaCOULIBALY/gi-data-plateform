"""silver_temps.py — Silver · Temps & Relevés d'heures : WTPRH/WTRHDON → Parquet S3.
Phase 0 · GI Data Lakehouse · Manifeste v2.0
# CORRECTIONS DDL (probe 2026-03-05) :
#   WTRHDON : RHD_RUBRIQUE→RHD_ORIRUB, RHD_BASEPAYE→RHD_BASEP, RHD_TAUXPAYE→RHD_TAUXP
#             RHD_BASEFACT→RHD_BASEF, RHD_TAUXFACT→RHD_TAUXF, RHD_LIBELLE→RHD_LIBRUB
#   WTPRH   : PRH_DATEMODIF→PRH_MODIFDATE, PRH_VALIDE→PRH_FLAG_RH
#             PRH_PERIODE/RGPCNT_ID→NULL (absent DDL — WARN probe)
# RGPD : PER_ID absent de heures_detail — jointure via PRH_BTS uniquement.
"""
import json
from dataclasses import dataclass
from datetime import datetime, timezone

from shared import Config, RunMode, Stats, get_duckdb_connection, logger

DOMAIN = "temps"


@dataclass
class _Table:
    name: str
    bronze: str
    silver: str
    sql: str


_TABLES: list[_Table] = [
    _Table(
        name="WTPRH",
        bronze="wtprh",
        silver=f"slv_{DOMAIN}/releves_heures",
        sql="""
            SELECT
                CAST(PRH_BTS            AS INTEGER)     AS prh_bts,
                CAST(PER_ID             AS INTEGER)     AS per_id,
                CAST(CNT_ID             AS INTEGER)     AS cnt_id,
                CAST(TIE_ID             AS INTEGER)     AS tie_id,
                NULL::INTEGER                           AS rgpcnt_id,
                NULL::VARCHAR                           AS periode,
                CAST(PRH_MODIFDATE      AS TIMESTAMP)   AS date_modif,
                CAST(PRH_FLAG_RH        AS BOOLEAN)     AS valide,
                _batch_id,
                CAST(_loaded_at         AS TIMESTAMP)   AS _loaded_at
            FROM src
            WHERE PRH_BTS IS NOT NULL
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY PRH_BTS ORDER BY _loaded_at DESC
            ) = 1
        """,
    ),
    _Table(
        name="WTRHDON",
        bronze="wtrhdon",
        silver=f"slv_{DOMAIN}/heures_detail",
        # RGPD : PER_ID absent — jointure via PRH_BTS → releves_heures
        sql="""
            SELECT
                CAST(PRH_BTS            AS INTEGER)         AS prh_bts,
                CAST(RHD_LIGNE          AS INTEGER)         AS rhd_ligne,
                TRIM(RHD_ORIRUB)                            AS rubrique,
                CAST(RHD_BASEP          AS DECIMAL(10,2))   AS base_paye,
                CAST(RHD_TAUXP          AS DECIMAL(10,4))   AS taux_paye,
                CAST(RHD_BASEF          AS DECIMAL(10,2))   AS base_fact,
                CAST(RHD_TAUXF          AS DECIMAL(10,4))   AS taux_fact,
                TRIM(RHD_LIBRUB)                            AS libelle,
                _batch_id,
                CAST(_loaded_at         AS TIMESTAMP)       AS _loaded_at
            FROM src
            WHERE PRH_BTS IS NOT NULL AND RHD_LIGNE IS NOT NULL
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY PRH_BTS, RHD_LIGNE ORDER BY _loaded_at DESC
            ) = 1
        """,
    ),
]


def _process(ddb, cfg: Config, t: _Table, stats: Stats) -> None:
    bronze_path = f"s3://{cfg.bucket_bronze}/raw_{t.bronze}/{cfg.date_partition}/*.json"
    silver_path = f"s3://{cfg.bucket_silver}/{t.silver}/**/*.parquet"
    try:
        ddb.execute(
            f"CREATE OR REPLACE VIEW src AS SELECT * FROM read_json_auto('{bronze_path}')")
        if cfg.mode in (RunMode.OFFLINE, RunMode.PROBE):
            count = ddb.execute(
                f"SELECT COUNT(*) FROM ({t.sql})").fetchone()[0]
            logger.info(json.dumps(
                {"mode": cfg.mode.value, "table": t.name, "rows": count}))
            stats.tables_processed += 1
            return
        ddb.execute(
            f"COPY ({t.sql}) TO '{silver_path}' (FORMAT PARQUET, COMPRESSION ZSTD, OVERWRITE_OR_IGNORE TRUE)")
        count = ddb.execute(
            f"SELECT COUNT(*) FROM read_parquet('{silver_path}')").fetchone()[0]
        stats.tables_processed += 1
        stats.rows_ingested += count
        logger.info(json.dumps(
            {"table": t.name, "rows": count, "silver": silver_path}))
    except Exception as e:
        logger.exception(json.dumps({"table": t.name, "error": str(e)}))
        stats.errors.append({"table": t.name, "error": str(e)})


def run(cfg: Config) -> dict:
    stats = Stats()
    with get_duckdb_connection(cfg) as ddb:
        for t in _TABLES:
            _process(ddb, cfg, t, stats)
    return stats.finish()


if __name__ == "__main__":
    run(Config())
