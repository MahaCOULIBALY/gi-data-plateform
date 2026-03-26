"""silver_factures.py — Silver · Facturation : WTEFAC/WTLFAC → Parquet S3.
Phase 0 · GI Data Lakehouse · Manifeste v2.0
# CORRECTIONS DDL (probe 2026-03-05) :
#   WTEFAC : EFAC_TYPE→EFAC_TYPF, EFAC_DATE→EFAC_DTEEDI, EFAC_ECHEANCE→EFAC_DTEECH
#            EFAC_MONTANTHT/TTC absent DDL → NULL (montants via WTLFAC)
#            PRH_BTS absent DDL → NULL
#   WTLFAC : LFAC_LIBELLE→LFAC_LIB, LFAC_RUBRIQUE absent DDL → NULL
"""
import json
from dataclasses import dataclass
from shared import Config, RunMode, Stats, get_duckdb_connection, s3_has_files, logger

PIPELINE = "silver_factures"
DOMAIN = "facturation"


@dataclass
class _Table:
    name: str
    bronze: str
    silver: str
    sql: str


_TABLES: list[_Table] = [
    _Table(
        name="WTEFAC",
        bronze="wtefac",
        silver=f"slv_{DOMAIN}/factures",
        sql="""
            SELECT
                TRIM(EFAC_NUM)                              AS efac_num,
                CAST(RGPCNT_ID          AS INTEGER)         AS rgpcnt_id,
                CAST(TIE_ID             AS INTEGER)         AS tie_id,
                CAST(TIES_SERV          AS INTEGER)         AS ties_serv,
                TRIM(EFAC_TYPF)                             AS type_facture,
                TRY_CAST(EFAC_DTEEDI   AS DATE)            AS date_facture,
                TRY_CAST(EFAC_DTEECH   AS DATE)            AS date_echeance,
                NULL::DECIMAL(18,2)                         AS montant_ht,
                NULL::DECIMAL(18,2)                         AS montant_ttc,
                TRY_CAST(EFAC_TAUXTVA  AS DECIMAL(6,4))    AS taux_tva,
                NULL::INTEGER                               AS prh_bts,
                _batch_id,
                TRY_CAST(_loaded_at    AS TIMESTAMP)       AS _loaded_at
            FROM src
            WHERE EFAC_NUM IS NOT NULL
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY EFAC_NUM ORDER BY _loaded_at DESC
            ) = 1
        """,
    ),
    _Table(
        name="WTLFAC",
        bronze="wtlfac",
        silver=f"slv_{DOMAIN}/lignes_factures",
        sql="""
            SELECT
                TRIM(FAC_NUM)                               AS fac_num,
                CAST(LFAC_ORD           AS INTEGER)         AS lfac_ord,
                TRIM(LFAC_LIB)                              AS libelle,
                TRY_CAST(LFAC_BASE     AS DECIMAL(10,2))   AS base,
                TRY_CAST(LFAC_TAUX     AS DECIMAL(10,4))   AS taux,
                TRY_CAST(LFAC_MNT      AS DECIMAL(18,2))   AS montant,
                NULL::VARCHAR                               AS rubrique,
                _batch_id,
                TRY_CAST(_loaded_at    AS TIMESTAMP)       AS _loaded_at
            FROM src
            WHERE FAC_NUM IS NOT NULL AND LFAC_ORD IS NOT NULL
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY FAC_NUM, LFAC_ORD ORDER BY _loaded_at DESC
            ) = 1
        """,
    ),
]


def _process(ddb, cfg: Config, t: _Table, stats: Stats) -> None:
    bronze_path = f"s3://{cfg.bucket_bronze}/raw_{t.bronze}/{cfg.date_partition}/*.json"
    silver_path = f"s3://{cfg.bucket_silver}/{t.silver}/**/*.parquet"
    # Guard : table de facturation critique — WARNING si source bronze absente
    if not s3_has_files(cfg, cfg.bucket_bronze, f"raw_{t.bronze}/{cfg.date_partition}/"):
        logger.warning(json.dumps({"table": t.name, "rows": 0, "status": "empty"}))
        return
    try:
        ddb.execute(
            f"CREATE OR REPLACE VIEW src AS "
            f"SELECT * FROM read_json_auto('{bronze_path}', union_by_name=true, hive_partitioning=false)"
        )
        if cfg.mode in (RunMode.OFFLINE, RunMode.PROBE):
            count = ddb.execute(
                f"SELECT COUNT(*) FROM ({t.sql})").fetchone()[0]
            logger.info(json.dumps(
                {"mode": cfg.mode.value, "table": t.name, "rows": count}))
            stats.tables_processed += 1
            return
        ddb.execute(
            f"COPY ({t.sql}) TO '{silver_path}' "
            f"(FORMAT PARQUET, COMPRESSION ZSTD, OVERWRITE_OR_IGNORE TRUE)"
        )
        count = ddb.execute(
            f"SELECT COUNT(*) FROM read_parquet('{silver_path}')").fetchone()[0]
        stats.tables_processed += 1
        stats.rows_ingested += count
        logger.info(json.dumps({"table": t.name, "rows": count}))
    except Exception as e:
        logger.exception(json.dumps({"table": t.name, "error": str(e)}))
        stats.errors.append({"table": t.name, "error": str(e)})


def run(cfg: Config) -> dict:
    stats = Stats()
    with get_duckdb_connection(cfg) as ddb:
        for t in _TABLES:
            _process(ddb, cfg, t, stats)
    return stats.finish(cfg, PIPELINE)


if __name__ == "__main__":
    run(Config())
