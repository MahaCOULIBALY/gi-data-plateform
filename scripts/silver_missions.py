"""silver_missions.py — Silver · Missions & Contrats : WTMISS/WTCNTI/WTCMD/WTPLAC → Parquet S3.
Phase 0 · GI Data Lakehouse · Manifeste v2.0
# CORRECTIONS DDL (probe 2026-03-05) :
#   WTCNTI : ORDRE→CNTI_ORDRE, CNT_DATEDEBUT→CNTI_DATEFFET, CNT_DATEFIN→CNTI_DATEFINCNTI
#            CNT_TAUXPAYE→CNTI_THPAYE, CNT_TAUXFACT→CNTI_THFACT, CNT_NBHEURE→CNTI_DURHEBDO
#            CNT_POSTE→CNTI_POSTE, TPCI_CODE→PCS_CODE_2003
#   WTMISS : MISS_CODEFIN→FINMISS_CODE, MISS_DATEFIN→MISS_SAISIE_DTFIN (closest)
#            MISS_DATEDEBUT/MOTIF/PRH_BTS→NULL (absent bronze _COLS, WARN DDL)
#   WTCMD  : CMD_DATE→CMD_DTE, CMD_NBSAL→CMD_NBSALS, CMD_STATUT→NULL (absent DDL)
#   WTPLAC : PLAC_DATE→PLAC_DTEEDI, PLAC_STATUT→NULL (absent DDL visible)
"""
import json
from dataclasses import dataclass, field
from datetime import datetime, timezone

from shared import Config, RunMode, Stats, get_duckdb_connection, logger

DOMAIN = "missions"


@dataclass
class _Table:
    name: str
    bronze: str
    silver: str
    dedup_key: str
    sql: str


_TABLES: list[_Table] = [
    _Table(
        name="WTMISS",
        bronze="wtmiss",
        silver=f"slv_{DOMAIN}/missions",
        dedup_key="PER_ID, CNT_ID",
        sql="""
            SELECT
                CAST(PER_ID             AS INTEGER)       AS per_id,
                CAST(CNT_ID             AS INTEGER)       AS cnt_id,
                CAST(TIE_ID             AS INTEGER)       AS tie_id,
                CAST(TIES_SERV          AS INTEGER)       AS ties_serv,
                CAST(RGPCNT_ID          AS INTEGER)       AS rgpcnt_id,
                NULL::DATE                                AS date_debut,
                CAST(MISS_SAISIE_DTFIN  AS DATE)          AS date_fin,
                NULL::VARCHAR                             AS motif,
                TRIM(FINMISS_CODE)                        AS code_fin,
                NULL::INTEGER                             AS prh_bts,
                _batch_id,
                CAST(_loaded_at         AS TIMESTAMP)     AS _loaded_at
            FROM src
            WHERE PER_ID IS NOT NULL AND CNT_ID IS NOT NULL
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY PER_ID, CNT_ID ORDER BY _loaded_at DESC
            ) = 1
        """,
    ),
    _Table(
        name="WTCNTI",
        bronze="wtcnti",
        silver=f"slv_{DOMAIN}/contrats",
        dedup_key="PER_ID, CNT_ID, CNTI_ORDRE",
        sql="""
            SELECT
                CAST(PER_ID             AS INTEGER)         AS per_id,
                CAST(CNT_ID             AS INTEGER)         AS cnt_id,
                CAST(CNTI_ORDRE         AS INTEGER)         AS ordre,
                CAST(MET_ID             AS INTEGER)         AS met_id,
                CAST(PCS_CODE_2003      AS VARCHAR)         AS tpci_code,
                CAST(CNTI_DATEFFET      AS DATE)            AS date_debut,
                CAST(CNTI_DATEFINCNTI   AS DATE)            AS date_fin,
                CAST(CNTI_THPAYE        AS DECIMAL(10,4))   AS taux_paye,
                CAST(CNTI_THFACT        AS DECIMAL(10,4))   AS taux_fact,
                CAST(CNTI_DURHEBDO      AS DECIMAL(10,2))   AS nb_heures,
                TRIM(CNTI_POSTE)                            AS poste,
                _batch_id,
                CAST(_loaded_at         AS TIMESTAMP)       AS _loaded_at
            FROM src
            WHERE PER_ID IS NOT NULL AND CNT_ID IS NOT NULL
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY PER_ID, CNT_ID, CNTI_ORDRE ORDER BY _loaded_at DESC
            ) = 1
        """,
    ),
    _Table(
        name="WTCMD",
        bronze="wtcmd",
        silver=f"slv_{DOMAIN}/commandes",
        dedup_key="CMD_ID",
        sql="""
            SELECT
                CAST(CMD_ID         AS INTEGER)     AS cmd_id,
                CAST(RGPCNT_ID      AS INTEGER)     AS rgpcnt_id,
                CAST(TIE_ID         AS INTEGER)     AS tie_id,
                CAST(CMD_DTE        AS DATE)        AS cmd_date,
                NULL::VARCHAR                       AS statut,
                CAST(CMD_NBSALS     AS INTEGER)     AS nb_sal,
                CAST(MET_ID         AS INTEGER)     AS met_id,
                _batch_id,
                CAST(_loaded_at     AS TIMESTAMP)   AS _loaded_at
            FROM src
            WHERE CMD_ID IS NOT NULL
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY CMD_ID ORDER BY _loaded_at DESC
            ) = 1
        """,
    ),
    _Table(
        name="WTPLAC",
        bronze="wtplac",
        silver=f"slv_{DOMAIN}/placements",
        dedup_key="PLAC_ID",
        sql="""
            SELECT
                CAST(PLAC_ID        AS INTEGER)     AS plac_id,
                CAST(RGPCNT_ID      AS INTEGER)     AS rgpcnt_id,
                CAST(TIE_ID         AS INTEGER)     AS tie_id,
                CAST(MET_ID         AS INTEGER)     AS met_id,
                NULL::VARCHAR                       AS statut,
                CAST(PLAC_DTEEDI    AS DATE)        AS plac_date,
                _batch_id,
                CAST(_loaded_at     AS TIMESTAMP)   AS _loaded_at
            FROM src
            WHERE PLAC_ID IS NOT NULL
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY PLAC_ID ORDER BY _loaded_at DESC
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
