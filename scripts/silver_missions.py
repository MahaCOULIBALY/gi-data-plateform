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
# CORRECTIONS DDL (2026-03-11) :
#   WTCMD  : TIE_ID/MET_ID absents DDL → retirés (state card silver_required)
#            +STAT_CODE, +STAT_TYPE ajoutés (bronze_missions v2 confirmés)
# ENRICHISSEMENT (2026-03-13) :
#   WTMISS : +statut_dpae (MISS_FLAGDPAE datetime2), +ecart_heures (vs CNTI_DATEFFET),
#            +delai_placement_heures (CMD_DTE→CNTI_DATEFFET), +categorie_delai
#            Anomalie : MISS_FLAGDPAE est datetime2 (date transmission), pas un booléen
#            Anomalie : CMD_ID stocké float → TRY_CAST(CMD_ID AS INT) obligatoire
"""
import json
from dataclasses import dataclass, field
from typing import Callable

from shared import Config, RunMode, Stats, get_duckdb_connection, filter_tables, logger

DOMAIN = "missions"


@dataclass
class _Table:
    name: str
    bronze: str
    silver: str
    dedup_key: str
    sql: str = ""
    sql_fn: "Callable[[Config], str] | None" = field(default=None)


def _build_wtmiss_sql(cfg: Config) -> str:
    """WTMISS enrichi : statut_dpae + ecart_heures + delai_placement_heures + categorie_delai.
    JOIN WTCNTI (min CNTI_DATEFFET = premier début contrat) + JOIN WTCMD (CMD_DTE).
    Anomalie CMD_ID : stocké float dans Evolia → TRY_CAST obligatoire.
    date_debut = MIN(CNTI_DATEFFET) — MISS_DATEDEBUT absent DDL, CNTI_DATEFFET est la source de vérité.
    """
    b = cfg.bucket_bronze
    dp = cfg.date_partition
    return f"""
    WITH wtcnti AS (
        SELECT PER_ID, CNT_ID, MIN(CNTI_DATEFFET) AS CNTI_DATEFFET
        FROM read_json_auto('s3://{b}/raw_wtcnti/{dp}/*.json',
                            union_by_name=true, hive_partitioning=false)
        GROUP BY PER_ID, CNT_ID
    ),
    wtcmd AS (
        SELECT cmd_id, CMD_DTE FROM (
            SELECT TRY_CAST(CMD_ID AS INT) AS cmd_id, CMD_DTE,
                   ROW_NUMBER() OVER (PARTITION BY CMD_ID ORDER BY _loaded_at DESC) AS rn
            FROM read_json_auto('s3://{b}/raw_wtcmd/{dp}/*.json',
                                union_by_name=true, hive_partitioning=false)
        ) WHERE rn = 1 AND cmd_id IS NOT NULL
    )
    SELECT
        CAST(src.PER_ID             AS INTEGER)       AS per_id,
        CAST(src.CNT_ID             AS INTEGER)       AS cnt_id,
        CAST(src.TIE_ID             AS INTEGER)       AS tie_id,
        CAST(src.TIES_SERV          AS INTEGER)       AS ties_serv,
        CAST(src.RGPCNT_ID          AS INTEGER)       AS rgpcnt_id,
        TRY_CAST(cnti.CNTI_DATEFFET AS DATE)          AS date_debut,
        CAST(src.MISS_SAISIE_DTFIN  AS DATE)          AS date_fin,
        NULL::VARCHAR                                 AS motif,
        TRIM(src.FINMISS_CODE)                        AS code_fin,
        NULL::INTEGER                                 AS prh_bts,
        TRY_CAST(src.MISS_FLAGDPAE  AS TIMESTAMP)     AS statut_dpae,
        CASE WHEN src.MISS_FLAGDPAE IS NOT NULL AND cnti.CNTI_DATEFFET IS NOT NULL
             THEN DATEDIFF('hour',
                  TRY_CAST(cnti.CNTI_DATEFFET AS TIMESTAMP),
                  TRY_CAST(src.MISS_FLAGDPAE  AS TIMESTAMP))
             ELSE NULL END                            AS ecart_heures,
        CASE WHEN cnti.CNTI_DATEFFET IS NOT NULL AND cmd.CMD_DTE IS NOT NULL
             THEN DATEDIFF('hour',
                  TRY_CAST(cmd.CMD_DTE         AS TIMESTAMP),
                  TRY_CAST(cnti.CNTI_DATEFFET  AS TIMESTAMP))
             ELSE NULL END                            AS delai_placement_heures,
        CASE
            WHEN cnti.CNTI_DATEFFET IS NULL OR cmd.CMD_DTE IS NULL THEN 'inconnu'
            WHEN DATEDIFF('hour',
                 TRY_CAST(cmd.CMD_DTE AS TIMESTAMP),
                 TRY_CAST(cnti.CNTI_DATEFFET AS TIMESTAMP)) <= 24  THEN 'urgent'
            WHEN DATEDIFF('hour',
                 TRY_CAST(cmd.CMD_DTE AS TIMESTAMP),
                 TRY_CAST(cnti.CNTI_DATEFFET AS TIMESTAMP)) <= 72  THEN 'court'
            WHEN DATEDIFF('hour',
                 TRY_CAST(cmd.CMD_DTE AS TIMESTAMP),
                 TRY_CAST(cnti.CNTI_DATEFFET AS TIMESTAMP)) <= 168 THEN 'standard'
            ELSE 'long'
        END                                           AS categorie_delai,
        src._batch_id,
        CAST(src._loaded_at AS TIMESTAMP)             AS _loaded_at
    FROM src
    LEFT JOIN wtcnti cnti ON cnti.PER_ID = src.PER_ID AND cnti.CNT_ID = src.CNT_ID
    LEFT JOIN wtcmd  cmd  ON cmd.cmd_id  = TRY_CAST(src.CMD_ID AS INT)
    WHERE src.PER_ID IS NOT NULL AND src.CNT_ID IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY src.PER_ID, src.CNT_ID ORDER BY src._loaded_at DESC
    ) = 1
    """


_TABLES: list[_Table] = [
    _Table(
        name="WTMISS",
        bronze="wtmiss",
        silver=f"slv_{DOMAIN}/missions",
        dedup_key="PER_ID, CNT_ID",
        sql_fn=_build_wtmiss_sql,
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
        # TIE_ID/MET_ID absents DDL — retirés (bronze_missions v2 + state card 2026-03-11)
        # STAT_CODE/STAT_TYPE ajoutés (bronze_missions v2 confirmés)
        sql="""
            SELECT
                CAST(CMD_ID         AS INTEGER)     AS cmd_id,
                CAST(RGPCNT_ID      AS INTEGER)     AS rgpcnt_id,
                CAST(CMD_DTE        AS DATE)        AS cmd_date,
                CAST(CMD_NBSALS     AS INTEGER)     AS nb_sal,
                TRIM(COALESCE(STAT_CODE, ''))       AS stat_code,
                TRIM(COALESCE(STAT_TYPE, ''))       AS stat_type,
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
    # PYCONTRAT (2026-03-11) — table maîtresse contrats paie
    # Jointures Gold : PER_ID+CNT_ID ↔ WTMISS/WTCNTI/WTPRH · ETA_ID ↔ PYETABLISSEMENT
    _Table(
        name="PYCONTRAT",
        bronze="pycontrat",
        silver=f"slv_{DOMAIN}/contrats_paie",
        dedup_key="PER_ID, CNT_ID",
        sql="""
            SELECT
                CAST(PER_ID         AS INTEGER)     AS per_id,
                CAST(CNT_ID         AS INTEGER)     AS cnt_id,
                CAST(ETA_ID         AS INTEGER)     AS eta_id,
                CAST(RGPCNT_ID      AS INTEGER)     AS rgpcnt_id,
                CAST(CNT_DATEDEB    AS DATE)        AS date_debut,
                CAST(CNT_DATEFIN    AS DATE)        AS date_fin,
                CAST(CNT_FINPREVU   AS DATE)        AS date_fin_prevue,
                TRIM(LOTPAYE_CODE)                  AS lot_paye_code,
                TRIM(TYPCOT_CODE)                   AS typ_cotisation_code,
                CAST(CNT_AVT_ORDRE  AS INTEGER)     AS avt_ordre,
                CAST(CNT_INI_ORDRE  AS INTEGER)     AS ini_ordre,
                _batch_id,
                CAST(_loaded_at     AS TIMESTAMP)   AS _loaded_at
            FROM src
            WHERE PER_ID IS NOT NULL AND CNT_ID IS NOT NULL
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY PER_ID, CNT_ID ORDER BY _loaded_at DESC
            ) = 1
        """,
    ),
]


def _process(ddb, cfg: Config, t: _Table, stats: Stats) -> None:
    bronze_path = f"s3://{cfg.bucket_bronze}/raw_{t.bronze}/{cfg.date_partition}/*.json"
    silver_path = f"s3://{cfg.bucket_silver}/{t.silver}/**/*.parquet"
    query = t.sql_fn(cfg) if t.sql_fn is not None else t.sql
    try:
        ddb.execute(
            f"CREATE OR REPLACE VIEW src AS SELECT * FROM read_json_auto('{bronze_path}')")
        if cfg.mode in (RunMode.OFFLINE, RunMode.PROBE):
            row = ddb.execute(f"SELECT COUNT(*) FROM ({query})").fetchone()
            count = row[0] if row else 0
            logger.info(json.dumps(
                {"mode": cfg.mode.value, "table": t.name, "rows": count}))
            stats.tables_processed += 1
            return
        ddb.execute(
            f"COPY ({query}) TO '{silver_path}' "
            f"(FORMAT PARQUET, COMPRESSION ZSTD, OVERWRITE_OR_IGNORE TRUE)")
        row = ddb.execute(f"SELECT COUNT(*) FROM read_parquet('{silver_path}')").fetchone()
        count = row[0] if row else 0
        stats.tables_processed += 1
        stats.rows_ingested += count
        logger.info(json.dumps({"table": t.name, "rows": count}))
    except Exception as e:
        logger.exception(json.dumps({"table": t.name, "error": str(e)}))
        stats.errors.append({"table": t.name, "error": str(e)})


def run(cfg: Config) -> dict:
    stats = Stats()
    with get_duckdb_connection(cfg) as ddb:
        for t in filter_tables(_TABLES, cfg):
            _process(ddb, cfg, t, stats)
    return stats.finish()


if __name__ == "__main__":
    run(Config())
