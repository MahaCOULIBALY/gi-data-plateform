"""bronze_missions.py — Bronze · Missions & Contrats : 9 tables → S3.
Phase 0 · GI Data Lakehouse · Manifeste v2.0
# CORRECTIONS DDL (probe 2026-03-05) :
#   WTFACINFO  : suppression EFAC_NUM (absent DDL) + RGPCNT_ID (absent DDL)
#              + FACINFO_DATEMODIF absent → full-load
#   WTCMD      : CMD_DATE→CMD_DTE (extra DDL) + full-load (CMD_DATEMODIF absent)
#   WTPLAC     : PLAC_DATE→PLAC_DTEEDI (extra DDL) + full-load (PLAC_DATEMODIF absent)
#   WTLFAC     : LFAC_LIBELLE→LFAC_LIB (extra DDL) + full-load (pas de delta col)
"""
import json
import time
from datetime import datetime, timezone

import pyodbc

from shared import (
    Config, Stats, TableConfig, RunMode,
    generate_batch_id, today_s3_prefix,
    get_evolia_connection, get_pg_connection, upload_to_s3, logger,
)
from pipeline_utils import WatermarkStore, with_retry

PIPELINE = "bronze_missions"
FALLBACK_SINCE = datetime(2023, 1, 1, tzinfo=timezone.utc)

# Tables avec colonne delta confirmée DDL
TABLES_DELTA: list[TableConfig] = [
    TableConfig("WTMISS", "CNTI_CREATE", ["PER_ID", "CNT_ID"]),
    TableConfig("WTCNTI", "CNTI_CREATE", ["PER_ID", "CNT_ID", "CNTI_ORDRE"]),
    TableConfig("WTEFAC", "EFAC_DTEEDI", ["EFAC_NUM"]),
    TableConfig("WTPRH", "PRH_MODIFDATE", ["PRH_BTS"]),
    TableConfig("WTRHDON", "RHD_DATED", ["RINT_ID", "RHD_LIGNE"]),
]

# Tables sans delta col DDL → full-load à chaque run
TABLES_FULL: list[TableConfig] = [
    TableConfig("WTCMD", "", ["CMD_ID"]),       # CMD_DATEMODIF absent DDL
    TableConfig("WTPLAC", "", ["PLAC_ID"]),      # PLAC_DATEMODIF absent DDL
    TableConfig("WTLFAC", "", ["FAC_NUM", "LFAC_ORD"]),  # pas de delta col
    # FACINFO_DATEMODIF absent DDL
    TableConfig("WTFACINFO", "", ["CNT_ID", "FAC_NUM"]),
]

_COLS: dict[str, str] = {
    "WTMISS": (
        "PER_ID,CNT_ID,TIE_ID,MISS_TIEID,TIES_SERV,MISS_CODE,MISS_JUSTIFICATION,"
        "MISS_DPAE,MISS_ETRANGER,MISS_QUAL,MISS_PERFERM,RGPCNT_ID,MISS_NDPAE,"
        "CNTI_CREATE,FINMISS_CODE,MISS_SAISIE_DTFIN,MISS_TRANSDATE,MISS_MODIFDATE,"
        "MISS_FLAGDPAE,MISS_BTP,MISS_TYPCOEF,MISS_LOGIN"
    ),
    "WTCNTI": (
        "PER_ID,CNT_ID,CNTI_ORDRE,TIE_ID,MET_ID,CNTI_CREATE,CNTI_DATEFFET,"
        "CNTI_DATEFINCNTI,CNTI_SOUPDEB,CNTI_SOUPFIN,CNTI_HPART,CNTI_RETCT,"
        "CNTI_RETINT,CNTI_THPAYE,CNTI_THFACT,CNTI_SOUPMODIF,CNTI_POSTE,"
        "LOTFAC_CODE,CNTI_SALREF1,CNTI_SALREF2,CNTI_SALREF3,CNTI_SALREF4,"
        "CNTI_DESCRIPT1,CNTI_DESCRIPT2,CNTI_PROTEC1,CNTI_PROTEC2,CNTI_DURHEBDO,PCS_CODE_2003"
    ),
    "WTEFAC": (
        "EFAC_NUM,EFAC_LIB,RGPCNT_ID,TIE_ID,TIES_SERV,EFAC_IDRUPT,EFAC_RUPTURE,"
        "DEV_CODE,MRG_CODE,EFAC_DTEEDI,EFAC_DTEECH,EFAC_TYPF,EFAC_TYPG,"
        "WTE_EFAC_NUM,EFAC_MATR,EFAC_VERROU,EFAC_TRANS,EFAC_TRANS_FACTO,"
        "EFAC_ORDNUM,EFAC_USER,JSTFAV_ID,EFAC_JSTFCOMM,TVA_CODE,EFAC_DTEREGLF,"
        "EFAC_TAUXTVA,EFAC_PLAC_ID"
    ),
    "WTPRH": (
        "PRH_BTS,PER_ID,CNT_ID,TIE_ID,PRH_DTEDEBSEM,LOTPAYE_CODE,CAL_AN,"
        "CAL_NPERIODE,LOTFAC_CODE,CALF_AN,CALF_NPERIODE,CNTI_ORDRE,"
        "PRH_DTEFINSEM,VENTHEU_DTEDEB,PRH_IFM,PRH_CP,PRH_FLAG_RH,PRH_MODIFDATE"
    ),
    "WTRHDON": (
        "RINT_ID,RHD_LIGNE,RHD_BASEP,RHD_TAUXP,RHD_BASEF,RHD_TAUXF,PRH_BTS,"
        "FAC_NUM,BUL_ID,RHD_RAPPEL,RHD_ORIRUB,RHD_PORTEE,RHD_LIBRUB,RHD_EXCLDEP,"
        "RHD_SEUILP,RHD_SEUILF,RHD_BASEPROV,RHD_TAUXPROV,RHD_DATED,RHD_DATEF"
    ),
    # Tables full-load — noms DDL confirmés par probe
    "WTCMD": "CMD_ID,RGPCNT_ID,TIE_ID,MET_ID,CMD_DTE,CMD_NBSALS,CMD_CODE",
    "WTPLAC": "PLAC_ID,RGPCNT_ID,TIE_ID,MET_ID,PLAC_DTEEDI",
    "WTLFAC": "FAC_NUM,LFAC_ORD,LFAC_LIB,LFAC_BASE,LFAC_TAUX,LFAC_MNT",
    # WTFACINFO : EFAC_NUM et RGPCNT_ID absents DDL — colonnes DDL confirmées seulement
    "WTFACINFO": "CNT_ID,FAC_NUM,PER_ID,TIE_ID",
}


def extract_delta(conn: pyodbc.Connection, tc: TableConfig, since: datetime) -> list[dict]:
    since_str = since.strftime("%Y-%m-%d %H:%M:%S")
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT {_COLS[tc.name]} FROM {tc.name} WHERE {tc.delta_col} >= ?", since_str)
        h = [d[0] for d in cur.description]
        return [dict(zip(h, row)) for row in cur.fetchall()]


def extract_full(conn: pyodbc.Connection, tc: TableConfig) -> list[dict]:
    with conn.cursor() as cur:
        cur.execute(f"SELECT {_COLS[tc.name]} FROM {tc.name}")
        h = [d[0] for d in cur.description]
        return [dict(zip(h, row)) for row in cur.fetchall()]


@with_retry(max_attempts=3, base_delay=2.0, backoff=2.0)
def ingest(cfg, conn, tc, batch_id, since, stats):
    t0 = time.monotonic()
    rows = extract_delta(conn, tc, since) if since else extract_full(conn, tc)
    if not rows:
        logger.info(json.dumps(
            {"table": tc.name, "rows": 0, "status": "empty"}))
        return
    enriched = [{"_loaded_at": datetime.now(timezone.utc).isoformat(),
                 "_batch_id": batch_id, "_source_table": tc.name, **r} for r in rows]
    key = f"raw_{tc.name.lower()}/{today_s3_prefix()}/batch_{batch_id}.json"
    upload_to_s3(cfg, enriched, cfg.bucket_bronze, key, stats)
    stats.tables_processed += 1
    stats.rows_ingested += len(rows)
    logger.info(json.dumps({"table": tc.name, "rows": len(rows),
                            "mode": "delta" if since else "full",
                            "duration_s": round(time.monotonic() - t0, 2)}))


def run(cfg: Config) -> dict:
    stats = Stats()
    batch_id = generate_batch_id()
    if cfg.mode == RunMode.OFFLINE:
        for tc in TABLES_DELTA + TABLES_FULL:
            logger.info(json.dumps({"mode": "offline", "table": tc.name}))
        return stats.finish()

    with get_pg_connection(cfg) as pg_conn:
        wm = WatermarkStore(pg_conn, PIPELINE)
        with get_evolia_connection(cfg) as conn:
            for tc in TABLES_DELTA:
                since = wm.get(tc.name) or FALLBACK_SINCE
                if cfg.mode == RunMode.PROBE:
                    with conn.cursor() as cur:
                        cur.execute(f"SELECT COUNT(*) FROM {tc.name} WHERE {tc.delta_col} >= ?",
                                    since.strftime("%Y-%m-%d %H:%M:%S"))
                        logger.info(json.dumps({"mode": "probe", "table": tc.name,
                                                "count": cur.fetchone()[0]}))
                    stats.tables_processed += 1
                    continue
                try:
                    ingest(cfg, conn, tc, batch_id, since, stats)
                    wm.set(tc.name, datetime.now(timezone.utc), stats)
                except Exception as e:
                    logger.exception(json.dumps(
                        {"table": tc.name, "error": str(e)}))
                    wm.mark_failed(tc.name, str(e))
                    stats.errors.append({"table": tc.name, "error": str(e)})
            if cfg.mode != RunMode.PROBE:
                for tc in TABLES_FULL:
                    try:
                        ingest(cfg, conn, tc, batch_id, None, stats)
                        wm.set(tc.name, datetime.now(timezone.utc), stats)
                    except Exception as e:
                        logger.exception(json.dumps(
                            {"table": tc.name, "error": str(e)}))
                        wm.mark_failed(tc.name, str(e))
                        stats.errors.append(
                            {"table": tc.name, "error": str(e)})
    return stats.finish()


if __name__ == "__main__":
    run(Config())
