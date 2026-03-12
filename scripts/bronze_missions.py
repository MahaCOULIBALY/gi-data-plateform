"""bronze_missions.py — Bronze · Missions & Contrats : 10 tables → S3.
Phase 0 · GI Data Lakehouse · Manifeste v2.0
# CORRECTIONS DDL (probe 2026-03-05) :
#   WTFACINFO  : suppression EFAC_NUM (absent DDL) + RGPCNT_ID (absent DDL)
#              + FACINFO_DATEMODIF absent → full-load
#   WTCMD      : CMD_DATE→CMD_DTE (extra DDL) + full-load (CMD_DATEMODIF absent)
#   WTPLAC     : PLAC_DATE→PLAC_DTEEDI (extra DDL) + full-load (PLAC_DATEMODIF absent)
#   WTLFAC     : LFAC_LIBELLE→LFAC_LIB (extra DDL) + full-load (pas de delta col)
# AJOUT (2026-03-11) :
#   PYCONTRAT  : PK=PER_ID+CNT_ID, delta_col=CNT_DATEFIN (datetime2)
#                allow_null_delta=True → contrats actifs (CNT_DATEFIN IS NULL) toujours capturés
#                Colonnes Gold : PER_ID,CNT_ID,ETA_ID,RGPCNT_ID,CNT_DATEDEB/FIN/FINPREVU,
#                LOTPAYE_CODE,TYPCOT_CODE,CNT_AVT_ORDRE,CNT_INI_ORDRE
"""
import json
import time
from datetime import datetime, timezone

import pyodbc

from shared import (
    Config, Stats, TableConfig, RunMode, _CHUNK_SIZE,
    generate_batch_id, today_s3_prefix,
    get_evolia_connection, get_pg_connection, upload_to_s3, logger,
    filter_tables,
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
    # RHD_DATED = date métier (début rubrique), pas de DATEMODIF en DDL → full-load (probe 2026-03-12)
    # allow_null_delta=True : contrats actifs ont CNT_DATEFIN IS NULL → capturés à chaque run
    TableConfig("PYCONTRAT", "CNT_DATEFIN", ["PER_ID", "CNT_ID"], allow_null_delta=True),
]

# Tables sans delta col DDL → full-load à chaque run
TABLES_FULL: list[TableConfig] = [
    TableConfig("WTCMD", "", ["CMD_ID"]),
    TableConfig("WTPLAC", "", ["PLAC_ID"]),
    TableConfig("WTLFAC", "", ["FAC_NUM", "LFAC_ORD"]),
    # FACINFO_DATEMODIF absent DDL
    TableConfig("WTFACINFO", "", ["CNT_ID", "FAC_NUM"]),
    # RHD_DATED = date métier, pas DATEMODIF — full-load (reclassé depuis TABLES_DELTA 2026-03-12)
    TableConfig("WTRHDON", "", ["RINT_ID", "RHD_LIGNE"]),
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
    # TIE_ID/MET_ID absents DDL (probe 2026-03-10). STAT_CODE+STAT_TYPE ajoutés → résout DT-11
    "WTCMD": "CMD_ID,RGPCNT_ID,CMD_DTE,CMD_NBSALS,CMD_CODE,STAT_CODE,STAT_TYPE",
    "WTPLAC": "PLAC_ID,RGPCNT_ID,TIE_ID,MET_ID,PLAC_DTEEDI",
    "WTLFAC": "FAC_NUM,LFAC_ORD,LFAC_LIB,LFAC_BASE,LFAC_TAUX,LFAC_MNT",
    # WTFACINFO : EFAC_NUM et RGPCNT_ID absents DDL — colonnes DDL confirmées seulement
    "WTFACINFO": "CNT_ID,FAC_NUM,PER_ID,TIE_ID",
    # PYCONTRAT : table maîtresse contrats paie — colonnes Gold utiles (DDL confirmé 2026-03-11)
    "PYCONTRAT": (
        "PER_ID,CNT_ID,ETA_ID,RGPCNT_ID,CNT_DATEDEB,CNT_DATEFIN,CNT_FINPREVU,"
        "LOTPAYE_CODE,TYPCOT_CODE,CNT_AVT_ORDRE,CNT_INI_ORDRE"
    ),
}


def _extract_delta(conn: pyodbc.Connection, tc: TableConfig, since: datetime) -> list[dict]:
    since_str = since.strftime("%Y-%m-%d %H:%M:%S")
    # allow_null_delta : inclut les lignes où delta_col IS NULL (ex: contrats actifs sans date fin)
    null_clause = f" OR {tc.delta_col} IS NULL" if tc.allow_null_delta else ""
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT {_COLS[tc.name]} FROM {tc.name} "
            f"WHERE {tc.delta_col} >= ?{null_clause}", since_str)
        h = [d[0] for d in cur.description]
        return [dict(zip(h, row)) for row in cur.fetchall()]


def _extract_full(conn: pyodbc.Connection, tc: TableConfig) -> list[dict]:
    with conn.cursor() as cur:
        cur.execute(f"SELECT {_COLS[tc.name]} FROM {tc.name}")
        h = [d[0] for d in cur.description]
        return [dict(zip(h, row)) for row in cur.fetchall()]


@with_retry(max_attempts=3, base_delay=2.0, backoff=2.0)
def _ingest(cfg: Config, conn: pyodbc.Connection, tc: TableConfig,
            batch_id: str, since: datetime | None, stats: Stats) -> int:
    """Extrait, enrichit et charge une table vers S3 Bronze. Retourne le nombre de lignes ingérées."""
    t0 = time.monotonic()
    rows = _extract_delta(conn, tc, since) if since else _extract_full(conn, tc)
    if not rows:
        logger.info(json.dumps({"table": tc.name, "rows": 0, "status": "empty"}))
        return 0
    loaded_at = datetime.now(timezone.utc).isoformat()
    enriched = [{"_loaded_at": loaded_at, "_batch_id": batch_id,
                 "_source_table": tc.name, **r} for r in rows]
    # Chunking : 1 fichier S3 par tranche de _CHUNK_SIZE lignes (évite EntityTooLarge OVH)
    chunks = [enriched[i:i + _CHUNK_SIZE] for i in range(0, len(enriched), _CHUNK_SIZE)]
    prefix = f"raw_{tc.name.lower()}/{today_s3_prefix()}"
    for idx, chunk in enumerate(chunks):
        key = f"{prefix}/batch_{batch_id}_{idx:04d}.json"
        upload_to_s3(cfg, chunk, cfg.bucket_bronze, key, stats)
    stats.tables_processed += 1
    stats.rows_ingested += len(rows)
    logger.info(json.dumps({"table": tc.name, "rows": len(rows), "chunks": len(chunks),
                            "mode": "delta" if since else "full",
                            "duration_s": round(time.monotonic() - t0, 2)}))
    return len(rows)


def run(cfg: Config) -> dict:
    stats = Stats()
    batch_id = generate_batch_id()
    if cfg.mode == RunMode.OFFLINE:
        for tc in filter_tables(TABLES_DELTA + TABLES_FULL, cfg):
            logger.info(json.dumps({"mode": "offline", "table": tc.name}))
        return stats.finish()

    with get_pg_connection(cfg) as pg_conn:
        wm = WatermarkStore(pg_conn, PIPELINE)
        with get_evolia_connection(cfg) as conn:
            for tc in filter_tables(TABLES_DELTA, cfg):
                since = wm.get(tc.name) or FALLBACK_SINCE
                if cfg.mode == RunMode.PROBE:
                    with conn.cursor() as cur:
                        null_clause = f" OR {tc.delta_col} IS NULL" if tc.allow_null_delta else ""
                        cur.execute(
                            f"SELECT COUNT(*) FROM {tc.name} "
                            f"WHERE {tc.delta_col} >= ?{null_clause}",
                            since.strftime("%Y-%m-%d %H:%M:%S"))
                        row = cur.fetchone()
                        logger.info(json.dumps({"mode": "probe", "table": tc.name,
                                                "count": row[0] if row else 0}))
                    stats.tables_processed += 1
                    continue
                try:
                    n = _ingest(cfg, conn, tc, batch_id, since, stats) or 0
                    wm.set(tc.name, datetime.now(timezone.utc), n)
                except Exception as e:
                    logger.exception(json.dumps({"table": tc.name, "error": str(e)}))
                    wm.mark_failed(tc.name, str(e))
                    stats.errors.append({"table": tc.name, "error": str(e)})
            for tc in filter_tables(TABLES_FULL, cfg):
                if cfg.mode == RunMode.PROBE:
                    with conn.cursor() as cur:
                        cur.execute(f"SELECT COUNT(*) FROM {tc.name}")
                        row = cur.fetchone()
                        logger.info(json.dumps({"mode": "probe", "table": tc.name,
                                                "count": row[0] if row else 0, "load": "full"}))
                    stats.tables_processed += 1
                    continue
                try:
                    n = _ingest(cfg, conn, tc, batch_id, None, stats) or 0
                    wm.set(tc.name, datetime.now(timezone.utc), n)
                except Exception as e:
                    logger.exception(json.dumps({"table": tc.name, "error": str(e)}))
                    wm.mark_failed(tc.name, str(e))
                    stats.errors.append({"table": tc.name, "error": str(e)})
    return stats.finish()


if __name__ == "__main__":
    run(Config())
