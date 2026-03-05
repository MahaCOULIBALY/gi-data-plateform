"""bronze_clients.py — Bronze · 8 tables clients Evolia → S3.
Phase 1 · GI Data Lakehouse · Manifeste v2.0
# CORRECTIONS DDL (probe 2026-03-05) :
#   WTTIESERV  : TIES_DATEMODIF/SIRET/NAF/NUMVOIE/RS absent → FULL-LOAD
#                TIES_RAISOC/ADR1/ADR2/CODPOS/VILLE confirmés
#   WTCLPT     : CLPT_DATEMODIF/ACTIF/EFFECTIF absent → FULL-LOAD
#                CLPT_PROSPEC/CAESTIME/DATCREA toujours utilisés (non réfutés)
#   WTTIEINT   : TIEINT_* → TIEI_*, ORDRE→TIEI_ORDRE, TIEINT_DATEMODIF absent → FULL-LOAD
#   WTCOEF     : TQUA→TQUA_ID, COEF_DATEDEB→COEF_DEFF, COEF_DATEFIN→COEF_DFIN,
#                COEF_DATEMODIF absent → FULL-LOAD. RGPCNT/TIE confirmés.
#   WTENCOURSG : SIREN→ENC_SIREN, tous ENCGRP_* sauf ENCGRP_ID absent → FULL-LOAD
#   WTUGCLI    : UGCLI_DATE/MONTANT absent, UGCLI_ORIG (extra) → FULL-LOAD
#   WTUGAG     : UGAG_MONTANT→UGAG_MT, UGAG_DATE/DATEMODIF absent → FULL-LOAD
#   CMTIER     : delta col TIE_DATEMODIF maintenu (non réfuté par probe)
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

PIPELINE = "bronze_clients"
FALLBACK_SINCE = datetime(2023, 1, 1, tzinfo=timezone.utc)

# CMTIER : delta col non réfutée par probe → maintien delta
TABLES_DELTA: list[TableConfig] = [
    TableConfig("CMTIER", "TIE_DATEMODIF", ["TIE_ID"]),
]

# Toutes autres : aucune colonne delta DDL confirmée → full-load
TABLES_FULL: list[TableConfig] = [
    TableConfig("WTTIESERV", "", ["TIE_ID", "TIES_SERV"]),
    TableConfig("WTCLPT", "", ["TIE_ID"]),
    TableConfig("WTTIEINT", "", ["TIE_ID", "TIEI_ORDRE"]),
    TableConfig("WTCOEF", "", ["TQUA_ID", "RGPCNT", "TIE"]),
    TableConfig("WTENCOURSG", "", ["ENCGRP_ID"]),
    TableConfig("WTUGCLI", "", ["RGPCNT_ID", "TIE_ID"]),
    TableConfig("WTUGAG", "", ["RGPCNT_ID", "TIE_ID"]),
]

_COLS: dict[str, str] = {
    "CMTIER": "TIE_ID,TIE_NOM,TIE_MATR,TIE_RAISOC,TIE_DATEMODIF",
    # TIES_SIRET/TIES_NAF/TIES_NUMVOIE/TIES_TYPVOIE/TIES_VOIE/TIES_RS absent DDL
    "WTTIESERV": "TIE_ID,TIES_SERV,TIES_RAISOC,TIES_ADR1,TIES_ADR2,TIES_CODPOS,TIES_VILLE",
    # CLPT_EFFECTIF/CLPT_ACTIF/CLPT_DATEMODIF absent DDL — CLPT_PROSPEC/CAESTIME/DATCREA non réfutés
    "WTCLPT": "TIE_ID,CLPT_PROSPEC,CLPT_CAESTIME,CLPT_DATCREA",
    # TIEINT_* → TIEI_*, ORDRE→TIEI_ORDRE
    "WTTIEINT": "TIE_ID,TIEI_ORDRE,TIEI_NOM,TIEI_PRENOM,TIEI_EMAIL,TIEI_BUREAU,FCTI_CODE",
    # TQUA→TQUA_ID, COEF_DATEDEB→COEF_DEFF, COEF_DATEFIN→COEF_DFIN
    "WTCOEF": "TQUA_ID,RGPCNT,TIE,COEF_VAL,COEF_DEFF,COEF_DFIN",
    # SIREN→ENC_SIREN, ENCGRP_LIMITE/DECISION/MONTANT/STATUT absent DDL
    "WTENCOURSG": "ENCGRP_ID,ENC_SIREN,ENCG_DECISIONLIB",
    "WTUGCLI": "RGPCNT_ID,TIE_ID,UGCLI_ORIG",
    # UGAG_MONTANT→UGAG_MT
    "WTUGAG": "RGPCNT_ID,TIE_ID,UGAG_MT",
}


def _extract_delta(conn: pyodbc.Connection, tc: TableConfig, since: datetime) -> list[dict]:
    since_str = since.strftime("%Y-%m-%d %H:%M:%S")
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT {_COLS[tc.name]} FROM {tc.name} WHERE {tc.delta_col} >= ?", since_str)
        h = [d[0] for d in cur.description]
        return [dict(zip(h, row)) for row in cur.fetchall()]


def _extract_full(conn: pyodbc.Connection, tc: TableConfig) -> list[dict]:
    with conn.cursor() as cur:
        cur.execute(f"SELECT {_COLS[tc.name]} FROM {tc.name}")
        h = [d[0] for d in cur.description]
        return [dict(zip(h, row)) for row in cur.fetchall()]


@with_retry(max_attempts=3, base_delay=2.0, backoff=2.0)
def _ingest(cfg: Config, conn: pyodbc.Connection, tc: TableConfig,
            batch_id: str, since: datetime | None, stats: Stats) -> None:
    t0 = time.monotonic()
    rows = _extract_delta(
        conn, tc, since) if since else _extract_full(conn, tc)
    if not rows:
        logger.info(json.dumps(
            {"table": tc.name, "rows": 0, "status": "empty"}))
        return
    enriched = [
        {"_loaded_at": datetime.now(timezone.utc).isoformat(),
         "_batch_id": batch_id, "_source_table": tc.name, **r}
        for r in rows
    ]
    key = f"raw_{tc.name.lower()}/{today_s3_prefix()}/batch_{batch_id}.json"
    upload_to_s3(cfg, enriched, cfg.bucket_bronze, key, stats)
    stats.tables_processed += 1
    stats.rows_ingested += len(rows)
    logger.info(json.dumps({
        "table": tc.name, "rows": len(rows),
        "mode": "full" if not since else "delta",
        "duration_s": round(time.monotonic() - t0, 2),
    }))


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
                        cur.execute(
                            f"SELECT COUNT(*) FROM {tc.name} WHERE {tc.delta_col} >= ?",
                            since.strftime("%Y-%m-%d %H:%M:%S"))
                        logger.info(json.dumps({
                            "mode": "probe", "table": tc.name,
                            "count": cur.fetchone()[0]}))
                    stats.tables_processed += 1
                    continue
                try:
                    _ingest(cfg, conn, tc, batch_id, since, stats)
                    wm.set(tc.name, datetime.now(timezone.utc), stats)
                except Exception as e:
                    logger.exception(json.dumps(
                        {"table": tc.name, "error": str(e)}))
                    wm.mark_failed(tc.name, str(e))
                    stats.errors.append({"table": tc.name, "error": str(e)})

            if cfg.mode != RunMode.PROBE:
                for tc in TABLES_FULL:
                    try:
                        _ingest(cfg, conn, tc, batch_id, None, stats)
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
