"""bronze_agences.py — Bronze · Structure organisationnelle Evolia → S3.
Tables : PYREGROUPCNT, PYENTREPRISE, PYETABLISSEMENT, WTUG, PYOSPETA.
Phase 0 · GI Data Lakehouse · Manifeste v2.0
# CORRECTIONS DDL (probe 2026-03-05) :
#   PYETABLISSEMENT : ETA_SIRET/ETA_DATEMODIF absent → FULL-LOAD, ETA_ADR2_COMP (extra)
#   WTUG (ajouté)   : UG_CLOTURE→UG_CLOTURE_DATE, UG_PILOTE→PIL_ID, UG_GPS_LAT/LNG→UG_GPS,
#                     UG_NOM/UG_DATEMODIF absent → FULL-LOAD. Nom agence via PYREGROUPCNT.
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

PIPELINE = "bronze_agences"
FALLBACK_SINCE = datetime(2020, 1, 1, tzinfo=timezone.utc)

TABLES_DELTA: list[TableConfig] = [
    TableConfig("PYREGROUPCNT", "RGPCNT_DATEMODIF", ["RGPCNT_ID"]),
    TableConfig("PYENTREPRISE", "ENT_DATEMODIF", ["ENT_ID"]),
]

# PYETABLISSEMENT : ETA_DATEMODIF absent DDL → full-load
# WTUG : UG_DATEMODIF absent DDL → full-load
TABLES_FULL: list[TableConfig] = [
    TableConfig("PYETABLISSEMENT", "", ["ETA_ID"]),
    TableConfig("WTUG", "", ["RGPCNT_ID"]),
    TableConfig("PYOSPETA", "", ["RGPCNT_ID", "ETA_ID"]),
]

_COLS: dict[str, str] = {
    "PYREGROUPCNT": (
        "RGPCNT_ID,RGPCNT_LIBELLE,RGPCNT_CODE,RGPCNT_ADR1,RGPCNT_ADR2,"
        "RGPCNT_CODPOS,RGPCNT_VILLE,RGPCNT_EMAIL,RGPCNT_GPS_LAT,RGPCNT_GPS_LON,"
        "RGPCNT_ACTIF,RGPCNT_DATEMODIF"
    ),
    "PYENTREPRISE": (
        "ENT_ID,ENT_RAISON,ENT_SIREN,ENT_ADR1,ENT_ADR2,ENT_CODPOS,ENT_VILLE,ENT_DATEMODIF"
    ),
    # ETA_SIRET absent DDL → supprimé. ETA_ADR2_COMP extra → ajouté.
    "PYETABLISSEMENT": (
        "ETA_ID,ENT_ID,ETA_NIC,ETA_ADR1,ETA_ADR2,ETA_ADR2_COMP,ETA_CODPOS,ETA_VILLE,"
        "ETA_NAF,ETA_COMMUNE,ETA_ACTIVITE"
    ),
    # UG_NOM absent → nom vient de PYREGROUPCNT.RGPCNT_LIBELLE (join Silver)
    # UG_CLOTURE→UG_CLOTURE_DATE, UG_PILOTE→PIL_ID, UG_GPS_LAT/LNG→UG_GPS (champ combiné)
    "WTUG": "RGPCNT_ID,UG_GPS,UG_CLOTURE_DATE,UG_CLOTURE_USER,PIL_ID",
    "PYOSPETA": "RGPCNT_ID,ETA_ID",
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
