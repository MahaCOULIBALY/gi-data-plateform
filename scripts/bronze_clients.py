"""bronze_clients.py — Bronze · 8 tables clients Evolia → S3.
Phase 1 · GI Data Lakehouse · Manifeste v2.0
# CORRECTIONS DDL (probe 2026-03-05) :
# WTTIESERV : TIES_DATEMODIF/SIRET/NAF/NUMVOIE/RS absent → FULL-LOAD
#             TIES_RAISOC/ADR1/ADR2/CODPOS/VILLE confirmés
# WTCLPT    : CLPT_DATEMODIF/ACTIF/EFFECTIF absent → FULL-LOAD
#             CLPT_PROSPEC/CAESTIME/DATCREA toujours utilisés (non réfutés)
# WTTIEINT  : TIEINT_* → TIEI_*, ORDRE→TIEI_ORDRE, TIEINT_DATEMODIF absent → FULL-LOAD
# WTCOEF    : TQUA→TQUA_ID, COEF_DATEDEB→COEF_DEFF, COEF_DATEFIN→COEF_DFIN,
#             COEF_DATEMODIF absent → FULL-LOAD. RGPCNT/TIE confirmés.
# WTENCOURSG: SIREN→ENC_SIREN, tous ENCGRP_* sauf ENCGRP_ID absent → FULL-LOAD
# WTUGCLI   : UGCLI_DATE/MONTANT absent, UGCLI_ORIG (extra) → FULL-LOAD
# WTUGAG    : UGAG_MONTANT→UGAG_MT, UGAG_DATE/DATEMODIF absent → FULL-LOAD
# CMTIER    : delta col TIE_DATEMODIF maintenu (non réfuté par probe)
# CORRECTIONS (2026-03-23) :
#   s3_delete_prefix centralisée depuis shared.py — purge avant écriture FULL généralisée
"""
import json
import time
from datetime import datetime, timezone

from shared import (
    Config, Stats, TableConfig, RunMode, _CHUNK_SIZE,
    generate_batch_id, today_s3_prefix,
    get_evolia_connection, get_pg_connection, upload_to_s3, s3_delete_prefix, logger,
    filter_tables,
)
from pipeline_utils import WatermarkStore, with_retry

PIPELINE = "bronze_clients"
FALLBACK_SINCE = datetime(2023, 1, 1, tzinfo=timezone.utc)

# CMTIER : delta col non réfutée par probe → maintien delta
# CMTIER absent DDL → table réelle = CMTIERS (TIE_DATEMODIF absent → full-load)
TABLES_DELTA: list[TableConfig] = []

# Toutes autres : aucune colonne delta DDL confirmée → full-load
TABLES_FULL: list[TableConfig] = [
    # CMTIER absent DDL → CMTIERS
    TableConfig("CMTIERS", "", ["TIE_ID"]),
    TableConfig("WTTIESERV", "", ["TIE_ID", "TIES_SERV"]),
    TableConfig("WTCLPT", "", ["TIE_ID"]),
    TableConfig("WTTIEINT", "", ["TIE_ID", "TIEI_ORDRE"]),
    # WTCOEF : table vide confirmée probe 2026-03-12 (count=0) — retirée
    TableConfig("WTENCOURSG", "", ["ENCGRP_ID"]),
    TableConfig("WTUGCLI", "", ["RGPCNT_ID", "TIE_ID"]),
    # WTUGAG : table vide confirmée probe 2026-03-12 (count=0) — retirée
]

_COLS: dict[str, str] = {
    # CMTIERS (avec S) : TIE_MATR/RAISOC/DATEMODIF absents DDL
    "CMTIERS": "TIE_ID,TIE_NOM,TIE_NOMC,TIE_SIRET,TIE_CODE",
    # TIES_RAISOC→TIES_DESIGNATION, TIES_CODPOS→TIES_CODEP
    # NIC ajouté (SIRET complet), EMAIL+PAYS_CODE+CLOT_DAT+CDMODIFDATE ajoutés (2026-03-11)
    # TIES_DATEMODIF absent DDL — TIES_CDMODIFDATE différent (modif côté commercial) → à confirmer probe
    "WTTIESERV": (
        "TIE_ID,TIES_SERV,TIES_DESIGNATION,TIES_NOM,TIES_ADR1,TIES_ADR2,"
        "TIES_CODEP,TIES_VILLE,PAYS_CODE,TIES_SIREN,TIES_NIC,NAF,NAF2008,"
        "TIES_EMAIL,CLOT_DAT,TIES_CDMODIFDATE,RGPCNT_ID"
    ),
    # CLPT_PROSPEC→CLPT_PROS, CLPT_CAESTIME→CLPT_CAPT, CLPT_DATCREA→CLPT_DCREA
    "WTCLPT": "TIE_ID,CLPT_PROS,CLPT_CAPT,CLPT_DCREA,CLPT_EFFT,CLPT_MODIFDATE",
    # TIEINT_* → TIEI_*, ORDRE→TIEI_ORDRE
    "WTTIEINT": "TIE_ID,TIEI_ORDRE,TIEI_NOM,TIEI_PRENOM,TIEI_EMAIL,TIEI_BUREAU,FCTI_CODE",
    # SIREN→ENC_SIREN, ENCGRP_LIMITE/DECISION/MONTANT/STATUT absent DDL
    "WTENCOURSG": "ENCGRP_ID,ENC_SIREN,ENCG_DECISIONLIB",
    "WTUGCLI": "RGPCNT_ID,TIE_ID,UGCLI_ORIG",
    # UGAG_MONTANT→UGAG_MT
    "WTUGAG": "RGPCNT_ID,TIE_ID,UGAG_MT",
}


def _extract_delta(conn, tc: TableConfig, since: datetime) -> list[dict]:
    since_str = since.strftime("%Y-%m-%d %H:%M:%S")
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT {_COLS[tc.name]} FROM {tc.name} WHERE {tc.delta_col} >= %s",
            since_str,
        )
        h = [d[0] for d in cur.description]
        return [dict(zip(h, row)) for row in cur.fetchall()]


def _extract_full(conn, tc: TableConfig) -> list[dict]:
    with conn.cursor() as cur:
        cur.execute(f"SELECT {_COLS[tc.name]} FROM {tc.name}")
        h = [d[0] for d in cur.description]
        return [dict(zip(h, row)) for row in cur.fetchall()]


@with_retry(max_attempts=3, base_delay=2.0, backoff=2.0)
def _ingest(
    cfg: Config, conn, tc: TableConfig,
    batch_id: str, since: datetime | None, stats: Stats,
) -> int:
    """Extrait, enrichit et charge une table vers S3 Bronze. Retourne le nombre de lignes ingérées."""
    t0 = time.monotonic()
    rows = _extract_delta(
        conn, tc, since) if since else _extract_full(conn, tc)
    if not rows:
        logger.info(json.dumps(
            {"table": tc.name, "rows": 0, "status": "empty"}))
        return 0
    loaded_at = datetime.now(timezone.utc).isoformat()
    enriched = [{"_loaded_at": loaded_at, "_batch_id": batch_id,
                 "_source_table": tc.name, **r} for r in rows]
    chunks = [enriched[i:i + _CHUNK_SIZE]
              for i in range(0, len(enriched), _CHUNK_SIZE)]
    prefix = f"raw_{tc.name.lower()}/{today_s3_prefix()}"

    # Purge avant écriture — tables FULL uniquement (since is None)
    # s3_delete_prefix gère le guard OFFLINE/PROBE : no-op hors mode LIVE
    if since is None:
        s3_delete_prefix(cfg, cfg.bucket_bronze, prefix)

    for idx, chunk in enumerate(chunks):
        key = f"{prefix}/batch_{batch_id}_{idx:04d}.json"
        upload_to_s3(cfg, chunk, cfg.bucket_bronze, key, stats)
    stats.tables_processed += 1
    stats.rows_ingested += len(rows)
    logger.info(json.dumps({
        "table": tc.name,
        "rows": len(rows),
        "chunks": len(chunks),
        "mode": "full" if not since else "delta",
        "duration_s": round(time.monotonic() - t0, 2),
    }))
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
                        cur.execute(
                            f"SELECT COUNT(*) FROM {tc.name} WHERE {tc.delta_col} >= %s",
                            since.strftime("%Y-%m-%d %H:%M:%S"),
                        )
                        row = cur.fetchone()
                        logger.info(json.dumps({
                            "mode": "probe",
                            "table": tc.name,
                            "count": row[0] if row else 0,
                        }))
                    stats.tables_processed += 1
                    continue
                try:
                    n = _ingest(cfg, conn, tc, batch_id, since, stats) or 0
                    wm.set(tc.name, datetime.now(timezone.utc), n)
                except Exception as e:
                    logger.exception(json.dumps(
                        {"table": tc.name, "error": str(e)}))
                    wm.mark_failed(tc.name, str(e))
                    stats.errors.append({"table": tc.name, "error": str(e)})
                    try:
                        getattr(conn, "cancel", lambda: None)()
                    except Exception:
                        pass

            for tc in filter_tables(TABLES_FULL, cfg):
                if cfg.mode == RunMode.PROBE:
                    with conn.cursor() as cur:
                        cur.execute(f"SELECT COUNT(*) FROM {tc.name}")
                        row = cur.fetchone()
                        logger.info(json.dumps({
                            "mode": "probe",
                            "table": tc.name,
                            "count": row[0] if row else 0,
                            "load": "full",
                        }))
                    stats.tables_processed += 1
                    continue
                try:
                    n = _ingest(cfg, conn, tc, batch_id, None, stats) or 0
                    wm.set(tc.name, datetime.now(timezone.utc), n)
                except Exception as e:
                    logger.exception(json.dumps(
                        {"table": tc.name, "error": str(e)}))
                    wm.mark_failed(tc.name, str(e))
                    stats.errors.append({"table": tc.name, "error": str(e)})
                    try:
                        getattr(conn, "cancel", lambda: None)()
                    except Exception:
                        pass

    return stats.finish()


if __name__ == "__main__":
    run(Config())
