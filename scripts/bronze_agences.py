"""bronze_agences.py — Bronze · Structure organisationnelle Evolia → S3.
Tables : PYREGROUPECNT, PYENTREPRISE, PYETABLISSEMENT, WTUG, PYDOSPETA, AGENCE_GESTION.
Phase 0 · GI Data Lakehouse · Manifeste v2.0
# CORRECTIONS DDL (probe 2026-03-05) :
#   PYETABLISSEMENT : ETA_SIRET/ETA_DATEMODIF absent → FULL-LOAD, ETA_ADR2_COMP (extra)
#   WTUG (ajouté)   : UG_CLOTURE→UG_CLOTURE_DATE, UG_PILOTE→PIL_ID, UG_GPS_LAT/LNG→UG_GPS,
#                     UG_NOM/UG_DATEMODIF absent → FULL-LOAD. Nom agence via PYREGROUPECNT.
# CORRECTIONS DDL (2026-03-11) — DDL_EVOLIA_FILTERED confirmé :
#   PYREGROUPECNT   : +DATE_CLOTURE (source de vérité is_cloture agence)
#                     RGPCNT_VILLE/EMAIL/GPS_LAT/GPS_LON ABSENTS DDL — colonnes fantômes supprimées Silver
# AJOUTS (2026-03-14) :
#   AGENCE_GESTION  : [Agence Gestion] — résout DT-04 (marque/branche/nom_commercial/code_comm)
#                     Table avec espaces → _SQL_TABLE_NAME pour l'accès SQL
#                     1.4M lignes total — filtrage DATE_UG_GEST >= 2024-01-01 via TABLES_DELTA
#   WTCOEF supprimé : toujours vide en production
#   _SQL_TABLE_NAME : mapping nom logique → nom SQL réel (tables avec espaces/caractères spéciaux)
#                     Appliqué dans _extract_delta, _extract_full ET probe COUNT(*)
"""
import json
import time
from datetime import datetime, timezone

import pymssql

from shared import (
    Config, Stats, TableConfig, RunMode, _CHUNK_SIZE,
    generate_batch_id, today_s3_prefix,
    get_evolia_connection, get_pg_connection, upload_to_s3, logger,
    filter_tables,
)
from pipeline_utils import WatermarkStore, with_retry


PIPELINE      = "bronze_agences"
FALLBACK_SINCE = datetime(2020, 1, 1, tzinfo=timezone.utc)

# AGENCE_GESTION : 1.4M lignes total — filtrage à partir de 2024-01-01.
# DATE_UG_GEST = date métier de gestion, utilisée comme proxy delta.
# Limitation : modifications rétroactives pré-2024 non capturées.
FALLBACK_SINCE_OVERRIDE: dict[str, datetime] = {
    "AGENCE_GESTION": datetime(2022, 1, 1, tzinfo=timezone.utc),
}

# Mapping nom logique → nom SQL réel (tables avec espaces ou caractères spéciaux)
# Appliqué systématiquement dans _extract_delta, _extract_full et les probes COUNT(*)
_SQL_TABLE_NAME: dict[str, str] = {
    "AGENCE_GESTION": "[Agence Gestion]",
    "Secteurs":       "INTERACTION.dbo.Secteurs",
}

# AGENCE_GESTION promu TABLES_DELTA — filtre DATE_UG_GEST >= 2024-01-01
TABLES_DELTA: list[TableConfig] = [
    TableConfig("AGENCE_GESTION", "DATE_UG_GEST", ["ID_UG"]),
]

# Tables sans colonne delta DDL confirmée → full-load à chaque run
TABLES_FULL: list[TableConfig] = [
    TableConfig("PYREGROUPECNT",   "", ["RGPCNT_ID"]),
    TableConfig("PYENTREPRISE",    "", ["ENT_ID"]),
    TableConfig("PYETABLISSEMENT", "", ["ETA_ID"]),
    TableConfig("WTUG",            "", ["RGPCNT_ID"]),
    TableConfig("PYDOSPETA",       "", ["RGPCNT_ID", "ETA_ID"]),
    # WTCOEF retiré — toujours vide en production (2026-03-14)
    TableConfig("Secteurs", "", []), 
]

_COLS: dict[str, str] = {
    # DATE_CLOTURE ajouté (DDL confirmé 2026-03-11) — source de vérité is_active agence
    "PYREGROUPECNT": "RGPCNT_ID,RGPCNT_CODE,RGPCNT_LIBELLE,DOS_ID,ENT_ID,DATE_CLOTURE",
    "PYENTREPRISE":  "ENT_ID,ENT_SIREN,ENT_RAISON,ENT_APE,ENT_ETT,RGPCNT_ID,ENT_MONOETAB",
    "PYETABLISSEMENT": (
        "ETA_ID,ENT_ID,ETA_ACTIVITE,ETA_COMMUNE,ETA_ADR2_COMP,"
        "ETA_ADR2_VOIE,ETA_ADR2_CP,ETA_ADR2_VILLE,ETA_PSEUDO_SIRET,ETA_DATE_CESACT"
    ),
    "WTUG": (
        "RGPCNT_ID,ETA_ID,CAST(UG_GPS AS NVARCHAR(MAX)) AS UG_GPS,"
        "UG_CLOTURE_DATE,UG_CLOTURE_USER,PIL_ID,UG_EMAIL"
    ),
    "PYDOSPETA":      "RGPCNT_ID,ETA_ID",
    # DT-04 résolu : marque/branche/nom_commercial/code_comm désormais disponibles
    "AGENCE_GESTION": (
        "DATE_UG_GEST,NOM_COMMERCIAL,ID_UG,NOM_UG,"
        "CODE_COMM,MARQUE,BRANCHE,COMMERCIAL_GESTION,TAUX"
    ),
    "Secteurs": (
        "[currentTime],[Agence de gestion],"
        "[Secteur],[Périmètre],[Zone Géographique]"
    ),
}


def _extract_delta(conn, tc: TableConfig, since: datetime) -> list[dict]:
    since_str = since.strftime("%Y-%m-%d %H:%M:%S")
    sql_name  = _SQL_TABLE_NAME.get(tc.name, tc.name)
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT {_COLS[tc.name]} FROM {sql_name} WHERE {tc.delta_col} >= %s",
            since_str,
        )
        h = [d[0] for d in cur.description]
        return [dict(zip(h, row)) for row in cur.fetchall()]


def _extract_full(conn, tc: TableConfig) -> list[dict]:
    sql_name = _SQL_TABLE_NAME.get(tc.name, tc.name)
    with conn.cursor() as cur:
        cur.execute(f"SELECT {_COLS[tc.name]} FROM {sql_name}")
        h = [d[0] for d in cur.description]
        return [dict(zip(h, row)) for row in cur.fetchall()]
    
def _s3_delete_prefix(cfg: Config, bucket: str, prefix: str) -> None:
    import boto3
    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            s3.delete_object(Bucket=bucket, Key=obj["Key"])


@with_retry(max_attempts=3, base_delay=2.0, backoff=2.0)
def _ingest(
    cfg: Config, conn, tc: TableConfig,
    batch_id: str, since: datetime | None, stats: Stats,
) -> int:
    """Extrait, enrichit et charge une table vers S3 Bronze. Retourne le nombre de lignes ingérées."""
    t0   = time.monotonic()
    rows = _extract_delta(conn, tc, since) if since else _extract_full(conn, tc)
    if not rows:
        logger.info(json.dumps({"table": tc.name, "rows": 0, "status": "empty"}))
        return 0
    loaded_at = datetime.now(timezone.utc).isoformat()
    enriched  = [{"_loaded_at": loaded_at, "_batch_id": batch_id,
                  "_source_table": tc.name, **r} for r in rows]
    chunks = [enriched[i:i + _CHUNK_SIZE] for i in range(0, len(enriched), _CHUNK_SIZE)]
    prefix = f"raw_{tc.name.lower()}/{today_s3_prefix()}"
    
    # Purge avant écriture pour les tables FULL
    if since is None:  # mode full
        _s3_delete_prefix(cfg, cfg.bucket_bronze, prefix)
        
    for idx, chunk in enumerate(chunks):
        key = f"{prefix}/batch_{batch_id}_{idx:04d}.json"
        upload_to_s3(cfg, chunk, cfg.bucket_bronze, key, stats)
    stats.tables_processed += 1
    stats.rows_ingested    += len(rows)
    logger.info(json.dumps({
        "table":      tc.name,
        "rows":       len(rows),
        "chunks":     len(chunks),
        "mode":       "delta" if since else "full",
        "duration_s": round(time.monotonic() - t0, 2),
    }))
    return len(rows)


def run(cfg: Config) -> dict:
    stats    = Stats()
    batch_id = generate_batch_id()

    if cfg.mode == RunMode.OFFLINE:
        for tc in filter_tables(TABLES_DELTA + TABLES_FULL, cfg):
            logger.info(json.dumps({"mode": "offline", "table": tc.name}))
        return stats.finish()

    with get_pg_connection(cfg) as pg_conn:
        wm = WatermarkStore(pg_conn, PIPELINE)
        with get_evolia_connection(cfg) as conn:

            # ── Tables DELTA ─────────────────────────────────────────────────
            for tc in filter_tables(TABLES_DELTA, cfg):
                since    = wm.get(tc.name) or FALLBACK_SINCE_OVERRIDE.get(tc.name, FALLBACK_SINCE)
                sql_name = _SQL_TABLE_NAME.get(tc.name, tc.name)
                if cfg.mode == RunMode.PROBE:
                    with conn.cursor() as cur:
                        cur.execute(
                            f"SELECT COUNT(*) FROM {sql_name} "
                            f"WHERE {tc.delta_col} >= %s",
                            since.strftime("%Y-%m-%d %H:%M:%S"),
                        )
                        logger.info(json.dumps({
                            "mode":  "probe",
                            "table": tc.name,
                            "count": cur.fetchone()[0],
                            "since": since.strftime("%Y-%m-%d"),
                        }))
                    stats.tables_processed += 1
                    continue
                try:
                    n = _ingest(cfg, conn, tc, batch_id, since, stats)
                    wm.set(tc.name, datetime.now(timezone.utc), n)
                except Exception as e:
                    logger.exception(json.dumps({"table": tc.name, "error": str(e)}))
                    wm.mark_failed(tc.name, str(e))
                    stats.errors.append({"table": tc.name, "error": str(e)})
                    try:
                        getattr(conn, "cancel", lambda: None)()
                    except Exception:
                        pass

            # ── Tables FULL ──────────────────────────────────────────────────
            for tc in filter_tables(TABLES_FULL, cfg):
                sql_name = _SQL_TABLE_NAME.get(tc.name, tc.name)
                if cfg.mode == RunMode.PROBE:
                    with conn.cursor() as cur:
                        cur.execute(f"SELECT COUNT(*) FROM {sql_name}")
                        row = cur.fetchone()
                        logger.info(json.dumps({
                            "mode":  "probe",
                            "table": tc.name,
                            "count": row[0] if row else 0,
                            "load":  "full",
                        }))
                    stats.tables_processed += 1
                    continue
                try:
                    n = _ingest(cfg, conn, tc, batch_id, None, stats) or 0
                    wm.set(tc.name, datetime.now(timezone.utc), n)
                except Exception as e:
                    logger.exception(json.dumps({"table": tc.name, "error": str(e)}))
                    wm.mark_failed(tc.name, str(e))
                    stats.errors.append({"table": tc.name, "error": str(e)})
                    try:
                        getattr(conn, "cancel", lambda: None)()
                    except Exception:
                        pass

    return stats.finish()


if __name__ == "__main__":
    run(Config())
