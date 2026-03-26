"""bronze_interimaires.py — Bronze · 10 tables intérimaires + 3 référentiels compétences Evolia → S3.
Phase 0 · GI Data Lakehouse · Manifeste v2.0
# CORRECTIONS DDL (probe 2026-03-05) :
# PYPERSONNE   : PER_DATENAIS→PER_NAISSANCE, PER_NATIONALITE→NAT_CODE,
#                PER_PAYS→PAYS_CODE, PER_ADRESSE/COMPL/LIEUNAIS absent.
#                PER_DATEMODIF absent → FULL-LOAD
# PYSALARIE    : SAL_DATEDEBUT→SAL_DATEENTREE, SAL_DATEMODIF absent → FULL-LOAD
# WTPINT       : PINT_PLACEMENT/RGPCNT_ID absent → FULL-LOAD
#                +PINT_PREVENDTE,PINT_DERVENDTE,PINT_MODIFDATE,PINT_CREATDTE (2026-03-13, fidélisation)
# PYCOORDONNEE : TYPTEL→TYPTEL_CODE, COORD_VALEUR→PER_TEL_NTEL,
#                COORD_DATEMODIF absent → FULL-LOAD
# WTPMET       : ORDRE→PMET_ORDRE, PMET_DATEMODIF absent → FULL-LOAD
# WTPHAB       : PHAB_DATEDEB→PHAB_DELIVR, PHAB_DATEFIN→PHAB_EXPIR,
#                PHAB_DATEMODIF absent → FULL-LOAD
# WTPDIP       : PDIP_ANNEE→PDIP_DATE, PDIP_DATEMODIF absent → FULL-LOAD
# WTEXP        : ORDRE→EXP_ORDRE, EXP_SOCIETE→EXP_NOM,
#                EXP_DATEDEB→EXP_DEBUT, EXP_DATEFIN→EXP_FIN,
#                EXP_DATEMODIF absent → FULL-LOAD
# WTPEVAL      : PEVAL_DATE→PEVAL_DU (delta), PEVAL_NOTE→PEVAL_EVALUATION,
#                PEVAL_AGENT→PEVAL_UTL, PEVAL_COMMENTAIRE/RGPCNT_ID absent
# WTUGPINT     : UGPINT_DATEMODIF → delta (non modifié)
# AJOUT RÉFÉRENTIELS (2026-03-12) — DDL confirmé DDL_EVOLIA_FILTERED.sql :
# WTMET  : MET_ID (PK), MET_LIBELLE → libellés métiers pour silver_competences
# WTTHAB : THAB_ID (PK), THAB_LIBELLE → libellés habilitations
# WTTDIP : TDIP_ID (PK), TDIP_LIB (pas TDIP_LIBELLE) → libellés diplômes
# ENRICHISSEMENT RÉFÉRENTIELS (2026-03-12) :
# WTMET  : +NIVQ_ID, +SPE_ID, +PCS_CODE_2003 (classif. INSEE), +DFS_ID, +MET_DELETE (soft-delete)
# WTTHAB : +THAB_NBMOIS (durée validité std → calcul date_expir théorique silver)
# WTTDIP : +TDIP_REF (catégorie/niveau diplôme)
# WTQUA  : AJOUT (TQUA_ID PK, TQUA_CODE, TQUA_LIBELLE) → libellé qualification pour dim_metiers Gold
# CORRECTIONS (2026-03-23) :
#   s3_delete_prefix centralisée depuis shared.py — purge avant écriture FULL généralisée
"""
import json
import time
from datetime import datetime, timezone
from typing import Any

from shared import (
    Config, Stats, TableConfig, RunMode, _CHUNK_SIZE,
    generate_batch_id, today_s3_prefix,
    get_evolia_connection, get_pg_connection, upload_to_s3, s3_delete_prefix, logger,
    filter_tables,
)
from pipeline_utils import WatermarkStore, with_retry

PIPELINE = "bronze_interimaires"
FALLBACK_SINCE = datetime(2023, 1, 1, tzinfo=timezone.utc)

# Seules 2 tables ont une colonne delta confirmée DDL
# UGPINT_DATEMODIF absent DDL (probe 2026-03-10) → WTUGPINT déplacé en full-load
TABLES_DELTA: list[TableConfig] = [
    TableConfig("WTPEVAL", "PEVAL_DU", ["PER_ID", "PEVAL_DU"]),
]

# Toutes les autres : aucune colonne delta en DDL → full-load
TABLES_FULL: list[TableConfig] = [
    TableConfig("PYPERSONNE", "", ["PER_ID"], rgpd_flag="SENSIBLE"),
    TableConfig("PYSALARIE", "", ["PER_ID"], rgpd_flag="PERSONNEL"),
    TableConfig("WTPINT", "", ["PER_ID"], rgpd_flag="PERSONNEL"),
    TableConfig("PYCOORDONNEE", "", [
                "PER_ID", "TYPTEL_CODE"], rgpd_flag="SENSIBLE"),
    TableConfig("WTPMET", "", ["PER_ID", "PMET_ORDRE"]),
    TableConfig("WTPHAB", "", ["PER_ID", "THAB_ID"]),
    TableConfig("WTPDIP", "", ["PER_ID", "TDIP_ID"]),
    TableConfig("WTEXP", "", ["PER_ID", "EXP_ORDRE"]),
    # UGPINT_DATEMODIF absent DDL
    TableConfig("WTUGPINT", "", ["PER_ID", "RGPCNT_ID"]),
    # Référentiels compétences — petites tables statiques, full-load (DDL confirmé 2026-03-12)
    TableConfig("WTMET", "", ["MET_ID"]),
    TableConfig("WTTHAB", "", ["THAB_ID"]),
    TableConfig("WTTDIP", "", ["TDIP_ID"]),
    # Types de qualification (libellé pour dim_metiers)
    TableConfig("WTQUA", "", ["TQUA_ID"]),
]

# Noms DDL confirmés par probe 2026-03-05. NIR/PER_TEL_NTEL conservés Bronze,
# pseudonymisés au Silver.
_COLS: dict[str, str] = {
    "PYPERSONNE": (
        "PER_ID,PER_NOM,PER_PRENOM,PER_NAISSANCE,PER_NIR,"
        "NAT_CODE,PAYS_CODE,PER_BISVOIE,PER_COMPVOIE,PER_CP,PER_VILLE,PER_COMMUNE"
    ),
    "PYSALARIE": "PER_ID,SAL_MATRICULE,SAL_DATEENTREE,SAL_ACTIF",
    "WTPINT": "PER_ID,PINT_CANDIDAT,PINT_DOSSIER,PINT_PERMANENT,PINT_PREVENDTE,PINT_DERVENDTE,PINT_MODIFDATE,PINT_CREATDTE",
    # TYPTEL_CODE remplace TYPTEL, PER_TEL_NTEL remplace COORD_VALEUR
    "PYCOORDONNEE": "PER_ID,TYPTEL_CODE,PER_TEL_NTEL,PER_TEL_POSTE",
    "WTPMET": "PER_ID,PMET_ORDRE,MET_ID",
    "WTPHAB": "PER_ID,THAB_ID,PHAB_DELIVR,PHAB_EXPIR,PHAB_ORDRE",
    "WTPDIP": "PER_ID,TDIP_ID,PDIP_DATE",
    # EXP_SOCIETE→EXP_NOM, EXP_DATEDEB→EXP_DEBUT, EXP_DATEFIN→EXP_FIN, ORDRE→EXP_ORDRE
    "WTEXP": "PER_ID,EXP_ORDRE,EXP_NOM,EXP_DEBUT,EXP_FIN,EXP_INTERNE",
    # PEVAL_DATE→PEVAL_DU, PEVAL_NOTE→PEVAL_EVALUATION, PEVAL_AGENT→PEVAL_UTL
    "WTPEVAL": "PER_ID,PEVAL_DU,PEVAL_EVALUATION,PEVAL_UTL",
    "WTUGPINT": "PER_ID,RGPCNT_ID,AUG_ORI",  # UGPINT_DATEMODIF absent DDL
    # Référentiels compétences — DDL confirmé DDL_EVOLIA_FILTERED.sql (2026-03-12)
    # Enrichissement 2026-03-12 : PCS_CODE_2003/MET_DELETE/NIVQ_ID/SPE_ID/DFS_ID, THAB_NBMOIS, TDIP_REF
    "WTMET": "MET_ID,MET_CODE,MET_LIBELLE,TQUA_ID,NIVQ_ID,SPE_ID,PCS_CODE_2003,DFS_ID,MET_DELETE",
    # THAB_NBMOIS = durée validité (mois)
    "WTTHAB": "THAB_ID,THAB_CDE,THAB_LIBELLE,THAB_NBMOIS",
    # TDIP_LIB (pas TDIP_LIBELLE), TDIP_REF = catégorie
    "WTTDIP": "TDIP_ID,TDIP_CODE,TDIP_LIB,TDIP_REF",
    # Types qualification → libellé pour dim_metiers Gold
    "WTQUA": "TQUA_ID,TQUA_CODE,TQUA_LIBELLE",
}

_RGPD_SENSITIVE: frozenset[str] = frozenset({"PER_NIR", "PER_TEL_NTEL"})


def _extract_delta(conn: Any, tc: TableConfig, since: datetime) -> list[dict]:
    since_str = since.strftime("%Y-%m-%d %H:%M:%S")
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT {_COLS[tc.name]} FROM {tc.name} WHERE {tc.delta_col} >= %s",
            since_str,
        )
        h = [d[0] for d in cur.description]
        return [dict(zip(h, row)) for row in cur.fetchall()]


def _extract_full(conn: Any, tc: TableConfig) -> list[dict]:
    with conn.cursor() as cur:
        cur.execute(f"SELECT {_COLS[tc.name]} FROM {tc.name}")
        h = [d[0] for d in cur.description]
        return [dict(zip(h, row)) for row in cur.fetchall()]


@with_retry(max_attempts=3, base_delay=2.0, backoff=2.0)
def _ingest(
    cfg: Config, conn: Any, tc: TableConfig,
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
    detected = [c for c in rows[0] if c in _RGPD_SENSITIVE]
    if detected:
        logger.warning(json.dumps({
            "rgpd_alert": True, "table": tc.name, "columns": detected,
            "action": "pseudonymize_at_silver",
        }))
        stats.extra.setdefault("rgpd_columns", []).extend(
            [f"{tc.name}.{c}" for c in detected])
    loaded_at = datetime.now(timezone.utc).isoformat()
    enriched = [
        {"_loaded_at": loaded_at, "_batch_id": batch_id,
         "_source_table": tc.name, "_rgpd_flag": tc.rgpd_flag, **r}
        for r in rows
    ]
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
        "rgpd": tc.rgpd_flag,
        "duration_s": round(time.monotonic() - t0, 2),
    }))
    return len(rows)


def run(cfg: Config) -> dict:
    stats = Stats()
    batch_id = generate_batch_id()
    if cfg.mode == RunMode.OFFLINE:
        for tc in filter_tables(TABLES_DELTA + TABLES_FULL, cfg):
            logger.info(json.dumps({"mode": "offline", "table": tc.name}))
        return stats.finish(cfg, PIPELINE)

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

    return stats.finish(cfg, PIPELINE)


if __name__ == "__main__":
    run(Config())
