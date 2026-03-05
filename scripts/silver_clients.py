"""silver_clients.py — Bronze → Silver dim_clients SCD Type 2.
Phase 1 · GI Data Lakehouse · Manifeste v2.0
# FinOps (2026-03-05) : lecture partitionnée Bronze ({cfg.date_partition})
# CORRECTIONS DDL (probe 2026-03-05) :
#   WTTIESERV : TIES_RS/TIES_SIREN/TIES_SIRET/TIES_NAF absent DDL → supprimés
#               Raison sociale : TIES_RAISOC (confirmé en bronze _COLS)
#               Adresse : TIES_ADR1/ADR2/CODPOS/VILLE confirmés, NUMVOIE/TYPVOIE/VOIE absent
#   WTCLPT    : CLPT_CAPOT→CLPT_CAESTIME, CLPT_DATEMODIF/ACTIF/EFFECTIF absent
#               CLPT_STATUT absent → CLPT_PROSPEC utilisé comme proxy statut
#               CLPT_DATECREA → CLPT_DATCREA (bronze _COLS)
"""
import hashlib
from datetime import datetime, timezone

from shared import Config, RunMode, Stats, get_duckdb_connection, hash_sk, logger, s3_bronze

SCD2_TRACKED_COLS = ("raison_sociale", "adresse_complete", "ville",
                     "code_postal", "statut_client", "ca_potentiel")


def build_staging_query(cfg: Config) -> str:
    b = f"s3://{cfg.bucket_bronze}"
    return f"""
    WITH raw_ties AS (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY TIE_ID, TIES_SERV ORDER BY _loaded_at DESC) AS rn
        FROM read_json_auto('{b}/raw_wttieserv/{cfg.date_partition}/*.json', union_by_name=true, hive_partitioning=false)
    ),
    raw_clpt AS (
        SELECT * FROM read_json_auto('{b}/raw_wtclpt/{cfg.date_partition}/*.json', union_by_name=true, hive_partitioning=false)
    )
    SELECT
        t.TIE_ID::INT                                               AS tie_id,
        TRIM(t.TIES_RAISOC)                                         AS raison_sociale,
        NULL::VARCHAR                                               AS siren,
        NULL::VARCHAR                                               AS siret,
        NULL::VARCHAR                                               AS naf_code,
        CONCAT_WS(' ', t.TIES_ADR1, t.TIES_ADR2, t.TIES_CODPOS, t.TIES_VILLE) AS adresse_complete,
        TRIM(t.TIES_VILLE)                                          AS ville,
        TRIM(t.TIES_CODPOS)                                         AS code_postal,
        COALESCE(TRIM(c.CLPT_PROSPEC::VARCHAR), 'INCONNU')         AS statut_client,
        TRY_CAST(c.CLPT_CAESTIME AS DECIMAL(18,2))                 AS ca_potentiel,
        TRY_CAST(c.CLPT_DATCREA AS DATE)                           AS date_creation_fiche,
        t._batch_id                                                  AS _source_raw_id
    FROM raw_ties t
    LEFT JOIN raw_clpt c ON c.TIE_ID::INT = t.TIE_ID::INT
    WHERE t.rn = 1 AND t.TIE_ID IS NOT NULL
    """


def _hash(row: dict) -> str:
    parts = "|".join(str(row.get(c, "")) for c in SCD2_TRACKED_COLS)
    return hashlib.md5(parts.encode()).hexdigest()


def run(cfg: Config) -> dict:
    stats = Stats()
    now = datetime.now(timezone.utc)
    silver_path = f"s3://{cfg.bucket_silver}/slv_clients/dim_clients"

    with get_duckdb_connection(cfg) as ddb:
        res = ddb.execute(build_staging_query(cfg))
        cols = [d[0] for d in res.description]
        staging = [dict(zip(cols, row)) for row in res.fetchall()]
        if not staging:
            stats.warnings.append("No Bronze clients data")
            return stats.finish()

        existing: list[dict] = []
        try:
            res2 = ddb.execute(
                f"SELECT * FROM read_parquet('{silver_path}/**/*.parquet')")
            cols2 = [d[0] for d in res2.description]
            existing = [dict(zip(cols2, row)) for row in res2.fetchall()]
        except Exception:
            logger.info("No existing Silver dim_clients — first run")

        current_by_id: dict[int, dict] = {
            int(r["tie_id"]): r for r in existing if r.get("is_current")}
        historical = [r for r in existing if not r.get("is_current")]
        new_records: list[dict] = []
        closed_records: list[dict] = []

        for rd in staging:
            tie_id = int(rd["tie_id"])
            new_hash = _hash(rd)
            if tie_id in current_by_id and current_by_id[tie_id].get("change_hash") == new_hash:
                continue
            if tie_id in current_by_id:
                old = dict(current_by_id[tie_id])
                old["valid_to"] = now.isoformat()
                old["is_current"] = False
                closed_records.append(old)
            new_records.append({
                "client_sk": hash_sk(tie_id, now.isoformat()),
                "tie_id": tie_id,
                "change_hash": new_hash,
                "raison_sociale": rd.get("raison_sociale", ""),
                "siren": None,
                "siret": None,
                "naf_code": None,
                "adresse_complete": rd.get("adresse_complete", ""),
                "ville": rd.get("ville", ""),
                "code_postal": rd.get("code_postal", ""),
                "statut_client": rd.get("statut_client", "INCONNU"),
                "ca_potentiel": rd.get("ca_potentiel"),
                "date_creation_fiche": rd.get("date_creation_fiche"),
                "is_current": True,
                "valid_from": now.isoformat(),
                "valid_to": None,
                "_source_raw_id": rd.get("_source_raw_id", ""),
                "_loaded_at": now.isoformat(),
            })

        changed_ids = {int(r["tie_id"]) for r in new_records}
        unchanged = [r for pid, r in current_by_id.items()
                     if pid not in changed_ids]
        all_records = historical + closed_records + unchanged + new_records
        stats.rows_transformed = len(new_records)
        stats.extra = {"records_written": len(
            all_records), "closed": len(closed_records)}

        if cfg.mode in (RunMode.OFFLINE, RunMode.PROBE):
            logger.info(
                f"[{cfg.mode.value}] Would write {len(all_records)} rows")
        elif all_records:
            ddb.execute(
                "CREATE OR REPLACE TABLE _scd2 AS SELECT * FROM ?", [all_records])
            ddb.execute(
                f"COPY _scd2 TO '{silver_path}' (FORMAT PARQUET, OVERWRITE_OR_IGNORE true)")
            ddb.execute("DROP TABLE _scd2")

    return stats.finish()


if __name__ == "__main__":
    run(Config())
