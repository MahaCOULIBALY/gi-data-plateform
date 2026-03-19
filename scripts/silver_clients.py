"""silver_clients.py — Bronze → Iceberg OVH · dim_clients SCD Type 2.
Phase 1 · GI Data Lakehouse · Manifeste v2.0
# FinOps (2026-03-05) : lecture partitionnée Bronze ({cfg.date_partition})
# CORRECTIONS DDL (probe 2026-03-05) :
#   WTTIESERV : TIES_RS→TIES_RAISOC→TIES_DESIGNATION (confirmé bronze_clients v2)
#               TIES_CODPOS→TIES_CODEP (confirmé bronze_clients v2)
#               TIES_SIRET absent DDL → siren via TIES_SIREN, naf via NAF/NAF2008 (sans préfixe TIES_)
#               Adresse : TIES_ADR1/ADR2/CODEP/VILLE confirmés
#   WTCLPT    : CLPT_PROSPEC→CLPT_PROS, CLPT_CAESTIME→CLPT_CAPT,
#               CLPT_DATCREA→CLPT_DCREA (confirmés bronze_clients v2)
# CORRECTIONS DDL (2026-03-11) :
#   Alignement colonnes WTTIESERV/WTCLPT sur Bronze v2 (state card silver_required)
"""
import hashlib
import json
import os
import tempfile
from datetime import datetime, timezone

from shared import Config, Stats, get_duckdb_connection, hash_sk, write_silver_iceberg, logger

# TIES_SIREN/NIC et naf_code ajoutés au tracking SCD2 (NIC ajouté 2026-03-11)
SCD2_TRACKED_COLS = ("raison_sociale", "siren", "nic", "naf_code",
                     "adresse_complete", "ville", "code_postal",
                     "statut_client", "ca_potentiel")


def build_staging_query(cfg: Config) -> str:
    b = f"s3://{cfg.bucket_bronze}"
    return f"""
    WITH raw_ties AS (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY TIE_ID, TIES_SERV ORDER BY _loaded_at DESC) AS rn
        FROM read_json_auto('{b}/raw_wttieserv/{cfg.date_partition}/*.json',
                            union_by_name=true, hive_partitioning=false)
    ),
    raw_clpt AS (
        SELECT * FROM read_json_auto('{b}/raw_wtclpt/{cfg.date_partition}/*.json',
                                     union_by_name=true, hive_partitioning=false)
    )
    SELECT
        t.TIE_ID::INT                                               AS tie_id,
        TRIM(t.TIES_DESIGNATION)                                    AS raison_sociale,
        TRIM(COALESCE(t.TIES_SIREN, ''))                            AS siren,
        CASE WHEN LEN(TRIM(t.TIES_NIC)) = 5 THEN TRIM(t.TIES_NIC) ELSE NULL END AS nic,
        TRIM(COALESCE(t.NAF, COALESCE(t.NAF2008, '')))              AS naf_code,
        CONCAT_WS(' ', t.TIES_ADR1, t.TIES_ADR2, t.TIES_CODEP, t.TIES_VILLE) AS adresse_complete,
        TRIM(t.TIES_VILLE)                                          AS ville,
        TRIM(t.TIES_CODEP)                                          AS code_postal,
        TRIM(COALESCE(c.CLPT_PROS::VARCHAR, 'INCONNU'))             AS statut_client,
        TRY_CAST(c.CLPT_CAPT AS DECIMAL(18,2))                     AS ca_potentiel,
        TRY_CAST(c.CLPT_DCREA AS DATE)                             AS date_creation_fiche,
        t._batch_id                                                  AS _source_raw_id
    FROM raw_ties t
    LEFT JOIN raw_clpt c ON c.TIE_ID::INT = t.TIE_ID::INT
    WHERE t.rn = 1 AND t.TIE_ID IS NOT NULL
    """


def _hash(row: dict) -> str:
    parts = "|".join(str(row.get(c, "")) for c in SCD2_TRACKED_COLS)
    return hashlib.md5(parts.encode()).hexdigest()


def _apply_scd2(
    staging: list[dict],
    existing: list[dict],
    now: "datetime",
) -> tuple[list[dict], list[dict], list[dict]]:
    """Logique SCD Type 2 pure — sans I/O, testable unitairement.
    Retourne (new_records, closed_records, unchanged_current).
    """
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
            "siren": rd.get("siren") or None,
            "nic": rd.get("nic") or None,
            "naf_code": rd.get("naf_code") or None,
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
    unchanged_current = [r for pid, r in current_by_id.items() if pid not in changed_ids]
    return new_records, closed_records, unchanged_current


def run(cfg: Config) -> dict:
    stats = Stats()
    now = datetime.now(timezone.utc)

    with get_duckdb_connection(cfg) as ddb:
        res = ddb.execute(build_staging_query(cfg))
        cols = [d[0] for d in res.description]
        staging = [dict(zip(cols, row)) for row in res.fetchall()]
        if not staging:
            stats.warnings.append("No Bronze clients data")
            return stats.finish()

        existing: list[dict] = []
        historical = [r for r in existing if not r.get("is_current")]
        new_records, closed_records, unchanged_current = _apply_scd2(staging, existing, now)
        all_records = historical + closed_records + unchanged_current + new_records
        stats.extra = {"records_written": len(all_records), "closed": len(closed_records)}

        if all_records:
            with tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False) as f:
                for r in all_records:
                    f.write(json.dumps(r, default=str) + "\n")
                tmp = f.name
            try:
                write_silver_iceberg(
                    ddb,
                    f"SELECT * FROM read_json_auto('{tmp}', union_by_name=true)",
                    "silver.clients.dim_clients", cfg, stats,
                )
            finally:
                os.unlink(tmp)
        stats.rows_transformed = len(new_records)

    return stats.finish()


if __name__ == "__main__":
    run(Config())
