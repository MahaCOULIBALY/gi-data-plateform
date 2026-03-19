"""silver_interimaires.py — Bronze → Iceberg OVH · dim_interimaires SCD Type 2 + RGPD NIR.
Phase 2 · GI Data Lakehouse · Manifeste v2.0
# FinOps (2026-03-05) : lecture partitionnée Bronze ({cfg.date_partition})
# CORRECTIONS DDL (probe 2026-03-05) :
#   PER_DATENAIS→PER_NAISSANCE, PER_NATIONALITE→NAT_CODE, PER_PAYS→PAYS_CODE
#   PER_NUMVOIE/TYPVOIE/VOIE → PER_BISVOIE/COMPVOIE (adresse partielle DDL)
#   RGPCNT_ID absent de WTPINT → NULL::INT  (agence_rattachement)
#   SAL_DATESORTIE absent DDL → NULL::DATE
"""
import hashlib
import json
import os
import tempfile
from datetime import datetime, timezone

from shared import Config, Stats, get_duckdb_connection, hash_sk, pseudonymize_nir, write_silver_iceberg, logger

SCD2_TRACKED = ("nom", "prenom", "adresse", "ville", "code_postal",
                "is_actif", "is_candidat", "is_permanent", "agence_rattachement")


def build_staging_query(cfg: Config) -> str:
    b = f"s3://{cfg.bucket_bronze}"
    return f"""
    WITH raw_per AS (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY PER_ID ORDER BY _loaded_at DESC) AS rn
        FROM read_json_auto('{b}/raw_pypersonne/{cfg.date_partition}/*.json', union_by_name=true, hive_partitioning=false)
    ),
    raw_sal AS (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY PER_ID ORDER BY _loaded_at DESC) AS rn
        FROM read_json_auto('{b}/raw_pysalarie/{cfg.date_partition}/*.json', union_by_name=true, hive_partitioning=false)
    ),
    raw_pint AS (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY PER_ID ORDER BY _loaded_at DESC) AS rn
        FROM read_json_auto('{b}/raw_wtpint/{cfg.date_partition}/*.json', union_by_name=true, hive_partitioning=false)
    )
    SELECT
        p.PER_ID::INT                                               AS per_id,
        TRIM(s.SAL_MATRICULE)                                       AS matricule,
        TRIM(p.PER_NOM)                                             AS nom,
        TRIM(p.PER_PRENOM)                                          AS prenom,
        TRY_CAST(p.PER_NAISSANCE AS DATE)                          AS date_naissance,
        TRIM(p.PER_NIR)                                             AS nir_brut,
        TRIM(COALESCE(p.NAT_CODE, ''))                              AS nationalite,
        TRIM(COALESCE(p.PAYS_CODE, ''))                             AS pays,
        CONCAT_WS(' ', p.PER_BISVOIE, p.PER_COMPVOIE, p.PER_CP, p.PER_VILLE) AS adresse,
        TRIM(p.PER_VILLE)                                           AS ville,
        TRIM(p.PER_CP)                                              AS code_postal,
        TRY_CAST(s.SAL_DATEENTREE AS DATE)                         AS date_entree,
        NULL::DATE                                                   AS date_sortie,
        COALESCE(TRY_CAST(s.SAL_ACTIF AS BOOLEAN), false)          AS is_actif,
        COALESCE(TRY_CAST(i.PINT_CANDIDAT AS BOOLEAN), false)      AS is_candidat,
        COALESCE(TRY_CAST(i.PINT_PERMANENT AS BOOLEAN), false)     AS is_permanent,
        NULL::INT                                                    AS agence_rattachement,
        p._batch_id                                                  AS _source_raw_id
    FROM raw_per p
    LEFT JOIN raw_sal s ON s.PER_ID::INT = p.PER_ID::INT AND s.rn = 1
    LEFT JOIN raw_pint i ON i.PER_ID::INT = p.PER_ID::INT AND i.rn = 1
    WHERE p.rn = 1 AND p.PER_ID IS NOT NULL
    """


def _hash(row: dict) -> str:
    parts = "|".join(str(row.get(c, "")) for c in SCD2_TRACKED)
    return hashlib.md5(parts.encode()).hexdigest()


def run(cfg: Config) -> dict:
    stats = Stats()
    stats.extra = {"nir_pseudonymized": 0, "nir_null": 0, "records_closed": 0}
    now = datetime.now(timezone.utc)

    with get_duckdb_connection(cfg) as ddb:
        res = ddb.execute(build_staging_query(cfg))
        cols = [d[0] for d in res.description]
        staging = [dict(zip(cols, row)) for row in res.fetchall()]
        logger.info(f"Staging: {len(staging)} intérimaires from Bronze")
        if not staging:
            return stats.finish()

        existing: list[dict] = []
        current_by_id: dict[int, dict] = {
            int(r["per_id"]): r for r in existing if r.get("is_current")}
        historical = [r for r in existing if not r.get("is_current")]
        new_records: list[dict] = []
        closed_records: list[dict] = []

        for rd in staging:
            per_id = int(rd["per_id"])
            new_hash = _hash(rd)
            if per_id in current_by_id and current_by_id[per_id].get("change_hash") == new_hash:
                continue
            if per_id in current_by_id:
                old = dict(current_by_id[per_id])
                old["valid_to"] = now.isoformat()
                old["is_current"] = False
                closed_records.append(old)
            nir_pseudo = pseudonymize_nir(rd.get("nir_brut"), cfg.rgpd_salt)
            stats.extra["nir_pseudonymized" if nir_pseudo else "nir_null"] += 1
            new_records.append({
                "interimaire_sk": hash_sk(per_id, now.isoformat()),
                "per_id": per_id,
                "change_hash": new_hash,
                "matricule": rd.get("matricule", ""),
                "nom": rd.get("nom", ""),
                "prenom": rd.get("prenom", ""),
                "date_naissance": rd.get("date_naissance"),
                "nir_pseudo": nir_pseudo,
                "nationalite": rd.get("nationalite", ""),
                "pays": rd.get("pays", ""),
                "adresse": rd.get("adresse", ""),
                "ville": rd.get("ville", ""),
                "code_postal": rd.get("code_postal", ""),
                "date_entree": rd.get("date_entree"),
                "date_sortie": None,
                "is_actif": rd.get("is_actif", False),
                "is_candidat": rd.get("is_candidat", False),
                "is_permanent": rd.get("is_permanent", False),
                "agence_rattachement": None,
                "is_current": True,
                "valid_from": now.isoformat(),
                "valid_to": None,
                "_source_raw_id": rd.get("_source_raw_id", ""),
                "_loaded_at": now.isoformat(),
            })

        changed_ids = {int(r["per_id"]) for r in new_records}
        unchanged_current = [
            r for pid, r in current_by_id.items() if pid not in changed_ids]
        all_records = historical + closed_records + unchanged_current + new_records
        stats.extra["records_closed"] = len(closed_records)
        stats.extra["all_records_written"] = len(all_records)

        if all_records:
            with tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False) as f:
                for r in all_records:
                    f.write(json.dumps(r, default=str) + "\n")
                tmp = f.name
            try:
                write_silver_iceberg(
                    ddb,
                    f"SELECT * FROM read_json_auto('{tmp}', union_by_name=true)",
                    "silver.interimaires.dim_interimaires", cfg, stats,
                )
            finally:
                os.unlink(tmp)
        stats.rows_transformed = len(new_records)

    return stats.finish()


if __name__ == "__main__":
    run(Config())
