"""silver_clients_detail.py — Silver · sites_mission + contacts + encours_credit + coefficients.
Phase 1 · GI Data Lakehouse · Manifeste v2.0
# CORRECTIONS DDL (probe 2026-03-05) :
#   WTTIESERV   : TIES_RS→TIES_DESIGNATION, TIES_SIRET→NULL (absent DDL),
#                 TIES_CODPOS→TIES_CODEP, +TIES_SIREN, RGPCNT_ID absent → NULL
#   WTTIEINT    : TIEINT_NOM→TIEI_NOM, TIEINT_PRENOM→TIEI_PRENOM,
#                 TIEINT_EMAIL→TIEI_EMAIL, TIEINT_TEL→TIEI_BUREAU,
#                 TIEINT_FONCTION→FCTI_CODE, ORDRE→TIEI_ORDRE
#   WTENCOURSG  : SIREN→ENC_SIREN, ENCGRP_MONTANT/LIMITE/DATE/STATUT absent → NULL
#                 ENCG_DECISIONLIB (extra) disponible
# CORRECTIONS DDL (2026-03-11) :
#   WTTIESERV : TIES_RAISOC→TIES_DESIGNATION, TIES_CODPOS→TIES_CODEP, +TIES_SIREN
#   WTCOEF    : RGPCNT_ID, TIE_ID (confirmés bronze_clients v2) — ajout process_coefficients
#   WTTIESERV : +NIC, +EMAIL, +PAYS_CODE, +CLOT_DAT → is_active corrigé (n'était pas hardcodé true)
#   WTCOEF    : TQUA→TQUA_ID (bug fix — colonne renommée dans bronze_clients v2)
#   Partition date appliquée sur toutes les sources Bronze (FinOps — évite full-scan S3)
"""
import json
from shared import Config, RunMode, Stats, get_duckdb_connection, logger


def process_sites_mission(ddb, cfg: Config, stats: Stats) -> int:
    b = f"s3://{cfg.bucket_bronze}"
    silver = f"s3://{cfg.bucket_silver}/slv_clients/sites_mission"
    query = f"""
    WITH raw AS (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY TIE_ID, TIES_SERV ORDER BY _loaded_at DESC) AS rn
        FROM read_json_auto('{b}/raw_wttieserv/{cfg.date_partition}/*.json',
                            union_by_name=true, hive_partitioning=false)
    )
    SELECT
        CAST(TIES_SERV AS INT)                        AS site_id,
        CAST(TIE_ID AS INT)                           AS tie_id,
        TRIM(TIES_DESIGNATION)                        AS nom_site,
        TRIM(COALESCE(TIES_SIREN, ''))                AS siren,
        CASE WHEN LEN(TRIM(TIES_NIC)) = 5
             THEN TRIM(TIES_NIC) ELSE NULL END        AS nic,
        CONCAT_WS(' ', TIES_ADR1, TIES_ADR2, TIES_CODEP, TIES_VILLE) AS adresse,
        TRIM(TIES_VILLE)                              AS ville,
        TRIM(TIES_CODEP)                              AS code_postal,
        UPPER(TRIM(COALESCE(PAYS_CODE, 'FR')))        AS pays_code,
        NULL::VARCHAR                                 AS siret_site,
        LOWER(TRIM(TIES_EMAIL))                       AS email,
        CAST(RGPCNT_ID AS INT)                        AS agence_id,
        CLOT_DAT IS NULL                              AS is_active,
        TRY_CAST(CLOT_DAT AS DATE)                    AS clot_at,
        MD5(CONCAT_WS('|',
            TRIM(COALESCE(TIES_SIREN, '')),
            CASE WHEN LEN(TRIM(TIES_NIC)) = 5 THEN TRIM(TIES_NIC) ELSE '' END,
            TRIM(COALESCE(TIES_ADR1, '')),
            TRIM(COALESCE(TIES_CODEP, '')),
            TRIM(COALESCE(TIES_VILLE, '')),
            LOWER(TRIM(COALESCE(TIES_EMAIL, '')))
        ))                                            AS row_hash,
        CURRENT_TIMESTAMP                             AS _loaded_at
    FROM raw WHERE rn = 1 AND TIE_ID IS NOT NULL
    """
    if cfg.mode in (RunMode.OFFLINE, RunMode.PROBE):
        count = ddb.execute(f"SELECT COUNT(*) FROM ({query})").fetchone()[0]
        logger.info(json.dumps({"mode": cfg.mode.value,
                    "table": "sites_mission", "rows": count}))
        return count
    count = ddb.execute(
        f"COPY ({query}) TO '{silver}' (FORMAT PARQUET, OVERWRITE_OR_IGNORE true)").fetchone()[0]
    logger.info(json.dumps({"table": "sites_mission", "rows": count}))
    return count


def process_contacts(ddb, cfg: Config, stats: Stats) -> int:
    """RGPD : email/tel Silver-only — jamais exposé en Gold."""
    b = f"s3://{cfg.bucket_bronze}"
    silver = f"s3://{cfg.bucket_silver}/slv_clients/contacts"
    query = f"""
    WITH raw AS (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY TIE_ID, TIEI_ORDRE ORDER BY _loaded_at DESC) AS rn
        FROM read_json_auto('{b}/raw_wttieint/{cfg.date_partition}/*.json',
                            union_by_name=true, hive_partitioning=false)
    )
    SELECT
        MD5(CONCAT(CAST(TIE_ID AS VARCHAR), '|', CAST(TIEI_ORDRE AS VARCHAR))) AS contact_id,
        CAST(TIE_ID AS INT)                           AS tie_id,
        TRIM(TIEI_NOM)                                AS nom,
        TRIM(TIEI_PRENOM)                             AS prenom,
        TRIM(TIEI_EMAIL)                              AS email,
        TRIM(TIEI_BUREAU)                             AS telephone,
        TRIM(FCTI_CODE)                               AS fonction_code,
        CURRENT_TIMESTAMP                             AS _loaded_at
    FROM raw WHERE rn = 1 AND TIE_ID IS NOT NULL
    """
    if cfg.mode in (RunMode.OFFLINE, RunMode.PROBE):
        count = ddb.execute(f"SELECT COUNT(*) FROM ({query})").fetchone()[0]
        logger.info(json.dumps({"mode": cfg.mode.value,
                    "table": "contacts", "rows": count}))
        return count
    count = ddb.execute(
        f"COPY ({query}) TO '{silver}' (FORMAT PARQUET, OVERWRITE_OR_IGNORE true)").fetchone()[0]
    logger.info(json.dumps({"table": "contacts", "rows": count}))
    return count


def process_encours_credit(ddb, cfg: Config, stats: Stats) -> int:
    b = f"s3://{cfg.bucket_bronze}"
    silver = f"s3://{cfg.bucket_silver}/slv_clients/encours_credit"
    query = f"""
    WITH raw AS (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY ENCGRP_ID ORDER BY _loaded_at DESC) AS rn
        FROM read_json_auto('{b}/raw_wtencoursg/{cfg.date_partition}/*.json',
                            union_by_name=true, hive_partitioning=false, ignore_errors=true)
    )
    SELECT
        CAST(ENCGRP_ID AS INT)                        AS encours_id,
        TRIM(ENC_SIREN)                               AS siren,
        NULL::DECIMAL(18,2)                           AS montant_encours,
        NULL::DECIMAL(18,2)                           AS limite_credit,
        NULL::DATE                                    AS date_decision,
        TRIM(COALESCE(ENCG_DECISIONLIB::VARCHAR, ''))  AS decision_libelle,
        CURRENT_TIMESTAMP                             AS _loaded_at
    FROM raw WHERE rn = 1 AND ENCGRP_ID IS NOT NULL
    """
    if cfg.mode in (RunMode.OFFLINE, RunMode.PROBE):
        count = ddb.execute(f"SELECT COUNT(*) FROM ({query})").fetchone()[0]
        logger.info(json.dumps({"mode": cfg.mode.value,
                    "table": "encours_credit", "rows": count}))
        return count
    count = ddb.execute(
        f"COPY ({query}) TO '{silver}' (FORMAT PARQUET, OVERWRITE_OR_IGNORE true)").fetchone()[0]
    logger.info(json.dumps({"table": "encours_credit", "rows": count}))
    return count


def run(cfg: Config) -> dict:
    stats = Stats()
    with get_duckdb_connection(cfg) as ddb:
        stats.extra["sites_mission"] = process_sites_mission(ddb, cfg, stats)
        stats.extra["contacts"] = process_contacts(ddb, cfg, stats)
        stats.extra["encours_credit"] = process_encours_credit(ddb, cfg, stats)
        # try:
        #     stats.extra["coefficients"] = process_coefficients(ddb, cfg, stats)
        # except Exception as e:
        #     # WTCOEF vide en source → bronze n'écrit aucun fichier S3 → 0 coefficients
        #     logger.warning(json.dumps({"warning": "coefficients skipped", "error": str(e)}))
        #     stats.extra["coefficients"] = 0
        stats.tables_processed = 4
        stats.rows_transformed = sum(stats.extra.values())
    return stats.finish()


if __name__ == "__main__":
    run(Config())
