"""silver_competences.py — Silver · Union normalisée métiers + habilitations + diplômes + expériences.
Phase 2 · GI Data Lakehouse · Manifeste v2.0
# CORRECTIONS DDL (probe 2026-03-05) :
#   WTPMET      : ORDRE→PMET_ORDRE, PMET_NIVEAU absent DDL → NULL
#   WTPHAB      : PHAB_DATEDEBUT→PHAB_DELIVR, PHAB_DATEFIN→PHAB_EXPIR
#   WTPDIP      : PDIP_DATE confirmé (remplace PDIP_ANNEE)
#   WTEXP       : ORDRE→EXP_ORDRE, EXP_POSTE absent, EXP_ENTREPRISE→EXP_NOM,
#                 EXP_DUREE absent, EXP_DATEDEBUT→EXP_DEBUT, EXP_DATEFIN→EXP_FIN
"""
import json
from shared import Config, RunMode, Stats, get_duckdb_connection, logger


def build_competences_query(cfg: Config) -> str:
    b = f"s3://{cfg.bucket_bronze}"
    return f"""
    WITH metiers AS (
        SELECT
            MD5(CONCAT(PER_ID::VARCHAR, '|METIER|',
                COALESCE(MET_ID::VARCHAR, PMET_ORDRE::VARCHAR))) AS competence_id,
            PER_ID::INT AS per_id,
            'METIER' AS type_competence,
            COALESCE(m.MET_ID::VARCHAR, '') AS code,
            COALESCE(TRIM(r.MET_LIBELLE), 'Métier inconnu') AS libelle,
            NULL::VARCHAR AS niveau,
            NULL::DATE AS date_obtention,
            NULL::DATE AS date_expiration,
            true AS is_active,
            'WTPMET' AS _source_table,
            m._loaded_at
        FROM read_json_auto('{b}/raw_wtpmet/**/*.json', union_by_name=true, hive_partitioning=false) m
        LEFT JOIN read_json_auto('{b}/raw_ref_metiers/**/*.json', union_by_name=true, hive_partitioning=false) r
            ON r.MET_ID = m.MET_ID
    ),
    habilitations AS (
        SELECT
            MD5(CONCAT(PER_ID::VARCHAR, '|HABILITATION|', THAB_ID::VARCHAR)) AS competence_id,
            PER_ID::INT AS per_id,
            'HABILITATION' AS type_competence,
            COALESCE(THAB_ID::VARCHAR, '') AS code,
            COALESCE(TRIM(r.THAB_LIBELLE), 'Habilitation inconnue') AS libelle,
            '' AS niveau,
            TRY_CAST(h.PHAB_DELIVR AS DATE) AS date_obtention,
            TRY_CAST(h.PHAB_EXPIR  AS DATE) AS date_expiration,
            CASE WHEN TRY_CAST(h.PHAB_EXPIR AS DATE) IS NULL
                      OR TRY_CAST(h.PHAB_EXPIR AS DATE) > CURRENT_DATE
                 THEN true ELSE false END AS is_active,
            'WTPHAB' AS _source_table,
            h._loaded_at
        FROM read_json_auto('{b}/raw_wtphab/**/*.json', union_by_name=true, hive_partitioning=false) h
        LEFT JOIN read_json_auto('{b}/raw_ref_habilitations/**/*.json', union_by_name=true, hive_partitioning=false) r
            ON r.THAB_ID = h.THAB_ID
    ),
    diplomes AS (
        SELECT
            MD5(CONCAT(PER_ID::VARCHAR, '|DIPLOME|', TDIP_ID::VARCHAR)) AS competence_id,
            PER_ID::INT AS per_id,
            'DIPLOME' AS type_competence,
            COALESCE(TDIP_ID::VARCHAR, '') AS code,
            COALESCE(TRIM(r.TDIP_LIBELLE), 'Diplôme inconnu') AS libelle,
            '' AS niveau,
            TRY_CAST(d.PDIP_DATE AS DATE) AS date_obtention,
            NULL::DATE AS date_expiration,
            true AS is_active,
            'WTPDIP' AS _source_table,
            d._loaded_at
        FROM read_json_auto('{b}/raw_wtpdip/**/*.json', union_by_name=true, hive_partitioning=false) d
        LEFT JOIN read_json_auto('{b}/raw_ref_diplomes/**/*.json', union_by_name=true, hive_partitioning=false) r
            ON r.TDIP_ID = d.TDIP_ID
    ),
    experiences AS (
        SELECT
            MD5(CONCAT(PER_ID::VARCHAR, '|EXPERIENCE|', EXP_ORDRE::VARCHAR)) AS competence_id,
            PER_ID::INT AS per_id,
            'EXPERIENCE' AS type_competence,
            COALESCE(EXP_ORDRE::VARCHAR, '') AS code,
            TRIM(COALESCE(EXP_NOM, 'Expérience inconnue')) AS libelle,
            NULL::VARCHAR AS niveau,
            TRY_CAST(e.EXP_DEBUT AS DATE) AS date_obtention,
            TRY_CAST(e.EXP_FIN   AS DATE) AS date_expiration,
            true AS is_active,
            'WTEXP' AS _source_table,
            e._loaded_at
        FROM read_json_auto('{b}/raw_wtexp/**/*.json', union_by_name=true, hive_partitioning=false) e
    ),
    all_comp AS (
        SELECT * FROM metiers
        UNION ALL SELECT * FROM habilitations
        UNION ALL SELECT * FROM diplomes
        UNION ALL SELECT * FROM experiences
    ),
    dedup AS (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY competence_id ORDER BY _loaded_at DESC) AS rn
        FROM all_comp
    )
    SELECT competence_id, per_id, type_competence, code, libelle, niveau,
           date_obtention, date_expiration, is_active, _source_table,
           CURRENT_TIMESTAMP AS _loaded_at
    FROM dedup WHERE rn = 1 AND per_id IS NOT NULL
    """


def run(cfg: Config) -> dict:
    stats = Stats()
    silver_path = f"s3://{cfg.bucket_silver}/slv_interimaires/competences"

    with get_duckdb_connection(cfg) as ddb:
        q = build_competences_query(cfg)
        if cfg.mode in (RunMode.OFFLINE, RunMode.PROBE):
            count = ddb.execute(f"SELECT COUNT(*) FROM ({q})").fetchone()[0]
            logger.info(json.dumps(
                {"mode": cfg.mode.value, "table": "competences", "rows": count}))
            stats.rows_transformed = count
        else:
            ddb.execute(
                f"COPY ({q}) TO '{silver_path}' (FORMAT PARQUET, OVERWRITE_OR_IGNORE true)")
            count = ddb.execute(f"SELECT COUNT(*) FROM ({q})").fetchone()[0]
            stats.rows_transformed = count
            logger.info(json.dumps({"table": "competences", "rows": count}))

    stats.tables_processed = 1
    return stats.finish()


if __name__ == "__main__":
    run(Config())
