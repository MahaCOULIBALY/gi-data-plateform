"""silver_competences.py — Bronze → Iceberg OVH · Union normalisée métiers + habilitations + diplômes + expériences.
Phase 2 · GI Data Lakehouse · Manifeste v2.0
# CORRECTIONS DDL (probe 2026-03-05) :
#   WTPMET      : ORDRE→PMET_ORDRE, PMET_NIVEAU absent DDL → NULL
#   WTPHAB      : PHAB_DATEDEBUT→PHAB_DELIVR, PHAB_DATEFIN→PHAB_EXPIR
#   WTPDIP      : PDIP_DATE confirmé (remplace PDIP_ANNEE)
#   WTEXP       : ORDRE→EXP_ORDRE, EXP_POSTE absent, EXP_ENTREPRISE→EXP_NOM,
#                 EXP_DUREE absent, EXP_DATEDEBUT→EXP_DEBUT, EXP_DATEFIN→EXP_FIN
# ENRICHISSEMENT RÉFÉRENTIELS (2026-03-12) :
#   WTMET  : is_active basé sur MET_DELETE (vs hardcode true) + pcs_code (PCS_CODE_2003 INSEE)
#   WTTHAB : date_expiration = COALESCE(PHAB_EXPIR, PHAB_DELIVR + THAB_NBMOIS mois)
#            is_active recalculé sur date_expiration composite
#   WTTDIP : niveau = TDIP_REF (catégorie/niveau diplôme)
"""
import json
from shared import Config, Stats, get_duckdb_connection, write_silver_iceberg, logger


def build_competences_query(cfg: Config) -> str:
    b = f"s3://{cfg.bucket_bronze}"
    return f"""
    WITH metiers AS (
        SELECT
            MD5(CONCAT(PER_ID::VARCHAR, '|METIER|',
                COALESCE(r.MET_ID::VARCHAR, PMET_ORDRE::VARCHAR))) AS competence_id,
            PER_ID::INT AS per_id,
            'METIER' AS type_competence,
            COALESCE(m.MET_ID::VARCHAR, '') AS code,
            COALESCE(TRIM(r.MET_LIBELLE), 'Métier inconnu') AS libelle,
            NULL::VARCHAR AS niveau,
            NULL::DATE AS date_obtention,
            NULL::DATE AS date_expiration,
            -- MET_DELETE : 1 = supprimé logiquement, NULL/0 = actif
            COALESCE(TRY_CAST(r.MET_DELETE AS INT), 0) = 0 AS is_active,
            NULLIF(TRIM(r.PCS_CODE_2003::VARCHAR), '') AS pcs_code,
            'WTPMET' AS _source_table,
            m._loaded_at
        FROM read_json_auto('{b}/raw_wtpmet/**/*.json', union_by_name=true, hive_partitioning=false) m
        LEFT JOIN read_json_auto('{b}/raw_wtmet/**/*.json', union_by_name=true, hive_partitioning=false) r
            ON r.MET_ID = m.MET_ID
    ),
    habilitations AS (
        -- date_expiration = PHAB_EXPIR explicite OU date théorique (PHAB_DELIVR + THAB_NBMOIS mois)
        SELECT
            MD5(CONCAT(PER_ID::VARCHAR, '|HABILITATION|', THAB_ID::VARCHAR)) AS competence_id,
            PER_ID::INT AS per_id,
            'HABILITATION' AS type_competence,
            COALESCE(THAB_ID::VARCHAR, '') AS code,
            COALESCE(TRIM(h.THAB_LIBELLE), 'Habilitation inconnue') AS libelle,
            '' AS niveau,
            TRY_CAST(h.PHAB_DELIVR AS DATE) AS date_obtention,
            exp AS date_expiration,
            CASE WHEN exp IS NULL OR exp > CURRENT_DATE THEN true ELSE false END AS is_active,
            NULL::VARCHAR AS pcs_code,
            'WTPHAB' AS _source_table,
            h._loaded_at
        FROM (
            SELECT
                h.*,
                r.THAB_LIBELLE,
                COALESCE(
                    TRY_CAST(h.PHAB_EXPIR AS DATE),
                    CASE WHEN TRY_CAST(r.THAB_NBMOIS AS INT) IS NOT NULL
                              AND TRY_CAST(h.PHAB_DELIVR AS DATE) IS NOT NULL
                         THEN TRY_CAST(h.PHAB_DELIVR AS DATE)
                              + (TRY_CAST(r.THAB_NBMOIS AS INT) * INTERVAL '1 month')
                         ELSE NULL END
                ) AS exp
            FROM read_json_auto('{b}/raw_wtphab/**/*.json', union_by_name=true, hive_partitioning=false) h
            LEFT JOIN read_json_auto('{b}/raw_wtthab/**/*.json', union_by_name=true, hive_partitioning=false) r
                ON r.THAB_ID = h.THAB_ID
        ) h
    ),
    diplomes AS (
        SELECT
            MD5(CONCAT(PER_ID::VARCHAR, '|DIPLOME|', d.TDIP_ID::VARCHAR)) AS competence_id,
            PER_ID::INT AS per_id,
            'DIPLOME' AS type_competence,
            COALESCE(d.TDIP_ID::VARCHAR, '') AS code,
            COALESCE(TRIM(r.TDIP_LIB), 'Diplôme inconnu') AS libelle,
            -- TDIP_REF = catégorie/niveau diplôme (référentiel)
            NULLIF(TRY_CAST(r.TDIP_REF AS VARCHAR), '') AS niveau,
            TRY_CAST(d.PDIP_DATE AS DATE) AS date_obtention,
            NULL::DATE AS date_expiration,
            true AS is_active,
            NULL::VARCHAR AS pcs_code,
            'WTPDIP' AS _source_table,
            d._loaded_at
        FROM read_json_auto('{b}/raw_wtpdip/**/*.json', union_by_name=true, hive_partitioning=false) d
        LEFT JOIN read_json_auto('{b}/raw_wttdip/**/*.json', union_by_name=true, hive_partitioning=false) r
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
            NULL::VARCHAR AS pcs_code,
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
           date_obtention, date_expiration, is_active, pcs_code, _source_table,
           CURRENT_TIMESTAMP AS _loaded_at
    FROM dedup WHERE rn = 1 AND per_id IS NOT NULL
    """


def run(cfg: Config) -> dict:
    stats = Stats()
    try:
        with get_duckdb_connection(cfg) as ddb:
            q = build_competences_query(cfg)
            count = write_silver_iceberg(ddb, q, "silver.interimaires.competences", cfg, stats)
            stats.rows_transformed = count
    except Exception as e:
        logger.exception(json.dumps({"table": "competences", "error": str(e)}))
        stats.errors.append({"table": "competences", "error": str(e)})
    stats.tables_processed = 1
    return stats.finish()


if __name__ == "__main__":
    run(Config())
