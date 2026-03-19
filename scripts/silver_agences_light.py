"""silver_agences_light.py — PYREGROUPECNT + [Agence Gestion] + Secteurs → Iceberg OVH (dim_agences + hierarchie_territoriale).
Phase 3 · GI Data Lakehouse · Manifeste v2.0
# CORRECTIONS DDL (probe 2026-03-05) :
# Bronze écrit raw_pyregroupecnt (PYREGROUPECNT — E manquant corrigé)
# UG_NOM absent DDL → RGPCNT_LIBELLE (from PYREGROUPECNT) utilisé comme nom agence
# UG_MARQUE/BRANCHE → absent DDL WTUG — résolu via [Agence Gestion].ID_UG (2026-03-14)
# CORRECTIONS DDL (2026-03-11) :
# RGPCNT_VILLE/EMAIL/GPS_LAT/GPS_LON absents DDL_EVOLIA_FILTERED → NULL (colonnes fantômes)
# DATE_CLOTURE ajouté PYREGROUPECNT Bronze → is_cloture/is_active depuis source de vérité
# CORRECTIONS (2026-03-14) :
# DT-04 résolu : marque/branche/nom_commercial/code_comm depuis raw_agence_gestion (ID_UG = RGPCNT_ID)
# hierarchie_territoriale réactivée : jointure [Agence Gestion].NOM_UG ↔ Secteurs.[Agence de gestion]
# Secteurs.ID_UG absent DDL → pont via NOM_UG (INNER JOIN, LOWER+TRIM, non-bloquant si absent)
# raw_secteurs et raw_agence_gestion : optionnels (graceful fallback si Bronze absent)
"""
import json
from shared import Config, Stats, get_duckdb_connection, write_silver_iceberg, logger


def run(cfg: Config) -> dict:
    stats = Stats()
    b = f"s3://{cfg.bucket_bronze}"

    with get_duckdb_connection(cfg) as ddb:

        # ── dim_agences ───────────────────────────────────────────────────────
        # DT-04 résolu : LEFT JOIN raw_agence_gestion pour marque/branche/nom_commercial/code_comm
        # Graceful fallback : si raw_agence_gestion absent → colonnes NULL (pas de crash)
        q_agences = f"""
        WITH raw AS (
            SELECT *,
                   ROW_NUMBER() OVER (PARTITION BY RGPCNT_ID ORDER BY _loaded_at DESC) AS rn
            FROM read_json_auto('{b}/raw_pyregroupecnt/**/*.json',
                                union_by_name=true, hive_partitioning=false)
        ),
        agence_gest AS (
            SELECT
                ID_UG::INT                  AS rgpcnt_id,
                TRIM(MARQUE)                AS marque,
                TRIM(BRANCHE)               AS branche,
                TRIM(NOM_COMMERCIAL)        AS nom_commercial,
                TRIM(CODE_COMM)             AS code_comm,
                ROW_NUMBER() OVER (
                    PARTITION BY ID_UG ORDER BY DATE_UG_GEST DESC
                ) AS rn_ag
            FROM read_json_auto('{b}/raw_agence_gestion/**/*.json',
                                union_by_name=true, hive_partitioning=false)
            QUALIFY rn_ag = 1
        )
        SELECT
            MD5(r.RGPCNT_ID::VARCHAR)           AS agence_sk,
            r.RGPCNT_ID::INT                    AS rgpcnt_id,
            TRIM(r.RGPCNT_LIBELLE)              AS nom,
            TRIM(r.RGPCNT_CODE)                 AS code,
            NULLIF(TRIM(COALESCE(ag.marque,        '')), '') AS marque,
            NULLIF(TRIM(COALESCE(ag.branche,       '')), '') AS branche,
            ag.nom_commercial                   AS nom_commercial,
            ag.code_comm                        AS code_comm,
            NULL::VARCHAR                       AS ville,
            NULL::VARCHAR                       AS email,
            NULL::DOUBLE                        AS latitude,
            NULL::DOUBLE                        AS longitude,
            r.DATE_CLOTURE IS NOT NULL          AS is_cloture,
            r.DATE_CLOTURE IS NULL              AS is_active,
            TRY_CAST(r.DATE_CLOTURE AS DATE)    AS cloture_date,
            CURRENT_TIMESTAMP                   AS _loaded_at
        FROM raw r
        LEFT JOIN agence_gest ag ON ag.rgpcnt_id = r.RGPCNT_ID::INT
        WHERE r.rn = 1 AND r.RGPCNT_ID IS NOT NULL
        """

        # ── hierarchie_territoriale ───────────────────────────────────────────
        # Pont : [Agence Gestion].NOM_UG ↔ Secteurs.[Agence de gestion] (LOWER+TRIM)
        # INNER JOIN : seules les agences avec correspondance confirmée sont incluses
        # Secteurs n'a pas d'ID_UG → linkage texte assumé, LEFT JOIN depuis agence_gest
        q_hier = f"""
        WITH agence_gest AS (
            SELECT
                ID_UG::INT                  AS rgpcnt_id,
                LOWER(TRIM(NOM_UG))         AS nom_ug_norm
            FROM read_json_auto('{b}/raw_agence_gestion/**/*.json',
                                union_by_name=true, hive_partitioning=false)
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY ID_UG ORDER BY DATE_UG_GEST DESC
            ) = 1
        ),
        secteurs AS (
            SELECT
                LOWER(TRIM("Agence de gestion")) AS nom_ug_norm,
                TRIM(Secteur)                    AS secteur,
                TRIM(Périmètre)                  AS perimetre,
                TRIM("Zone Géographique")        AS zone_geo
            FROM read_json_auto('{b}/raw_secteurs/**/*.json',
                                union_by_name=true, hive_partitioning=false)
        )
        SELECT
            ag.rgpcnt_id,
            sec.secteur,
            sec.perimetre,
            sec.zone_geo,
            CURRENT_TIMESTAMP AS _loaded_at
        FROM agence_gest ag
        INNER JOIN secteurs sec ON sec.nom_ug_norm = ag.nom_ug_norm
        WHERE ag.rgpcnt_id IS NOT NULL
        """

        c1, c2 = 0, 0

        # ── dim_agences — obligatoire ─────────────────────────────────────────
        try:
            c1 = write_silver_iceberg(ddb, q_agences, "silver.agences.dim_agences", cfg, stats)
        except Exception as e:
            logger.exception(json.dumps({"table": "dim_agences", "error": str(e)}))
            stats.errors.append({"table": "dim_agences", "error": str(e)})

        # ── hierarchie_territoriale — optionnel ───────────────────────────────
        # raw_agence_gestion ou raw_secteurs absents → warning non-bloquant
        try:
            c2 = write_silver_iceberg(ddb, q_hier, "silver.agences.hierarchie_territoriale", cfg, stats)
        except Exception as e:
            logger.warning(json.dumps({
                "warning": "hierarchie_territoriale skipped — raw_agence_gestion ou raw_secteurs absent",
                "error": str(e),
            }))

        logger.info(json.dumps({
            "mode": cfg.mode.value,
            "dim_agences": c1,
            "hierarchie": c2,
        }))

        stats.tables_processed = 2
        stats.rows_transformed = c1 + c2
        stats.extra = {"dim_agences": c1, "hierarchie": c2}

    return stats.finish()


if __name__ == "__main__":
    run(Config())
