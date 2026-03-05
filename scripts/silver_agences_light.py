"""silver_agences_light.py — PYREGROUPCNT + Secteurs → dim_agences + hierarchie_territoriale.
Phase 3 · GI Data Lakehouse · Manifeste v2.0
# CORRECTIONS DDL (probe 2026-03-05) :
#   Bronze écrit raw_pyregroupcnt (pas raw_agences) → chemin S3 corrigé
#   UG_NOM absent DDL → RGPCNT_LIBELLE (from PYREGROUPCNT) utilisé comme nom agence
#   UG_GPS_LAT/LNG absent → RGPCNT_GPS_LAT/LON (confirmés dans PYREGROUPCNT)
#   UG_CLOTURE → récupéré depuis raw_wtug (table ajoutée en bronze_agences v2)
#                Si raw_wtug absent → UG_CLOTURE_DATE = NULL (graceful fallback)
#   UG_MARQUE/BRANCHE → absent DDL, NULL en attendant table de référence externe
"""
import json
from shared import Config, RunMode, Stats, get_duckdb_connection, logger


def run(cfg: Config) -> dict:
    stats = Stats()
    b = f"s3://{cfg.bucket_bronze}"
    silver_dim = f"s3://{cfg.bucket_silver}/slv_agences/dim_agences"
    silver_hier = f"s3://{cfg.bucket_silver}/slv_agences/hierarchie_territoriale"

    with get_duckdb_connection(cfg) as ddb:
        # dim_agences : source principale = PYREGROUPCNT (nom/adresse/GPS confirmés)
        # Enrichissement optionnel WTUG (cloture). JOIN LEFT OUTER → pas de blocage si absent.
        q_agences = f"""
        WITH raw AS (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY RGPCNT_ID ORDER BY _loaded_at DESC) AS rn
            FROM read_json_auto('{b}/raw_pyregroupcnt/**/*.json',
                                union_by_name=true, hive_partitioning=false)
        ),
        raw_ug AS (
            SELECT RGPCNT_ID, UG_CLOTURE_DATE, PIL_ID, UG_GPS,
                   ROW_NUMBER() OVER (PARTITION BY RGPCNT_ID ORDER BY _loaded_at DESC) AS rn
            FROM read_json_auto('{b}/raw_wtug/**/*.json',
                                union_by_name=true, hive_partitioning=false)
        )
        SELECT
            MD5(r.RGPCNT_ID::VARCHAR)                           AS agence_sk,
            r.RGPCNT_ID::INT                                    AS rgpcnt_id,
            TRIM(r.RGPCNT_LIBELLE)                              AS nom,
            TRIM(r.RGPCNT_CODE)                                 AS code,
            NULL::VARCHAR                                       AS marque,
            NULL::VARCHAR                                       AS branche,
            TRIM(COALESCE(r.RGPCNT_VILLE, ''))                  AS ville,
            TRIM(COALESCE(r.RGPCNT_EMAIL, ''))                  AS email,
            TRY_CAST(r.RGPCNT_GPS_LAT AS DOUBLE)               AS latitude,
            TRY_CAST(r.RGPCNT_GPS_LON AS DOUBLE)               AS longitude,
            COALESCE(TRY_CAST(u.UG_CLOTURE_DATE AS BOOLEAN), false) AS is_cloture,
            NOT COALESCE(TRY_CAST(u.UG_CLOTURE_DATE AS BOOLEAN), false) AS is_active,
            CURRENT_TIMESTAMP                                   AS _loaded_at
        FROM raw r
        LEFT JOIN raw_ug u ON u.RGPCNT_ID::INT = r.RGPCNT_ID::INT AND u.rn = 1
        WHERE r.rn = 1 AND r.RGPCNT_ID IS NOT NULL
        """

        q_hier = f"""
        WITH raw AS (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY ID_UG ORDER BY _loaded_at DESC) AS rn
            FROM read_json_auto('{b}/raw_secteurs/**/*.json',
                                union_by_name=true, hive_partitioning=false)
        )
        SELECT
            TRY_CAST(ID_UG AS INT)              AS rgpcnt_id,
            TRIM(COALESCE(SECTEUR, ''))          AS secteur,
            TRIM(COALESCE(PERIMETRE, ''))        AS perimetre,
            TRIM(COALESCE(ZONE_GEO, ''))         AS zone_geo,
            CURRENT_TIMESTAMP                    AS _loaded_at
        FROM raw WHERE rn = 1 AND ID_UG IS NOT NULL
        """

        try:
            if cfg.mode in (RunMode.OFFLINE, RunMode.PROBE):
                c1 = ddb.execute(
                    f"SELECT COUNT(*) FROM ({q_agences})").fetchone()[0]
                c2 = ddb.execute(
                    f"SELECT COUNT(*) FROM ({q_hier})").fetchone()[0]
                logger.info(json.dumps({"mode": cfg.mode.value,
                                        "dim_agences": c1, "hierarchie": c2}))
            else:
                ddb.execute(
                    f"COPY ({q_agences}) TO '{silver_dim}' (FORMAT PARQUET, OVERWRITE_OR_IGNORE true)")
                ddb.execute(
                    f"COPY ({q_hier}) TO '{silver_hier}' (FORMAT PARQUET, OVERWRITE_OR_IGNORE true)")
                c1 = ddb.execute(
                    f"SELECT COUNT(*) FROM ({q_agences})").fetchone()[0]
                c2 = ddb.execute(
                    f"SELECT COUNT(*) FROM ({q_hier})").fetchone()[0]
                logger.info(json.dumps({"dim_agences": c1, "hierarchie": c2}))
        except Exception as e:
            # raw_wtug peut être absent si bronze_agences pas encore exécuté
            logger.warning(json.dumps(
                {"warning": "raw_wtug may be missing", "error": str(e)}))
            raise

    stats.tables_processed = 2
    stats.rows_transformed = c1 + c2
    stats.extra = {"dim_agences": c1, "hierarchie": c2}
    return stats.finish()


if __name__ == "__main__":
    run(Config())
