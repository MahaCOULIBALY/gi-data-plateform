"""silver_agences_light.py — PYREGROUPECNT + Secteurs → dim_agences + hierarchie_territoriale.
Phase 3 · GI Data Lakehouse · Manifeste v2.0
# CORRECTIONS DDL (probe 2026-03-05) :
#   Bronze écrit raw_pyregroupecnt (PYREGROUPECNT — E manquant corrigé)
#   UG_NOM absent DDL → RGPCNT_LIBELLE (from PYREGROUPECNT) utilisé comme nom agence
#   UG_GPS_LAT/LNG absent → RGPCNT_GPS_LAT/LON (confirmés dans PYREGROUPECNT)
#   UG_CLOTURE → récupéré depuis raw_wtug (table ajoutée en bronze_agences v2)
#                Si raw_wtug absent → UG_CLOTURE_DATE = NULL (graceful fallback)
#   UG_MARQUE/BRANCHE → absent DDL, NULL en attendant table de référence externe
# CORRECTIONS DDL (2026-03-11) :
#   raw_pyregroupcnt → raw_pyregroupecnt (E manquant — aligné bronze_agences)
#   RGPCNT_VILLE/EMAIL/GPS_LAT/GPS_LON absents DDL_EVOLIA_FILTERED → NULL (colonnes fantômes)
#   DATE_CLOTURE ajouté PYREGROUPECNT Bronze → is_cloture/is_active depuis source de vérité
#   TRY_CAST(UG_CLOTURE_DATE AS BOOLEAN) supprimé (cast incohérent, DATE_CLOTURE suffisant)
#   c1/c2 initialisés à 0 avant try — UnboundLocalError éliminé
#   COUNT post-COPY via read_parquet (évite double exécution de la transformation)
"""
import json
from shared import Config, RunMode, Stats, get_duckdb_connection, logger


def run(cfg: Config) -> dict:
    stats = Stats()
    b = f"s3://{cfg.bucket_bronze}"
    silver_dim = f"s3://{cfg.bucket_silver}/slv_agences/dim_agences"
    silver_hier = f"s3://{cfg.bucket_silver}/slv_agences/hierarchie_territoriale"

    with get_duckdb_connection(cfg) as ddb:
        q_agences = f"""
        WITH raw AS (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY RGPCNT_ID ORDER BY _loaded_at DESC) AS rn
            FROM read_json_auto('{b}/raw_pyregroupecnt/**/*.json',
                                union_by_name=true, hive_partitioning=false)
        )
        SELECT
            MD5(r.RGPCNT_ID::VARCHAR)                           AS agence_sk,
            r.RGPCNT_ID::INT                                    AS rgpcnt_id,
            TRIM(r.RGPCNT_LIBELLE)                              AS nom,
            TRIM(r.RGPCNT_CODE)                                 AS code,
            NULL::VARCHAR                                       AS marque,
            NULL::VARCHAR                                       AS branche,
            NULL::VARCHAR                                       AS ville,
            NULL::VARCHAR                                       AS email,
            NULL::DOUBLE                                        AS latitude,
            NULL::DOUBLE                                        AS longitude,
            r.DATE_CLOTURE IS NOT NULL                          AS is_cloture,
            r.DATE_CLOTURE IS NULL                              AS is_active,
            TRY_CAST(r.DATE_CLOTURE AS DATE)                    AS cloture_date,
            CURRENT_TIMESTAMP                                   AS _loaded_at
        FROM raw r
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

        c1, c2 = 0, 0
        # dim_agences — obligatoire
        if cfg.mode in (RunMode.OFFLINE, RunMode.PROBE):
            row1 = ddb.execute(f"SELECT COUNT(*) FROM ({q_agences})").fetchone()
            c1 = row1[0] if row1 else 0
        else:
            ddb.execute(
                f"COPY ({q_agences}) TO '{silver_dim}' (FORMAT PARQUET, OVERWRITE_OR_IGNORE true)")
            c1 = ddb.execute(
                f"SELECT COUNT(*) FROM read_parquet('{silver_dim}/**/*.parquet')").fetchone()[0]

        # hierarchie_territoriale — optionnel (raw_secteurs = référentiel externe non garanti)
        try:
            if cfg.mode in (RunMode.OFFLINE, RunMode.PROBE):
                row2 = ddb.execute(f"SELECT COUNT(*) FROM ({q_hier})").fetchone()
                c2 = row2[0] if row2 else 0
            else:
                ddb.execute(
                    f"COPY ({q_hier}) TO '{silver_hier}' (FORMAT PARQUET, OVERWRITE_OR_IGNORE true)")
                c2 = ddb.execute(
                    f"SELECT COUNT(*) FROM read_parquet('{silver_hier}/**/*.parquet')").fetchone()[0]
        except Exception as e:
            # raw_secteurs absent (référentiel externe) → hiérarchie ignorée ce run
            logger.warning(json.dumps({"warning": "hierarchie_territoriale skipped — raw_secteurs absent",
                                       "error": str(e)}))

        logger.info(json.dumps({"mode": cfg.mode.value if cfg.mode in (RunMode.OFFLINE, RunMode.PROBE) else "live",
                                "dim_agences": c1, "hierarchie": c2}))

    stats.tables_processed = 2
    stats.rows_transformed = c1 + c2
    stats.extra = {"dim_agences": c1, "hierarchie": c2}
    return stats.finish()


if __name__ == "__main__":
    run(Config())
