"""gold_qualite_missions.py — Silver → Gold : 4 tables performance/qualité missions.
Phase 4 · GI Data Lakehouse · Manifeste v4.0
Tables Gold :
  gld_performance.fact_rupture_contrat       ← slv_missions/fin_mission + missions
  gld_performance.fact_duree_mission         ← slv_missions/missions + contrats
  gld_performance.fact_coeff_facturation     ← slv_missions/contrats + slv_temps
  gld_commercial.fact_renouvellement_mission ← slv_missions/missions + contrats

=== CORRECTIONS SESSION 3 (audit Gold) ===
- G-QM-B01 : TRUNCATE avant pg_bulk_insert (idempotence PG)
- G-QM-B02 : rgpcnt_id, tie_id, date_debut absents de fin_mission →
             JOIN slv_missions/missions sur per_id + cnt_id pour ces colonnes
- G-QM-M01 : RunMode importé + guards OFFLINE/PROBE
- G-QM-M02 : try/except dans run()

# PHASE 2 (2026-03-26) :
# fact_duree_mission    : DMM par agence/client/métier/mois — profil MICRO/COURTE/MOYENNE/LONGUE
# fact_coeff_facturation: coeff THF/THP pondéré par heures, niveau BON/MOYEN/A_REVOIR
# fact_renouvellement_mission : taux de reconduction chez même client (LAG date_fin, fenêtre 7j)

# MIGRÉ : iceberg_scan → read_parquet(s3://gi-poc-silver/slv_*) (D01)
"""
import json

from shared import (
    Config, RunMode, Stats,
    get_duckdb_connection, get_pg_connection, pg_bulk_insert,
    filter_tables, logger,
)

PIPELINE = "gold_qualite_missions"

_RUPTURE_COLS = [
    "agence_id", "tie_id", "mois",
    "nb_missions_total", "nb_ruptures", "nb_annulations", "nb_terme_normal",
    "taux_rupture_pct", "taux_fin_anticipee_pct", "duree_moy_avant_rupture",
]
_DMM_COLS = [
    "agence_id", "tie_id", "metier_id", "mois",
    "nb_missions", "dmm_jours", "dmm_semaines", "duree_mediane_jours",
    "nb_missions_1j", "nb_missions_1semaine", "nb_missions_1mois", "nb_missions_long",
    "profil_duree",
]
_COEFF_COLS = [
    "agence_id", "tie_id", "metier_id", "mois",
    "nb_missions", "coeff_moyen_pondere", "coeff_moyen_simple",
    "thf_moyen", "thp_moyen", "marge_coeff_pct", "niveau_coeff",
]
_RENOUVELLEMENT_COLS = [
    "agence_id", "tie_id", "mois",
    "nb_missions", "nb_renouvellements", "taux_renouvellement_pct", "ecart_moyen_jours",
]


def build_rupture_contrat_query(cfg: Config) -> str:
    slv = f"s3://{cfg.bucket_silver}"
    return f"""
    -- G-QM-B02 : fin_mission contient per_id/cnt_id + statut_fin_mission/duree_reelle_jours
    -- JOIN missions pour rgpcnt_id, tie_id, date_debut
    SELECT
        m.rgpcnt_id::INT                                        AS agence_id,
        m.tie_id::INT                                           AS tie_id,
        DATE_TRUNC('month', TRY_CAST(m.date_debut AS DATE))     AS mois,
        COUNT(*)                                                AS nb_missions_total,
        SUM(CASE WHEN fm.statut_fin_mission = 'RUPTURE'
                 THEN 1 ELSE 0 END)                             AS nb_ruptures,
        SUM(CASE WHEN fm.statut_fin_mission = 'ANNULEE'
                 THEN 1 ELSE 0 END)                             AS nb_annulations,
        SUM(CASE WHEN fm.statut_fin_mission = 'TERME_NORMAL'
                 THEN 1 ELSE 0 END)                             AS nb_terme_normal,
        ROUND(
            SUM(CASE WHEN fm.statut_fin_mission = 'RUPTURE'
                     THEN 1 ELSE 0 END)::FLOAT / NULLIF(COUNT(*), 0) * 100
        , 1)                                                    AS taux_rupture_pct,
        ROUND(
            SUM(CASE WHEN fm.statut_fin_mission IN ('RUPTURE', 'ANNULEE')
                     THEN 1 ELSE 0 END)::FLOAT / NULLIF(COUNT(*), 0) * 100
        , 1)                                                    AS taux_fin_anticipee_pct,
        ROUND(
            AVG(CASE WHEN fm.statut_fin_mission = 'RUPTURE'
                     THEN fm.duree_reelle_jours END)
        , 1)                                                    AS duree_moy_avant_rupture
    FROM read_parquet('{slv}/slv_missions/fin_mission/**/*.parquet') fm
    INNER JOIN read_parquet('{slv}/slv_missions/missions/**/*.parquet') m
        ON m.per_id = fm.per_id AND m.cnt_id = fm.cnt_id
    WHERE m.date_debut IS NOT NULL
      AND fm.statut_fin_mission != 'EN_COURS'
    GROUP BY 1, 2, 3
    """


def build_duree_mission_query(cfg: Config) -> str:
    """DMM (Durée Moyenne des Missions) par agence/client/métier/mois.
    Source : slv_missions/contrats (date_debut/fin canoniques + met_id)
             + slv_missions/missions (rgpcnt_id, tie_id).
    Filtre : durée > 0 uniquement (exclut les annulations et missions 0-jour).
    """
    slv = f"s3://{cfg.bucket_silver}"
    return f"""
    WITH missions AS (
        SELECT per_id, cnt_id, rgpcnt_id::INT AS rgpcnt_id, tie_id::INT AS tie_id
        FROM read_parquet('{slv}/slv_missions/missions/**/*.parquet')
        WHERE per_id IS NOT NULL AND cnt_id IS NOT NULL
        QUALIFY ROW_NUMBER() OVER (PARTITION BY per_id, cnt_id ORDER BY _loaded_at DESC NULLS LAST) = 1
    ),
    contrats AS (
        SELECT per_id, cnt_id,
               TRY_CAST(met_id AS INT)        AS met_id,
               TRY_CAST(date_debut AS DATE)   AS date_debut,
               TRY_CAST(date_fin   AS DATE)   AS date_fin
        FROM read_parquet('{slv}/slv_missions/contrats/**/*.parquet')
        WHERE date_debut IS NOT NULL AND date_fin IS NOT NULL
        QUALIFY ROW_NUMBER() OVER (PARTITION BY per_id, cnt_id ORDER BY _loaded_at DESC NULLS LAST) = 1
    ),
    base AS (
        SELECT
            m.rgpcnt_id                                             AS agence_id,
            m.tie_id                                                AS tie_id,
            c.met_id                                                AS metier_id,
            DATE_TRUNC('month', c.date_debut)::DATE                AS mois,
            DATEDIFF('day', c.date_debut, c.date_fin)              AS duree_jours
        FROM missions m
        INNER JOIN contrats c ON c.per_id = m.per_id AND c.cnt_id = m.cnt_id
        WHERE DATEDIFF('day', c.date_debut, c.date_fin) > 0
    )
    SELECT
        agence_id, tie_id, metier_id, mois,
        COUNT(*)                                                    AS nb_missions,
        ROUND(AVG(duree_jours), 1)::DECIMAL(10,1)                  AS dmm_jours,
        ROUND(AVG(duree_jours) / 7.0, 1)::DECIMAL(10,1)           AS dmm_semaines,
        ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP
              (ORDER BY duree_jours), 1)::DECIMAL(10,1)            AS duree_mediane_jours,
        SUM(CASE WHEN duree_jours <= 1  THEN 1 ELSE 0 END)         AS nb_missions_1j,
        SUM(CASE WHEN duree_jours <= 7  THEN 1 ELSE 0 END)         AS nb_missions_1semaine,
        SUM(CASE WHEN duree_jours <= 30 THEN 1 ELSE 0 END)         AS nb_missions_1mois,
        SUM(CASE WHEN duree_jours > 30  THEN 1 ELSE 0 END)         AS nb_missions_long,
        CASE
            WHEN AVG(duree_jours) <= 1  THEN 'MICRO'
            WHEN AVG(duree_jours) <= 7  THEN 'COURTE'
            WHEN AVG(duree_jours) <= 30 THEN 'MOYENNE'
            ELSE 'LONGUE'
        END                                                         AS profil_duree
    FROM base
    WHERE mois IS NOT NULL
    GROUP BY 1, 2, 3, 4
    ORDER BY mois DESC, agence_id, nb_missions DESC
    """


def build_coeff_facturation_query(cfg: Config) -> str:
    """Coefficient de facturation (THF / THP) pondéré par les heures réelles.
    Source : slv_missions/contrats (taux_paye, taux_fact)
             + slv_temps/releves_heures + heures_detail (heures_totales).
    Filtre : taux_paye > 0 ET taux_fact > 0 uniquement.
    """
    slv = f"s3://{cfg.bucket_silver}"
    return f"""
    WITH missions AS (
        SELECT per_id, cnt_id, rgpcnt_id::INT AS rgpcnt_id, tie_id::INT AS tie_id
        FROM read_parquet('{slv}/slv_missions/missions/**/*.parquet')
        WHERE per_id IS NOT NULL AND cnt_id IS NOT NULL
        QUALIFY ROW_NUMBER() OVER (PARTITION BY per_id, cnt_id ORDER BY _loaded_at DESC NULLS LAST) = 1
    ),
    contrats AS (
        SELECT per_id, cnt_id,
               TRY_CAST(met_id AS INT)            AS met_id,
               TRY_CAST(date_debut AS DATE)       AS date_debut,
               taux_paye::DECIMAL(10,4)           AS taux_paye,
               taux_fact::DECIMAL(10,4)           AS taux_fact
        FROM read_parquet('{slv}/slv_missions/contrats/**/*.parquet')
        WHERE taux_paye > 0 AND taux_fact > 0
        QUALIFY ROW_NUMBER() OVER (PARTITION BY per_id, cnt_id ORDER BY _loaded_at DESC NULLS LAST) = 1
    ),
    heures AS (
        SELECT r.per_id, r.cnt_id, COALESCE(SUM(h.base_paye::DECIMAL(10,2)), 0) AS heures_totales
        FROM read_parquet('{slv}/slv_temps/releves_heures/**/*.parquet') r
        LEFT JOIN read_parquet('{slv}/slv_temps/heures_detail/**/*.parquet') h
            ON h.prh_bts = r.prh_bts
        WHERE r.per_id IS NOT NULL AND r.cnt_id IS NOT NULL
        GROUP BY r.per_id, r.cnt_id
    )
    SELECT
        m.rgpcnt_id                                                AS agence_id,
        m.tie_id                                                   AS tie_id,
        c.met_id                                                   AS metier_id,
        DATE_TRUNC('month', c.date_debut)::DATE                   AS mois,
        COUNT(*)                                                   AS nb_missions,
        ROUND(
            SUM(c.taux_fact * COALESCE(h.heures_totales, 0))
            / NULLIF(SUM(c.taux_paye * COALESCE(h.heures_totales, 0)), 0)
        , 3)::DECIMAL(10,3)                                        AS coeff_moyen_pondere,
        ROUND(AVG(c.taux_fact / NULLIF(c.taux_paye, 0)), 3)
             ::DECIMAL(10,3)                                       AS coeff_moyen_simple,
        ROUND(AVG(c.taux_fact), 2)::DECIMAL(10,2)                 AS thf_moyen,
        ROUND(AVG(c.taux_paye), 2)::DECIMAL(10,2)                 AS thp_moyen,
        ROUND((
            SUM(c.taux_fact * COALESCE(h.heures_totales, 0))
            / NULLIF(SUM(c.taux_paye * COALESCE(h.heures_totales, 0)), 0) - 1
        ) * 100, 1)::DECIMAL(8,1)                                 AS marge_coeff_pct,
        CASE
            WHEN AVG(c.taux_fact / NULLIF(c.taux_paye, 0)) >= 1.35 THEN 'BON'
            WHEN AVG(c.taux_fact / NULLIF(c.taux_paye, 0)) >= 1.25 THEN 'MOYEN'
            ELSE 'A_REVOIR'
        END                                                        AS niveau_coeff
    FROM missions m
    INNER JOIN contrats c ON c.per_id = m.per_id AND c.cnt_id = m.cnt_id
    LEFT JOIN  heures h   ON h.per_id = m.per_id AND h.cnt_id = m.cnt_id
    WHERE c.date_debut IS NOT NULL
    GROUP BY 1, 2, 3, 4
    ORDER BY mois DESC, agence_id, nb_missions DESC
    """


def build_renouvellement_mission_query(cfg: Config) -> str:
    """Taux de renouvellement : part des missions reconduites chez le même client.
    Définition : mission reconduite = même (per_id, tie_id, agence_id) avec
    date_debut dans les 7 jours suivant date_fin de la mission précédente (LAG).
    """
    slv = f"s3://{cfg.bucket_silver}"
    return f"""
    WITH base AS (
        SELECT
            m.per_id::INT                               AS per_id,
            m.tie_id::INT                               AS tie_id,
            m.rgpcnt_id::INT                            AS agence_id,
            TRY_CAST(c.date_debut AS DATE)              AS date_debut,
            TRY_CAST(c.date_fin   AS DATE)              AS date_fin
        FROM read_parquet('{slv}/slv_missions/missions/**/*.parquet') m
        INNER JOIN read_parquet('{slv}/slv_missions/contrats/**/*.parquet') c
            ON c.per_id = m.per_id AND c.cnt_id = m.cnt_id
        WHERE c.date_debut IS NOT NULL AND c.date_fin IS NOT NULL
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY m.per_id, m.cnt_id ORDER BY m._loaded_at DESC NULLS LAST
        ) = 1
    ),
    avec_lag AS (
        SELECT *,
            LAG(date_fin) OVER (
                PARTITION BY per_id, tie_id, agence_id
                ORDER BY date_debut
            )                                           AS date_fin_precedente
        FROM base
    )
    SELECT
        agence_id, tie_id,
        DATE_TRUNC('month', date_debut)::DATE           AS mois,
        COUNT(*)                                        AS nb_missions,
        SUM(CASE
            WHEN date_fin_precedente IS NOT NULL
             AND DATEDIFF('day', date_fin_precedente, date_debut) BETWEEN 0 AND 7
            THEN 1 ELSE 0 END)                          AS nb_renouvellements,
        ROUND(
            SUM(CASE
                WHEN date_fin_precedente IS NOT NULL
                 AND DATEDIFF('day', date_fin_precedente, date_debut) BETWEEN 0 AND 7
                THEN 1 ELSE 0 END)::FLOAT
            / NULLIF(COUNT(*), 0) * 100
        , 1)::DECIMAL(6,1)                              AS taux_renouvellement_pct,
        ROUND(AVG(CASE
            WHEN date_fin_precedente IS NOT NULL
             AND DATEDIFF('day', date_fin_precedente, date_debut) BETWEEN 0 AND 7
            THEN DATEDIFF('day', date_fin_precedente, date_debut)
        END), 0)::DECIMAL(6,0)                          AS ecart_moyen_jours
    FROM avec_lag
    WHERE date_debut IS NOT NULL
    GROUP BY 1, 2, 3
    ORDER BY mois DESC, agence_id, nb_missions DESC
    """


# TABLE_MAP : (query_builder, schema_gold, colonnes)
_TABLE_MAP = {
    "fact_rupture_contrat": (
        build_rupture_contrat_query, "gld_performance", _RUPTURE_COLS,
    ),
    "fact_duree_mission": (
        build_duree_mission_query, "gld_performance", _DMM_COLS,
    ),
    "fact_coeff_facturation": (
        build_coeff_facturation_query, "gld_performance", _COEFF_COLS,
    ),
    "fact_renouvellement_mission": (
        build_renouvellement_mission_query, "gld_commercial", _RENOUVELLEMENT_COLS,
    ),
}


def run(cfg: Config) -> dict:
    stats = Stats()
    active = filter_tables(list(_TABLE_MAP), cfg)

    if cfg.mode in (RunMode.OFFLINE, RunMode.PROBE):  # G-QM-M01
        for name in active:
            logger.info(json.dumps(
                {"mode": cfg.mode.name, "table": name, "action": "skipped"}))
            stats.tables_processed += 1
        return stats.finish(cfg, PIPELINE)

    # --- DuckDB build ---
    rows_map: dict[str, list] = {}
    try:
        with get_duckdb_connection(cfg) as ddb:
            for name in active:
                query_fn, _, _ = _TABLE_MAP[name]
                rows_map[name] = ddb.execute(query_fn(cfg)).fetchall()
                logger.info(json.dumps({"table": name, "rows": len(rows_map[name])}))
    except Exception as e:  # G-QM-M02
        logger.exception(json.dumps({"step": "duckdb_build", "error": str(e)}))
        stats.errors.append({"step": "duckdb_build", "error": str(e)})
        return stats.finish(cfg, PIPELINE)

    # --- PostgreSQL write ---
    try:
        with get_pg_connection(cfg) as pg:
            for name in active:
                if name not in rows_map:
                    continue
                _, schema, cols = _TABLE_MAP[name]
                try:
                    with pg.cursor() as cur:
                        cur.execute(
                            f"TRUNCATE TABLE {schema}.{name}")  # G-QM-B01
                    pg.commit()
                    pg_bulk_insert(cfg, pg, schema, name,
                                   cols, rows_map[name], stats)
                    stats.tables_processed += 1
                    stats.rows_transformed += len(rows_map[name])
                    stats.extra[f"{name}_rows"] = len(rows_map[name])
                    logger.info(json.dumps(
                        {"table": name, "rows": len(rows_map[name]), "status": "ok"}))
                except Exception as e:
                    logger.exception(json.dumps({"table": name, "error": str(e)}))
                    stats.errors.append({"table": name, "error": str(e)})
    except Exception as e:  # G-QM-M02
        logger.exception(json.dumps({"step": "pg_connect", "error": str(e)}))
        stats.errors.append({"step": "pg_connect", "error": str(e)})

    return stats.finish(cfg, PIPELINE)


if __name__ == "__main__":
    run(Config())
