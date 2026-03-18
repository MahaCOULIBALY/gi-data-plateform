"""gold_qualite_missions.py — Silver → Gold fact_rupture_contrat.
Phase 4 · GI Data Lakehouse · Manifeste v4.0

Tables Gold produites :
  gld_performance.fact_rupture_contrat  ← slv_missions/fin_mission

Dépendances :
  - silver_missions.py → slv_missions/fin_mission (FIN_MISSION)
  - Bronze : WTMISS (MISS_ANNULE ajouté 2026-03-15) + WTFINMISS (ingéré 2026-03-15)

Notes :
  - Blocker B1 (FINMISS_LIBELLE non standardisés) : classification LIKE MVP.
    Valider SELECT DISTINCT finmiss_code, finmiss_libelle FROM fin_mission après probe.
  - Blocker B2 (MISS_ANNULE) : probe WTMISS GROUP BY MISS_ANNULE requis avant déploiement prod.
  - La table Gold ne contient que les missions clôturées (statut_fin_mission != 'EN_COURS').
"""
import logging
from shared import Config, Stats, get_duckdb_connection, get_pg_connection, pg_bulk_insert, logger


def build_rupture_contrat_query(cfg: Config) -> str:
    silver = f"s3://{cfg.bucket_silver}"
    return f"""
    SELECT
        sm.rgpcnt_id::INT                                        AS agence_id,
        sm.tie_id::INT                                          AS tie_id,
        DATE_TRUNC('month', TRY_CAST(sm.date_debut AS DATE))    AS mois,
        COUNT(*)                                                AS nb_missions_total,
        SUM(CASE WHEN sm.statut_fin_mission = 'RUPTURE'
                 THEN 1 ELSE 0 END)                             AS nb_ruptures,
        SUM(CASE WHEN sm.statut_fin_mission = 'ANNULEE'
                 THEN 1 ELSE 0 END)                             AS nb_annulations,
        SUM(CASE WHEN sm.statut_fin_mission = 'TERME_NORMAL'
                 THEN 1 ELSE 0 END)                             AS nb_terme_normal,
        ROUND(
            SUM(CASE WHEN sm.statut_fin_mission = 'RUPTURE'
                     THEN 1 ELSE 0 END)::FLOAT / NULLIF(COUNT(*), 0) * 100
        , 1)                                                    AS taux_rupture_pct,
        ROUND(
            SUM(CASE WHEN sm.statut_fin_mission IN ('RUPTURE', 'ANNULEE')
                     THEN 1 ELSE 0 END)::FLOAT / NULLIF(COUNT(*), 0) * 100
        , 1)                                                    AS taux_fin_anticipee_pct,
        ROUND(
            AVG(CASE WHEN sm.statut_fin_mission = 'RUPTURE'
                     THEN sm.duree_reelle_jours END)
        , 1)                                                    AS duree_moy_avant_rupture
    FROM read_parquet(
        '{silver}/slv_missions/fin_mission/**/*.parquet',
        hive_partitioning=true
    ) sm
    WHERE sm.date_debut IS NOT NULL
      AND sm.statut_fin_mission != 'EN_COURS'
    GROUP BY 1, 2, 3
    """


_RUPTURE_COLS = [
    "agence_id", "tie_id", "mois",
    "nb_missions_total", "nb_ruptures", "nb_annulations", "nb_terme_normal",
    "taux_rupture_pct", "taux_fin_anticipee_pct", "duree_moy_avant_rupture",
]


def run(cfg: Config) -> dict:
    stats = Stats()

    with get_duckdb_connection(cfg) as ddb:
        rupture_rows = ddb.execute(build_rupture_contrat_query(cfg)).fetchall()
        logger.info(f"fact_rupture_contrat: {len(rupture_rows)} rows")

    with get_pg_connection(cfg) as pg:
        pg_bulk_insert(cfg, pg, "gld_performance", "fact_rupture_contrat",
                       _RUPTURE_COLS, rupture_rows, stats)

    stats.tables_processed = 1
    stats.rows_transformed = len(rupture_rows)
    stats.extra["rupture_rows"] = len(rupture_rows)
    return stats.finish()


if __name__ == "__main__":
    run(Config())
