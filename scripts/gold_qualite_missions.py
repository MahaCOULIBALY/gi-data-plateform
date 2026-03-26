"""gold_qualite_missions.py — Silver → Gold fact_rupture_contrat.
Phase 4 · GI Data Lakehouse · Manifeste v4.0
Tables Gold : gld_performance.fact_rupture_contrat ← slv_missions/fin_mission + missions

=== CORRECTIONS SESSION 3 (audit Gold) ===
- G-QM-B01 : TRUNCATE avant pg_bulk_insert (idempotence PG)
- G-QM-B02 : rgpcnt_id, tie_id, date_debut absents de fin_mission (référence Silver :
             per_id, cnt_id, statut_fin_mission, duree_reelle_jours seulement) →
             JOIN slv_missions/missions sur per_id + cnt_id pour ces colonnes
- G-QM-M01 : RunMode importé + guards OFFLINE/PROBE
- G-QM-M02 : try/except dans run()
- G-QM-m01 : import logging supprimé

Notes :
  - Blocker B1 (FINMISS_LIBELLE non standardisés) : classification LIKE MVP.
  - Blocker B2 (MISS_ANNULE) : probe WTMISS GROUP BY MISS_ANNULE requis avant prod.
  - La table Gold ne contient que les missions clôturées (statut_fin_mission != 'EN_COURS').

# MIGRÉ : iceberg_scan → read_parquet(s3://gi-poc-silver/slv_*) (D01)
"""
import json

from shared import Config, RunMode, Stats, get_duckdb_connection, get_pg_connection, pg_bulk_insert, logger


PIPELINE = "gold_qualite_missions"
_RUPTURE_COLS = [
    "agence_id", "tie_id", "mois",
    "nb_missions_total", "nb_ruptures", "nb_annulations", "nb_terme_normal",
    "taux_rupture_pct", "taux_fin_anticipee_pct", "duree_moy_avant_rupture",
]


def build_rupture_contrat_query(cfg: Config) -> str:
    slv = f"s3://{cfg.bucket_silver}"
    return f"""
    -- G-QM-B02 : fin_mission ne contient que per_id, cnt_id, statut_fin_mission, duree_reelle_jours
    -- → JOIN missions pour récupérer rgpcnt_id, tie_id, date_debut
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


def run(cfg: Config) -> dict:
    stats = Stats()

    if cfg.mode in (RunMode.OFFLINE, RunMode.PROBE):  # G-QM-M01
        logger.info(json.dumps({
            "mode": cfg.mode.name, "table": "fact_rupture_contrat", "action": "skipped"
        }))
        stats.tables_processed += 1
        return stats.finish(cfg, PIPELINE)

    rupture_rows = []
    try:
        with get_duckdb_connection(cfg) as ddb:
            rupture_rows = ddb.execute(
                build_rupture_contrat_query(cfg)).fetchall()
            logger.info(json.dumps(
                {"table": "fact_rupture_contrat", "rows": len(rupture_rows)}))
    except Exception as e:  # G-QM-M02
        logger.exception(json.dumps({"step": "duckdb_build", "error": str(e)}))
        stats.errors.append({"step": "duckdb_build", "error": str(e)})
        return stats.finish(cfg, PIPELINE)

    try:
        with get_pg_connection(cfg) as pg:
            # G-QM-B01 : idempotence PG
            with pg.cursor() as cur:
                cur.execute(
                    "TRUNCATE TABLE gld_performance.fact_rupture_contrat")
            pg.commit()
            pg_bulk_insert(cfg, pg, "gld_performance", "fact_rupture_contrat",
                           _RUPTURE_COLS, rupture_rows, stats)
    except Exception as e:  # G-QM-M02
        logger.exception(json.dumps({"step": "pg_insert", "error": str(e)}))
        stats.errors.append({"step": "pg_insert", "error": str(e)})
        return stats.finish(cfg, PIPELINE)

    stats.tables_processed = 1
    stats.rows_transformed = len(rupture_rows)
    stats.extra["rupture_rows"] = len(rupture_rows)
    return stats.finish(cfg, PIPELINE)


if __name__ == "__main__":
    run(Config())
