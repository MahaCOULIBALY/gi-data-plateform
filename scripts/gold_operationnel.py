"""gold_operationnel.py — Gold · Opérationnel : fact_heures_hebdo + fact_commandes_pipeline → PostgreSQL.
Phase 0 · GI Data Lakehouse · Manifeste v2.0
Sources Silver : slv_temps/releves_heures, slv_temps/heures_detail, slv_missions/commandes
Schemas Gold   : gld_operationnel
Note : fact_anomalies_rh — reporté Phase 1 (données WTRHECART non encore en Bronze)

# MIGRÉ : iceberg_scan(cfg.iceberg_path(*)) → read_parquet(s3://gi-poc-silver/slv_*) (D01)
# BUG-3 corrigé : execute_batch → pg_bulk_insert (probe mode désormais respecté, 4 tables unifiées)
"""
import json

from shared import Config, RunMode, Stats, get_duckdb_connection, get_pg_connection, pg_bulk_insert, logger

DOMAIN = "gld_operationnel"

SQL = {
    "fact_heures_hebdo": """
        SELECT
            rh.rgpcnt_id                                        AS agence_id,
            rh.tie_id                                           AS tie_id,
            DATE_TRUNC('week', rh.date_modif)::DATE             AS semaine_debut,
            SUM(rd.base_paye)                                   AS heures_paye,
            SUM(rd.base_fact)                                   AS heures_fact,
            COUNT(DISTINCT rh.prh_bts)                          AS nb_releves
        FROM silver_rh rh
        JOIN silver_rd rd ON rd.prh_bts = rh.prh_bts
        WHERE rh.valide = true
          AND rh.rgpcnt_id IS NOT NULL
          AND rh.tie_id IS NOT NULL
        GROUP BY 1, 2, 3
    """,
    "fact_commandes_pipeline": """
        SELECT
            rgpcnt_id                                           AS agence_id,
            DATE_TRUNC('week', cmd_date)::DATE                  AS semaine_debut,
            COUNT(*)                                            AS nb_commandes,
            COUNT(*) FILTER (WHERE stat_code = 'P')             AS nb_pourvues,
            COUNT(*) FILTER (WHERE stat_code = 'O')             AS nb_ouvertes,
            COALESCE(
                COUNT(*) FILTER (WHERE stat_code = 'P')::DECIMAL
                / NULLIF(COUNT(*), 0), 0
            )                                                   AS taux_satisfaction
        FROM silver_cmd
        WHERE rgpcnt_id IS NOT NULL AND cmd_date IS NOT NULL
        GROUP BY 1, 2
    """,
}

COLS = {
    "fact_heures_hebdo": ["agence_id", "tie_id", "semaine_debut",
                          "heures_paye", "heures_fact", "nb_releves"],
    "fact_commandes_pipeline": ["agence_id", "semaine_debut", "nb_commandes",
                                "nb_pourvues", "nb_ouvertes", "taux_satisfaction"],
}


def _register_views(ddb, cfg: Config) -> None:
    ddb.execute(
        f"CREATE OR REPLACE VIEW silver_rh  AS SELECT * FROM read_parquet("
        f"'s3://{cfg.bucket_silver}/slv_temps/releves_heures/**/*.parquet')"
    )
    ddb.execute(
        f"CREATE OR REPLACE VIEW silver_rd  AS SELECT * FROM read_parquet("
        f"'s3://{cfg.bucket_silver}/slv_temps/heures_detail/**/*.parquet')"
    )
    ddb.execute(
        f"CREATE OR REPLACE VIEW silver_cmd AS SELECT * FROM read_parquet("
        f"'s3://{cfg.bucket_silver}/slv_missions/commandes/**/*.parquet')"
    )


def _aggregate(ddb, name: str) -> list[tuple]:
    return ddb.execute(SQL[name]).fetchall()


def build_delai_placement_query(cfg: Config) -> str:
    """Délai entre commande (CMD_DTE) et début mission (CNTI_DATEFFET) par agence/semaine/catégorie.
    Nécessite WTMISS Silver enrichi (delai_placement_heures, categorie_delai ajoutés 2026-03-13).
    """
    return f"""
    SELECT
        m.rgpcnt_id                                                AS agence_id,
        DATE_TRUNC('week', m.date_fin)::DATE                       AS semaine_debut,
        m.categorie_delai,
        COUNT(*)                                                    AS nb_missions,
        ROUND(AVG(m.delai_placement_heures), 2)::DECIMAL(10,2)     AS delai_moyen_heures,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY m.delai_placement_heures)
                                                                   ::DECIMAL(10,2) AS delai_median_heures
    FROM read_parquet('s3://{cfg.bucket_silver}/slv_missions/missions/**/*.parquet') m
    WHERE m.rgpcnt_id IS NOT NULL
      AND m.date_fin IS NOT NULL
      AND m.delai_placement_heures IS NOT NULL
    GROUP BY 1, 2, 3
    ORDER BY 2 DESC, 1
    """


def build_conformite_dpae_query(cfg: Config) -> str:
    """Taux de conformité DPAE par agence/mois.
    statut_dpae = MISS_FLAGDPAE (datetime2, date de transmission) — NULL = non transmis.
    ecart_heures < 0 → DPAE transmise avant début mission (conforme).
    ecart_heures > 0 → DPAE transmise après début mission (non conforme).
    """
    return f"""
    SELECT
        m.rgpcnt_id                                                AS agence_id,
        DATE_TRUNC('month', m.date_fin)::DATE                      AS mois,
        COUNT(*)                                                    AS nb_missions,
        COUNT(*) FILTER (WHERE m.statut_dpae IS NOT NULL)          AS nb_dpae_transmises,
        COUNT(*) FILTER (WHERE m.statut_dpae IS NULL)              AS nb_dpae_manquantes,
        ROUND(
            COUNT(*) FILTER (WHERE m.statut_dpae IS NOT NULL)::DECIMAL
            / NULLIF(COUNT(*), 0),
        4)                                                         AS taux_conformite_dpae,
        ROUND(AVG(m.ecart_heures) FILTER (WHERE m.ecart_heures IS NOT NULL),
              2)::DECIMAL(10,2)                                    AS ecart_moyen_heures
    FROM read_parquet('s3://{cfg.bucket_silver}/slv_missions/missions/**/*.parquet') m
    WHERE m.rgpcnt_id IS NOT NULL AND m.date_fin IS NOT NULL
    GROUP BY 1, 2
    ORDER BY 2 DESC, 1
    """


def run(cfg: Config) -> dict:
    stats = Stats()
    with get_duckdb_connection(cfg) as ddb:
        try:
            _register_views(ddb, cfg)
        except Exception as e:
            logger.exception(json.dumps(
                {"step": "register_views", "error": str(e)}))
            stats.errors.append({"step": "register_views", "error": str(e)})
            return stats.finish()

        with get_pg_connection(cfg) as pg:
            # fact_heures_hebdo + fact_commandes_pipeline — pg_bulk_insert gère le mode
            for name in ("fact_heures_hebdo", "fact_commandes_pipeline"):
                try:
                    rows = _aggregate(ddb, name)
                    pg_bulk_insert(cfg, pg, DOMAIN, name, COLS[name], rows, stats)
                    logger.info(json.dumps(
                        {"table": name, "rows": len(rows), "status": "ok"}))
                except Exception as e:
                    logger.exception(json.dumps({"table": name, "error": str(e)}))
                    stats.errors.append({"table": name, "error": str(e)})

            # fact_delai_placement + fact_conformite_dpae
            _new_tables = {
                "fact_delai_placement": (
                    build_delai_placement_query(cfg),
                    ["agence_id", "semaine_debut", "categorie_delai",
                     "nb_missions", "delai_moyen_heures", "delai_median_heures"],
                ),
                "fact_conformite_dpae": (
                    build_conformite_dpae_query(cfg),
                    ["agence_id", "mois", "nb_missions", "nb_dpae_transmises",
                     "nb_dpae_manquantes", "taux_conformite_dpae", "ecart_moyen_heures"],
                ),
            }
            for name, (query, cols) in _new_tables.items():
                try:
                    rows = ddb.execute(query).fetchall()
                    logger.info(json.dumps({"table": name, "rows": len(rows)}))
                    pg_bulk_insert(cfg, pg, DOMAIN, name, cols, rows, stats)
                except Exception as e:
                    logger.exception(json.dumps({"table": name, "error": str(e)}))
                    stats.errors.append({"table": name, "error": str(e)})

    return stats.finish()


if __name__ == "__main__":
    run(Config())
