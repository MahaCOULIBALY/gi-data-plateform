"""gold_operationnel.py — Gold · Opérationnel : fact_heures_hebdo + fact_commandes_pipeline → PostgreSQL.
Phase 0 · GI Data Lakehouse · Manifeste v2.0
Sources Silver : slv_temps/releves_heures, slv_temps/heures_detail, slv_missions/commandes
Schemas Gold   : gld_operationnel
Note : fact_anomalies_rh — reporté Phase 1 (données WTRHECART non encore en Bronze)
"""
import json

import psycopg2.extras

from shared import Config, RunMode, Stats, get_duckdb_connection, get_pg_connection, pg_bulk_insert, logger

DOMAIN = "gld_operationnel"

DDL = {
    "fact_heures_hebdo": """
        CREATE TABLE IF NOT EXISTS gld_operationnel.fact_heures_hebdo (
            agence_id       INTEGER,
            tie_id          INTEGER,
            semaine_debut   DATE,
            heures_paye     DECIMAL(12,2),
            heures_fact     DECIMAL(12,2),
            nb_releves      INTEGER,
            _loaded_at      TIMESTAMPTZ DEFAULT NOW(),
            PRIMARY KEY (agence_id, tie_id, semaine_debut)
        )
    """,
    "fact_commandes_pipeline": """
        CREATE TABLE IF NOT EXISTS gld_operationnel.fact_commandes_pipeline (
            agence_id           INTEGER,
            semaine_debut       DATE,
            nb_commandes        INTEGER,
            nb_pourvues         INTEGER,
            nb_ouvertes         INTEGER,
            taux_satisfaction   DECIMAL(6,4),
            _loaded_at          TIMESTAMPTZ DEFAULT NOW(),
            PRIMARY KEY (agence_id, semaine_debut)
        )
    """,
}

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
            COUNT(*) FILTER (WHERE statut = 'P')                AS nb_pourvues,
            COUNT(*) FILTER (WHERE statut = 'O')                AS nb_ouvertes,
            COALESCE(
                COUNT(*) FILTER (WHERE statut = 'P')::DECIMAL
                / NULLIF(COUNT(*), 0), 0
            )                                                   AS taux_satisfaction
        FROM silver_cmd
        WHERE rgpcnt_id IS NOT NULL AND cmd_date IS NOT NULL
        GROUP BY 1, 2
    """,
}

UPSERT = {
    "fact_heures_hebdo": """
        INSERT INTO gld_operationnel.fact_heures_hebdo
            (agence_id, tie_id, semaine_debut, heures_paye, heures_fact, nb_releves)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (agence_id, tie_id, semaine_debut) DO UPDATE SET
            heures_paye = EXCLUDED.heures_paye,
            heures_fact = EXCLUDED.heures_fact,
            nb_releves  = EXCLUDED.nb_releves,
            _loaded_at  = NOW()
    """,
    "fact_commandes_pipeline": """
        INSERT INTO gld_operationnel.fact_commandes_pipeline
            (agence_id, semaine_debut, nb_commandes, nb_pourvues, nb_ouvertes, taux_satisfaction)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (agence_id, semaine_debut) DO UPDATE SET
            nb_commandes        = EXCLUDED.nb_commandes,
            nb_pourvues         = EXCLUDED.nb_pourvues,
            nb_ouvertes         = EXCLUDED.nb_ouvertes,
            taux_satisfaction   = EXCLUDED.taux_satisfaction,
            _loaded_at          = NOW()
    """,
}


def _register_views(ddb, cfg: Config) -> None:
    ddb.execute(
        f"CREATE OR REPLACE VIEW silver_rh  AS SELECT * FROM iceberg_scan('{cfg.iceberg_path('temps', 'releves_heures')}')")
    ddb.execute(
        f"CREATE OR REPLACE VIEW silver_rd  AS SELECT * FROM iceberg_scan('{cfg.iceberg_path('temps', 'heures_detail')}')")
    ddb.execute(
        f"CREATE OR REPLACE VIEW silver_cmd AS SELECT * FROM iceberg_scan('{cfg.iceberg_path('missions', 'commandes')}')")


def _aggregate(ddb, name: str) -> list[tuple]:
    return ddb.execute(SQL[name]).fetchall()


def _write_pg(pg_conn, name: str, rows: list[tuple], stats: Stats) -> None:
    """Upsert via execute_batch (lot 500) — connexion partagée par run(), pas de re-ouverture TCP."""
    with pg_conn.cursor() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS gld_operationnel")
        cur.execute(DDL[name])
        psycopg2.extras.execute_batch(cur, UPSERT[name], rows, page_size=500)
    pg_conn.commit()
    stats.tables_processed += 1
    stats.rows_ingested += len(rows)


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
    FROM iceberg_scan('{cfg.iceberg_path("missions", "missions")}') m
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
    FROM iceberg_scan('{cfg.iceberg_path("missions", "missions")}') m
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
            for name in ("fact_heures_hebdo", "fact_commandes_pipeline"):
                try:
                    rows = _aggregate(ddb, name)
                    if cfg.mode in (RunMode.OFFLINE, RunMode.PROBE):
                        logger.info(json.dumps(
                            {"mode": cfg.mode.value, "table": name, "rows": len(rows)}))
                        stats.tables_processed += 1
                        continue
                    _write_pg(pg, name, rows, stats)
                    logger.info(json.dumps(
                        {"table": name, "rows": len(rows), "status": "ok"}))
                except Exception as e:
                    logger.exception(json.dumps({"table": name, "error": str(e)}))
                    stats.errors.append({"table": name, "error": str(e)})

            # Priorité 7 : fact_delai_placement + fact_conformite_dpae
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
