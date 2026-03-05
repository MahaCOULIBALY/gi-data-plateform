"""gold_operationnel.py — Gold · Opérationnel : fact_heures_hebdo + fact_commandes_pipeline → PostgreSQL.
Phase 0 · GI Data Lakehouse · Manifeste v2.0
Sources Silver : slv_temps/releves_heures, slv_temps/heures_detail, slv_missions/commandes
Schemas Gold   : gld_operationnel
Note : fact_anomalies_rh — reporté Phase 1 (données WTRHECART non encore en Bronze)
"""
import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import duckdb
import psycopg2

from shared import Config, RunMode, Stats, get_duckdb_connection, logger

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
    b = cfg.bucket_silver
    ddb.execute(
        f"CREATE OR REPLACE VIEW silver_rh  AS SELECT * FROM read_parquet('s3://{b}/slv_temps/releves_heures/**/*.parquet')")
    ddb.execute(
        f"CREATE OR REPLACE VIEW silver_rd  AS SELECT * FROM read_parquet('s3://{b}/slv_temps/heures_detail/**/*.parquet')")
    ddb.execute(
        f"CREATE OR REPLACE VIEW silver_cmd AS SELECT * FROM read_parquet('s3://{b}/slv_missions/commandes/**/*.parquet')")


def _aggregate(ddb, name: str) -> list[tuple]:
    return ddb.execute(SQL[name]).fetchall()


def _write_pg(cfg: Config, name: str, rows: list[tuple], stats: Stats) -> None:
    with psycopg2.connect(
        host=cfg.pg_host, port=cfg.pg_port, dbname=cfg.pg_db,
        user=cfg.pg_user, password=cfg.pg_password, sslmode="require"
    ) as conn:
        with conn.cursor() as cur:
            cur.execute("CREATE SCHEMA IF NOT EXISTS gld_operationnel")
            cur.execute(DDL[name])
            for row in rows:
                cur.execute(UPSERT[name], row)
        conn.commit()
    stats.tables_processed += 1
    stats.rows_ingested += len(rows)


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

        for name in ("fact_heures_hebdo", "fact_commandes_pipeline"):
            try:
                rows = _aggregate(ddb, name)
                if cfg.mode in (RunMode.OFFLINE, RunMode.PROBE):
                    logger.info(json.dumps(
                        {"mode": cfg.mode.value, "table": name, "rows": len(rows)}))
                    stats.tables_processed += 1
                    continue
                _write_pg(cfg, name, rows, stats)
                logger.info(json.dumps(
                    {"table": name, "rows": len(rows), "status": "ok"}))
            except Exception as e:
                logger.exception(json.dumps({"table": name, "error": str(e)}))
                stats.errors.append({"table": name, "error": str(e)})

    return stats.finish()


if __name__ == "__main__":
    run(Config())
