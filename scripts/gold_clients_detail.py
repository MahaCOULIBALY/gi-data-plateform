"""gold_clients_detail.py — Gold · Clients Detail : vue_360_client + fact_retention_client → PostgreSQL.
Phase 0 · GI Data Lakehouse · Manifeste v2.0
Sources Silver : slv_clients/dim_clients, sites_mission, encours_credit + gld_commercial.fact_ca_mensuel_client
Schemas Gold   : gld_clients
Note : fact_rentabilite_client — reporté Phase 2 (nécessite fact_marge_mission non encore calculé)
RGPD : contacts (email/tel) restent Silver-only — jamais en Gold
"""
import json
from datetime import datetime, timezone

import psycopg2

from shared import Config, RunMode, Stats, get_duckdb_connection, logger

DOMAIN = "gld_clients"

DDL = {
    "vue_360_client": """
        CREATE TABLE IF NOT EXISTS gld_clients.vue_360_client (
            client_sk           INTEGER PRIMARY KEY,
            siren               VARCHAR(9),
            raison_sociale      VARCHAR(255),
            naf_code            VARCHAR(6),
            ville               VARCHAR(100),
            nb_sites            INTEGER,
            ca_ytd              DECIMAL(18,2),
            ca_n1               DECIMAL(18,2),
            delta_ca_pct        DECIMAL(8,4),
            encours_montant     DECIMAL(18,2),
            encours_limite      DECIMAL(18,2),
            risque_credit       VARCHAR(20),
            _loaded_at          TIMESTAMPTZ DEFAULT NOW()
        )
    """,
    "fact_retention_client": """
        CREATE TABLE IF NOT EXISTS gld_clients.fact_retention_client (
            client_sk       INTEGER,
            trimestre       DATE,
            ca              DECIMAL(18,2),
            delta_ca        DECIMAL(18,2),
            nb_missions     INTEGER,
            risque_churn    VARCHAR(20),
            _loaded_at      TIMESTAMPTZ DEFAULT NOW(),
            PRIMARY KEY (client_sk, trimestre)
        )
    """,
}

SQL = {
    "vue_360_client": """
        WITH ca AS (
            SELECT
                tie_id,
                SUM(CASE WHEN date_trunc('year', mois) = date_trunc('year', current_date) THEN ca_ht ELSE 0 END) AS ca_ytd,
                SUM(CASE WHEN date_trunc('year', mois) = date_trunc('year', current_date) - INTERVAL '1 year' THEN ca_ht ELSE 0 END) AS ca_n1
            FROM silver_ca GROUP BY tie_id
        ),
        sites AS (
            SELECT tie_id, COUNT(*) AS nb_sites FROM silver_sites GROUP BY tie_id
        ),
        enc AS (
            SELECT siren, montant_encours, limite_credit
            FROM silver_enc
            QUALIFY ROW_NUMBER() OVER (PARTITION BY siren ORDER BY _loaded_at DESC) = 1
        )
        SELECT
            c.tie_id                                                AS client_sk,
            TRIM(c.siren)                                           AS siren,
            TRIM(c.raison_sociale)                                  AS raison_sociale,
            TRIM(c.naf_code)                                        AS naf_code,
            TRIM(c.ville)                                           AS ville,
            COALESCE(s.nb_sites, 0)                                 AS nb_sites,
            COALESCE(ca.ca_ytd, 0)                                  AS ca_ytd,
            COALESCE(ca.ca_n1, 0)                                   AS ca_n1,
            CASE WHEN COALESCE(ca.ca_n1, 0) = 0 THEN NULL
                 ELSE ROUND((ca.ca_ytd - ca.ca_n1) / ca.ca_n1, 4)
            END                                                     AS delta_ca_pct,
            COALESCE(enc.montant_encours, 0)                        AS encours_montant,
            COALESCE(enc.limite_credit, 0)                          AS encours_limite,
            CASE
                WHEN enc.montant_encours > enc.limite_credit * 0.9 THEN 'CRITIQUE'
                WHEN enc.montant_encours > enc.limite_credit * 0.7 THEN 'ELEVE'
                ELSE 'NORMAL'
            END                                                     AS risque_credit
        FROM silver_clients c
        LEFT JOIN ca   ON ca.tie_id  = c.tie_id
        LEFT JOIN sites s ON s.tie_id = c.tie_id
        LEFT JOIN enc  ON enc.siren  = c.siren
        WHERE c.tie_id IS NOT NULL
    """,
    "fact_retention_client": """
        SELECT
            tie_id                                                  AS client_sk,
            DATE_TRUNC('quarter', mois)::DATE                       AS trimestre,
            SUM(ca_ht)                                              AS ca,
            SUM(ca_ht) - LAG(SUM(ca_ht), 1, 0) OVER (
                PARTITION BY tie_id ORDER BY DATE_TRUNC('quarter', mois)
            )                                                       AS delta_ca,
            SUM(nb_missions)                                        AS nb_missions,
            CASE
                WHEN SUM(ca_ht) < LAG(SUM(ca_ht), 1, 0) OVER (
                    PARTITION BY tie_id ORDER BY DATE_TRUNC('quarter', mois)
                ) * 0.7 THEN 'RISQUE_FORT'
                WHEN SUM(ca_ht) = 0 THEN 'INACTIF'
                ELSE 'STABLE'
            END                                                     AS risque_churn
        FROM silver_ca
        GROUP BY 1, 2
    """,
}

UPSERT = {
    "vue_360_client": """
        INSERT INTO gld_clients.vue_360_client
            (client_sk, siren, raison_sociale, naf_code, ville, nb_sites,
             ca_ytd, ca_n1, delta_ca_pct, encours_montant, encours_limite, risque_credit)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (client_sk) DO UPDATE SET
            ca_ytd=EXCLUDED.ca_ytd, ca_n1=EXCLUDED.ca_n1,
            delta_ca_pct=EXCLUDED.delta_ca_pct, encours_montant=EXCLUDED.encours_montant,
            encours_limite=EXCLUDED.encours_limite, risque_credit=EXCLUDED.risque_credit,
            nb_sites=EXCLUDED.nb_sites, _loaded_at=NOW()
    """,
    "fact_retention_client": """
        INSERT INTO gld_clients.fact_retention_client
            (client_sk, trimestre, ca, delta_ca, nb_missions, risque_churn)
        VALUES (%s,%s,%s,%s,%s,%s)
        ON CONFLICT (client_sk, trimestre) DO UPDATE SET
            ca=EXCLUDED.ca, delta_ca=EXCLUDED.delta_ca,
            nb_missions=EXCLUDED.nb_missions, risque_churn=EXCLUDED.risque_churn,
            _loaded_at=NOW()
    """,
}


def _register_views(ddb, cfg: Config, pg_ca_rows: list[tuple]) -> None:
    """Enregistre les vues Silver S3 + injecte fact_ca_mensuel_client depuis PostgreSQL (évite Gold→Gold)."""
    b = cfg.bucket_silver
    ddb.execute(
        f"CREATE OR REPLACE VIEW silver_clients AS SELECT * FROM read_parquet('s3://{b}/slv_clients/dim_clients/**/*.parquet')")
    ddb.execute(
        f"CREATE OR REPLACE VIEW silver_sites   AS SELECT * FROM read_parquet('s3://{b}/slv_clients/sites_mission/**/*.parquet')")
    ddb.execute(
        f"CREATE OR REPLACE VIEW silver_enc     AS SELECT * FROM read_parquet('s3://{b}/slv_clients/encours_credit/**/*.parquet')")
    # Gold CA injecté depuis PostgreSQL — pas de lecture S3 Gold (anti-pattern corrigé B1)
    ddb.execute("""
        CREATE OR REPLACE TABLE silver_ca AS
        SELECT * FROM (VALUES (0::INT, 0::INT, CURRENT_DATE::DATE, 0.0::DECIMAL(18,2), 0::INT))
        t(agence_id, tie_id, mois, ca_ht, nb_missions) WHERE 1=0
    """)
    if pg_ca_rows:
        ddb.executemany(
            "INSERT INTO silver_ca VALUES (?,?,?,?,?)", pg_ca_rows
        )


def _fetch_pg_ca(cfg: Config) -> list[tuple]:
    """Charge fact_ca_mensuel_client depuis PostgreSQL → list[tuple] pour injection DuckDB."""
    if cfg.mode == RunMode.OFFLINE:
        return []
    with psycopg2.connect(
        host=cfg.pg_host, port=cfg.pg_port, dbname=cfg.pg_db,
        user=cfg.pg_user, password=cfg.pg_password, sslmode="require"
    ) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT agence_principale::INT, tie_id::INT, mois::DATE,
                       ca_net_ht::DECIMAL, nb_missions_facturees::INT
                FROM gld_commercial.fact_ca_mensuel_client
            """)
            return cur.fetchall()


def _write_pg(cfg: Config, name: str, rows: list, stats: Stats) -> None:
    with psycopg2.connect(
        host=cfg.pg_host, port=cfg.pg_port, dbname=cfg.pg_db,
        user=cfg.pg_user, password=cfg.pg_password, sslmode="require"
    ) as conn:
        with conn.cursor() as cur:
            cur.execute("CREATE SCHEMA IF NOT EXISTS gld_clients")
            cur.execute(DDL[name])
            for row in rows:
                cur.execute(UPSERT[name], row)
        conn.commit()
    stats.tables_processed += 1
    stats.rows_ingested += len(rows)


def run(cfg: Config) -> dict:
    stats = Stats()
    pg_ca_rows = _fetch_pg_ca(cfg)
    logger.info(json.dumps({"step": "fetch_pg_ca", "rows": len(pg_ca_rows)}))
    with get_duckdb_connection(cfg) as ddb:
        try:
            _register_views(ddb, cfg, pg_ca_rows)
        except Exception as e:
            logger.exception(json.dumps(
                {"step": "register_views", "error": str(e)}))
            stats.errors.append({"step": "register_views", "error": str(e)})
            return stats.finish()

        for name in ("vue_360_client", "fact_retention_client"):
            try:
                rows = ddb.execute(SQL[name]).fetchall()
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
