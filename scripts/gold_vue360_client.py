"""gold_vue360_client.py — Table dénormalisée vue 360° client → gld_clients.
Phase 3 · GI Data Lakehouse · Manifeste v2.0

=== CORRECTIONS SESSION 2 (audit Silver→Gold) ===
- EFAC_DATE    → date_facture (Silver alias) in derniere_fact CTE
- TIE_ID::INT  → tie_id (lowercase, Silver alias) in derniere_fact CTE
- d.naf_libelle → naf_libelle (NULL — absent DDL, OK as NULL)
- e.date_decision → # TODO: verify Silver encours_credit schema
"""
import sys
import json
import logging
from datetime import datetime, timezone

from shared import Config, Stats, get_duckdb_connection, get_pg_connection, pg_bulk_insert, logger


def build_vue360_query(cfg: Config) -> str:
    """CTE massif : agrège toutes les sources Silver (S3) + Gold (PostgreSQL) pour la vue 360° client.
    Les tables Gold (fact_ca_mensuel_client, fact_missions_detail) vivent dans PostgreSQL — pg_bulk_insert.
    DuckDB lit PostgreSQL via postgres_scan (extension native, zéro dépendance supplémentaire).
    """
    silver = f"s3://{cfg.bucket_silver}"
    pg_dsn = (
        f"host={cfg.ovh_pg_host} port={cfg.ovh_pg_port} "
        f"dbname={cfg.ovh_pg_database} user={cfg.ovh_pg_user} password={cfg.ovh_pg_password}"
    )
    return f"""
    WITH dim_c AS (
        SELECT * FROM read_parquet('{silver}/slv_clients/dim_clients/**/*.parquet')
        WHERE is_current = true
    ),
    ca_mensuel AS (
        SELECT
            tie_id,
            SUM(CASE WHEN EXTRACT(year FROM mois) = EXTRACT(year FROM CURRENT_DATE) THEN ca_net_ht ELSE 0 END) AS ca_ytd,
            SUM(CASE WHEN EXTRACT(year FROM mois) = EXTRACT(year FROM CURRENT_DATE) - 1 THEN ca_net_ht ELSE 0 END) AS ca_n1,
            SUM(CASE WHEN mois >= CURRENT_DATE - INTERVAL '12 months' THEN ca_net_ht ELSE 0 END) AS ca_12m
        FROM postgres_scan('{pg_dsn}', 'gld_commercial', 'fact_ca_mensuel_client')
        GROUP BY tie_id
    ),
    missions_agg AS (
        SELECT
            tie_id,
            COUNT(DISTINCT CASE WHEN date_fin >= CURRENT_DATE OR date_fin IS NULL THEN mission_sk END) AS nb_missions_actives,
            COUNT(DISTINCT mission_sk) AS nb_missions_total,
            COUNT(DISTINCT CASE WHEN date_fin >= CURRENT_DATE OR date_fin IS NULL THEN per_id END) AS nb_int_actifs,
            COUNT(DISTINCT per_id) AS nb_int_historique,
            COUNT(DISTINCT agence_id) AS nb_agences_partenaires,
            MIN(date_debut) AS premiere_mission,
            AVG(taux_marge) AS marge_moyenne_pct
        FROM postgres_scan('{pg_dsn}', 'gld_staffing', 'fact_missions_detail')
        GROUP BY tie_id
    ),
    top_metiers AS (
        SELECT tie_id,
               LIST(metier_id ORDER BY cnt DESC LIMIT 3) AS top_3
        FROM (
            SELECT tie_id, metier_id, COUNT(*) AS cnt
            FROM postgres_scan('{pg_dsn}', 'gld_staffing', 'fact_missions_detail')
            WHERE metier_id IS NOT NULL
            GROUP BY tie_id, metier_id
        ) GROUP BY tie_id
    ),
    encours AS (
        SELECT siren,
               FIRST(montant_encours ORDER BY _loaded_at DESC) AS montant_encours,
               FIRST(limite_credit ORDER BY _loaded_at DESC) AS limite_credit
        FROM read_parquet('{silver}/slv_clients/encours_credit/**/*.parquet')
        GROUP BY siren
    ),
    derniere_fact AS (
        SELECT tie_id::INT AS tie_id,
               MAX(TRY_CAST(date_facture AS DATE)) AS derniere_facture_date
        FROM read_parquet('{silver}/slv_facturation/factures/**/*.parquet')
        GROUP BY tie_id
    )
    SELECT
        d.client_sk,
        d.tie_id,
        d.raison_sociale,
        d.siren,
        d.ville,
        d.naf_libelle                                          AS secteur_activite,
        d.effectif_tranche                                     AS effectif,
        d.statut_client                                        AS statut,
        COALESCE(ca.ca_ytd, 0)                                AS ca_ytd,
        COALESCE(ca.ca_n1, 0)                                 AS ca_n1,
        CASE WHEN ca.ca_n1 > 0
             THEN ROUND((ca.ca_ytd - ca.ca_n1) / ca.ca_n1 * 100, 2)
             ELSE NULL END                                     AS delta_ca_pct,
        COALESCE(ca.ca_12m, 0)                                AS ca_12_mois_glissants,
        COALESCE(ma.nb_missions_actives, 0)                   AS nb_missions_actives,
        COALESCE(ma.nb_missions_total, 0)                     AS nb_missions_total,
        COALESCE(ma.nb_int_actifs, 0)                         AS nb_int_actifs,
        COALESCE(ma.nb_int_historique, 0)                     AS nb_int_historique,
        COALESCE(CAST(tm.top_3 AS VARCHAR), '[]')             AS top_3_metiers,
        CASE WHEN ma.premiere_mission IS NOT NULL
             THEN DATEDIFF('day', ma.premiere_mission, CURRENT_DATE)
             ELSE 0 END                                        AS anciennete_jours,
        COALESCE(ROUND(ma.marge_moyenne_pct, 4), 0)           AS marge_moyenne_pct,
        COALESCE(e.montant_encours, 0)                        AS montant_encours,
        COALESCE(e.limite_credit, 0)                          AS limite_credit,
        CASE WHEN e.limite_credit > 0 AND e.montant_encours > 0.8 * e.limite_credit THEN 'HIGH'
             WHEN e.limite_credit > 0 AND e.montant_encours > 0.5 * e.limite_credit THEN 'MEDIUM'
             ELSE 'LOW' END                                    AS risque_credit_score,
        COALESCE(ma.nb_agences_partenaires, 0)                AS nb_agences_partenaires,
        df.derniere_facture_date,
        CASE WHEN df.derniere_facture_date IS NOT NULL
             THEN DATEDIFF('day', df.derniere_facture_date, CURRENT_DATE)
             ELSE 9999 END                                     AS jours_depuis_derniere,
        CASE WHEN df.derniere_facture_date IS NULL OR DATEDIFF('day', df.derniere_facture_date, CURRENT_DATE) > 90 THEN 'HIGH'
             WHEN DATEDIFF('day', df.derniere_facture_date, CURRENT_DATE) > 45 THEN 'MEDIUM'
             ELSE 'LOW' END                                    AS risque_churn,
        CURRENT_TIMESTAMP                                      AS _computed_at
    FROM dim_c d
    LEFT JOIN ca_mensuel ca ON ca.tie_id = d.tie_id
    LEFT JOIN missions_agg ma ON ma.tie_id = d.tie_id
    LEFT JOIN top_metiers tm ON tm.tie_id = d.tie_id
    LEFT JOIN encours e ON e.siren = d.siren
    LEFT JOIN derniere_fact df ON df.tie_id = d.tie_id
    """


COLUMNS = [
    "client_sk", "tie_id", "raison_sociale", "siren", "ville", "secteur_activite",
    "effectif", "statut", "ca_ytd", "ca_n1", "delta_ca_pct", "ca_12_mois_glissants",
    "nb_missions_actives", "nb_missions_total", "nb_int_actifs", "nb_int_historique",
    "top_3_metiers", "anciennete_jours", "marge_moyenne_pct", "montant_encours",
    "limite_credit", "risque_credit_score", "nb_agences_partenaires",
    "derniere_facture_date", "jours_depuis_derniere", "risque_churn", "_computed_at",
]


def run(cfg: Config) -> dict:
    stats = Stats()
    with get_duckdb_connection(cfg) as ddb:
        try:
            rows = ddb.execute(build_vue360_query(cfg)).fetchall()
        except Exception as e:
            logger.error(f"vue_360 query failed: {e}")
            logger.info("Attempting fallback via PostgreSQL Gold direct read")
            rows = run_pg_fallback(cfg)
            if not rows:
                stats.errors.append({"error": str(e)})
                return stats.finish()

    logger.info(f"vue_360_client: {len(rows)} rows computed")
    stats.rows_transformed = len(rows)

    with get_pg_connection(cfg) as pg:
        pg_bulk_insert(cfg, pg, "gld_clients",
                       "vue_360_client", COLUMNS, rows, stats)

    stats.tables_processed = 1
    return stats.finish()


def run_pg_fallback(cfg: Config) -> list[tuple]:
    """Fallback : construire vue_360 directement depuis PostgreSQL Gold (pas de S3)."""
    query = """
    WITH ca AS (
        SELECT tie_id,
               SUM(CASE WHEN EXTRACT(year FROM mois) = EXTRACT(year FROM CURRENT_DATE) THEN ca_net_ht::NUMERIC ELSE 0 END) AS ca_ytd,
               SUM(CASE WHEN EXTRACT(year FROM mois) = EXTRACT(year FROM CURRENT_DATE) - 1 THEN ca_net_ht::NUMERIC ELSE 0 END) AS ca_n1,
               SUM(CASE WHEN mois >= CURRENT_DATE - INTERVAL '12 months' THEN ca_net_ht::NUMERIC ELSE 0 END) AS ca_12m
        FROM gld_commercial.fact_ca_mensuel_client GROUP BY tie_id
    ),
    miss AS (
        SELECT tie_id,
               COUNT(DISTINCT CASE WHEN date_fin >= CURRENT_DATE THEN mission_sk END) AS nb_actives,
               COUNT(DISTINCT mission_sk) AS nb_total,
               COUNT(DISTINCT CASE WHEN date_fin >= CURRENT_DATE THEN per_id END) AS nb_int_actifs,
               COUNT(DISTINCT per_id) AS nb_int_hist,
               COUNT(DISTINCT agence_id) AS nb_agences,
               MIN(date_debut) AS first_mission,
               AVG(taux_marge::NUMERIC) AS marge_avg
        FROM gld_staffing.fact_missions_detail GROUP BY tie_id
    )
    SELECT d.client_sk, d.tie_id::INT, d.raison_sociale, d.siren, d.ville,
           d.naf_libelle, d.effectif_tranche, d.statut_client,
           COALESCE(ca.ca_ytd, 0), COALESCE(ca.ca_n1, 0),
           CASE WHEN ca.ca_n1 > 0 THEN ROUND((ca.ca_ytd - ca.ca_n1) / ca.ca_n1 * 100, 2) END,
           COALESCE(ca.ca_12m, 0),
           COALESCE(miss.nb_actives, 0), COALESCE(miss.nb_total, 0),
           COALESCE(miss.nb_int_actifs, 0), COALESCE(miss.nb_int_hist, 0),
           '[]',
           COALESCE(EXTRACT(day FROM CURRENT_DATE - miss.first_mission)::INT, 0),
           COALESCE(ROUND(miss.marge_avg, 4), 0), 0, 0, 'LOW',
           COALESCE(miss.nb_agences, 0), NULL, 9999, 'HIGH', NOW()
    FROM gld_shared.dim_clients d
    LEFT JOIN ca ON ca.tie_id = d.tie_id::INT
    LEFT JOIN miss ON miss.tie_id = d.tie_id::INT
    """
    try:
        with get_pg_connection(cfg) as pg:
            with pg.cursor() as cur:
                cur.execute(query)
                return cur.fetchall()
    except Exception as e:
        logger.error(f"PG fallback also failed: {e}")
        return []


if __name__ == "__main__":
    cfg = Config()
    run(cfg)
