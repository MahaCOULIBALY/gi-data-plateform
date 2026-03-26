"""gold_vue360_client.py — Table dénormalisée vue 360° client → gld_clients.vue_360_client.
Phase 3 · GI Data Lakehouse · Manifeste v2.0

=== CORRECTIONS SESSION 2 (audit Silver→Gold) ===
- EFAC_DATE → date_facture / TIE_ID → tie_id (lowercase Silver aliases)

=== CORRECTIONS SESSION 3 (audit Gold) ===
- G-V360-B01 : TRUNCATE avant pg_bulk_insert (idempotence PG)
- G-V360-M01 : RunMode importé + guards OFFLINE/PROBE
- G-V360-M02 : try/except autour pg_bulk_insert
- G-V360-M03 : _run_pg_fallback accepte pg_conn — connexion PG unique dans run()
- G-V360-m01 : imports sys/logging/datetime supprimés

Note architecture :
  - DuckDB lit Gold PG via postgres_scan (INSTALL postgres requis, tables PG seulement)
  - Fallback PG natif si postgres_scan échoue (réseau, extension absente)
  - TODO m02 Phase 2 : migrer vers ATTACH ... (TYPE postgres) — API moderne DuckDB

# MIGRÉ : iceberg_scan → read_parquet(s3://gi-poc-silver/slv_*) (D01)
"""
import json

from shared import (
    Config, RunMode, Stats,
    get_duckdb_connection, get_pg_connection, pg_bulk_insert,
    logger,
)


PIPELINE = "gold_vue360_client"


def build_vue360_query(cfg: Config) -> str:
    """CTE massif : Silver S3 + Gold PG via postgres_scan.
    Requiert : INSTALL postgres; LOAD postgres; dans la session DuckDB.
    """
    pg_dsn = (
        f"host={cfg.ovh_pg_host} port={cfg.ovh_pg_port} "
        f"dbname={cfg.ovh_pg_database} user={cfg.ovh_pg_user} "
        f"password={cfg.ovh_pg_password}"
    )
    return f"""
    WITH dim_c AS (
        SELECT * FROM read_parquet('s3://{cfg.bucket_silver}/slv_clients/dim_clients/**/*.parquet')
        WHERE is_current = true
        QUALIFY ROW_NUMBER() OVER (PARTITION BY tie_id ORDER BY valid_from DESC NULLS LAST) = 1
    ),
    ca_mensuel AS (
        SELECT
            tie_id,
            SUM(CASE WHEN EXTRACT(year FROM mois) = EXTRACT(year FROM CURRENT_DATE)
                     THEN ca_net_ht ELSE 0 END)                           AS ca_ytd,
            SUM(CASE WHEN EXTRACT(year FROM mois) = EXTRACT(year FROM CURRENT_DATE) - 1
                     THEN ca_net_ht ELSE 0 END)                           AS ca_n1,
            SUM(CASE WHEN mois >= CURRENT_DATE - INTERVAL '12 months'
                     THEN ca_net_ht ELSE 0 END)                           AS ca_12m
        FROM postgres_scan('{pg_dsn}', 'gld_commercial', 'fact_ca_mensuel_client')
        GROUP BY tie_id
    ),
    missions_agg AS (
        SELECT
            tie_id,
            COUNT(DISTINCT CASE WHEN date_fin >= CURRENT_DATE
                                  OR date_fin IS NULL THEN mission_sk END) AS nb_missions_actives,
            COUNT(DISTINCT mission_sk)                                     AS nb_missions_total,
            COUNT(DISTINCT CASE WHEN date_fin >= CURRENT_DATE
                                  OR date_fin IS NULL THEN per_id END)     AS nb_int_actifs,
            COUNT(DISTINCT per_id)                                         AS nb_int_historique,
            COUNT(DISTINCT agence_id)                                      AS nb_agences_partenaires,
            MIN(date_debut)                                                AS premiere_mission,
            AVG(taux_marge)                                                AS marge_moyenne_pct
        FROM postgres_scan('{pg_dsn}', 'gld_staffing', 'fact_missions_detail')
        GROUP BY tie_id
    ),
    top_metiers AS (
        SELECT tie_id,
               list(metier_id ORDER BY cnt DESC)[1:3] AS top_3
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
               FIRST(limite_credit   ORDER BY _loaded_at DESC) AS limite_credit
        FROM read_parquet('s3://{cfg.bucket_silver}/slv_clients/encours_credit/**/*.parquet')
        GROUP BY siren
    ),
    derniere_fact AS (
        SELECT tie_id::INT                           AS tie_id,
               MAX(TRY_CAST(date_facture AS DATE))   AS derniere_facture_date
        FROM read_parquet('s3://{cfg.bucket_silver}/slv_facturation/factures/**/*.parquet')
        GROUP BY tie_id
    )
    SELECT
        d.client_sk,
        d.tie_id,
        d.raison_sociale,
        d.siren,
        d.ville,
        d.code_postal,
        d.adresse_complete,
        NULL::VARCHAR                                                      AS secteur_activite,
        d.effectif_tranche                                                 AS effectif,
        d.statut_client                                                    AS statut,
        COALESCE(ca.ca_ytd, 0)                                            AS ca_ytd,
        COALESCE(ca.ca_n1, 0)                                             AS ca_n1,
        CASE WHEN ca.ca_n1 > 0
             THEN ROUND((ca.ca_ytd - ca.ca_n1) / ca.ca_n1 * 100, 2)
             ELSE NULL END                                                 AS delta_ca_pct,
        COALESCE(ca.ca_12m, 0)                                            AS ca_12_mois_glissants,
        COALESCE(ma.nb_missions_actives, 0)                               AS nb_missions_actives,
        COALESCE(ma.nb_missions_total, 0)                                 AS nb_missions_total,
        COALESCE(ma.nb_int_actifs, 0)                                     AS nb_int_actifs,
        COALESCE(ma.nb_int_historique, 0)                                 AS nb_int_historique,
        COALESCE(CAST(tm.top_3 AS VARCHAR), '[]')                         AS top_3_metiers,
        CASE WHEN ma.premiere_mission IS NOT NULL
             THEN DATEDIFF('day', ma.premiere_mission, CURRENT_DATE)
             ELSE 0 END                                                    AS anciennete_jours,
        COALESCE(ROUND(ma.marge_moyenne_pct, 4), 0)                       AS marge_moyenne_pct,
        COALESCE(e.montant_encours, 0)                                    AS montant_encours,
        COALESCE(e.limite_credit, 0)                                      AS limite_credit,
        CASE WHEN e.limite_credit > 0 AND e.montant_encours > 0.8 * e.limite_credit THEN 'HIGH'
             WHEN e.limite_credit > 0 AND e.montant_encours > 0.5 * e.limite_credit THEN 'MEDIUM'
             ELSE 'LOW' END                                                AS risque_credit_score,
        COALESCE(ma.nb_agences_partenaires, 0)                            AS nb_agences_partenaires,
        df.derniere_facture_date,
        CASE WHEN df.derniere_facture_date IS NOT NULL
             THEN DATEDIFF('day', df.derniere_facture_date, CURRENT_DATE)
             ELSE 9999 END                                                 AS jours_depuis_derniere,
        CASE WHEN df.derniere_facture_date IS NULL
               OR DATEDIFF('day', df.derniere_facture_date, CURRENT_DATE) > 90 THEN 'HIGH'
             WHEN DATEDIFF('day', df.derniere_facture_date, CURRENT_DATE) > 45  THEN 'MEDIUM'
             ELSE 'LOW' END                                                AS risque_churn,
        CURRENT_TIMESTAMP                                                  AS _computed_at
    FROM dim_c d
    LEFT JOIN ca_mensuel  ca ON ca.tie_id = d.tie_id
    LEFT JOIN missions_agg ma ON ma.tie_id = d.tie_id
    LEFT JOIN top_metiers  tm ON tm.tie_id = d.tie_id
    LEFT JOIN encours       e ON e.siren   = d.siren
    LEFT JOIN derniere_fact df ON df.tie_id = d.tie_id
    """


COLUMNS = [
    "client_sk", "tie_id", "raison_sociale", "siren", "ville", "code_postal",
    "adresse_complete", "secteur_activite",
    "effectif", "statut", "ca_ytd", "ca_n1", "delta_ca_pct", "ca_12_mois_glissants",
    "nb_missions_actives", "nb_missions_total", "nb_int_actifs", "nb_int_historique",
    "top_3_metiers", "anciennete_jours", "marge_moyenne_pct", "montant_encours",
    "limite_credit", "risque_credit_score", "nb_agences_partenaires",
    "derniere_facture_date", "jours_depuis_derniere", "risque_churn", "_computed_at",
]


def _run_pg_fallback(cfg: Config, pg_conn) -> list[tuple]:
    """Fallback : vue_360 depuis PostgreSQL Gold uniquement (pas de S3).
    G-V360-M03 : accepte pg_conn partagé — une seule connexion PG dans run().
    Note : secteur_activite = d.naf_libelle (NULL en Gold) — équivalent à NULL::VARCHAR.
    """
    query = """
    WITH ca AS (
        SELECT tie_id,
               SUM(CASE WHEN EXTRACT(year FROM mois) = EXTRACT(year FROM CURRENT_DATE)
                        THEN ca_net_ht::NUMERIC ELSE 0 END) AS ca_ytd,
               SUM(CASE WHEN EXTRACT(year FROM mois) = EXTRACT(year FROM CURRENT_DATE) - 1
                        THEN ca_net_ht::NUMERIC ELSE 0 END) AS ca_n1,
               SUM(CASE WHEN mois >= CURRENT_DATE - INTERVAL '12 months'
                        THEN ca_net_ht::NUMERIC ELSE 0 END) AS ca_12m
        FROM gld_commercial.fact_ca_mensuel_client
        GROUP BY tie_id
    ),
    miss AS (
        SELECT tie_id,
               COUNT(DISTINCT CASE WHEN date_fin >= CURRENT_DATE THEN mission_sk END) AS nb_actives,
               COUNT(DISTINCT mission_sk)                                              AS nb_total,
               COUNT(DISTINCT CASE WHEN date_fin >= CURRENT_DATE THEN per_id END)     AS nb_int_actifs,
               COUNT(DISTINCT per_id)                                                  AS nb_int_hist,
               COUNT(DISTINCT agence_id)                                               AS nb_agences,
               MIN(date_debut)                                                         AS first_mission,
               AVG(taux_marge::NUMERIC)                                                AS marge_avg
        FROM gld_staffing.fact_missions_detail
        GROUP BY tie_id
    )
    SELECT
        d.client_sk,
        d.tie_id::INT,
        d.raison_sociale,
        d.siren,
        d.ville,
        d.code_postal,
        NULL::TEXT,                                          -- adresse_complete (absent de gld_shared.dim_clients)
        d.naf_libelle,                                       -- NULL en Gold (Phase 2)
        d.effectif_tranche,
        d.statut_client,
        COALESCE(ca.ca_ytd, 0),
        COALESCE(ca.ca_n1, 0),
        CASE WHEN ca.ca_n1 > 0
             THEN ROUND((ca.ca_ytd - ca.ca_n1) / ca.ca_n1 * 100, 2) END,
        COALESCE(ca.ca_12m, 0),
        COALESCE(miss.nb_actives, 0),
        COALESCE(miss.nb_total, 0),
        COALESCE(miss.nb_int_actifs, 0),
        COALESCE(miss.nb_int_hist, 0),
        '[]',
        COALESCE((CURRENT_DATE - miss.first_mission)::INT, 0),
        COALESCE(ROUND(miss.marge_avg, 4), 0),
        0,                                                   -- montant_encours (absent PG fallback)
        0,                                                   -- limite_credit
        'LOW',                                               -- risque_credit_score
        COALESCE(miss.nb_agences, 0),
        NULL,                                                -- derniere_facture_date
        9999,                                                -- jours_depuis_derniere
        'HIGH',                                              -- risque_churn (dégradé sans factures)
        NOW()
    FROM gld_shared.dim_clients d
    LEFT JOIN ca   ON ca.tie_id   = d.tie_id::INT
    LEFT JOIN miss ON miss.tie_id = d.tie_id::INT
    """
    try:
        with pg_conn.cursor() as cur:
            cur.execute(query)
            return cur.fetchall()
    except Exception as e:
        logger.error(json.dumps({"step": "pg_fallback", "error": str(e)}))
        return []


def run(cfg: Config) -> dict:
    stats = Stats()

    if cfg.mode in (RunMode.OFFLINE, RunMode.PROBE):  # G-V360-M01
        logger.info(json.dumps({
            "mode": cfg.mode.name, "table": "vue_360_client", "action": "skipped"
        }))
        stats.tables_processed += 1
        return stats.finish(cfg, PIPELINE)

    # G-V360-M03 : une seule connexion PG — partagée entre fallback et bulk insert
    with get_pg_connection(cfg) as pg:
        rows = []
        fallback_used = False

        try:
            with get_duckdb_connection(cfg) as ddb:
                rows = ddb.execute(build_vue360_query(cfg)).fetchall()
                logger.info(json.dumps({
                    "table": "vue_360_client", "rows": len(rows), "source": "duckdb"
                }))
        except Exception as e:
            logger.error(json.dumps({"step": "duckdb_build", "error": str(e)}))
            logger.info("Attempting fallback via PostgreSQL Gold direct read")
            rows = _run_pg_fallback(cfg, pg)   # G-V360-M03 : pg_conn partagé
            fallback_used = True
            if not rows:
                stats.errors.append({"step": "duckdb_build", "error": str(e)})
                return stats.finish(cfg, PIPELINE)

        logger.info(json.dumps({
            "vue_360_client": len(rows), "fallback": fallback_used
        }))
        stats.rows_transformed = len(rows)

        try:  # G-V360-M02
            # G-V360-B01 : idempotence PG
            with pg.cursor() as cur:
                cur.execute("TRUNCATE TABLE gld_clients.vue_360_client")
            pg.commit()
            pg_bulk_insert(cfg, pg, "gld_clients", "vue_360_client",
                           COLUMNS, rows, stats)
        except Exception as e:
            logger.exception(json.dumps(
                {"step": "pg_insert", "error": str(e)}))
            stats.errors.append({"step": "pg_insert", "error": str(e)})
            return stats.finish(cfg, PIPELINE)

    stats.tables_processed = 1
    return stats.finish(cfg, PIPELINE)


if __name__ == "__main__":
    run(Config())
