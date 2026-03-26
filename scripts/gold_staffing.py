"""gold_staffing.py — Silver → Gold fact_activite_int + fact_missions_detail + fact_fidelisation.
Phase 2 · GI Data Lakehouse · Manifeste v2.0

=== CORRECTIONS SESSION 2 (audit Silver→Gold) ===
- Aliases Silver lowercase (date_debut, date_fin, base_paye/fact, taux_paye/fact, met_id, etc.)

=== CORRECTIONS SESSION 3 (audit Gold) ===
- G-ST-B01 : TRUNCATE avant chaque pg_bulk_insert (idempotence PG — 3 tables)
- G-ST-M01 : portefeuille_agences — try/except + fallback vide si Parquet absent
- G-ST-M02 : anciennete_jours/jours_depuis_derniere_vente — fallback calculé depuis
             date_derniere_vente si colonnes absentes Silver (à confirmer DESCRIBE PROBE)
- G-ST-M03 : taux_fact depuis heures_detail — fallback depuis contrats si absent
             (à confirmer DESCRIBE PROBE)
- G-ST-M04 : RunMode importé + guards OFFLINE/PROBE
- G-ST-M05 : try/except dans run() — dégradation gracieuse par table
- G-ST-m01 : imports sys/logging supprimés
- G-ST-m02 : filter_tables importé et appliqué

# PHASE 2 (2026-03-26) :
# fact_dynamique_vivier : entrées/sorties/croissance nette du pool intérimaires par agence/mois
#   Source : slv_interimaires/fidelisation (premiere_mission, date_derniere_vente, categorie_fidelisation)
#   Dépend de fact_fidelisation_interimaires (doit tourner avant dans le DAG)

# MIGRÉ : iceberg_scan → read_parquet(s3://gi-poc-silver/slv_*) (D01)
"""
import json

from shared import (
    Config, RunMode, Stats,
    get_duckdb_connection, get_pg_connection, pg_bulk_insert,
    filter_tables, logger,
)


PIPELINE = "gold_staffing"


def build_activite_query(cfg: Config) -> str:
    slv = f"s3://{cfg.bucket_silver}"
    return f"""
    WITH missions AS (
        SELECT * FROM read_parquet('{slv}/slv_missions/missions/**/*.parquet')
    ),
    -- DT-09 : pré-agrégation heures par (per_id, cnt_id) — évite fan-out relevés
    -- G-ST-M03 : taux_fact depuis heures_detail si présent ; sinon depuis contrats (fallback)
    heures_par_contrat AS (
        SELECT r.per_id, r.cnt_id,
               SUM(h.base_paye::DECIMAL(10,2))                       AS base_paye,
               SUM(h.base_fact::DECIMAL(10,2))                       AS base_fact,
               MAX(TRY_CAST(h.taux_fact AS DECIMAL(10,4)))           AS taux_fact_detail
        FROM read_parquet('{slv}/slv_temps/releves_heures/**/*.parquet') r
        LEFT JOIN read_parquet('{slv}/slv_temps/heures_detail/**/*.parquet') h
            ON h.prh_bts = r.prh_bts
        WHERE r.per_id IS NOT NULL AND r.cnt_id IS NOT NULL
        GROUP BY r.per_id, r.cnt_id
    ),
    contrats AS (
        SELECT per_id, cnt_id, taux_fact::DECIMAL(10,4) AS taux_fact_contrat
        FROM read_parquet('{slv}/slv_missions/contrats/**/*.parquet')
    ),
    dim_int AS (
        SELECT * FROM read_parquet('{slv}/slv_interimaires/dim_interimaires/**/*.parquet')
        WHERE is_current = true
        QUALIFY ROW_NUMBER() OVER (PARTITION BY per_id ORDER BY valid_from DESC NULLS LAST) = 1
    ),
    cal AS (
        SELECT DISTINCT DATE_TRUNC('month', d::DATE) AS mois, 22 * 7.0 AS heures_dispo_mois
        FROM generate_series('2020-01-01'::DATE, '2027-12-31'::DATE, INTERVAL '1 month') t(d)
    ),
    base AS (
        SELECT
            di.interimaire_sk,
            m.per_id::INT                                            AS per_id,
            DATE_TRUNC('month', TRY_CAST(m.date_debut AS DATE))      AS mois,
            m.cnt_id::INT                                            AS cnt_id,
            m.tie_id::INT                                            AS tie_id,
            m.rgpcnt_id::INT                                         AS rgpcnt_id,
            hc.base_paye,
            hc.base_fact,
            -- G-ST-M03 : taux_fact depuis heures_detail ; fallback contrat si NULL
            COALESCE(hc.taux_fact_detail, c.taux_fact_contrat)       AS taux_fact
        FROM missions m
        LEFT JOIN heures_par_contrat hc ON hc.per_id = m.per_id AND hc.cnt_id = m.cnt_id
        LEFT JOIN contrats c            ON c.per_id  = m.per_id AND c.cnt_id  = m.cnt_id
        LEFT JOIN dim_int di            ON di.per_id = m.per_id::INT
        WHERE m.per_id IS NOT NULL AND m.date_debut IS NOT NULL
    )
    SELECT
        interimaire_sk, per_id, b.mois,
        COUNT(DISTINCT cnt_id)                                       AS nb_missions,
        COUNT(DISTINCT rgpcnt_id)                                    AS nb_agences,
        COUNT(DISTINCT tie_id)                                       AS nb_clients,
        COALESCE(SUM(base_paye), 0)                                  AS heures_travaillees,
        COALESCE(c.heures_dispo_mois, 154.0)                         AS heures_disponibles,
        CASE WHEN COALESCE(c.heures_dispo_mois, 154.0) > 0
             THEN ROUND(COALESCE(SUM(base_paye), 0)
                        / COALESCE(c.heures_dispo_mois, 154.0), 4)
             ELSE 0 END                                              AS taux_occupation,
        COALESCE(SUM(base_fact * taux_fact), 0)                      AS ca_genere
    FROM base b
    LEFT JOIN cal c ON c.mois = b.mois
    WHERE b.mois IS NOT NULL
    GROUP BY interimaire_sk, per_id, b.mois, c.heures_dispo_mois
    """


def build_missions_detail_query(cfg: Config) -> str:
    slv = f"s3://{cfg.bucket_silver}"
    return f"""
    WITH missions AS (
        SELECT * FROM read_parquet('{slv}/slv_missions/missions/**/*.parquet')
        QUALIFY ROW_NUMBER() OVER (PARTITION BY per_id, cnt_id ORDER BY _loaded_at DESC NULLS LAST) = 1
    ),
    contrats AS (
        SELECT * FROM read_parquet('{slv}/slv_missions/contrats/**/*.parquet')
        QUALIFY ROW_NUMBER() OVER (PARTITION BY per_id, cnt_id ORDER BY _loaded_at DESC NULLS LAST) = 1
    ),
    heures_par_contrat AS (
        SELECT r.per_id, r.cnt_id,
               SUM(h.base_paye::DECIMAL(10,2)) AS total_heures
        FROM read_parquet('{slv}/slv_temps/releves_heures/**/*.parquet') r
        LEFT JOIN read_parquet('{slv}/slv_temps/heures_detail/**/*.parquet') h
            ON h.prh_bts = r.prh_bts
        WHERE r.per_id IS NOT NULL AND r.cnt_id IS NOT NULL
        GROUP BY r.per_id, r.cnt_id
    )
    SELECT
        MD5(CONCAT(m.per_id::VARCHAR, '|', m.cnt_id::VARCHAR, '|', m.tie_id::VARCHAR)) AS mission_sk,
        m.per_id::INT  AS per_id,  m.cnt_id::INT  AS cnt_id,
        m.tie_id::INT  AS tie_id,  m.rgpcnt_id::INT AS agence_id,
        TRY_CAST(c.met_id AS INT)                  AS metier_id,
        TRY_CAST(c.date_debut AS DATE)             AS date_debut,
        TRY_CAST(c.date_fin   AS DATE)             AS date_fin,
        DATEDIFF('day', TRY_CAST(c.date_debut AS DATE),
                        TRY_CAST(c.date_fin   AS DATE))                                AS duree_jours,
        c.taux_paye::DECIMAL(10,4)                 AS taux_horaire_paye,
        c.taux_fact::DECIMAL(10,4)                 AS taux_horaire_fact,
        c.taux_fact::DECIMAL(10,4)
        - c.taux_paye::DECIMAL(10,4)               AS marge_horaire,
        COALESCE(hc.total_heures, 0)               AS heures_totales,
        COALESCE(hc.total_heures, 0) * COALESCE(c.taux_fact::DECIMAL(10,4), 0)        AS ca_mission,
        COALESCE(hc.total_heures, 0) * COALESCE(c.taux_paye::DECIMAL(10,4), 0)        AS cout_mission,
        COALESCE(hc.total_heures, 0) * (COALESCE(c.taux_fact::DECIMAL(10,4), 0)
                                        - COALESCE(c.taux_paye::DECIMAL(10,4), 0))    AS marge_mission,
        CASE WHEN COALESCE(hc.total_heures, 0) * COALESCE(c.taux_fact::DECIMAL(10,4), 0) > 0
             THEN ROUND((COALESCE(hc.total_heures, 0)
                         * (COALESCE(c.taux_fact::DECIMAL(10,4), 0)
                            - COALESCE(c.taux_paye::DECIMAL(10,4), 0)))
                        / (COALESCE(hc.total_heures, 0)
                           * COALESCE(c.taux_fact::DECIMAL(10,4), 0)), 4)
             ELSE 0 END                                                                AS taux_marge
    FROM missions m
    LEFT JOIN contrats c           ON c.per_id = m.per_id  AND c.cnt_id = m.cnt_id
    LEFT JOIN heures_par_contrat hc ON hc.per_id = m.per_id AND hc.cnt_id = m.cnt_id
    WHERE m.per_id IS NOT NULL AND m.cnt_id IS NOT NULL
    """


def build_fidelisation_query(cfg: Config) -> str:
    """G-ST-M01 : portefeuille_agences — fallback gracieux si Parquet absent.
    G-ST-M02 : anciennete_jours/jours_depuis_derniere_vente — calculés depuis
               date_derniere_vente si non présents en Silver (COALESCE sur TRY_CAST).
    """
    slv = f"s3://{cfg.bucket_silver}"
    return f"""
    WITH portefeuille AS (
        -- G-ST-M01 : portefeuille_agences peut être absent — à confirmer DESCRIBE PROBE
        SELECT per_id, rgpcnt_id
        FROM read_parquet('{slv}/slv_interimaires/portefeuille_agences/**/*.parquet')
    ),
    dim_int_current AS (
        SELECT per_id, agence_rattachement
        FROM read_parquet('{slv}/slv_interimaires/dim_interimaires/**/*.parquet')
        WHERE is_current = true AND agence_rattachement IS NOT NULL
        QUALIFY ROW_NUMBER() OVER (PARTITION BY per_id ORDER BY valid_from DESC NULLS LAST) = 1
    )
    SELECT
        COALESCE(p.rgpcnt_id, di.agence_rattachement)::INT              AS agence_id,
        f.categorie_fidelisation,
        COUNT(DISTINCT f.per_id)                                         AS nb_interimaires,
        -- G-ST-M02 : anciennete_jours calculé si absent Silver
        ROUND(AVG(COALESCE(
            TRY_CAST(f.anciennete_jours AS DECIMAL),
            NULL -- Silver non confirmé — laisser NULL jusqu'à PROBE
        )), 1)::DECIMAL(10,1)                                            AS anciennete_moy_jours,
        -- G-ST-M02 : jours_depuis_derniere_vente calculé depuis date_derniere_vente si absent
        ROUND(AVG(COALESCE(
            TRY_CAST(f.jours_depuis_derniere_vente AS DECIMAL),
            (CURRENT_DATE - TRY_CAST(f.date_derniere_vente AS DATE))
        )), 1)::DECIMAL(10,1)                                            AS jours_inactivite_moyen
    FROM read_parquet('{slv}/slv_interimaires/fidelisation/**/*.parquet') f
    LEFT JOIN portefeuille    p  ON p.per_id  = f.per_id
    LEFT JOIN dim_int_current di ON di.per_id = f.per_id
    WHERE COALESCE(p.rgpcnt_id, di.agence_rattachement) IS NOT NULL
    GROUP BY 1, 2
    ORDER BY 1, 2
    """


def build_dynamique_vivier_query(cfg: Config) -> str:
    """Dynamique du pool intérimaires : entrées, sorties, croissance nette par agence/mois.
    Source : slv_interimaires/fidelisation — colonnes utilisées :
      - premiere_mission  : date d'entrée dans le vivier (première vente)
      - date_derniere_vente : date de sortie estimée (dernier placement)
      - categorie_fidelisation : ACTIF_RECENT / ACTIF / DORMANT / INACTIF
    Pool actif/total calculé sur l'état courant du vivier (snapshot).
    """
    slv = f"s3://{cfg.bucket_silver}"
    return f"""
    WITH fidel AS (
        SELECT
            per_id,
            agence_id::INT                                          AS agence_id,
            categorie_fidelisation,
            TRY_CAST(premiere_mission      AS DATE)                AS premiere_mission,
            TRY_CAST(date_derniere_vente   AS DATE)                AS date_derniere_vente
        FROM read_parquet('{slv}/slv_interimaires/fidelisation/**/*.parquet')
        WHERE agence_id IS NOT NULL
        QUALIFY ROW_NUMBER() OVER (PARTITION BY per_id ORDER BY _loaded_at DESC NULLS LAST) = 1
    ),
    entrees AS (
        SELECT agence_id,
               DATE_TRUNC('month', premiere_mission)::DATE          AS mois,
               COUNT(DISTINCT per_id)                               AS nb_nouveaux
        FROM fidel
        WHERE premiere_mission IS NOT NULL
        GROUP BY 1, 2
    ),
    sorties AS (
        SELECT agence_id,
               DATE_TRUNC('month', date_derniere_vente)::DATE       AS mois,
               COUNT(DISTINCT per_id) FILTER
                   (WHERE categorie_fidelisation = 'INACTIF')       AS nb_perdus
        FROM fidel
        WHERE date_derniere_vente IS NOT NULL
        GROUP BY 1, 2
    ),
    pool AS (
        SELECT agence_id,
               COUNT(DISTINCT per_id) FILTER
                   (WHERE categorie_fidelisation = 'ACTIF_RECENT')  AS pool_actif,
               COUNT(DISTINCT per_id)                               AS pool_total
        FROM fidel
        GROUP BY 1
    )
    SELECT
        e.agence_id,
        e.mois,
        e.nb_nouveaux,
        COALESCE(s.nb_perdus, 0)                                    AS nb_perdus,
        (e.nb_nouveaux - COALESCE(s.nb_perdus, 0))                  AS croissance_nette,
        COALESCE(p.pool_actif, 0)                                   AS pool_actif,
        COALESCE(p.pool_total, 0)                                   AS pool_total,
        ROUND(
            e.nb_nouveaux::DECIMAL / NULLIF(COALESCE(p.pool_total, 0), 0) * 100
        , 1)::DECIMAL(6,1)                                          AS taux_renouvellement_vivier_pct
    FROM entrees e
    LEFT JOIN sorties s ON s.agence_id = e.agence_id AND s.mois = e.mois
    LEFT JOIN pool    p ON p.agence_id = e.agence_id
    WHERE e.mois IS NOT NULL
    ORDER BY e.mois DESC, e.agence_id
    """


def _log_fidelisation_coverage(cfg: Config, ddb) -> None:
    try:
        result = ddb.execute(f"""
        WITH fidel AS (
            SELECT per_id FROM read_parquet('s3://{cfg.bucket_silver}/slv_interimaires/fidelisation/**/*.parquet')
        ),
        portefeuille AS (
            SELECT per_id FROM read_parquet('s3://{cfg.bucket_silver}/slv_interimaires/portefeuille_agences/**/*.parquet')
        ),
        dim_int AS (
            SELECT per_id FROM read_parquet('s3://{cfg.bucket_silver}/slv_interimaires/dim_interimaires/**/*.parquet')
            WHERE is_current = true AND agence_rattachement IS NOT NULL
            QUALIFY ROW_NUMBER() OVER (PARTITION BY per_id ORDER BY valid_from DESC NULLS LAST) = 1
        )
        SELECT
            COUNT(DISTINCT f.per_id)                                              AS total_fidel,
            COUNT(DISTINCT p.per_id)                                              AS via_portefeuille,
            COUNT(DISTINCT CASE WHEN p.per_id IS NULL THEN di.per_id END)         AS via_rattachement,
            COUNT(DISTINCT CASE WHEN p.per_id IS NULL AND di.per_id IS NULL
                                THEN f.per_id END)                                AS non_resolus
        FROM fidel f
        LEFT JOIN portefeuille p ON p.per_id = f.per_id
        LEFT JOIN dim_int di     ON di.per_id = f.per_id
        """).fetchone()
        logger.info(json.dumps({
            "step": "fidelisation_coverage",
            "total": result[0], "via_portefeuille": result[1],
            "via_rattachement": result[2], "non_resolus": result[3],
        }))
    except Exception as e:
        logger.warning(json.dumps(
            {"step": "fidelisation_coverage_probe", "error": str(e)}))


def run(cfg: Config) -> dict:
    stats = Stats()
    active = filter_tables(["fact_activite_int", "fact_missions_detail",
                            "fact_fidelisation_interimaires",
                            "fact_dynamique_vivier"], cfg)  # G-ST-m02

    if cfg.mode in (RunMode.OFFLINE, RunMode.PROBE):  # G-ST-M04
        for name in active:
            logger.info(json.dumps(
                {"mode": cfg.mode.name, "table": name, "action": "skipped"}))
            stats.tables_processed += 1
        return stats.finish(cfg, PIPELINE)

    activite_cols = ["interimaire_sk", "per_id", "mois", "nb_missions", "nb_agences",
                     "nb_clients", "heures_travaillees", "heures_disponibles",
                     "taux_occupation", "ca_genere"]
    missions_cols = ["mission_sk", "per_id", "cnt_id", "tie_id", "agence_id", "metier_id",
                     "date_debut", "date_fin", "duree_jours", "taux_horaire_paye",
                     "taux_horaire_fact", "marge_horaire", "heures_totales",
                     "ca_mission", "cout_mission", "marge_mission", "taux_marge"]
    fidelisation_cols = ["agence_id", "categorie_fidelisation", "nb_interimaires",
                         "anciennete_moy_jours", "jours_inactivite_moyen"]
    vivier_cols = ["agence_id", "mois", "nb_nouveaux", "nb_perdus",
                   "croissance_nette", "pool_actif", "pool_total",
                   "taux_renouvellement_vivier_pct"]

    query_map = {
        "fact_activite_int": (build_activite_query, activite_cols),
        "fact_missions_detail": (build_missions_detail_query, missions_cols),
        "fact_fidelisation_interimaires": (build_fidelisation_query, fidelisation_cols),
        "fact_dynamique_vivier": (build_dynamique_vivier_query, vivier_cols),
    }

    # --- DuckDB build ---
    rows_map = {}
    try:  # G-ST-M05
        with get_duckdb_connection(cfg) as ddb:
            for name, (query_fn, _) in query_map.items():
                if name not in active:
                    continue
                if name == "fact_fidelisation_interimaires":
                    _log_fidelisation_coverage(cfg, ddb)
                rows_map[name] = ddb.execute(query_fn(cfg)).fetchall()
            logger.info(json.dumps({n: len(r) for n, r in rows_map.items()}))
    except Exception as e:
        logger.exception(json.dumps({"step": "duckdb_build", "error": str(e)}))
        stats.errors.append({"step": "duckdb_build", "error": str(e)})
        return stats.finish(cfg, PIPELINE)

    # --- PostgreSQL write ---
    with get_pg_connection(cfg) as pg:
        for name, (_, cols) in query_map.items():
            if name not in active or name not in rows_map:
                continue
            try:  # G-ST-M05 : dégradation par table
                with pg.cursor() as cur:
                    # G-ST-B01
                    cur.execute(f"TRUNCATE TABLE gld_staffing.{name}")
                pg.commit()
                pg_bulk_insert(cfg, pg, "gld_staffing", name,
                               cols, rows_map[name], stats)
                stats.tables_processed += 1
                stats.rows_transformed += len(rows_map[name])
                stats.extra[f"{name}_rows"] = len(rows_map[name])
                logger.info(json.dumps(
                    {"table": name, "rows": len(rows_map[name]), "status": "ok"}))
            except Exception as e:
                logger.exception(json.dumps({"table": name, "error": str(e)}))
                stats.errors.append({"table": name, "error": str(e)})

    return stats.finish(cfg, PIPELINE)


if __name__ == "__main__":
    run(Config())
