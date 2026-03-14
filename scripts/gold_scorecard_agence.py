"""gold_scorecard_agence.py — Silver → Gold scorecard_agence + ranking_agences + tendances.
Phase 3 · GI Data Lakehouse · Manifeste v2.0

=== CORRECTIONS SESSION 2 (audit Silver→Gold) ===
- f.EFAC_DATE      → f.date_facture   (Silver alias)
- f.EFAC_TYPE      → f.type_facture
- f.EFAC_MONTANTHT → montant_mnt_total via lignes_factures (B-02 NULL fix)
- f.EFAC_NUM       → f.efac_num
- m.MISS_DATEDEBUT → m.date_debut
- c.CNT_TAUXFACT   → c.taux_horaire_fact
- c.CNT_TAUXPAYE   → c.taux_horaire_paye
- h.RHD_BASEPAYE   → h.base_paye
- h.RHD_BASEFACT   → h.base_fact
- cmd.CMD_DATE     → cmd.cmd_date
- cmd.CMD_STATUT   → cmd.statut  (NULL — absent DDL)
- m.PRH_BTS        → jointure via releves (per_id+cnt_id)
"""
import sys
import logging
from shared import Config, Stats, get_duckdb_connection, get_pg_connection, pg_bulk_insert, logger
from gold_helpers import cte_montants_factures, cte_heures_par_contrat


def build_scorecard_query(cfg: Config) -> str:
    silver = f"s3://{cfg.bucket_silver}"
    return f"""
    WITH factures AS (
        SELECT * FROM read_parquet('{silver}/slv_facturation/factures/**/*.parquet', hive_partitioning=true)
    ),
    {cte_montants_factures(silver)},
    missions AS (
        SELECT * FROM read_parquet('{silver}/slv_missions/missions/**/*.parquet', hive_partitioning=true)
    ),
    contrats AS (
        SELECT * FROM read_parquet('{silver}/slv_missions/contrats/**/*.parquet', hive_partitioning=true)
    ),
    releves AS (
        SELECT * FROM read_parquet('{silver}/slv_temps/releves_heures/**/*.parquet', hive_partitioning=true)
    ),
    heures AS (
        SELECT prh_bts,
               SUM(base_paye::DECIMAL(10,2)) AS h_paye,
               SUM(base_fact::DECIMAL(10,2)) AS h_fact
        FROM read_parquet('{silver}/slv_temps/heures_detail/**/*.parquet', hive_partitioning=true)
        GROUP BY prh_bts
    ),
    commandes AS (
        SELECT * FROM read_parquet('{silver}/slv_missions/commandes/**/*.parquet', hive_partitioning=true)
    ),
    base_ca AS (
        SELECT
            f.rgpcnt_id::INT AS agence_id,
            DATE_TRUNC('month', TRY_CAST(f.date_facture AS DATE)) AS mois,
            SUM(CASE WHEN f.type_facture='F' THEN COALESCE(mt.montant_ht_calc, 0)::DECIMAL(18,2) ELSE 0 END) AS ca_ht,
            SUM(CASE WHEN f.type_facture='A' THEN COALESCE(mt.montant_ht_calc, 0)::DECIMAL(18,2) ELSE 0 END) AS avoir_ht,
            COUNT(DISTINCT CASE WHEN f.type_facture='F' THEN f.efac_num END) AS nb_factures
        FROM factures f
        LEFT JOIN montants mt ON mt.fac_num = f.efac_num
        WHERE f.date_facture IS NOT NULL
        GROUP BY 1, 2
    ),
    base_staffing AS (
        SELECT
            m.rgpcnt_id::INT AS agence_id,
            DATE_TRUNC('month', TRY_CAST(m.date_debut AS DATE)) AS mois,
            COUNT(DISTINCT m.per_id) AS nb_int_actifs,
            COUNT(DISTINCT m.tie_id) AS nb_clients_actifs,
            COUNT(DISTINCT m.per_id||'|'||m.cnt_id) AS nb_missions
        FROM missions m
        WHERE m.date_debut IS NOT NULL
        GROUP BY 1, 2
    ),
    base_marge AS (
        SELECT
            m.rgpcnt_id::INT AS agence_id,
            DATE_TRUNC('month', TRY_CAST(m.date_debut AS DATE)) AS mois,
            COALESCE(SUM(hc.h_paye * c.taux_horaire_paye::DECIMAL(10,4)), 0) AS cout_missions,
            COALESCE(SUM(hc.h_fact * c.taux_horaire_fact::DECIMAL(10,4)), 0) AS ca_missions
        FROM missions m
        LEFT JOIN contrats c ON c.per_id=m.per_id AND c.cnt_id=m.cnt_id
        -- DT-09: pré-agréger heures par (per_id, cnt_id) pour éviter doublons de relevés
        LEFT JOIN (
            SELECT r.per_id, r.cnt_id,
                   SUM(h.h_paye) AS h_paye, SUM(h.h_fact) AS h_fact
            FROM releves r
            LEFT JOIN heures h ON h.prh_bts = r.prh_bts
            GROUP BY r.per_id, r.cnt_id
        ) hc ON hc.per_id = m.per_id AND hc.cnt_id = m.cnt_id
        WHERE m.date_debut IS NOT NULL
        GROUP BY 1, 2
    ),
    base_transfo AS (
        SELECT
            cmd.rgpcnt_id::INT AS agence_id,
            DATE_TRUNC('month', TRY_CAST(cmd.cmd_dte AS DATE)) AS mois,
            COUNT(*) AS nb_commandes,
            -- statut NULL (absent DDL WTCMD) → taux_transformation sera NULL, documenté
            COUNT(CASE WHEN cmd.statut IN ('P','C') THEN 1 END) AS nb_pourvues
        FROM commandes cmd
        WHERE cmd.cmd_dte IS NOT NULL
        GROUP BY 1, 2
    )
    SELECT
        ca.agence_id,
        ca.mois,
        ca.ca_ht - ca.avoir_ht AS ca_net_ht,
        CASE WHEN mg.ca_missions > 0
             THEN ROUND((mg.ca_missions - mg.cout_missions) / mg.ca_missions, 4)
             ELSE 0 END AS taux_marge,
        mg.ca_missions - mg.cout_missions AS marge_brute,
        COALESCE(s.nb_clients_actifs, 0) AS nb_clients_actifs,
        COALESCE(s.nb_int_actifs, 0) AS nb_int_actifs,
        COALESCE(s.nb_missions, 0) AS nb_missions,
        CASE WHEN COALESCE(t.nb_commandes, 0) > 0
             THEN ROUND(COALESCE(t.nb_pourvues, 0)::DECIMAL / t.nb_commandes, 4)
             ELSE NULL END AS taux_transformation,
        COALESCE(t.nb_commandes, 0) AS nb_commandes,
        COALESCE(t.nb_pourvues, 0) AS nb_pourvues
    FROM base_ca ca
    LEFT JOIN base_staffing s ON s.agence_id=ca.agence_id AND s.mois=ca.mois
    LEFT JOIN base_marge mg ON mg.agence_id=ca.agence_id AND mg.mois=ca.mois
    LEFT JOIN base_transfo t ON t.agence_id=ca.agence_id AND t.mois=ca.mois
    WHERE ca.mois IS NOT NULL
    ORDER BY ca.mois DESC, ca.ca_ht DESC
    """


def build_ranking_query() -> str:
    """Ranking multi-critères depuis scorecard (post-insert, requête PG)."""
    return """
    SELECT
        agence_id, mois,
        ca_net_ht, taux_marge, nb_int_actifs, taux_transformation,
        RANK() OVER (PARTITION BY mois ORDER BY ca_net_ht DESC)            AS rang_ca,
        RANK() OVER (PARTITION BY mois ORDER BY taux_marge DESC)           AS rang_marge,
        RANK() OVER (PARTITION BY mois ORDER BY nb_int_actifs DESC)        AS rang_placement,
        RANK() OVER (PARTITION BY mois ORDER BY taux_transformation DESC NULLS LAST) AS rang_transfo,
        ROUND((
            0.35 * PERCENT_RANK() OVER (PARTITION BY mois ORDER BY ca_net_ht)
          + 0.25 * PERCENT_RANK() OVER (PARTITION BY mois ORDER BY taux_marge)
          + 0.25 * PERCENT_RANK() OVER (PARTITION BY mois ORDER BY nb_int_actifs)
          + 0.15 * PERCENT_RANK() OVER (PARTITION BY mois ORDER BY taux_transformation NULLS FIRST)
        )::NUMERIC, 4) AS score_global
    FROM gld_performance.scorecard_agence
    ORDER BY mois DESC, score_global DESC
    """


def build_tendances_query() -> str:
    """Tendances N/N-1 et M/M-1 depuis scorecard (post-insert, requête PG)."""
    return """
    WITH curr AS (
        SELECT *, LAG(ca_net_ht) OVER (PARTITION BY agence_id ORDER BY mois) AS ca_m1,
                  LAG(taux_marge) OVER (PARTITION BY agence_id ORDER BY mois) AS marge_m1,
                  LAG(nb_int_actifs) OVER (PARTITION BY agence_id ORDER BY mois) AS int_m1
        FROM gld_performance.scorecard_agence
    ),
    yoy AS (
        SELECT c.agence_id, c.mois,
               c.ca_net_ht, c.taux_marge, c.nb_int_actifs,
               c.ca_net_ht - COALESCE(c.ca_m1, 0) AS delta_ca_mom,
               c.taux_marge - COALESCE(c.marge_m1, 0) AS delta_marge_mom,
               c.nb_int_actifs - COALESCE(c.int_m1, 0) AS delta_int_mom,
               c.ca_net_ht - COALESCE(prev.ca_net_ht, 0) AS delta_ca_yoy,
               c.taux_marge - COALESCE(prev.taux_marge, 0) AS delta_marge_yoy
        FROM curr c
        LEFT JOIN gld_performance.scorecard_agence prev
            ON prev.agence_id = c.agence_id
           AND prev.mois = c.mois - INTERVAL '12 months'
    )
    SELECT *,
        CASE WHEN delta_ca_mom > 0 AND delta_ca_yoy > 0 THEN 'HAUSSE'
             WHEN delta_ca_mom < 0 AND delta_ca_yoy < 0 THEN 'BAISSE'
             ELSE 'STABLE' END AS tendance
    FROM yoy
    ORDER BY mois DESC, ca_net_ht DESC
    """


def run(cfg: Config) -> dict:
    stats = Stats()
    sc_cols = ["agence_id", "mois", "ca_net_ht", "taux_marge", "marge_brute",
               "nb_clients_actifs", "nb_int_actifs", "nb_missions",
               "taux_transformation", "nb_commandes", "nb_pourvues"]
    rk_cols = ["agence_id", "mois", "ca_net_ht", "taux_marge", "nb_int_actifs",
               "taux_transformation", "rang_ca", "rang_marge", "rang_placement",
               "rang_transfo", "score_global"]
    td_cols = ["agence_id", "mois", "ca_net_ht", "taux_marge", "nb_int_actifs",
               "delta_ca_mom", "delta_marge_mom", "delta_int_mom",
               "delta_ca_yoy", "delta_marge_yoy", "tendance"]

    with get_duckdb_connection(cfg) as ddb:
        sc_rows = ddb.execute(build_scorecard_query(cfg)).fetchall()
        logger.info(f"scorecard_agence: {len(sc_rows)} rows")

    with get_pg_connection(cfg) as pg:
        pg_bulk_insert(cfg, pg, "gld_performance",
                       "scorecard_agence", sc_cols, sc_rows, stats)

        if not cfg.dry_run:
            with pg.cursor() as cur:
                cur.execute(build_ranking_query())
                rk_rows = cur.fetchall()
                cur.execute(build_tendances_query())
                td_rows = cur.fetchall()
            pg_bulk_insert(cfg, pg, "gld_performance",
                           "ranking_agences", rk_cols, rk_rows, stats)
            pg_bulk_insert(cfg, pg, "gld_performance",
                           "tendances_agence", td_cols, td_rows, stats)
            stats.extra["ranking_rows"] = len(rk_rows)
            stats.extra["tendances_rows"] = len(td_rows)

    stats.tables_processed = 3
    stats.rows_transformed = len(sc_rows)
    return stats.finish()


if __name__ == "__main__":
    cfg = Config()
    run(cfg)
