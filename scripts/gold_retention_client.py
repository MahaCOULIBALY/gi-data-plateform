"""gold_retention_client.py — Silver → Gold fact_retention_client + fact_rentabilite_client.
Phase 3 · GI Data Lakehouse · Manifeste v2.0

=== CORRECTIONS SESSION 2 (audit Silver→Gold) ===
- f.EFAC_DATE      → f.date_facture
- f.EFAC_TYPE      → f.type_facture
- f.EFAC_MONTANTHT → reconstitution via lignes_factures (B-02)
- f.EFAC_NUM       → f.efac_num
- m.PER_ID/CNT_ID/TIE_ID/RGPCNT_ID → lowercase (per_id, cnt_id, tie_id, rgpcnt_id)
- c.CNT_TAUXFACT   → c.taux_horaire_fact
- c.CNT_TAUXPAYE   → c.taux_horaire_paye
- h.RHD_BASEPAYE   → h.base_paye  (alias from RHD_BASEP)
- h.RHD_BASEFACT   → h.base_fact  (alias from RHD_BASEF)
- m.PRH_BTS        → jointure via releves (per_id+cnt_id)
"""
import sys
import logging
from shared import Config, Stats, get_duckdb_connection, get_pg_connection, pg_bulk_insert, logger


def build_retention_query(cfg: Config) -> str:
    """Suivi fidélisation par trimestre — ML-ready features."""
    silver = f"s3://{cfg.bucket_silver}"
    return f"""
    WITH factures AS (
        SELECT * FROM read_parquet('{silver}/slv_facturation/factures/**/*.parquet', hive_partitioning=true)
    ),
    lignes AS (
        SELECT * FROM read_parquet('{silver}/slv_facturation/lignes_factures/**/*.parquet', hive_partitioning=true)
    ),
    montants AS (
        SELECT fac_num, COALESCE(SUM(lfac_mnt), 0) AS montant_ht_calc
        FROM lignes GROUP BY fac_num
    ),
    missions AS (
        SELECT * FROM read_parquet('{silver}/slv_missions/missions/**/*.parquet', hive_partitioning=true)
    ),
    dim_clients AS (
        SELECT * FROM read_parquet('{silver}/slv_clients/dim_clients/**/*.parquet', hive_partitioning=true)
        WHERE is_current = true
    ),
    quarterly AS (
        SELECT
            c.client_sk,
            f.tie_id::INT AS tie_id,
            DATE_TRUNC('quarter', TRY_CAST(f.date_facture AS DATE)) AS trimestre,
            SUM(CASE WHEN f.type_facture='F' THEN COALESCE(mt.montant_ht_calc, 0)::DECIMAL(18,2) ELSE 0 END)
            - SUM(CASE WHEN f.type_facture='A' THEN COALESCE(mt.montant_ht_calc, 0)::DECIMAL(18,2) ELSE 0 END) AS ca_net,
            COUNT(DISTINCT m.per_id||'|'||m.cnt_id) AS nb_missions,
            COUNT(DISTINCT f.efac_num) AS nb_factures,
            MAX(TRY_CAST(f.date_facture AS DATE)) AS derniere_facture
        FROM factures f
        LEFT JOIN montants mt ON mt.fac_num = f.efac_num
        LEFT JOIN dim_clients c ON c.tie_id = f.tie_id::INT
        LEFT JOIN missions m ON m.tie_id = f.tie_id AND m.rgpcnt_id = f.rgpcnt_id
        WHERE f.date_facture IS NOT NULL
        GROUP BY 1, 2, 3
    ),
    with_lag AS (
        SELECT *,
            LAG(ca_net) OVER (PARTITION BY client_sk ORDER BY trimestre) AS ca_q1,
            LAG(ca_net, 4) OVER (PARTITION BY client_sk ORDER BY trimestre) AS ca_yoy,
            LAG(nb_missions) OVER (PARTITION BY client_sk ORDER BY trimestre) AS missions_q1,
            COUNT(*) OVER (PARTITION BY client_sk ORDER BY trimestre
                           ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS freq_4q
        FROM quarterly
    )
    SELECT
        client_sk,
        tie_id,
        trimestre,
        ca_net,
        COALESCE(ca_net - ca_q1, 0) AS delta_ca_qoq,
        CASE WHEN COALESCE(ca_q1, 0) > 0
             THEN ROUND((ca_net - ca_q1) / ca_q1, 4) ELSE NULL END AS delta_ca_qoq_pct,
        COALESCE(ca_net - ca_yoy, 0) AS delta_ca_yoy,
        CASE WHEN COALESCE(ca_yoy, 0) > 0
             THEN ROUND((ca_net - ca_yoy) / ca_yoy, 4) ELSE NULL END AS delta_ca_yoy_pct,
        nb_missions,
        nb_factures,
        freq_4q AS frequence_4_trimestres,
        derniere_facture,
        CURRENT_DATE - derniere_facture AS jours_inactivite,
        CASE
            WHEN CURRENT_DATE - derniere_facture > 180 THEN 'PERDU'
            WHEN CURRENT_DATE - derniere_facture > 90  THEN 'HIGH'
            WHEN CURRENT_DATE - derniere_facture > 45  THEN 'MEDIUM'
            WHEN delta_ca_qoq_pct < -0.3              THEN 'MEDIUM'
            WHEN freq_4q <= 1                          THEN 'MEDIUM'
            ELSE 'LOW'
        END AS risque_churn,
        ROUND(LEAST(1.0, GREATEST(0.0,
            0.3 * LEAST((CURRENT_DATE - derniere_facture)::DECIMAL / 180, 1.0)
          + 0.25 * (1.0 - LEAST(freq_4q::DECIMAL / 4, 1.0))
          + 0.25 * CASE WHEN COALESCE(delta_ca_qoq_pct, 0) < 0
                        THEN LEAST(ABS(delta_ca_qoq_pct), 1.0) ELSE 0 END
          + 0.20 * CASE WHEN COALESCE(delta_ca_yoy_pct, 0) < 0
                        THEN LEAST(ABS(delta_ca_yoy_pct), 1.0) ELSE 0 END
        )), 4) AS churn_score_ml
    FROM with_lag
    WHERE trimestre IS NOT NULL
    ORDER BY trimestre DESC, ca_net DESC
    """


def build_rentabilite_query(cfg: Config) -> str:
    """Rentabilité nette par client × année."""
    silver = f"s3://{cfg.bucket_silver}"
    return f"""
    WITH factures AS (
        SELECT * FROM read_parquet('{silver}/slv_facturation/factures/**/*.parquet', hive_partitioning=true)
    ),
    lignes AS (
        SELECT * FROM read_parquet('{silver}/slv_facturation/lignes_factures/**/*.parquet', hive_partitioning=true)
    ),
    montants AS (
        SELECT fac_num, COALESCE(SUM(lfac_mnt), 0) AS montant_ht_calc
        FROM lignes GROUP BY fac_num
    ),
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
    dim_clients AS (
        SELECT * FROM read_parquet('{silver}/slv_clients/dim_clients/**/*.parquet', hive_partitioning=true)
        WHERE is_current = true
    ),
    yearly AS (
        SELECT
            dc.client_sk,
            f.tie_id::INT AS tie_id,
            EXTRACT(YEAR FROM TRY_CAST(f.date_facture AS DATE))::INT AS annee,
            SUM(CASE WHEN f.type_facture='F' THEN COALESCE(mt.montant_ht_calc, 0)::DECIMAL(18,2) ELSE 0 END)
            - SUM(CASE WHEN f.type_facture='A' THEN COALESCE(mt.montant_ht_calc, 0)::DECIMAL(18,2) ELSE 0 END) AS ca_net,
            COALESCE(SUM(h.h_fact * c.taux_horaire_fact::DECIMAL(10,4)), 0) AS ca_missions,
            COALESCE(SUM(h.h_paye * c.taux_horaire_paye::DECIMAL(10,4)), 0) AS cout_paye,
            COUNT(DISTINCT m.per_id) AS nb_interimaires,
            SUM(CASE WHEN f.type_facture='F' THEN COALESCE(mt.montant_ht_calc, 0)::DECIMAL(18,2) ELSE 0 END) * 0.05 AS cout_gestion_estime
        FROM factures f
        LEFT JOIN montants mt ON mt.fac_num = f.efac_num
        LEFT JOIN missions m ON m.tie_id = f.tie_id AND m.rgpcnt_id = f.rgpcnt_id
        LEFT JOIN contrats c ON c.per_id = m.per_id AND c.cnt_id = m.cnt_id
        LEFT JOIN releves r ON r.per_id = m.per_id AND r.cnt_id = m.cnt_id
        LEFT JOIN heures h ON h.prh_bts = r.prh_bts
        LEFT JOIN dim_clients dc ON dc.tie_id = f.tie_id::INT
        WHERE f.date_facture IS NOT NULL
        GROUP BY 1, 2, 3
    )
    SELECT
        client_sk, tie_id, annee, ca_net, ca_missions, cout_paye,
        ca_missions - cout_paye AS marge_brute,
        CASE WHEN ca_missions > 0
             THEN ROUND((ca_missions - cout_paye) / ca_missions, 4)
             ELSE 0 END AS taux_marge,
        cout_gestion_estime,
        ca_missions - cout_paye - cout_gestion_estime AS rentabilite_nette,
        CASE WHEN ca_missions > 0
             THEN ROUND((ca_missions - cout_paye - cout_gestion_estime) / ca_missions, 4)
             ELSE 0 END AS taux_rentabilite_nette,
        nb_interimaires
    FROM yearly
    WHERE annee IS NOT NULL
    ORDER BY annee DESC, ca_net DESC
    """


def run(cfg: Config) -> dict:
    stats = Stats()
    ret_cols = ["client_sk", "tie_id", "trimestre", "ca_net", "delta_ca_qoq",
                "delta_ca_qoq_pct", "delta_ca_yoy", "delta_ca_yoy_pct",
                "nb_missions", "nb_factures", "frequence_4_trimestres",
                "derniere_facture", "jours_inactivite", "risque_churn", "churn_score_ml"]
    rent_cols = ["client_sk", "tie_id", "annee", "ca_net", "ca_missions", "cout_paye",
                 "marge_brute", "taux_marge", "cout_gestion_estime",
                 "rentabilite_nette", "taux_rentabilite_nette", "nb_interimaires"]

    with get_duckdb_connection(cfg) as ddb:
        ret_rows = ddb.execute(build_retention_query(cfg)).fetchall()
        rent_rows = ddb.execute(build_rentabilite_query(cfg)).fetchall()
        logger.info(
            f"fact_retention_client: {len(ret_rows)} | fact_rentabilite_client: {len(rent_rows)}")

    with get_pg_connection(cfg) as pg:
        pg_bulk_insert(cfg, pg, "gld_clients",
                       "fact_retention_client", ret_cols, ret_rows, stats)
        pg_bulk_insert(cfg, pg, "gld_clients",
                       "fact_rentabilite_client", rent_cols, rent_rows, stats)

    stats.tables_processed = 2
    stats.rows_transformed = len(ret_rows) + len(rent_rows)
    stats.extra["retention_rows"] = len(ret_rows)
    stats.extra["rentabilite_rows"] = len(rent_rows)
    return stats.finish()


if __name__ == "__main__":
    cfg = Config()
    run(cfg)
