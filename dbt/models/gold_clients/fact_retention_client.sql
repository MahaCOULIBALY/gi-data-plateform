-- fact_retention_client.sql — Suivi fidélisation trimestre + churn score ML-ready
-- Phase 3 · GI Data Lakehouse · dbt model
{{ config(materialized='table', schema='gld_clients') }}

WITH quarterly AS (
    SELECT
        c.client_sk, f.TIE_ID::INT AS tie_id,
        DATE_TRUNC('quarter', f.EFAC_DATE::DATE)::DATE AS trimestre,
        SUM(CASE WHEN f.EFAC_TYPE='F' THEN f.EFAC_MONTANTHT::NUMERIC ELSE 0 END)
        - SUM(CASE WHEN f.EFAC_TYPE='A' THEN f.EFAC_MONTANTHT::NUMERIC ELSE 0 END) AS ca_net,
        COUNT(DISTINCT m.PER_ID||'|'||m.CNT_ID) AS nb_missions,
        COUNT(DISTINCT f.EFAC_NUM) AS nb_factures,
        MAX(f.EFAC_DATE::DATE) AS derniere_facture
    FROM {{ source('slv_facturation', 'factures') }} f
    LEFT JOIN {{ source('slv_clients', 'dim_clients') }} c ON c.tie_id = f.TIE_ID::INT AND c.is_current
    LEFT JOIN {{ source('slv_missions', 'missions') }} m ON m."TIE_ID"=f."TIE_ID" AND m."RGPCNT_ID"=f."RGPCNT_ID"
    WHERE f.EFAC_DATE IS NOT NULL
    GROUP BY 1, 2, 3
),

with_lag AS (
    SELECT *,
        LAG(ca_net) OVER (PARTITION BY client_sk ORDER BY trimestre)    AS ca_q1,
        LAG(ca_net, 4) OVER (PARTITION BY client_sk ORDER BY trimestre) AS ca_yoy,
        COUNT(*) OVER (PARTITION BY client_sk ORDER BY trimestre
                       ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)       AS freq_4q
    FROM quarterly
)

SELECT client_sk, tie_id, trimestre, ca_net,
    ca_net - COALESCE(ca_q1, 0) AS delta_ca_qoq,
    CASE WHEN COALESCE(ca_q1,0)>0 THEN ROUND((ca_net-ca_q1)/ca_q1, 4) END AS delta_ca_qoq_pct,
    ca_net - COALESCE(ca_yoy, 0) AS delta_ca_yoy,
    CASE WHEN COALESCE(ca_yoy,0)>0 THEN ROUND((ca_net-ca_yoy)/ca_yoy, 4) END AS delta_ca_yoy_pct,
    nb_missions, nb_factures, freq_4q AS frequence_4_trimestres,
    derniere_facture,
    CURRENT_DATE - derniere_facture AS jours_inactivite,
    CASE
        WHEN CURRENT_DATE - derniere_facture > 180 THEN 'PERDU'
        WHEN CURRENT_DATE - derniere_facture > {{ var('churn_seuil_high_jours') }} THEN 'HIGH'
        WHEN CURRENT_DATE - derniere_facture > {{ var('churn_seuil_medium_jours') }} THEN 'MEDIUM'
        WHEN COALESCE(ca_net - ca_q1, 0) / NULLIF(ca_q1, 0) < -0.3 THEN 'MEDIUM'
        WHEN freq_4q <= 1 THEN 'MEDIUM'
        ELSE 'LOW'
    END AS risque_churn,
    ROUND(LEAST(1.0, GREATEST(0.0,
        0.30 * LEAST((CURRENT_DATE - derniere_facture)::NUMERIC / 180, 1.0)
      + 0.25 * (1.0 - LEAST(freq_4q::NUMERIC / 4, 1.0))
      + 0.25 * CASE WHEN ca_net < COALESCE(ca_q1,0) THEN LEAST(ABS(ca_net-ca_q1)/NULLIF(ca_q1,1), 1.0) ELSE 0 END
      + 0.20 * CASE WHEN ca_net < COALESCE(ca_yoy,0) THEN LEAST(ABS(ca_net-ca_yoy)/NULLIF(ca_yoy,1), 1.0) ELSE 0 END
    )), 4) AS churn_score_ml,
    CURRENT_TIMESTAMP AS _computed_at
FROM with_lag
