-- fact_marge_mission.sql — Marge brute par mission
-- Phase 2 · GI Data Lakehouse · dbt model
{{ config(materialized='table', schema='gld_commercial') }}

WITH missions AS (
    SELECT * FROM {{ source('slv_missions', 'missions') }}
    WHERE PER_ID IS NOT NULL AND CNT_ID IS NOT NULL
),

contrats AS (
    SELECT * FROM {{ source('slv_missions', 'contrats') }}
),

heures AS (
    SELECT PRH_BTS,
           SUM(RHD_BASEPAYE::NUMERIC(10,2)) AS h_paye,
           SUM(RHD_BASEFACT::NUMERIC(10,2)) AS h_fact
    FROM {{ source('slv_temps', 'heures_detail') }}
    GROUP BY 1
)

SELECT
    MD5(m.PER_ID::TEXT || '|' || m.CNT_ID::TEXT || '|' || m.TIE_ID::TEXT) AS mission_sk,
    m.PER_ID::INT AS per_id,
    m.CNT_ID::INT AS cnt_id,
    m.TIE_ID::INT AS tie_id,
    m.RGPCNT_ID::INT AS agence_id,
    c.MET_ID::INT AS metier_id,
    COALESCE(h.h_fact * c.CNT_TAUXFACT::NUMERIC, 0) AS ca_ht,
    COALESCE(h.h_paye * c.CNT_TAUXPAYE::NUMERIC, 0) AS cout_paye,
    COALESCE(h.h_fact * c.CNT_TAUXFACT::NUMERIC, 0)
    - COALESCE(h.h_paye * c.CNT_TAUXPAYE::NUMERIC, 0) AS marge_brute,
    CASE WHEN COALESCE(h.h_fact * c.CNT_TAUXFACT::NUMERIC, 0) > 0
         THEN ROUND((h.h_fact * c.CNT_TAUXFACT::NUMERIC - h.h_paye * c.CNT_TAUXPAYE::NUMERIC)
              / (h.h_fact * c.CNT_TAUXFACT::NUMERIC), 4)
         ELSE 0 END AS taux_marge,
    COALESCE(h.h_paye, 0) + COALESCE(h.h_fact, 0) AS heures_totales,
    CURRENT_TIMESTAMP AS _computed_at
FROM missions m
LEFT JOIN contrats c ON c."PER_ID"=m."PER_ID" AND c."CNT_ID"=m."CNT_ID"
LEFT JOIN heures h ON h."PRH_BTS"=m."PRH_BTS"
