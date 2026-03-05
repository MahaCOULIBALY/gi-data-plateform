-- fact_heures_hebdo.sql — Heures consolidées agence × client × semaine
-- Phase 2 · GI Data Lakehouse · dbt model
{{ config(materialized='table', schema='gld_operationnel') }}

WITH releves AS (
    SELECT * FROM {{ source('slv_temps', 'releves_heures') }}
),

detail AS (
    SELECT PRH_BTS,
           SUM(CASE WHEN RHD_RUBRIQUE IN ('HN','HNOR') THEN RHD_BASEPAYE::NUMERIC ELSE 0 END) AS heures_normales,
           SUM(CASE WHEN RHD_RUBRIQUE LIKE 'HS%' THEN RHD_BASEPAYE::NUMERIC ELSE 0 END)       AS heures_sup,
           SUM(CASE WHEN RHD_RUBRIQUE LIKE 'HN%UIT%' THEN RHD_BASEPAYE::NUMERIC ELSE 0 END)   AS heures_nuit,
           SUM(CASE WHEN RHD_RUBRIQUE LIKE 'HD%' THEN RHD_BASEPAYE::NUMERIC ELSE 0 END)        AS heures_dimanche,
           COUNT(*) AS nb_rubriques
    FROM {{ source('slv_temps', 'heures_detail') }}
    GROUP BY 1
)

SELECT
    r.RGPCNT_ID::INT AS agence_id,
    r.TIE_ID::INT AS tie_id,
    DATE_TRUNC('week', r.PRH_DATEDEBUT::DATE)::DATE AS semaine,
    COALESCE(d.heures_normales, 0) AS heures_normales,
    COALESCE(d.heures_sup, 0) AS heures_sup,
    COALESCE(d.heures_nuit, 0) AS heures_nuit,
    COALESCE(d.heures_dimanche, 0) AS heures_dimanche,
    COALESCE(d.heures_normales, 0) + COALESCE(d.heures_sup, 0)
    + COALESCE(d.heures_nuit, 0) + COALESCE(d.heures_dimanche, 0) AS heures_totales,
    COUNT(DISTINCT r.PRH_BTS) AS nb_releves,
    CURRENT_TIMESTAMP AS _computed_at
FROM releves r
LEFT JOIN detail d ON d."PRH_BTS" = r."PRH_BTS"
WHERE r.PRH_DATEDEBUT IS NOT NULL
GROUP BY r.RGPCNT_ID, r.TIE_ID,
         DATE_TRUNC('week', r.PRH_DATEDEBUT::DATE),
         d.heures_normales, d.heures_sup, d.heures_nuit, d.heures_dimanche
