-- fact_staffing_agence.sql — Indicateurs placement par agence × semaine
-- Phase 2 · GI Data Lakehouse · dbt model
{{ config(materialized='table', schema='gld_staffing') }}

WITH missions AS (
    SELECT RGPCNT_ID::INT AS agence_id,
           DATE_TRUNC('week', MISS_DATEDEBUT::DATE)::DATE AS semaine,
           PER_ID, CNT_ID, TIE_ID,
           MISS_DATEDEBUT::DATE AS debut, MISS_DATEFIN::DATE AS fin
    FROM {{ source('slv_missions', 'missions') }}
    WHERE MISS_DATEDEBUT IS NOT NULL
),

dim_int AS (
    SELECT per_id, agence_rattachement
    FROM {{ source('slv_interimaires', 'dim_interimaires') }}
    WHERE is_current = TRUE
),

commandes AS (
    SELECT RGPCNT_ID::INT AS agence_id,
           DATE_TRUNC('week', CMD_DATE::DATE)::DATE AS semaine,
           COUNT(*) AS nb_commandes,
           COUNT(*) FILTER (WHERE CMD_STATUT IN ('P','C')) AS nb_pourvues
    FROM {{ source('slv_missions', 'commandes') }}
    WHERE CMD_DATE IS NOT NULL
    GROUP BY 1, 2
),

en_mission AS (
    SELECT agence_id, semaine, COUNT(DISTINCT PER_ID) AS int_en_mission
    FROM missions
    GROUP BY 1, 2
),

disponibles AS (
    SELECT di.agence_rattachement::INT AS agence_id,
           m.semaine,
           COUNT(DISTINCT di.per_id) AS int_total
    FROM dim_int di
    CROSS JOIN (SELECT DISTINCT semaine FROM missions) m
    GROUP BY 1, 2
)

SELECT
    em.agence_id, em.semaine,
    em.int_en_mission,
    GREATEST(COALESCE(d.int_total, 0) - em.int_en_mission, 0) AS int_disponibles,
    CASE WHEN COALESCE(d.int_total, 0) > 0
         THEN ROUND(em.int_en_mission::NUMERIC / d.int_total, 4)
         ELSE NULL END AS taux_placement,
    COALESCE(c.nb_commandes, 0) AS nb_commandes,
    CASE WHEN COALESCE(c.nb_commandes, 0) > 0
         THEN ROUND(c.nb_pourvues::NUMERIC / c.nb_commandes, 4)
         ELSE NULL END AS taux_transformation,
    CURRENT_TIMESTAMP AS _computed_at
FROM en_mission em
LEFT JOIN disponibles d ON d.agence_id=em.agence_id AND d.semaine=em.semaine
LEFT JOIN commandes c ON c.agence_id=em.agence_id AND c.semaine=em.semaine
