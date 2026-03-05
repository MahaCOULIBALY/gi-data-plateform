-- fact_turnover_int.sql — Rotation intérimaires par agence × mois
-- Phase 2 · GI Data Lakehouse · dbt model
{{ config(materialized='table', schema='gld_staffing') }}

WITH missions AS (
    SELECT RGPCNT_ID::INT AS agence_id,
           PER_ID::INT AS per_id,
           MISS_DATEDEBUT::DATE AS debut,
           MISS_DATEFIN::DATE AS fin
    FROM {{ source('slv_missions', 'missions') }}
    WHERE MISS_DATEDEBUT IS NOT NULL
),

entrees AS (
    SELECT agence_id,
           DATE_TRUNC('month', debut)::DATE AS mois,
           COUNT(DISTINCT per_id) AS nb_entrees
    FROM missions
    GROUP BY 1, 2
),

sorties AS (
    SELECT agence_id,
           DATE_TRUNC('month', fin)::DATE AS mois,
           COUNT(DISTINCT per_id) AS nb_sorties
    FROM missions
    WHERE fin IS NOT NULL
    GROUP BY 1, 2
),

effectif AS (
    SELECT agence_id,
           DATE_TRUNC('month', debut)::DATE AS mois,
           COUNT(DISTINCT per_id) AS effectif_moyen
    FROM missions
    WHERE debut <= CURRENT_DATE AND (fin IS NULL OR fin >= debut)
    GROUP BY 1, 2
)

SELECT
    COALESCE(e.agence_id, s.agence_id) AS agence_id,
    COALESCE(e.mois, s.mois) AS mois,
    COALESCE(e.nb_entrees, 0) AS nb_entrees,
    COALESCE(s.nb_sorties, 0) AS nb_sorties,
    COALESCE(ef.effectif_moyen, 1) AS effectif_moyen,
    CASE WHEN COALESCE(ef.effectif_moyen, 0) > 0
         THEN ROUND((COALESCE(s.nb_sorties, 0)::NUMERIC) / ef.effectif_moyen, 4)
         ELSE 0 END AS taux_turnover,
    CURRENT_TIMESTAMP AS _computed_at
FROM entrees e
FULL OUTER JOIN sorties s ON s.agence_id=e.agence_id AND s.mois=e.mois
LEFT JOIN effectif ef ON ef.agence_id=COALESCE(e.agence_id,s.agence_id) AND ef.mois=COALESCE(e.mois,s.mois)
