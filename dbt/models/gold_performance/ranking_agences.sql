-- ranking_agences.sql — Classement multi-critères agences
-- Phase 3 · GI Data Lakehouse · dbt model
{{ config(materialized='table', schema='gld_performance') }}

SELECT
    agence_id, mois, ca_net_ht, taux_marge, nb_int_actifs, taux_transformation,
    RANK() OVER (PARTITION BY mois ORDER BY ca_net_ht DESC)                       AS rang_ca,
    RANK() OVER (PARTITION BY mois ORDER BY taux_marge DESC)                      AS rang_marge,
    RANK() OVER (PARTITION BY mois ORDER BY nb_int_actifs DESC)                   AS rang_placement,
    RANK() OVER (PARTITION BY mois ORDER BY taux_transformation DESC NULLS LAST)  AS rang_transfo,
    ROUND((
        0.35 * PERCENT_RANK() OVER (PARTITION BY mois ORDER BY ca_net_ht)
      + 0.25 * PERCENT_RANK() OVER (PARTITION BY mois ORDER BY taux_marge)
      + 0.25 * PERCENT_RANK() OVER (PARTITION BY mois ORDER BY nb_int_actifs)
      + 0.15 * PERCENT_RANK() OVER (PARTITION BY mois ORDER BY taux_transformation NULLS FIRST)
    )::NUMERIC, 4) AS score_global,
    CURRENT_TIMESTAMP AS _computed_at
FROM {{ ref('scorecard_agence') }}
ORDER BY mois DESC, score_global DESC
