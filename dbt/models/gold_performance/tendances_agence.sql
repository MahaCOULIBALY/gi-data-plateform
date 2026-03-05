-- tendances_agence.sql — Variations M/M-1 et N/N-1 par agence
-- Phase 3 · GI Data Lakehouse · dbt model
{{ config(materialized='table', schema='gld_performance') }}

WITH curr AS (
    SELECT *,
        LAG(ca_net_ht) OVER (PARTITION BY agence_id ORDER BY mois)    AS ca_m1,
        LAG(taux_marge) OVER (PARTITION BY agence_id ORDER BY mois)   AS marge_m1,
        LAG(nb_int_actifs) OVER (PARTITION BY agence_id ORDER BY mois) AS int_m1
    FROM {{ ref('scorecard_agence') }}
),

yoy AS (
    SELECT
        c.agence_id, c.mois,
        c.ca_net_ht, c.taux_marge, c.nb_int_actifs,
        c.ca_net_ht - COALESCE(c.ca_m1, 0)           AS delta_ca_mom,
        c.taux_marge - COALESCE(c.marge_m1, 0)        AS delta_marge_mom,
        c.nb_int_actifs - COALESCE(c.int_m1, 0)       AS delta_int_mom,
        c.ca_net_ht - COALESCE(prev.ca_net_ht, 0)     AS delta_ca_yoy,
        c.taux_marge - COALESCE(prev.taux_marge, 0)    AS delta_marge_yoy
    FROM curr c
    LEFT JOIN {{ ref('scorecard_agence') }} prev
        ON prev.agence_id = c.agence_id
       AND prev.mois = c.mois - INTERVAL '12 months'
)

SELECT *,
    CASE
        WHEN delta_ca_mom > 0 AND delta_ca_yoy > 0 THEN 'HAUSSE'
        WHEN delta_ca_mom < 0 AND delta_ca_yoy < 0 THEN 'BAISSE'
        ELSE 'STABLE'
    END AS tendance,
    CURRENT_TIMESTAMP AS _computed_at
FROM yoy
ORDER BY mois DESC, ca_net_ht DESC
