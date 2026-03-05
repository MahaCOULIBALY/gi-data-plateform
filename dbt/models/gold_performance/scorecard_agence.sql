-- scorecard_agence.sql — Tableau de bord agence mensuel (vision 360°)
-- Phase 3 · GI Data Lakehouse · dbt model
{{ config(materialized='table', schema='gld_performance') }}

WITH ca AS (
    SELECT agence_id,
           DATE_TRUNC('month', EFAC_DATE::DATE)::DATE AS mois,
           SUM(CASE WHEN EFAC_TYPE='F' THEN EFAC_MONTANTHT::NUMERIC(18,2) ELSE 0 END)
           - SUM(CASE WHEN EFAC_TYPE='A' THEN EFAC_MONTANTHT::NUMERIC(18,2) ELSE 0 END) AS ca_net_ht,
           COUNT(DISTINCT EFAC_NUM) AS nb_factures
    FROM {{ source('slv_facturation', 'factures') }}
    WHERE EFAC_DATE IS NOT NULL
    GROUP BY 1, 2
),

marge AS (
    SELECT m.RGPCNT_ID::INT AS agence_id,
           DATE_TRUNC('month', m.MISS_DATEDEBUT::DATE)::DATE AS mois,
           COALESCE(SUM(h.RHD_BASEFACT::NUMERIC * c.CNT_TAUXFACT::NUMERIC), 0) AS ca_missions,
           COALESCE(SUM(h.RHD_BASEPAYE::NUMERIC * c.CNT_TAUXPAYE::NUMERIC), 0) AS cout_missions
    FROM {{ source('slv_missions', 'missions') }} m
    LEFT JOIN {{ source('slv_missions', 'contrats') }} c ON c."PER_ID"=m."PER_ID" AND c."CNT_ID"=m."CNT_ID"
    LEFT JOIN {{ source('slv_temps', 'heures_detail') }} h ON h."PRH_BTS"=m."PRH_BTS"
    WHERE m.MISS_DATEDEBUT IS NOT NULL
    GROUP BY 1, 2
),

staffing AS (
    SELECT RGPCNT_ID::INT AS agence_id,
           DATE_TRUNC('month', MISS_DATEDEBUT::DATE)::DATE AS mois,
           COUNT(DISTINCT PER_ID) AS nb_int_actifs,
           COUNT(DISTINCT TIE_ID) AS nb_clients_actifs,
           COUNT(DISTINCT PER_ID||'|'||CNT_ID) AS nb_missions
    FROM {{ source('slv_missions', 'missions') }}
    WHERE MISS_DATEDEBUT IS NOT NULL
    GROUP BY 1, 2
),

transfo AS (
    SELECT RGPCNT_ID::INT AS agence_id,
           DATE_TRUNC('month', CMD_DATE::DATE)::DATE AS mois,
           COUNT(*) AS nb_commandes,
           COUNT(*) FILTER (WHERE CMD_STATUT IN ('P','C')) AS nb_pourvues
    FROM {{ source('slv_missions', 'commandes') }}
    WHERE CMD_DATE IS NOT NULL
    GROUP BY 1, 2
)

SELECT
    ca.agence_id, ca.mois,
    ca.ca_net_ht,
    CASE WHEN mg.ca_missions > 0
         THEN ROUND((mg.ca_missions - mg.cout_missions) / mg.ca_missions, 4)
         ELSE 0 END AS taux_marge,
    mg.ca_missions - mg.cout_missions AS marge_brute,
    COALESCE(s.nb_clients_actifs, 0) AS nb_clients_actifs,
    COALESCE(s.nb_int_actifs, 0) AS nb_int_actifs,
    COALESCE(s.nb_missions, 0) AS nb_missions,
    CASE WHEN COALESCE(t.nb_commandes, 0) > 0
         THEN ROUND(t.nb_pourvues::NUMERIC / t.nb_commandes, 4)
         ELSE NULL END AS taux_transformation,
    COALESCE(t.nb_commandes, 0) AS nb_commandes,
    COALESCE(t.nb_pourvues, 0) AS nb_pourvues,
    CURRENT_TIMESTAMP AS _computed_at
FROM ca
LEFT JOIN marge mg ON mg.agence_id=ca.agence_id AND mg.mois=ca.mois
LEFT JOIN staffing s ON s.agence_id=ca.agence_id AND s.mois=ca.mois
LEFT JOIN transfo t ON t.agence_id=ca.agence_id AND t.mois=ca.mois
