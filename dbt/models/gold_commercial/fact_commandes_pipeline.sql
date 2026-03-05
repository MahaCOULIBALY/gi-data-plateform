-- fact_commandes_pipeline.sql — Pipeline commandes agence × semaine
-- Phase 2 · GI Data Lakehouse · dbt model
{{ config(materialized='table', schema='gld_operationnel') }}

WITH commandes AS (
    SELECT
        RGPCNT_ID::INT AS agence_id,
        DATE_TRUNC('week', CMD_DATE::DATE)::DATE AS semaine,
        CMD_ID, CMD_STATUT, CMD_DATE::DATE AS date_cmd,
        CMD_NBSAL::INT AS nb_demandes,
        MET_ID::INT AS metier_id
    FROM {{ source('slv_missions', 'commandes') }}
    WHERE CMD_DATE IS NOT NULL
),

missions AS (
    SELECT DISTINCT CMD_ID::INT AS cmd_id, MISS_DATEDEBUT::DATE AS debut_mission
    FROM {{ source('slv_missions', 'missions') }}
    WHERE CMD_ID IS NOT NULL
)

SELECT
    c.agence_id, c.semaine,
    COUNT(DISTINCT c.CMD_ID) AS nb_commandes,
    COUNT(DISTINCT c.CMD_ID) FILTER (WHERE c.CMD_STATUT = 'O') AS nb_ouvertes,
    COUNT(DISTINCT c.CMD_ID) FILTER (WHERE c.CMD_STATUT IN ('P','C')) AS nb_pourvues,
    COUNT(DISTINCT c.CMD_ID) FILTER (WHERE c.CMD_STATUT = 'A') AS nb_annulees,
    CASE WHEN COUNT(DISTINCT c.CMD_ID) > 0
         THEN ROUND(COUNT(DISTINCT c.CMD_ID) FILTER (WHERE c.CMD_STATUT IN ('P','C'))::NUMERIC
              / COUNT(DISTINCT c.CMD_ID), 4)
         ELSE NULL END AS taux_satisfaction,
    AVG(m.debut_mission - c.date_cmd)::NUMERIC(10,1) AS delai_moyen_jours,
    SUM(c.nb_demandes) AS total_postes_demandes,
    CURRENT_TIMESTAMP AS _computed_at
FROM commandes c
LEFT JOIN missions m ON m.cmd_id = c.CMD_ID::INT
GROUP BY c.agence_id, c.semaine
