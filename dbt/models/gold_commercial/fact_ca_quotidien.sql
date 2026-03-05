-- fact_ca_quotidien.sql — CA quotidien grain fin (agence × client × jour)
-- Phase 1+ · GI Data Lakehouse · dbt model
{{ config(materialized='table', schema='gld_commercial') }}

WITH factures AS (
    SELECT
        RGPCNT_ID::INT              AS agence_id,
        TIE_ID::INT                 AS tie_id,
        EFAC_DATE::DATE             AS jour,
        EFAC_TYPE,
        EFAC_MONTANTHT::NUMERIC(18,2) AS montant_ht,
        EFAC_NUM
    FROM {{ source('slv_facturation', 'factures') }}
    WHERE EFAC_DATE IS NOT NULL
),

heures AS (
    SELECT f.RGPCNT_ID::INT AS agence_id, f.TIE_ID::INT AS tie_id,
           f.EFAC_DATE::DATE AS jour,
           SUM(h.RHD_BASEFACT::NUMERIC(10,2)) AS nb_heures
    FROM {{ source('slv_facturation', 'factures') }} f
    INNER JOIN {{ source('slv_temps', 'heures_detail') }} h ON h."PRH_BTS" = f."PRH_BTS"
    WHERE f.EFAC_DATE IS NOT NULL
    GROUP BY 1, 2, 3
),

int_actifs AS (
    SELECT RGPCNT_ID::INT AS agence_id, TIE_ID::INT AS tie_id,
           MISS_DATEDEBUT::DATE AS jour,
           COUNT(DISTINCT PER_ID) AS nb_interimaires_actifs
    FROM {{ source('slv_missions', 'missions') }}
    WHERE MISS_DATEDEBUT IS NOT NULL
    GROUP BY 1, 2, 3
)

SELECT
    f.agence_id, f.tie_id, f.jour,
    SUM(CASE WHEN f.EFAC_TYPE='F' THEN f.montant_ht ELSE 0 END)
    - SUM(CASE WHEN f.EFAC_TYPE='A' THEN f.montant_ht ELSE 0 END) AS ca_ht,
    COALESCE(h.nb_heures, 0) AS nb_heures,
    COALESCE(ia.nb_interimaires_actifs, 0) AS nb_interimaires_actifs,
    CURRENT_TIMESTAMP AS _computed_at
FROM factures f
LEFT JOIN heures h ON h.agence_id=f.agence_id AND h.tie_id=f.tie_id AND h.jour=f.jour
LEFT JOIN int_actifs ia ON ia.agence_id=f.agence_id AND ia.tie_id=f.tie_id AND ia.jour=f.jour
GROUP BY f.agence_id, f.tie_id, f.jour, h.nb_heures, ia.nb_interimaires_actifs
