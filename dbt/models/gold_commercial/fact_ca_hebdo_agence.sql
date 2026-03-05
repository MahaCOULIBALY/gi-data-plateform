-- fact_ca_hebdo_agence.sql — CA hebdomadaire par agence
-- Phase 1 · GI Data Lakehouse · dbt model
{{ config(materialized='table', schema='gld_commercial') }}

WITH factures AS (
    SELECT
        RGPCNT_ID::INT                                    AS agence_id,
        DATE_TRUNC('week', EFAC_DATE::DATE)::DATE         AS semaine_iso,
        EFAC_TYPE,
        EFAC_MONTANTHT::NUMERIC(18,2)                     AS montant_ht,
        EFAC_NUM
    FROM {{ source('slv_facturation', 'factures') }}
    WHERE EFAC_DATE IS NOT NULL
),

heures AS (
    SELECT
        f.RGPCNT_ID::INT                                  AS agence_id,
        DATE_TRUNC('week', f.EFAC_DATE::DATE)::DATE       AS semaine_iso,
        SUM(h.RHD_BASEFACT::NUMERIC(10,2))                AS heures_facturees
    FROM {{ source('slv_facturation', 'factures') }} f
    INNER JOIN {{ source('slv_temps', 'heures_detail') }} h
        ON h."PRH_BTS" = f."PRH_BTS"
    WHERE f.EFAC_DATE IS NOT NULL
    GROUP BY 1, 2
),

missions_actives AS (
    SELECT
        RGPCNT_ID::INT                                    AS agence_id,
        DATE_TRUNC('week', MISS_DATEDEBUT::DATE)::DATE   AS semaine_iso,
        COUNT(DISTINCT PER_ID)                             AS nb_interimaires_actifs
    FROM {{ source('slv_missions', 'missions') }}
    WHERE MISS_DATEDEBUT IS NOT NULL
    GROUP BY 1, 2
)

SELECT
    f.agence_id,
    da.nom                                                AS nom_agence,
    f.semaine_iso,
    SUM(CASE WHEN f.EFAC_TYPE = 'F' THEN f.montant_ht ELSE 0 END) AS ca_ht,
    SUM(CASE WHEN f.EFAC_TYPE = 'A' THEN f.montant_ht ELSE 0 END) AS avoir_ht,
    SUM(CASE WHEN f.EFAC_TYPE = 'F' THEN f.montant_ht ELSE 0 END)
    - SUM(CASE WHEN f.EFAC_TYPE = 'A' THEN f.montant_ht ELSE 0 END) AS ca_net_ht,
    COUNT(DISTINCT CASE WHEN f.EFAC_TYPE = 'F' THEN f.EFAC_NUM END) AS nb_factures,
    COUNT(DISTINCT CASE WHEN f.EFAC_TYPE = 'A' THEN f.EFAC_NUM END) AS nb_avoirs,
    COALESCE(h.heures_facturees, 0)                        AS heures_facturees,
    COALESCE(ma.nb_interimaires_actifs, 0)                 AS nb_interimaires_actifs,
    CURRENT_TIMESTAMP                                      AS _computed_at
FROM factures f
LEFT JOIN {{ source('gld_shared', 'dim_agences') }} da ON da.rgpcnt_id = f.agence_id
LEFT JOIN heures h ON h.agence_id = f.agence_id AND h.semaine_iso = f.semaine_iso
LEFT JOIN missions_actives ma ON ma.agence_id = f.agence_id AND ma.semaine_iso = f.semaine_iso
GROUP BY f.agence_id, da.nom, f.semaine_iso, h.heures_facturees, ma.nb_interimaires_actifs
ORDER BY f.semaine_iso DESC, ca_net_ht DESC
