# Analyse — KPI Complémentaires : Sources Evolia & Plan d'Implémentation

> **Version 1.1 · 2026-03-15**
> Périmètre : 8 indicateurs essentiels de l'intérim identifiés comme manquants
> après audit de couverture du dispositif Phase 2/3 GI Data Lakehouse.
> Objectif : valider la disponibilité des données sources, identifier les transformations
> requises et établir un plan d'implémentation par couche (Bronze → Silver → Gold).

---

## Sommaire

1. [Synthèse exécutive](#1-synthèse-exécutive)
2. [Durée Moyenne des Missions (DMM)](#2-durée-moyenne-des-missions-dmm)
3. [Taux de Rupture de Contrat (CTT)](#3-taux-de-rupture-de-contrat-ctt)
4. [DSO — Délai Moyen de Paiement](#4-dso--délai-moyen-de-paiement)
5. [Balance Âgée](#5-balance-âgée)
6. [Taux de Renouvellement de Mission](#6-taux-de-renouvellement-de-mission)
7. [Vivier — Nouveaux Intérimaires Inscrits](#7-vivier--nouveaux-intérimaires-inscrits)
8. [Coefficient de Facturation par Métier](#8-coefficient-de-facturation-par-métier)
9. [CA par Secteur NAF](#9-ca-par-secteur-naf)
10. [Roadmap d'implémentation](#10-roadmap-dimplémentation)
11. [Anomalies DDL détectées](#11-anomalies-ddl-détectées)

---

## 1. Synthèse exécutive

| Indicateur | Priorité | Données disponibles ? | Bronze modifié ? | Effort estimé |
| --- | --- | --- | --- | --- |
| Durée Moyenne des Missions | 🔴 Haute | ✅ Déjà en Gold (`fact_missions_detail.duree_jours`) | ❌ Non | Faible — agrégation Gold + chart |
| Taux de Rupture de Contrat | 🔴 Haute | ✅ `FINMISS_CODE` + `MISS_SAISIE_DTFIN` déjà en Bronze (`bronze_missions.py`) | ⚠️ Ajouter `MISS_ANNULE` + ingérer `WTFINMISS` | Moyen — logique Silver + Gold |
| DSO / Délai paiement | 🔴 Haute | ✅ `EFAC_DTEEDI/ECH/REGLF/TYPF/TRANS` + `MRG_CODE` déjà en Bronze (`bronze_missions.py`) | ❌ Non | Moyen — Silver + Gold uniquement |
| Balance Âgée | 🔴 Haute | ✅ Même source WTEFAC — déjà en Bronze | ❌ Non | Faible (dépend de la Silver DSO) |
| Taux de Renouvellement | 🟠 Important | ✅ Déjà en Gold (`fact_missions_detail`) | ❌ Non | Faible — agrégation Gold |
| Vivier — Nouveaux inscrits | 🟠 Important | ✅ `dim_interimaires.date_creation_fiche` | ❌ Non | Faible — agrégation Gold |
| Coeff facturation/métier | 🟠 Important | ✅ Déjà en Gold (`fact_missions_detail`) | ❌ Non | Faible — agrégation Gold |
| CA par secteur NAF | 🟠 Important | ✅ `dim_clients.naf_code` + `fact_ca_mensuel_client` | ❌ Non | Faible — agrégation Gold |

**Conclusion principale :** aucune modification de `bronze_clients.py` n'est nécessaire.
WTEFAC est déjà intégralement ingérée dans `bronze_missions.py` avec toutes les colonnes
requises pour le DSO et la balance âgée. Le seul ajout Bronze restant est mineur :
`MISS_ANNULE` dans WTMISS et l'ingestion du référentiel `WTFINMISS` (pour les ruptures CTT).

---

## 2. Durée Moyenne des Missions (DMM)

**Définition métier**

> La Durée Moyenne des Missions est un KPI structurant de l'intérim : elle caractérise
> le profil d'activité (micro-missions de quelques heures vs contrats longs de plusieurs
> semaines) et conditionne directement les charges administratives par ETP produit.
> Elle est suivie par secteur, métier, client et agence.

```text
DMM (jours) = AVG(CNTI_DATEFINCNTI − CNTI_DATEFFET)
DMM (semaines) = DMM (jours) / 7
```

**Analyse DDL**

La durée unitaire est **déjà calculée** dans `gld_staffing.fact_missions_detail` :

| Colonne Gold existante | Description |
| --- | --- |
| `date_debut` | = CNTI_DATEFFET |
| `date_fin` | = CNTI_DATEFINCNTI |
| `duree_jours` | = DATEDIFF(day, date_debut, date_fin) |
| `metier_id` | Jointure avec dim_metiers |
| `agence_id` | Dimension pivot |

**Statut des données**

✅ **Aucune modification Bronze ou Silver requise.**
La donnée source est déjà en Gold — seule une nouvelle table d'agrégation est nécessaire.

**Transformation Gold à créer**

```sql
-- gold_qualite_missions.py → gld_performance.fact_duree_mission
SELECT
    m.agence_id,
    m.tie_id,
    m.metier_id,
    DATE_TRUNC('month', m.date_debut)                    AS mois,
    COUNT(*)                                             AS nb_missions,
    ROUND(AVG(m.duree_jours), 1)                         AS dmm_jours,
    ROUND(AVG(m.duree_jours) / 7.0, 1)                  AS dmm_semaines,
    PERCENTILE_CONT(0.5) WITHIN GROUP
        (ORDER BY m.duree_jours)                         AS duree_mediane_jours,
    SUM(CASE WHEN m.duree_jours <= 1  THEN 1 ELSE 0 END) AS nb_missions_1j,
    SUM(CASE WHEN m.duree_jours <= 7  THEN 1 ELSE 0 END) AS nb_missions_1semaine,
    SUM(CASE WHEN m.duree_jours <= 30 THEN 1 ELSE 0 END) AS nb_missions_1mois,
    SUM(CASE WHEN m.duree_jours > 30  THEN 1 ELSE 0 END) AS nb_missions_long,
    CASE
        WHEN AVG(m.duree_jours) <= 1   THEN 'MICRO'
        WHEN AVG(m.duree_jours) <= 7   THEN 'COURTE'
        WHEN AVG(m.duree_jours) <= 30  THEN 'MOYENNE'
        ELSE 'LONGUE'
    END                                                  AS profil_duree
FROM gld_staffing.fact_missions_detail m
WHERE m.date_debut IS NOT NULL AND m.date_fin IS NOT NULL
  AND m.duree_jours > 0
GROUP BY 1, 2, 3, 4
```

**Table Gold cible :** `gld_performance.fact_duree_mission`

| Colonne | Type | Description |
| --- | --- | --- |
| `agence_id` | int | Clé agence |
| `tie_id` | int | Clé client |
| `metier_id` | int | Clé métier |
| `mois` | date | Mois de la mission |
| `nb_missions` | int | Nombre de missions |
| `dmm_jours` | float | Durée Moyenne Mission en jours |
| `dmm_semaines` | float | DMM en semaines |
| `duree_mediane_jours` | float | Médiane (résistante aux outliers) |
| `nb_missions_1j` | int | Missions d'1 jour ou moins |
| `nb_missions_1semaine` | int | Missions ≤ 7 jours |
| `nb_missions_1mois` | int | Missions ≤ 30 jours |
| `nb_missions_long` | int | Missions > 30 jours |
| `profil_duree` | varchar | MICRO / COURTE / MOYENNE / LONGUE |

---

## 3. Taux de Rupture de Contrat (CTT)

**Définition métier**

> Le taux de rupture mesure la part des contrats de travail temporaire (CTT) interrompus
> avant leur terme prévu. C'est un indicateur clé de qualité du placement :
> un taux élevé signale des problèmes de matching (mauvaise qualification, mésadaptation
> poste/intérimaire) ou des dysfonctionnements client (annulations de chantier, etc.).

```text
Taux de rupture (%) = nb_missions_rompues_avant_terme / nb_missions_total × 100

Mission rompue = FINMISS_CODE présent ET date_fin_réelle < date_fin_prévue
              OU MISS_ANNULE = 1
```

**Analyse DDL**

**Table WTMISS — colonnes pertinentes :**

| Colonne DDL | Type | Signification |
| --- | --- | --- |
| `FINMISS_CODE` | varchar(3) | **Code de fin de mission** — NULL si en cours, valorisé à la clôture |
| `MISS_ANNULE` | smallint | Flag d'annulation de mission (1=annulée) — **absent de `_COLS["WTMISS"]` actuel** |
| `MISS_SAISIE_DTFIN` | datetime2(3) | Date saisie fin de mission (par le consultant) |
| `CNTI_CREATE` | datetime2(3) | Date de création de la mission |

**Table WTFINMISS — référentiel des codes fin de mission :**

| Colonne DDL | Type | Signification |
| --- | --- | --- |
| `FINMISS_CODE` | varchar(3) PK | Code fin de mission |
| `FINMISS_LIBELLE` | varchar(35) | Libellé (ex : "Terme normal", "Rupture à l'initiative du client"…) |
| `FINMISS_IFM` | varchar(2) | Indicateur IFM (Indemnité de Fin de Mission) versée ? |
| `FINMISS_CP` | varchar(2) | Indicateur Congés Payés |

**Table WTCNTI — dates contrat :**

| Colonne DDL | Type | Signification |
| --- | --- | --- |
| `CNTI_DATEFFET` | datetime2(3) | **Date de début prévue** |
| `CNTI_DATEFINCNTI` | datetime2(3) | **Date de fin réelle enregistrée** |

> **Logique de rupture :** une mission est considérée rompue si `FINMISS_CODE` est
> présent ET que la durée réelle (`CNTI_DATEFINCNTI − CNTI_DATEFFET`) est inférieure
> à la durée initialement prévue, OU si `MISS_ANNULE = 1`.
> Les codes `FINMISS_CODE` dont le libellé contient "terme normal" ou "échéance"
> désignent les fins naturelles — les autres sont des ruptures.

**Statut des données**

`FINMISS_CODE` et `MISS_SAISIE_DTFIN` sont déjà extraits dans `bronze_missions.py`
(`_COLS["WTMISS"]`, ligne 70-71). Deux actions Bronze mineures restent nécessaires :

**Action 1 — Ajouter `MISS_ANNULE` dans `bronze_missions.py`** (colonne absente de `_COLS["WTMISS"]`) :

```python
# Dans bronze_missions.py — _COLS["WTMISS"], ajouter à la fin :
"MISS_ANNULE",        # smallint — 1 si mission annulée
```

**Action 2 — Ingérer le référentiel `WTFINMISS`** dans `bronze_missions.py` :

```python
# Ajouter dans TABLES_FULL (volume ~20 lignes, full-load) :
TableConfig("WTFINMISS", "", ["FINMISS_CODE"]),
# _COLS["WTFINMISS"] = "FINMISS_CODE,FINMISS_LIBELLE,FINMISS_IFM,FINMISS_CP"
```

**Transformation Silver — enrichir `silver_missions.py` :**

```sql
SELECT
    m.per_id,
    m.cnt_id,
    m.rgpcnt_id,
    m.tie_id,
    c.cnti_dateffet                                        AS date_debut,
    c.cnti_datefincnti                                     AS date_fin_reelle,
    m.miss_saisie_dtfin                                    AS date_fin_saisie,
    m.finmiss_code,
    f.finmiss_libelle,
    m.miss_annule,
    DATEDIFF('day', c.cnti_dateffet, c.cnti_datefincnti)   AS duree_reelle_jours,
    CASE
        WHEN m.miss_annule = 1                             THEN 'ANNULEE'
        WHEN m.finmiss_code IS NULL                        THEN 'EN_COURS'
        WHEN LOWER(f.finmiss_libelle) LIKE '%terme%'
          OR LOWER(f.finmiss_libelle) LIKE '%normal%'
          OR LOWER(f.finmiss_libelle) LIKE '%échéance%'    THEN 'TERME_NORMAL'
        ELSE 'RUPTURE'
    END                                                    AS statut_fin_mission
FROM raw_wtmiss m
JOIN raw_wtcnti c
    ON m.per_id = c.per_id AND m.cnt_id = c.cnt_id
LEFT JOIN raw_wtfinmiss f ON m.finmiss_code = f.finmiss_code
```

**Table Gold cible :** `gld_performance.fact_rupture_contrat`

```sql
-- gold_qualite_missions.py → gld_performance.fact_rupture_contrat
SELECT
    sm.rgpcnt_id                                           AS agence_id,
    sm.tie_id,
    DATE_TRUNC('month', sm.date_debut)                     AS mois,
    COUNT(*)                                               AS nb_missions_total,
    SUM(CASE WHEN sm.statut_fin_mission = 'RUPTURE'
             THEN 1 ELSE 0 END)                            AS nb_ruptures,
    SUM(CASE WHEN sm.statut_fin_mission = 'ANNULEE'
             THEN 1 ELSE 0 END)                            AS nb_annulations,
    SUM(CASE WHEN sm.statut_fin_mission = 'TERME_NORMAL'
             THEN 1 ELSE 0 END)                            AS nb_terme_normal,
    ROUND(
        SUM(CASE WHEN sm.statut_fin_mission = 'RUPTURE'
                 THEN 1 ELSE 0 END)::FLOAT / NULLIF(COUNT(*),0) * 100
    , 1)                                                   AS taux_rupture_pct,
    ROUND(
        SUM(CASE WHEN sm.statut_fin_mission IN ('RUPTURE','ANNULEE')
                 THEN 1 ELSE 0 END)::FLOAT / NULLIF(COUNT(*),0) * 100
    , 1)                                                   AS taux_fin_anticipee_pct,
    AVG(CASE WHEN sm.statut_fin_mission = 'RUPTURE'
             THEN sm.duree_reelle_jours END)               AS duree_moy_avant_rupture
FROM slv_interimaires.fin_mission sm
WHERE sm.date_debut IS NOT NULL
  AND sm.statut_fin_mission != 'EN_COURS'
GROUP BY 1, 2, 3
```

| Colonne | Type | Description |
| --- | --- | --- |
| `agence_id` | int | Agence |
| `tie_id` | int | Client |
| `mois` | date | Mois de début de la mission |
| `nb_missions_total` | int | Total missions clôturées |
| `nb_ruptures` | int | Missions rompues avant terme |
| `nb_annulations` | int | Missions annulées |
| `nb_terme_normal` | int | Fins naturelles |
| `taux_rupture_pct` | float | KPI principal (%) |
| `taux_fin_anticipee_pct` | float | Ruptures + annulations (%) |
| `duree_moy_avant_rupture` | float | Jours moyens avant rupture |

---

## 4. DSO — Délai Moyen de Paiement

**Définition métier**

> Le DSO (Days Sales Outstanding) mesure le délai moyen entre l'émission d'une facture
> et son règlement effectif par le client. C'est l'indicateur central de la gestion de
> trésorerie et du recouvrement. Il est généralement exprimé en jours.

```text
DSO (jours) = AVG(EFAC_DTEREGLF − EFAC_DTEEDI)    [factures réglées uniquement]
Retard (jours) = EFAC_DTEREGLF − EFAC_DTEECH       [positif = payé après échéance]
```

**Analyse DDL — Table WTEFAC**

| Colonne DDL | Type | Signification |
| --- | --- | --- |
| `EFAC_NUM` | varchar(15) PK | Numéro de facture |
| `EFAC_DTEEDI` | datetime2(3) | **Date d'édition (émission) de la facture** ← T0 DSO |
| `EFAC_DTEECH` | datetime2(3) | **Date d'échéance** ← référence contractuelle |
| `EFAC_DTEREGLF` | datetime2(3) | **Date de règlement** ← T1 DSO (NULL si non réglée) |
| `EFAC_TYPF` | varchar(1) | Type facture : F=Facture, A=Avoir, P=Proforma (à confirmer) |
| `EFAC_TYPG` | varchar(1) | Type gestion (regroupement, facturation directe…) |
| `RGPCNT_ID` | int | Agence émettrice |
| `TIE_ID` | int | Client |
| `TIES_SERV` | int | Site client |
| `EFAC_RUPTURE` | varchar(8) | Rupture de facturation (lot) |
| `EFAC_TRANS` | datetime2(3) | Date de transmission (comptabilité/dématérialisation) |
| `MRG_CODE` | varchar(1) | Mode de règlement (virement, prélèvement…) |

> **Note DDL :** `EFAC_MONTANTHT` est absent de la table WTEFAC. Le montant HT
> est calculé via `SUM(WTLFAC.LFAC_BASE × LFAC_TAUX)` — déjà implémenté dans
> `gold_ca_mensuel.py`.

**Statut des données**

✅ **Aucune modification Bronze requise.**
Toutes les colonnes nécessaires sont déjà extraites dans `bronze_missions.py`
(`_COLS["WTEFAC"]`, lignes 80-86) : `EFAC_DTEEDI`, `EFAC_DTEECH`, `EFAC_DTEREGLF`,
`EFAC_TYPF`, `EFAC_TRANS`, `MRG_CODE`, `RGPCNT_ID`, `TIE_ID` sont présentes.

**Transformation Silver — créer `slv_clients/facturation_detail` :**

```sql
SELECT
    e.efac_num,
    e.rgpcnt_id                                             AS agence_id,
    e.tie_id,
    e.ties_serv                                             AS site_id,
    e.efac_typf                                             AS type_facture,
    e.efac_dteedi                                           AS date_emission,
    e.efac_dteech                                           AS date_echeance,
    e.efac_dtereglf                                         AS date_reglement,
    e.mrg_code                                              AS mode_reglement,
    CASE WHEN e.efac_dtereglf IS NOT NULL
         THEN DATEDIFF('day', e.efac_dteedi, e.efac_dtereglf)
         ELSE NULL
    END                                                     AS dso_jours,
    CASE WHEN e.efac_dtereglf IS NOT NULL
         THEN DATEDIFF('day', e.efac_dteech, e.efac_dtereglf)
         ELSE DATEDIFF('day', e.efac_dteech, CURRENT_DATE)
    END                                                     AS retard_vs_echeance_jours,
    CASE
        WHEN e.efac_dtereglf IS NOT NULL
             AND e.efac_dtereglf <= e.efac_dteech           THEN 'REGLE_A_TEMPS'
        WHEN e.efac_dtereglf IS NOT NULL
             AND e.efac_dtereglf > e.efac_dteech            THEN 'REGLE_EN_RETARD'
        WHEN e.efac_dtereglf IS NULL
             AND CURRENT_DATE <= e.efac_dteech               THEN 'EN_COURS'
        WHEN e.efac_dtereglf IS NULL
             AND CURRENT_DATE > e.efac_dteech               THEN 'EN_RETARD'
        ELSE 'INCONNU'
    END                                                     AS statut_reglement,
    CASE
        WHEN e.efac_dtereglf IS NOT NULL                    THEN 'REGLE'
        WHEN CURRENT_DATE <= e.efac_dteech                  THEN '0_NON_ECHU'
        WHEN DATEDIFF('day', e.efac_dteech, CURRENT_DATE) <= 30  THEN '1_0_30J'
        WHEN DATEDIFF('day', e.efac_dteech, CURRENT_DATE) <= 60  THEN '2_30_60J'
        WHEN DATEDIFF('day', e.efac_dteech, CURRENT_DATE) <= 90  THEN '3_60_90J'
        ELSE '4_PLUS_90J'
    END                                                     AS tranche_retard
FROM read_json_auto('{b}/raw_wtefac/{cfg.date_partition}/*.json') e
WHERE e.efac_typf = 'F'
  AND e.efac_dteedi IS NOT NULL
```

**Table Gold cible :** `gld_commercial.fact_dso_client`

```sql
-- gold_recouvrement.py → gld_commercial.fact_dso_client
SELECT
    f.agence_id,
    f.tie_id,
    DATE_TRUNC('month', f.date_emission)                   AS mois,
    COUNT(*)                                               AS nb_factures,
    SUM(CASE WHEN f.statut_reglement LIKE 'REGLE%'
             THEN 1 ELSE 0 END)                            AS nb_reglees,
    SUM(CASE WHEN f.statut_reglement IN ('EN_RETARD', 'REGLE_EN_RETARD')
             THEN 1 ELSE 0 END)                            AS nb_en_retard,
    ROUND(AVG(CASE WHEN f.dso_jours IS NOT NULL
                   THEN f.dso_jours END), 0)               AS dso_moyen_jours,
    ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP
        (ORDER BY f.dso_jours) FILTER
        (WHERE f.dso_jours IS NOT NULL), 0)                AS dso_median_jours,
    ROUND(AVG(CASE WHEN f.retard_vs_echeance_jours > 0
                   THEN f.retard_vs_echeance_jours END), 0) AS retard_moyen_jours,
    CASE
        WHEN AVG(CASE WHEN f.dso_jours IS NOT NULL
                      THEN f.dso_jours END) <= 30          THEN 'BON'
        WHEN AVG(CASE WHEN f.dso_jours IS NOT NULL
                      THEN f.dso_jours END) <= 45          THEN 'MOYEN'
        ELSE 'CRITIQUE'
    END                                                    AS niveau_dso
FROM slv_clients.facturation_detail f
GROUP BY 1, 2, 3
```

| Colonne | Type | Description |
| --- | --- | --- |
| `agence_id` | int | Agence |
| `tie_id` | int | Client |
| `mois` | date | Mois d'émission des factures |
| `nb_factures` | int | Total factures émises |
| `nb_reglees` | int | Factures réglées |
| `nb_en_retard` | int | Factures en retard (non réglées après échéance) |
| `dso_moyen_jours` | int | **KPI principal** : délai moyen de règlement |
| `dso_median_jours` | int | Médiane (résistante aux gros retardataires) |
| `retard_moyen_jours` | int | Retard moyen sur les factures en retard |
| `niveau_dso` | varchar | BON (≤30j) / MOYEN (≤45j) / CRITIQUE (>45j) |

---

## 5. Balance Âgée

**Définition métier**

> La balance âgée est le rapport de recouvrement standard : elle présente les encours
> non réglés répartis par ancienneté de retard. C'est l'outil de pilotage quotidien
> des équipes de recouvrement et de crédit management.

```text
Encours total = Σ montants factures non réglées (EFAC_DTEREGLF IS NULL)
Répartition :
  - Non échu (avant date échéance)
  - 0-30 jours de retard
  - 31-60 jours de retard
  - 61-90 jours de retard
  - > 90 jours (litigieux)
```

**Statut des données**

✅ **Aucune modification Bronze requise.** Même source WTEFAC que §4 — déjà en Bronze.
Le montant HT de chaque facture doit être joint depuis `gld_commercial.fact_ca_mensuel_client`
ou recalculé via `WTLFAC`.

**Table Gold cible :** `gld_commercial.fact_balance_agee`

```sql
-- gold_recouvrement.py → gld_commercial.fact_balance_agee
-- Snapshot au jour courant des encours non réglés
SELECT
    f.agence_id,
    f.tie_id,
    SUM(CASE WHEN f.tranche_retard = '0_NON_ECHU'
             THEN m.ca_net_ht ELSE 0 END)                  AS montant_non_echu,
    SUM(CASE WHEN f.tranche_retard = '1_0_30J'
             THEN m.ca_net_ht ELSE 0 END)                  AS montant_0_30j,
    SUM(CASE WHEN f.tranche_retard = '2_30_60J'
             THEN m.ca_net_ht ELSE 0 END)                  AS montant_30_60j,
    SUM(CASE WHEN f.tranche_retard = '3_60_90J'
             THEN m.ca_net_ht ELSE 0 END)                  AS montant_60_90j,
    SUM(CASE WHEN f.tranche_retard = '4_PLUS_90J'
             THEN m.ca_net_ht ELSE 0 END)                  AS montant_plus_90j,
    SUM(m.ca_net_ht)                                       AS encours_total,
    ROUND(
        SUM(CASE WHEN f.tranche_retard IN ('3_60_90J','4_PLUS_90J')
                 THEN m.ca_net_ht ELSE 0 END)
        / NULLIF(SUM(m.ca_net_ht), 0) * 100
    , 1)                                                   AS taux_vetusite_pct,
    CASE
        WHEN SUM(CASE WHEN f.tranche_retard = '4_PLUS_90J'
                      THEN m.ca_net_ht ELSE 0 END)
             / NULLIF(SUM(m.ca_net_ht), 0) > 0.20          THEN 'LITIGIEUX'
        WHEN SUM(CASE WHEN f.tranche_retard IN ('3_60_90J','4_PLUS_90J')
                      THEN m.ca_net_ht ELSE 0 END)
             / NULLIF(SUM(m.ca_net_ht), 0) > 0.30          THEN 'A_RISQUE'
        ELSE 'NORMAL'
    END                                                    AS niveau_risque,
    CURRENT_DATE                                           AS date_snapshot
FROM slv_clients.facturation_detail f
JOIN gld_commercial.fact_ca_mensuel_client m
    ON m.tie_id = f.tie_id AND m.efac_num = f.efac_num
WHERE f.statut_reglement IN ('EN_COURS', 'EN_RETARD')
GROUP BY 1, 2
```

| Colonne | Type | Description |
| --- | --- | --- |
| `agence_id` | int | Agence |
| `tie_id` | int | Client |
| `montant_non_echu` | decimal | Montant non encore échu |
| `montant_0_30j` | decimal | Retard 0-30 jours |
| `montant_30_60j` | decimal | Retard 31-60 jours |
| `montant_60_90j` | decimal | Retard 61-90 jours |
| `montant_plus_90j` | decimal | Retard > 90 jours (litigieux) |
| `encours_total` | decimal | Total encours non réglés |
| `taux_vetusite_pct` | float | % encours > 60j (indicateur qualité recouvrement) |
| `niveau_risque` | varchar | LITIGIEUX / A_RISQUE / NORMAL |
| `date_snapshot` | date | Date du calcul (snapshot journalier) |

---

## 6. Taux de Renouvellement de Mission

**Définition métier**

> Le taux de renouvellement mesure la part des missions reconduites chez le même client.
> Un taux élevé témoigne de la satisfaction client et de la stabilité de la relation
> commerciale — différent du taux de transformation (commandes → missions).

```text
Taux de renouvellement (%) =
    missions reconduites (même client, même métier, écart ≤ 7j) / total missions × 100
```

**Statut des données**

✅ **Aucune modification Bronze ou Silver requise.**
Toutes les données sont disponibles dans `gld_staffing.fact_missions_detail`.

**Table Gold cible :** `gld_commercial.fact_renouvellement_mission`

```sql
-- gold_qualite_missions.py → gld_commercial.fact_renouvellement_mission
WITH missions_ordered AS (
    SELECT
        m.per_id,
        m.tie_id,
        m.agence_id,
        m.metier_id,
        m.date_debut,
        m.date_fin,
        LAG(m.date_fin) OVER (
            PARTITION BY m.per_id, m.tie_id, m.agence_id
            ORDER BY m.date_debut
        )                                                   AS date_fin_mission_precedente
    FROM gld_staffing.fact_missions_detail m
),
missions_renouvelees AS (
    SELECT *,
        DATEDIFF('day', date_fin_mission_precedente, date_debut) AS ecart_jours,
        CASE
            WHEN date_fin_mission_precedente IS NOT NULL
             AND DATEDIFF('day', date_fin_mission_precedente, date_debut) BETWEEN 0 AND 7
            THEN 1 ELSE 0
        END                                                 AS est_renouvellement
    FROM missions_ordered
)
SELECT
    agence_id,
    tie_id,
    DATE_TRUNC('month', date_debut)                         AS mois,
    COUNT(*)                                                AS nb_missions,
    SUM(est_renouvellement)                                 AS nb_renouvellements,
    ROUND(
        SUM(est_renouvellement)::FLOAT / NULLIF(COUNT(*),0) * 100
    , 1)                                                    AS taux_renouvellement_pct,
    ROUND(AVG(CASE WHEN est_renouvellement = 1
                   THEN ecart_jours END), 0)                AS ecart_moyen_jours
FROM missions_renouvelees
WHERE date_debut IS NOT NULL
GROUP BY 1, 2, 3
```

| Colonne | Type | Description |
| --- | --- | --- |
| `agence_id` | int | Agence |
| `tie_id` | int | Client |
| `mois` | date | Mois |
| `nb_missions` | int | Total missions |
| `nb_renouvellements` | int | Missions reconduites (écart ≤ 7j après fin précédente) |
| `taux_renouvellement_pct` | float | **KPI principal** (%) |
| `ecart_moyen_jours` | float | Écart moyen entre fin et reprise |

---

## 7. Vivier — Nouveaux Intérimaires Inscrits

**Définition métier**

> Le suivi des nouvelles inscriptions mesure la dynamique de recrutement du pool.
> Combiné au taux de fidélisation, il permet de calculer la croissance nette du vivier
> et d'anticiper les besoins de sourcing.

```text
Nouveaux inscrits = intérimaires dont date_creation_fiche est dans la période
Croissance nette = nouveaux inscrits − intérimaires perdus (inactifs > 365j)
```

**Statut des données**

✅ **Aucune modification Bronze ou Silver requise.**
La colonne `date_creation_fiche` est déjà disponible dans `gld_shared.dim_interimaires`
(via `PINT_CREATDTE` de WTPINT — déjà extrait suite à ANALYSE_KPI_MANQUANTS Phase A).

**Table Gold cible :** `gld_staffing.fact_dynamique_vivier`

```sql
-- gold_staffing.py → gld_staffing.fact_dynamique_vivier
WITH entrees AS (
    SELECT
        agence_id,
        DATE_TRUNC('month', premiere_mission)               AS mois,
        COUNT(*)                                            AS nb_nouveaux
    FROM gld_shared.dim_interimaires
    WHERE premiere_mission IS NOT NULL AND is_current
    GROUP BY 1, 2
),
sorties AS (
    SELECT
        agence_id,
        DATE_TRUNC('month', derniere_mission)               AS mois,
        COUNT(CASE WHEN categorie_fidelisation = 'INACTIF' THEN 1 END) AS nb_perdus
    FROM gld_staffing.fact_fidelisation_interimaires
    GROUP BY 1, 2
),
pool_actuel AS (
    SELECT
        agence_id,
        DATE_TRUNC('month', CURRENT_DATE)                   AS mois,
        COUNT(CASE WHEN categorie_fidelisation = 'ACTIF_RECENT' THEN 1 END) AS pool_actif,
        COUNT(CASE WHEN categorie_fidelisation != 'INACTIF' THEN 1 END)     AS pool_total
    FROM gld_staffing.fact_fidelisation_interimaires
    GROUP BY 1, 2
)
SELECT
    e.agence_id,
    e.mois,
    e.nb_nouveaux,
    COALESCE(s.nb_perdus, 0)                                AS nb_perdus,
    (e.nb_nouveaux - COALESCE(s.nb_perdus, 0))              AS croissance_nette,
    p.pool_actif,
    p.pool_total,
    ROUND(
        e.nb_nouveaux::FLOAT / NULLIF(p.pool_total, 0) * 100
    , 1)                                                    AS taux_renouvellement_vivier_pct
FROM entrees e
LEFT JOIN sorties s
    ON s.agence_id = e.agence_id AND s.mois = e.mois
LEFT JOIN pool_actuel p
    ON p.agence_id = e.agence_id
```

| Colonne | Type | Description |
| --- | --- | --- |
| `agence_id` | int | Agence |
| `mois` | date | Mois |
| `nb_nouveaux` | int | Nouveaux intérimaires inscrits/actifs |
| `nb_perdus` | int | Intérimaires passés inactifs |
| `croissance_nette` | int | Variation nette du pool |
| `pool_actif` | int | Pool actif courant |
| `pool_total` | int | Pool total (actifs + dormants) |
| `taux_renouvellement_vivier_pct` | float | % de turn-over du vivier |

---

## 8. Coefficient de Facturation par Métier

**Définition métier**

> Le coefficient de facturation (ou "coeff") est le rapport entre le taux horaire
> facturé au client et le taux horaire payé à l'intérimaire. C'est l'indicateur
> commercial fondamental en intérim : il condense la politique de marge en un chiffre
> unique, comparable entre métiers, clients et périodes.

```text
Coeff = taux_horaire_facturé / taux_horaire_payé
Marge coeff = (coeff − 1) × 100 %      (ex : coeff 1.35 = 35% de marge brute)
```

**Statut des données**

✅ **Aucune modification Bronze ou Silver requise.**
`fact_missions_detail` contient déjà `taux_horaire_paye` et `taux_horaire_fact`.

**Table Gold cible :** `gld_performance.fact_coeff_facturation`

```sql
-- gold_qualite_missions.py → gld_performance.fact_coeff_facturation
SELECT
    m.agence_id,
    m.tie_id,
    m.metier_id,
    dm.libelle_metier,
    DATE_TRUNC('month', m.date_debut)                       AS mois,
    COUNT(*)                                                AS nb_missions,
    ROUND(
        SUM(m.taux_horaire_fact * m.heures_totales)
        / NULLIF(SUM(m.taux_horaire_paye * m.heures_totales), 0)
    , 3)                                                    AS coeff_moyen_pondere,
    ROUND(AVG(m.taux_horaire_fact / NULLIF(m.taux_horaire_paye, 0)), 3) AS coeff_moyen_simple,
    ROUND(AVG(m.taux_horaire_fact), 2)                      AS thf_moyen,
    ROUND(AVG(m.taux_horaire_paye), 2)                      AS thp_moyen,
    ROUND(
        (SUM(m.taux_horaire_fact * m.heures_totales)
         / NULLIF(SUM(m.taux_horaire_paye * m.heures_totales), 0) - 1) * 100
    , 1)                                                    AS marge_coeff_pct,
    CASE
        WHEN AVG(m.taux_horaire_fact / NULLIF(m.taux_horaire_paye, 0)) >= 1.35 THEN 'BON'
        WHEN AVG(m.taux_horaire_fact / NULLIF(m.taux_horaire_paye, 0)) >= 1.25 THEN 'MOYEN'
        ELSE 'A_REVOIR'
    END                                                     AS niveau_coeff
FROM gld_staffing.fact_missions_detail m
JOIN gld_shared.dim_metiers dm ON dm.metier_id = m.metier_id
WHERE m.taux_horaire_paye > 0 AND m.taux_horaire_fact > 0
GROUP BY 1, 2, 3, 4, 5
```

| Colonne | Type | Description |
| --- | --- | --- |
| `agence_id` | int | Agence |
| `tie_id` | int | Client |
| `metier_id` | int | Métier |
| `libelle_metier` | varchar | Libellé métier |
| `mois` | date | Mois |
| `nb_missions` | int | Nombre de missions |
| `coeff_moyen_pondere` | float | **KPI principal** : coeff pondéré par heures |
| `coeff_moyen_simple` | float | Moyenne arithmétique des coefficients |
| `thf_moyen` | float | Taux horaire facturé moyen |
| `thp_moyen` | float | Taux horaire payé moyen |
| `marge_coeff_pct` | float | Marge en % (coeff − 1) |
| `niveau_coeff` | varchar | BON (≥1.35) / MOYEN (≥1.25) / A_REVOIR (<1.25) |

---

## 9. CA par Secteur NAF

**Définition métier**

> La ventilation du CA par secteur NAF permet d'analyser la composition du portefeuille
> client et d'identifier les secteurs porteurs ou en déclin. C'est un indicateur
> stratégique pour orienter la prospection commerciale.

```text
CA secteur = Σ CA_net_HT des clients du secteur NAF sur la période
Part de marché interne = CA secteur / CA total agence × 100
```

**Statut des données**

✅ **Aucune modification Bronze ou Silver requise.**
`dim_clients.naf_code` et `fact_ca_mensuel_client` sont déjà disponibles en Gold.
Il faut ajouter la table de libellés NAF (référentiel public, seed statique).

> **Note :** un référentiel NAF simplifié (sections A-U, 21 familles) peut être
> chargé comme table statique Gold sans ingestion Evolia.

**Table Gold cible :** `gld_commercial.fact_ca_secteur_naf`

```sql
-- gold_ca_mensuel.py (enrichir) → gld_commercial.fact_ca_secteur_naf
WITH naf_sections AS (
    SELECT naf_code, section_code, section_libelle
    FROM gld_shared.ref_naf_sections
)
SELECT
    c.agence_id,
    DATE_TRUNC('month', c.mois)                             AS mois,
    d.naf_code,
    LEFT(d.naf_code, 2)                                     AS naf_division,
    COALESCE(n.section_code, 'XX')                          AS naf_section,
    COALESCE(n.section_libelle, 'Non classifié')            AS secteur_libelle,
    COUNT(DISTINCT c.tie_id)                                AS nb_clients,
    SUM(c.ca_net_ht)                                        AS ca_net_ht,
    SUM(c.nb_missions_facturees)                            AS nb_missions,
    SUM(c.nb_heures_facturees)                              AS heures_facturees,
    ROUND(
        SUM(c.ca_net_ht) / SUM(SUM(c.ca_net_ht)) OVER
            (PARTITION BY c.agence_id, DATE_TRUNC('month', c.mois)) * 100
    , 1)                                                    AS part_ca_agence_pct
FROM gld_commercial.fact_ca_mensuel_client c
JOIN gld_shared.dim_clients d
    ON d.tie_id = c.tie_id AND d.is_current
LEFT JOIN naf_sections n
    ON n.naf_code = d.naf_code
GROUP BY 1, 2, 3, 4, 5, 6
```

**Table statique à créer :** `gld_shared.ref_naf_sections`

```sql
-- Script de seed : scripts/seed_ref_naf.py
-- 21 sections NAF (nomenclature INSEE) mappées sur les codes 2 chiffres
CREATE TABLE gld_shared.ref_naf_sections (
    naf_prefix      varchar(2) PRIMARY KEY,  -- 01-99 (division)
    section_code    varchar(1),              -- A à U
    section_libelle varchar(80)
);
```

| Colonne | Type | Description |
| --- | --- | --- |
| `agence_id` | int | Agence |
| `mois` | date | Mois |
| `naf_code` | varchar | Code NAF complet (ex : 7820Z) |
| `naf_division` | varchar | Division NAF 2 chiffres |
| `naf_section` | varchar | Section NAF (lettre A-U) |
| `secteur_libelle` | varchar | Libellé secteur (ex : "Industrie manufacturière") |
| `nb_clients` | int | Clients actifs dans ce secteur |
| `ca_net_ht` | decimal | CA net du secteur |
| `nb_missions` | int | Missions facturées |
| `heures_facturees` | float | Heures facturées |
| `part_ca_agence_pct` | float | Part du CA agence (%) |

---

## 10. Roadmap d'implémentation

### Phase A — Bronze (2 actions, impact nul sur l'existant)

| Action | Fichier | Cible | Détail | Effort |
| --- | --- | --- | --- | --- |
| Ajouter `MISS_ANNULE` | `bronze_missions.py` | `_COLS["WTMISS"]` | 1 colonne — flag annulation mission | 5 min |
| Ingérer `WTFINMISS` | `bronze_missions.py` | `TABLES_FULL` | Référentiel fin mission (~20 lignes) | 20 min |

> WTEFAC : **aucune action** — toutes les colonnes DSO déjà extraites (lignes 80-86).

### Phase B — Silver (nouveaux blocs de transformation)

| Action | Script | Table Silver produite | Effort |
| --- | --- | --- | --- |
| Créer `facturation_detail` | `silver_clients_detail.py` | `slv_clients/facturation_detail` | Moyen |
| Ajouter `fin_mission` | `silver_missions.py` | `slv_missions/fin_mission` | Moyen |

### Phase C — Gold (nouveaux scripts ou enrichissement)

| Script | Tables Gold produites | Dépendances Silver |
| --- | --- | --- |
| `gold_qualite_missions.py` (NOUVEAU) | `gld_performance.fact_duree_mission` | `fact_missions_detail` (dispo) |
| `gold_qualite_missions.py` | `gld_performance.fact_rupture_contrat` | `slv_missions/fin_mission` |
| `gold_qualite_missions.py` | `gld_commercial.fact_renouvellement_mission` | `fact_missions_detail` (dispo) |
| `gold_qualite_missions.py` | `gld_performance.fact_coeff_facturation` | `fact_missions_detail` (dispo) |
| `gold_recouvrement.py` (NOUVEAU) | `gld_commercial.fact_dso_client` | `slv_clients/facturation_detail` |
| `gold_recouvrement.py` | `gld_commercial.fact_balance_agee` | `slv_clients/facturation_detail` |
| `gold_staffing.py` (enrichir) | `gld_staffing.fact_dynamique_vivier` | `fact_fidelisation_interimaires` (dispo) |
| `gold_ca_mensuel.py` (enrichir) | `gld_commercial.fact_ca_secteur_naf` | `fact_ca_mensuel_client` (dispo) |
| `scripts/seed_ref_naf.py` (NOUVEAU) | `gld_shared.ref_naf_sections` | — (seed statique) |

### Ordre de déploiement recommandé

```text
1. Bronze A  → Ajouter MISS_ANNULE + ingérer WTFINMISS     [impact zéro sur l'existant]
2. Silver A  → Créer slv_clients/facturation_detail        [source : raw_wtefac déjà en S3]
3. Silver B  → Créer slv_missions/fin_mission              [dépend Bronze A]
4. Gold A    → gold_qualite_missions.py                    [DMM + Coeff + Renouvellement]
5. Gold B    → gold_recouvrement.py                        [DSO + Balance âgée : dépend Silver A]
6. Gold C    → gold_qualite_missions.py (rupture)          [dépend Silver B]
7. Gold D    → gold_staffing.py (vivier)                   [dépend fact_fidelisation dispo]
8. Gold E    → gold_ca_mensuel.py (secteur NAF)            [dépend seed NAF]
9. Seed      → seed_ref_naf.py                             [parallélisable dès le début]
```

---

## 11. Anomalies DDL détectées

Ces anomalies sont spécifiques aux nouveaux indicateurs (complémentaires à `ANALYSE_KPI_MANQUANTS.md` §8).

| ID | Table | Colonne | Anomalie | Impact | Traitement |
| --- | --- | --- | --- | --- | --- |
| B-01 | `WTEFAC` | `EFAC_MONTANTHT` | **Absent du DDL** — montant HT non stocké directement | DSO sans montant si non joint avec WTLFAC | JOIN `WTLFAC` ou utiliser `fact_ca_mensuel_client.ca_net_ht` |
| B-02 | `WTEFAC` | `EFAC_TYPF` | Valeurs non documentées dans DDL | Risque d'inclure avoirs dans le DSO | Filtrer sur `EFAC_TYPF = 'F'` après validation en Bronze |
| B-03 | `WTMISS` | `FINMISS_CODE` | NULL pour missions en cours — normal | Rupture non détectable sur missions actives | Filtrer `WHERE finmiss_code IS NOT NULL` pour les analyses historiques |
| B-04 | `WTFINMISS` | `FINMISS_LIBELLE` | Libellés non standardisés (langue, casse) | Classification TERME_NORMAL/RUPTURE fragile | Utiliser une table de mapping code → catégorie plutôt que LIKE sur libellé |
| B-05 | `WTTIESERV` | `NAF` / `NAF2008` | Double colonne NAF (ancienne + nouvelle) | Codes hétérogènes selon date création client | Priorité NAF2008 si non NULL, sinon NAF — déjà géré dans `silver_clients.py` |
