# DATA MAPPING — GI Data Lakehouse · Evolia → Superset

> **Manifeste v3.0 · Phases 0-3 · Dernière mise à jour : 2026-03-13**
> Lecture : de gauche à droite — Source Evolia SQL Server → Bronze S3 → Silver Parquet → Gold PostgreSQL → Superset

---

## Sommaire

1. [Vue d'ensemble — Flux de données](#1-vue-densemble--flux-de-données)
2. [Domaine Agences](#2-domaine-agences)
3. [Domaine Clients](#3-domaine-clients)
4. [Domaine Missions & Facturation](#4-domaine-missions--facturation)
5. [Domaine Intérimaires & Compétences](#5-domaine-intérimaires--compétences)
6. [Domaine Temps (Heures)](#6-domaine-temps-heures)
7. [Dimensions Partagées (Gold)](#7-dimensions-partagées-gold)
8. [Superset — Datasets & Dashboards](#8-superset--datasets--dashboards)
9. [Index colonnes RGPD](#9-index-colonnes-rgpd)
10. [Anomalies & Watchlist](#10-anomalies--watchlist)

---

## 1. Vue d'ensemble — Flux de données

```
EVOLIA SQL Server (on-prem · SRV-SQL2\cegi · DB: INTERACTION)
│
│  pyodbc · lecture seule · ODBC Driver 17
│
├── bronze_agences.py ──────────────────────────────────────────────────────┐
├── bronze_clients.py ──────────────────────────────────────────────────────┤
├── bronze_clients_external.py (SIRENE API + Salesforce) ──────────────────┤
├── bronze_missions.py ─────────────────────────────────────────────────────┤ → S3 Bronze
├── bronze_interimaires.py ─────────────────────────────────────────────────┤   gi-poc-bronze
                                                                            │   raw_{table}/YYYY/MM/DD/
                                                                            │   JSON newline-delimited
                                                                            │
                              DuckDB + httpfs · read_json_auto              │
                                                                            ▼
                        ┌── silver_agences_light.py ──────────────────────────────┐
                        ├── silver_clients.py (SCD2) ─────────────────────────────┤
                        ├── silver_clients_detail.py ─────────────────────────────┤ → S3 Silver
                        ├── silver_missions.py ───────────────────────────────────┤   gi-poc-silver
                        ├── silver_factures.py ───────────────────────────────────┤   slv_{domaine}/
                        ├── silver_interimaires.py (SCD2) ────────────────────────┤   {table}/**/*.parquet
                        ├── silver_interimaires_detail.py ───────────────────────┤
                        ├── silver_competences.py ────────────────────────────────┤
                        └── silver_temps.py ─────────────────────────────────────┘
                                                                            │
                              DuckDB + psycopg2 · pg_bulk_insert            │
                                                                            ▼
                        ┌── gold_dimensions.py ──── gld_shared.*  ──────────────┐
                        ├── gold_ca_mensuel.py ──── gld_commercial.* ───────────┤
                        ├── gold_staffing.py ─────── gld_staffing.* ────────────┤ → PostgreSQL
                        ├── gold_competences.py ─── gld_staffing.* ─────────────┤   frdc1datahub01
                        ├── gold_scorecard_agence.py ─ gld_performance.* ───────┤
                        ├── gold_etp.py ─────────── gld_operationnel.* ─────────┤
                        ├── gold_operationnel.py ── gld_operationnel.* ─────────┤
                        ├── gold_vue360_client.py ─ gld_clients.* ──────────────┤
                        ├── gold_retention_client.py ─ gld_clients.* ───────────┤
                        └── gold_clients_detail.py ─ gld_clients.* ─────────────┘
                                                                            │
                              SQL direct / REST API                         │
                                                                            ▼
                                              Superset (frdc1dataweb01)
                                              Datasets → Charts → Dashboards
```

---

## 2. Domaine Agences

### 2.1 Mapping Evolia → Bronze

| Table Evolia | S3 Bronze path | Colonnes extraites | Mode | PK |
|---|---|---|---|---|
| `PYREGROUPECNT` | `raw_pyregroupecnt/…` | RGPCNT_ID, RGPCNT_CODE, RGPCNT_LIBELLE, DOS_ID, ENT_ID, DATE_CLOTURE | FULL | RGPCNT_ID |
| `PYENTREPRISE` | `raw_pyentreprise/…` | ENT_ID, ENT_SIREN, ENT_RAISON, ENT_APE, ENT_ETT, RGPCNT_ID, ENT_MONOETAB | FULL | ENT_ID |
| `PYETABLISSEMENT` | `raw_pyetablissement/…` | ETA_ID, ENT_ID, ETA_ACTIVITE, ETA_COMMUNE, ETA_ADR2_COMP, ETA_ADR2_VOIE, ETA_ADR2_CP, ETA_ADR2_VILLE, ETA_PSEUDO_SIRET, ETA_DATE_CESACT | FULL | ETA_ID |
| `WTUG` | `raw_wtug/…` | RGPCNT_ID, ETA_ID, UG_GPS (NVARCHAR), UG_CLOTURE_DATE, UG_CLOTURE_USER, PIL_ID, UG_EMAIL | FULL | RGPCNT_ID, ETA_ID |
| `PYDOSPETA` | `raw_pydospeta/…` | RGPCNT_ID, ETA_ID | FULL | RGPCNT_ID, ETA_ID |

### 2.2 Bronze → Silver

| Script | Lire depuis Bronze | Écrire vers Silver | Colonnes Silver |
|---|---|---|---|
| `silver_agences_light.py` | raw_pyregroupecnt, raw_wtug | `slv_agences/dim_agences` | agence_sk *(MD5)*, rgpcnt_id, nom, code, marque, branche, ville, email, latitude, longitude, is_cloture, **is_active**, cloture_date, _loaded_at |
| `silver_agences_light.py` | raw_pyregroupecnt | `slv_agences/hierarchie_territoriale` | rgpcnt_id, secteur, perimetre, zone_geo, _loaded_at |

### 2.3 Silver → Gold

| Script | Lire depuis Silver | Table Gold | Colonnes Gold |
|---|---|---|---|
| `gold_dimensions.py` | slv_agences/dim_agences, slv_agences/hierarchie_territoriale | `gld_shared.dim_agences` | agence_sk, rgpcnt_id, nom_agence, marque, branche, secteur, perimetre, zone_geo, ville, **is_active** |
| `gold_scorecard_agence.py` | slv_missions, slv_facturation, slv_temps | `gld_performance.scorecard_agence` | agence_id, mois, ca_net_ht, taux_marge, marge_brute, nb_clients_actifs, nb_int_actifs, nb_missions, taux_transformation, nb_commandes, nb_pourvues |
| `gold_scorecard_agence.py` | idem | `gld_performance.ranking_agences` | agence_id, mois, ca_net_ht, taux_marge, nb_int_actifs, taux_transformation, rang_ca, rang_marge, rang_placement, rang_transfo, score_global |
| `gold_scorecard_agence.py` | idem | `gld_performance.tendances_agence` | agence_id, mois, ca_net_ht, taux_marge, nb_int_actifs, delta_ca_mom, delta_marge_mom, delta_int_mom, delta_ca_yoy, delta_marge_yoy, tendance |
| `gold_operationnel.py` | slv_temps/releves_heures, slv_missions/commandes | `gld_operationnel.fact_heures_hebdo` | agence_id, tie_id, semaine_debut, heures_paye, heures_fact, nb_releves, _loaded_at |
| `gold_operationnel.py` | idem | `gld_operationnel.fact_commandes_pipeline` | agence_id, semaine_debut, nb_commandes, nb_pourvues, nb_ouvertes, taux_satisfaction, _loaded_at |

---

## 3. Domaine Clients

### 3.1 Mapping Evolia → Bronze

| Table Evolia | S3 Bronze path | Colonnes extraites | Mode | PK |
|---|---|---|---|---|
| `CMTIERS` | `raw_cmtiers/…` | TIE_ID, TIE_NOM, TIE_NOMC, TIE_SIRET, TIE_CODE | FULL | TIE_ID |
| `WTTIESERV` | `raw_wttieserv/…` | TIE_ID, TIES_SERV, TIES_DESIGNATION, TIES_NOM, TIES_ADR1, TIES_ADR2, TIES_CODEP, TIES_VILLE, PAYS_CODE, TIES_SIREN, TIES_NIC, NAF, NAF2008, TIES_EMAIL, CLOT_DAT, TIES_CDMODIFDATE, RGPCNT_ID | FULL | TIE_ID, TIES_SERV |
| `WTCLPT` | `raw_wtclpt/…` | TIE_ID, CLPT_PROS, CLPT_CAPT, CLPT_DCREA, CLPT_EFFT, CLPT_MODIFDATE | FULL | TIE_ID |
| `WTTIEINT` | `raw_wttieint/…` | TIE_ID, TIEI_ORDRE, TIEI_NOM, TIEI_PRENOM, TIEI_EMAIL, TIEI_BUREAU, FCTI_CODE | FULL | TIE_ID, TIEI_ORDRE |
| `WTCOEF` | `raw_wtcoef/…` | TQUA_ID, RGPCNT_ID, TIE_ID, COEF_VAL, COEF_DEFF, COEF_DFIN | FULL | TQUA_ID, RGPCNT_ID, TIE_ID |
| `WTENCOURSG` | `raw_wtencoursg/…` | ENCGRP_ID, ENC_SIREN, ENCG_DECISIONLIB | FULL | ENCGRP_ID |
| `WTUGCLI` | `raw_wtugcli/…` | RGPCNT_ID, TIE_ID, UGCLI_ORIG | FULL | RGPCNT_ID, TIE_ID |

**Sources externes :**

| Source | S3 Bronze path | Colonnes extraites |
|---|---|---|
| SIRENE API | `raw_sirene/…` | siret, raison_sociale_officielle, date_creation, tranche_effectif, naf, adresse_siege, _loaded_at |
| Salesforce (stub) | `raw_salesforce_accounts/…` | Account_Id, Name, Industry, SIRET__c, BillingCity, CreatedDate, _loaded_at |

### 3.2 Bronze → Silver

| Script | Lire depuis Bronze | Écrire vers Silver | Colonnes Silver |
|---|---|---|---|
| `silver_clients.py` *(SCD2)* | raw_wttieserv, raw_wtclpt | `slv_clients/dim_clients` | client_sk *(MD5)*, tie_id, change_hash, raison_sociale, siren, nic, naf_code, adresse_complete, ville, code_postal, statut_client, ca_potentiel, date_creation_fiche, **is_current**, valid_from, valid_to, _loaded_at |
| `silver_clients_detail.py` | raw_wttieserv | `slv_clients/sites_mission` | site_id, tie_id, nom_site, siren, nic, adresse, ville, code_postal, pays_code, siret_site, email, agence_id, **is_active**, clot_at, row_hash, _loaded_at |
| `silver_clients_detail.py` | raw_wttieint | `slv_clients/contacts` | contact_id *(MD5)*, tie_id, nom, prenom, email, telephone, fonction_code, _loaded_at |
| `silver_clients_detail.py` | raw_wtencoursg | `slv_clients/encours_credit` | encours_id, siren, montant_encours, limite_credit, date_decision, decision_libelle, _loaded_at |
| `silver_clients_detail.py` | raw_wtcoef | `slv_clients/coefficients` | coef_id *(MD5)*, rgpcnt_id, tie_id, qualification_code, coefficient, date_effet, _loaded_at |

### 3.3 Silver → Gold

| Script | Lire depuis Silver | Table Gold | Colonnes Gold |
|---|---|---|---|
| `gold_dimensions.py` | slv_clients/dim_clients | `gld_shared.dim_clients` | client_sk, tie_id, raison_sociale, siren, nic, siret, naf_code, naf_libelle, ville, code_postal, statut_client, effectif_tranche |
| `gold_clients_detail.py` | slv_clients, slv_missions, gld_commercial.fact_ca_mensuel_client | `gld_clients.vue_360_client` | client_sk, siren, raison_sociale, naf_code, ville, nb_sites, ca_ytd, ca_n1, delta_ca_pct, encours_montant, encours_limite, **risque_credit** *(CRITIQUE/ELEVE/NORMAL)*, _loaded_at |
| `gold_clients_detail.py` | idem | `gld_clients.fact_retention_client` | client_sk, trimestre, ca, delta_ca, nb_missions, **risque_churn** *(RISQUE_FORT/INACTIF/STABLE)*, _loaded_at |
| `gold_vue360_client.py` | slv_clients, slv_missions, gld_commercial, gld_staffing | `gld_clients.vue_360_client` | client_sk, tie_id, raison_sociale, siren, ville, secteur_activite, effectif, statut, ca_ytd, ca_n1, delta_ca_pct, ca_12_mois_glissants, nb_missions_actives, nb_missions_total, nb_int_actifs, nb_int_historique, top_3_metiers *(JSON)*, anciennete_jours, marge_moyenne_pct, montant_encours, limite_credit, risque_credit_score, nb_agences_partenaires, derniere_facture_date, jours_depuis_derniere, **risque_churn**, _computed_at |
| `gold_retention_client.py` | slv_facturation, slv_missions, slv_temps | `gld_clients.fact_retention_client` | client_sk, tie_id, trimestre, ca_net, delta_ca_qoq, delta_ca_qoq_pct, delta_ca_yoy, delta_ca_yoy_pct, nb_missions, nb_factures, frequence_4_trimestres, derniere_facture, jours_inactivite, **risque_churn**, churn_score_ml |
| `gold_retention_client.py` | idem | `gld_clients.fact_rentabilite_client` | client_sk, tie_id, annee, ca_net, ca_missions, cout_paye, marge_brute, taux_marge, cout_gestion_estime, rentabilite_nette, taux_rentabilite_nette, nb_interimaires |
| `gold_ca_mensuel.py` | slv_facturation, slv_temps, slv_missions, slv_clients | `gld_commercial.fact_ca_mensuel_client` | client_sk, tie_id, mois, ca_ht, avoir_ht, ca_net_ht, nb_factures, nb_missions_facturees, nb_heures_facturees, taux_moyen_fact, agence_principale |

---

## 4. Domaine Missions & Facturation

### 4.1 Mapping Evolia → Bronze

| Table Evolia | S3 Bronze path | Colonnes extraites | Mode | Delta col |
|---|---|---|---|---|
| `WTMISS` | `raw_wtmiss/…` | PER_ID, CNT_ID, TIE_ID, MISS_TIEID, TIES_SERV, MISS_CODE, MISS_JUSTIFICATION, MISS_DPAE, MISS_ETRANGER, MISS_QUAL, MISS_PERFERM, RGPCNT_ID, MISS_NDPAE, CNTI_CREATE, FINMISS_CODE, MISS_SAISIE_DTFIN, MISS_TRANSDATE, MISS_MODIFDATE, **MISS_FLAGDPAE** ⚠️datetime2, MISS_BTP, MISS_TYPCOEF, MISS_LOGIN, **CMD_ID** ⚠️float | DELTA | CNTI_CREATE |
| `WTCNTI` | `raw_wtcnti/…` | PER_ID, CNT_ID, CNTI_ORDRE, TIE_ID, MET_ID, CNTI_CREATE, CNTI_DATEFFET, CNTI_DATEFINCNTI, CNTI_SOUPDEB, CNTI_SOUPFIN, CNTI_HPART, CNTI_RETCT, CNTI_RETINT, CNTI_THPAYE, CNTI_THFACT, CNTI_SOUPMODIF, CNTI_POSTE, LOTFAC_CODE, CNTI_SALREF1-4, CNTI_DESCRIPT1-2, CNTI_PROTEC1-2, CNTI_DURHEBDO, PCS_CODE_2003 | DELTA | CNTI_CREATE |
| `WTEFAC` | `raw_wtefac/…` | EFAC_NUM, EFAC_LIB, RGPCNT_ID, TIE_ID, TIES_SERV, EFAC_IDRUPT, EFAC_RUPTURE, DEV_CODE, MRG_CODE, EFAC_DTEEDI, EFAC_DTEECH, EFAC_TYPF, EFAC_TYPG, WTE_EFAC_NUM, EFAC_MATR, EFAC_VERROU, EFAC_TRANS, EFAC_TRANS_FACTO, EFAC_ORDNUM, EFAC_USER, JSTFAV_ID, EFAC_JSTFCOMM, TVA_CODE, EFAC_DTEREGLF, EFAC_TAUXTVA, EFAC_PLAC_ID | DELTA | EFAC_DTEEDI |
| `WTLFAC` | `raw_wtlfac/…` | FAC_NUM, LFAC_ORD, LFAC_LIB, LFAC_BASE, LFAC_TAUX, LFAC_MNT | FULL | — |
| `WTFACINFO` | `raw_wtfacinfo/…` | CNT_ID, FAC_NUM, PER_ID, TIE_ID | FULL | — |
| `WTCMD` | `raw_wtcmd/…` | CMD_ID, RGPCNT_ID, CMD_DTE, CMD_NBSALS, CMD_CODE, STAT_CODE, STAT_TYPE | FULL | — |
| `WTPLAC` | `raw_wtplac/…` | PLAC_ID, RGPCNT_ID, TIE_ID, MET_ID, PLAC_DTEEDI | FULL | — |
| `PYCONTRAT` | `raw_pycontrat/…` | PER_ID, CNT_ID, ETA_ID, RGPCNT_ID, CNT_DATEDEB, CNT_DATEFIN, CNT_FINPREVU, LOTPAYE_CODE, TYPCOT_CODE, CNT_AVT_ORDRE, CNT_INI_ORDRE | DELTA | CNT_DATEFIN *(allow_null)* |

### 4.2 Bronze → Silver

| Script | Lire depuis Bronze | Écrire vers Silver | Colonnes Silver |
|---|---|---|---|
| `silver_missions.py` | raw_wtmiss + raw_wtcnti + raw_wtcmd (JOIN multi-sources via sql_fn) | `slv_missions/missions` | per_id, cnt_id, tie_id, ties_serv, rgpcnt_id, date_debut, date_fin, motif, code_fin, prh_bts, **statut_dpae** (MISS_FLAGDPAE datetime2), **ecart_heures** (DPAE vs début contrat en heures), **delai_placement_heures** (CMD_DTE→CNTI_DATEFFET), **categorie_delai** (urgent/court/standard/long/inconnu), _batch_id, _loaded_at |
| `silver_missions.py` | idem | `slv_missions/contrats` | per_id, cnt_id, ordre, met_id, tpci_code, date_debut, date_fin, taux_paye, taux_fact, nb_heures, poste, _batch_id, _loaded_at |
| `silver_missions.py` | idem | `slv_missions/commandes` | cmd_id, rgpcnt_id, cmd_date, nb_sal, stat_code, stat_type, _batch_id, _loaded_at |
| `silver_missions.py` | idem | `slv_missions/placements` | plac_id, rgpcnt_id, tie_id, met_id, statut, plac_date, _batch_id, _loaded_at |
| `silver_missions.py` | idem | `slv_missions/contrats_paie` | per_id, cnt_id, eta_id, rgpcnt_id, date_debut, date_fin, date_fin_prevue, lot_paye_code, typ_cotisation_code, avt_ordre, ini_ordre, _batch_id, _loaded_at |
| `silver_factures.py` | raw_wtefac, raw_wtlfac | `slv_facturation/factures` | efac_num, rgpcnt_id, tie_id, ties_serv, type_facture, date_facture, date_echeance, montant_ht ⚠️NULL, montant_ttc ⚠️NULL, taux_tva, prh_bts, _batch_id, _loaded_at |
| `silver_factures.py` | idem | `slv_facturation/lignes_factures` | fac_num, lfac_ord, libelle, base, taux, montant, rubrique, _batch_id, _loaded_at |

> ⚠️ **Bug B-02** : `WTEFAC.EFAC_MONTANTHT` absent du DDL Evolia → `montant_ht` NULL en Silver.
> Contournement Gold : `ca_ht = SUM(lfac_base × lfac_taux)` via `lignes_factures`.

### 4.3 Silver → Gold

| Script | Lire depuis Silver | Table Gold | Colonnes Gold |
|---|---|---|---|
| `gold_ca_mensuel.py` | slv_facturation/factures, slv_facturation/lignes_factures, slv_temps/heures_detail, slv_missions/missions, slv_clients/dim_clients | `gld_commercial.fact_ca_mensuel_client` | client_sk, tie_id, mois, ca_ht *(reconstitué)*, avoir_ht, ca_net_ht, nb_factures, nb_missions_facturees, nb_heures_facturees, taux_moyen_fact, agence_principale |
| `gold_ca_mensuel.py` | slv_facturation (Pareto top-20% CA par agence/mois) | `gld_clients.fact_concentration_client` | agence_id, mois, nb_clients, nb_clients_top20, ca_net_total, ca_net_top20, taux_concentration |
| `gold_staffing.py` | slv_missions, slv_missions/contrats, slv_temps/releves_heures, slv_interimaires | `gld_staffing.fact_missions_detail` | mission_sk, per_id, cnt_id, tie_id, agence_id, metier_id, date_debut, date_fin, duree_jours, taux_horaire_paye, taux_horaire_fact, marge_horaire, heures_totales, ca_mission, cout_mission, marge_mission, taux_marge |
| `gold_scorecard_agence.py` | slv_missions, slv_facturation, slv_temps | `gld_performance.scorecard_agence` | agence_id, mois, ca_net_ht, taux_marge, marge_brute, nb_clients_actifs, nb_int_actifs, nb_missions, taux_transformation, nb_commandes, nb_pourvues |
| `gold_operationnel.py` | slv_missions/missions (statut_dpae, delai_placement_heures, categorie_delai) | `gld_operationnel.fact_delai_placement` | agence_id, semaine_debut, categorie_delai, nb_missions, delai_moyen_heures, delai_median_heures |
| `gold_operationnel.py` | slv_missions/missions (statut_dpae, ecart_heures) | `gld_operationnel.fact_conformite_dpae` | agence_id, mois, nb_missions, nb_dpae_transmises, nb_dpae_manquantes, taux_conformite_dpae, ecart_moyen_heures |

> ⚠️ **Prérequis** : `fact_delai_placement` et `fact_conformite_dpae` nécessitent un run Silver missions après le 2026-03-13 (colonnes enrichies absentes des Parquet antérieurs).

---

## 5. Domaine Intérimaires & Compétences

### 5.1 Mapping Evolia → Bronze

**Tables personne / dossier :**

| Table Evolia | S3 Bronze path | Colonnes extraites | Mode | RGPD |
|---|---|---|---|---|
| `PYPERSONNE` | `raw_pypersonne/…` | PER_ID, PER_NOM, PER_PRENOM, PER_NAISSANCE, **PER_NIR** 🔴, NAT_CODE, PAYS_CODE, PER_BISVOIE, PER_COMPVOIE, PER_CP, PER_VILLE, PER_COMMUNE | FULL | SENSIBLE |
| `PYSALARIE` | `raw_pysalarie/…` | PER_ID, SAL_MATRICULE, SAL_DATEENTREE, SAL_ACTIF | FULL | PERSONNEL |
| `WTPINT` | `raw_wtpint/…` | PER_ID, PINT_CANDIDAT, PINT_DOSSIER, PINT_PERMANENT, **PINT_PREVENDTE**, **PINT_DERVENDTE**, **PINT_MODIFDATE**, **PINT_CREATDTE** | FULL | PERSONNEL |
| `PYCOORDONNEE` | `raw_pycoordonnee/…` | PER_ID, TYPTEL_CODE, **PER_TEL_NTEL** 🔴, PER_TEL_POSTE | FULL | SENSIBLE |
| `WTPEVAL` | `raw_wtpeval/…` | PER_ID, PEVAL_DU, PEVAL_EVALUATION, PEVAL_UTL | DELTA | — |
| `WTUGPINT` | `raw_wtugpint/…` | PER_ID, RGPCNT_ID, AUG_ORI | FULL | — |

**Tables compétences (liens personne-référentiel) :**

| Table Evolia | S3 Bronze path | Colonnes extraites | Mode |
|---|---|---|---|
| `WTPMET` | `raw_wtpmet/…` | PER_ID, PMET_ORDRE, MET_ID | FULL |
| `WTPHAB` | `raw_wtphab/…` | PER_ID, THAB_ID, PHAB_DELIVR, PHAB_EXPIR, PHAB_ORDRE | FULL |
| `WTPDIP` | `raw_wtpdip/…` | PER_ID, TDIP_ID, PDIP_DATE | FULL |
| `WTEXP` | `raw_wtexp/…` | PER_ID, EXP_ORDRE, EXP_NOM, EXP_DEBUT, EXP_FIN, EXP_INTERNE | FULL |

**Tables référentiels compétences :**

| Table Evolia | S3 Bronze path | Colonnes extraites | Mode | Note |
|---|---|---|---|---|
| `WTMET` | `raw_wtmet/…` | MET_ID, MET_CODE, MET_LIBELLE, TQUA_ID, NIVQ_ID, SPE_ID, **PCS_CODE_2003**, DFS_ID, **MET_DELETE** | FULL | Classification INSEE |
| `WTTHAB` | `raw_wtthab/…` | THAB_ID, THAB_CDE, THAB_LIBELLE, **THAB_NBMOIS** | FULL | Durée validité standard |
| `WTTDIP` | `raw_wttdip/…` | TDIP_ID, TDIP_CODE, TDIP_LIB, **TDIP_REF** | FULL | Catégorie diplôme |
| `WTQUA` | `raw_wtqua/…` | TQUA_ID, TQUA_CODE, **TQUA_LIBELLE** | FULL | Libellé type qualification |

### 5.2 Bronze → Silver

| Script | Lire depuis Bronze | Écrire vers Silver | Colonnes Silver | Logique métier |
|---|---|---|---|---|
| `silver_interimaires.py` *(SCD2)* | raw_pypersonne, raw_pysalarie, raw_wtpint | `slv_interimaires/dim_interimaires` | interimaire_sk, per_id, change_hash, matricule, nom, prenom, date_naissance, **nir_pseudo** 🟡, nationalite, pays, adresse, ville, code_postal, date_entree, date_sortie, is_actif, is_candidat, is_permanent, agence_rattachement, is_current, valid_from, valid_to, _loaded_at | NIR → SHA-256+salt |
| `silver_interimaires_detail.py` | raw_wtpeval, raw_pycoordonnee, raw_wtugpint | `slv_interimaires/evaluations` | eval_id, per_id, date_eval, note, commentaire, evaluateur_id, _loaded_at | — |
| `silver_interimaires_detail.py` | idem | `slv_interimaires/coordonnees` 🟡 | coord_id, per_id, type_coord, valeur, poste, is_principal, _loaded_at | Silver uniquement — jamais en Gold |
| `silver_interimaires_detail.py` | idem | `slv_interimaires/portefeuille_agences` | per_id, rgpcnt_id, _loaded_at | — |
| `silver_interimaires_detail.py` | raw_wtpint (PINT_DERVENDTE proxy SAL_DATESORTIE) | `slv_interimaires/fidelisation` | per_id, date_premiere_vente, date_avant_derniere_vente, date_derniere_vente, anciennete_jours, jours_depuis_derniere_vente, **categorie_fidelisation** (4 valeurs : actif-recent / actif-annee / inactif-long / inactif), _loaded_at | SAL_DATESORTIE absent DDL |
| `silver_competences.py` | raw_wtpmet+raw_wtmet, raw_wtphab+raw_wtthab, raw_wtpdip+raw_wttdip, raw_wtexp | `slv_interimaires/competences` | competence_id, per_id, type_competence, code, libelle, niveau *(TDIP_REF)*, date_obtention, **date_expiration** *(COALESCE PHAB_EXPIR, PHAB_DELIVR+THAB_NBMOIS)*, **is_active** *(MET_DELETE+date_expir)*, **pcs_code** *(PCS_CODE_2003)*, _source_table, _loaded_at | Calcul date_expir théorique |

**Logique date_expiration habilitations :**
```sql
date_expiration = COALESCE(
  PHAB_EXPIR::DATE,
  PHAB_DELIVR::DATE + MAKE_INTERVAL(months := THAB_NBMOIS)  -- si PHAB_EXPIR absent
)
```

### 5.3 Silver → Gold

| Script | Lire depuis Silver | Table Gold | Colonnes Gold |
|---|---|---|---|
| `gold_dimensions.py` | slv_interimaires/dim_interimaires | `gld_shared.dim_interimaires` | interimaire_sk, per_id, matricule, nom, prenom, ville, code_postal, date_entree, is_actif, is_candidat, is_permanent, agence_rattachement |
| `gold_dimensions.py` | raw_wtmet + raw_wtqua *(Bronze direct)* | `gld_shared.dim_metiers` | metier_sk, met_id, code_metier, libelle_metier, qualification *(TQUA_LIBELLE)*, specialite *(SPE_ID)*, niveau *(NIVQ_ID)*, **pcs_code** *(PCS_CODE_2003)* |
| `gold_competences.py` | slv_interimaires/competences, slv_interimaires/dim_interimaires, slv_missions/missions | `gld_staffing.fact_competences_dispo` | metier_sk, agence_sk, met_id, rgpcnt_id, nb_qualifies, nb_disponibles, nb_en_mission, taux_couverture, _computed_at |
| `gold_staffing.py` | slv_interimaires/dim_interimaires, slv_missions, slv_temps | `gld_staffing.fact_activite_int` | interimaire_sk, per_id, mois, nb_missions, nb_agences, nb_clients, heures_travaillees, heures_disponibles, taux_occupation, ca_genere |
| `gold_staffing.py` | slv_interimaires/fidelisation, slv_interimaires/portefeuille_agences | `gld_staffing.fact_fidelisation_interimaires` | agence_id, categorie_fidelisation, nb_interimaires, anciennete_moy_jours, jours_inactivite_moyen |

> **Prérequis `fact_fidelisation_interimaires`** : nécessite un run `silver_interimaires_detail` après le 2026-03-13 (slv_interimaires/fidelisation absent des partitions antérieures).

---

## 6. Domaine Temps (Heures)

### 6.1 Mapping Evolia → Bronze

| Table Evolia | S3 Bronze path | Colonnes extraites | Mode | Delta col | Note |
|---|---|---|---|---|---|
| `WTPRH` | `raw_wtprh/…` | PRH_BTS, PER_ID, CNT_ID, TIE_ID, PRH_DTEDEBSEM, LOTPAYE_CODE, CAL_AN, CAL_NPERIODE, LOTFAC_CODE, CALF_AN, CALF_NPERIODE, CNTI_ORDRE, PRH_DTEFINSEM, VENTHEU_DTEDEB, PRH_IFM, PRH_CP, PRH_FLAG_RH, PRH_MODIFDATE | DELTA | PRH_MODIFDATE | — |
| `WTRHDON` | `raw_wtrhdon/…` | RINT_ID, RHD_LIGNE, RHD_BASEP, RHD_TAUXP, RHD_BASEF, RHD_TAUXF, PRH_BTS, FAC_NUM, BUL_ID, RHD_RAPPEL, RHD_ORIRUB, RHD_PORTEE, RHD_LIBRUB, RHD_EXCLDEP, RHD_SEUILP, RHD_SEUILF, RHD_BASEPROV, RHD_TAUXPROV, RHD_DATED, RHD_DATEF | DELTA | RHD_DATED | FALLBACK 2024-01-01 (49.6M total rows) |

### 6.2 Bronze → Silver

| Script | Lire depuis Bronze | Écrire vers Silver | Colonnes Silver |
|---|---|---|---|
| `silver_temps.py` | raw_wtprh | `slv_temps/releves_heures` | prh_bts, per_id, cnt_id, tie_id, rgpcnt_id, periode, date_modif, valide, _batch_id, _loaded_at |
| `silver_temps.py` | raw_wtrhdon | `slv_temps/heures_detail` | prh_bts, rhd_ligne, rubrique, base_paye, taux_paye, base_fact, taux_fact, libelle, _batch_id, _loaded_at |

### 6.3 Silver → Gold

| Script | Lire depuis Silver | Table Gold | Colonnes Gold |
|---|---|---|---|
| `gold_etp.py` | slv_temps/releves_heures (valide=true), slv_temps/heures_detail | `gld_operationnel.fact_etp_hebdo` | agence_id, semaine_debut, nb_releves, nb_interimaires, heures_totales, etp *(SUM(base_paye)/35)* |
| `gold_operationnel.py` | slv_temps/releves_heures, slv_temps/heures_detail | `gld_operationnel.fact_heures_hebdo` | agence_id, tie_id, semaine_debut, heures_paye, heures_fact, nb_releves, _loaded_at |
| `gold_ca_mensuel.py` | slv_temps/heures_detail | Agrégation dans `fact_ca_mensuel_client` | nb_heures_facturees |
| `gold_staffing.py` | slv_temps/releves_heures, slv_temps/heures_detail | `gld_staffing.fact_activite_int` | heures_travaillees, heures_disponibles, taux_occupation |
| `gold_staffing.py` | idem | `gld_staffing.fact_missions_detail` | heures_totales, ca_mission, cout_mission, marge_mission, taux_marge |

---

## 7. Dimensions Partagées (Gold)

Toutes produites par `gold_dimensions.py` → schéma `gld_shared` (PostgreSQL frdc1datahub01).

| Dimension | Source principale | Colonnes | Jointure type |
|---|---|---|---|
| `dim_calendrier` | Génération DuckDB 2020-2027 | date_id, jour_semaine, nom_jour, semaine_iso, mois, nom_mois, trimestre, annee, is_jour_ouvre, is_jour_ferie | `ON d.mois = DATE_TRUNC('month', f.mois)` |
| `dim_agences` | Silver slv_agences | agence_sk, rgpcnt_id, nom_agence, marque, branche, secteur, perimetre, zone_geo, ville, is_active | `ON f.agence_sk = d.agence_sk` |
| `dim_clients` | Silver slv_clients | client_sk, tie_id, raison_sociale, siren, nic, siret, naf_code, naf_libelle, ville, code_postal, statut_client, effectif_tranche | `ON f.client_sk = d.client_sk` |
| `dim_interimaires` | Silver slv_interimaires | interimaire_sk, per_id, matricule, nom, prenom, ville, code_postal, date_entree, is_actif, is_candidat, is_permanent, agence_rattachement | `ON f.interimaire_sk = d.interimaire_sk` |
| `dim_metiers` | Bronze raw_wtmet + raw_wtqua | metier_sk, met_id, code_metier, libelle_metier, qualification, specialite, niveau, pcs_code | `ON f.met_id = d.met_id` |

---

## 8. Superset — Datasets & Dashboards

> Superset est déployé sur **frdc1dataweb01.siege.interaction-interim.com**.
> Il se connecte directement au PostgreSQL Gold sur **frdc1datahub01** (connexion `gi_poc_ddi_gold`).

### 8.1 Datasets Superset recommandés

Chaque dataset correspond à une table ou vue Gold PostgreSQL.

#### Dataset : Scorecard Agences
| Propriété | Valeur |
|---|---|
| Schéma PG | `gld_performance` |
| Tables | `scorecard_agence` JOIN `gld_shared.dim_agences` |
| Métriques clés | ca_net_ht, taux_marge, nb_missions, taux_transformation, nb_int_actifs |
| Dimensions | agence_id, mois, marque, branche, zone_geo |
| Filtres suggérés | mois (date range), branche, marque |

#### Dataset : CA Mensuel Client
| Propriété | Valeur |
|---|---|
| Schéma PG | `gld_commercial` |
| Tables | `fact_ca_mensuel_client` JOIN `gld_shared.dim_clients` JOIN `gld_shared.dim_agences` |
| Métriques clés | ca_net_ht, nb_heures_facturees, taux_moyen_fact, nb_missions_facturees |
| Dimensions | mois, tie_id, raison_sociale, naf_code, agence_principale |
| Filtres suggérés | mois (date range), agence, secteur NAF |

#### Dataset : Pool Compétences Disponibles
| Propriété | Valeur |
|---|---|
| Schéma PG | `gld_staffing` |
| Tables | `fact_competences_dispo` JOIN `gld_shared.dim_metiers` JOIN `gld_shared.dim_agences` |
| Métriques clés | nb_qualifies, nb_disponibles, nb_en_mission, taux_couverture |
| Dimensions | met_id, libelle_metier, pcs_code, qualification, agence_id, zone_geo |
| Filtres suggérés | agence, pcs_code, qualification |

#### Dataset : Activité Intérimaires
| Propriété | Valeur |
|---|---|
| Schéma PG | `gld_staffing` |
| Tables | `fact_activite_int` JOIN `gld_shared.dim_interimaires` |
| Métriques clés | nb_missions, heures_travaillees, taux_occupation, ca_genere |
| Dimensions | per_id, mois, agence_rattachement, is_actif |

#### Dataset : Vue 360° Client
| Propriété | Valeur |
|---|---|
| Schéma PG | `gld_clients` |
| Tables | `vue_360_client` |
| Métriques clés | ca_ytd, ca_n1, delta_ca_pct, ca_12_mois_glissants, risque_churn, risque_credit |
| Dimensions | siren, raison_sociale, naf_code, ville, statut |
| Colonnes calculées Superset | `delta_ca_pct_color` (rouge si <0, vert si >10%) |

#### Dataset : Rétention & Churn Client
| Propriété | Valeur |
|---|---|
| Schéma PG | `gld_clients` |
| Tables | `fact_retention_client` JOIN `gld_shared.dim_clients` |
| Métriques clés | ca_net, delta_ca_qoq_pct, jours_inactivite, churn_score_ml |
| Dimensions | trimestre, risque_churn |

#### Dataset : Heures Opérationnelles
| Propriété | Valeur |
|---|---|
| Schéma PG | `gld_operationnel` |
| Tables | `fact_heures_hebdo` JOIN `gld_shared.dim_agences` JOIN `gld_shared.dim_clients` |
| Métriques clés | heures_paye, heures_fact, nb_releves |
| Dimensions | semaine_debut, agence_id, tie_id |

#### Dataset : Missions Détail
| Propriété | Valeur |
|---|---|
| Schéma PG | `gld_staffing` |
| Tables | `fact_missions_detail` JOIN `gld_shared.dim_metiers` JOIN `gld_shared.dim_agences` JOIN `gld_shared.dim_clients` |
| Métriques clés | taux_marge, marge_mission, heures_totales, duree_jours |
| Dimensions | date_debut, metier_id, agence_id, tie_id |

#### Dataset : ETP Hebdomadaire
| Propriété | Valeur |
|---|---|
| Schéma PG | `gld_operationnel` |
| Tables | `fact_etp_hebdo` JOIN `gld_shared.dim_agences` |
| Métriques clés | etp, heures_totales, nb_interimaires, nb_releves |
| Dimensions | semaine_debut, agence_id, marque, branche, zone_geo |
| Filtres suggérés | semaine (date range), agence, branche |

#### Dataset : Fidélisation Intérimaires
| Propriété | Valeur |
|---|---|
| Schéma PG | `gld_staffing` |
| Tables | `fact_fidelisation_interimaires` JOIN `gld_shared.dim_agences` |
| Métriques clés | nb_interimaires, anciennete_moy_jours, jours_inactivite_moyen |
| Dimensions | agence_id, categorie_fidelisation (actif_recent/actif_annee/inactif_long/inactif) |
| Filtres suggérés | agence, categorie_fidelisation |

#### Dataset : Concentration Client (Pareto)
| Propriété | Valeur |
|---|---|
| Schéma PG | `gld_clients` |
| Tables | `fact_concentration_client` JOIN `gld_shared.dim_agences` |
| Métriques clés | taux_concentration, ca_net_top20, ca_net_total, nb_clients_top20 |
| Dimensions | agence_id, mois |
| Colonnes calculées Superset | `risque_concentration` (rouge si taux_concentration > 0.80) |

### 8.2 Dashboards proposés

| Dashboard | Datasets utilisés | Public cible |
|---|---|---|
| **Tableau de bord Direction** | scorecard_agence, fact_ca_mensuel_client, ranking_agences | Direction, DG |
| **Performance Agences** | scorecard_agence, ranking_agences, tendances_agence, fact_commandes_pipeline | Directeurs régionaux |
| **Portefeuille Clients** | vue_360_client, fact_retention_client, fact_ca_mensuel_client | Commerciaux, KAM |
| **Pilotage Staffing** | fact_competences_dispo, fact_activite_int, fact_missions_detail | Recruteurs, chefs d'agence |
| **Opérationnel Hebdo** | fact_heures_hebdo, fact_commandes_pipeline | Chefs d'agence, ops |
| **Risques & Recouvrement** | vue_360_client (risque_credit, encours), fact_retention_client (churn) | Finance, crédit |

---

## 9. Index colonnes RGPD

| Colonne | Table(s) Bronze | Traitement Silver | Présent en Gold | Présent Superset |
|---|---|---|---|---|
| `PER_NIR` | PYPERSONNE | 🔴 Pseudonymisé → `nir_pseudo` (SHA-256+salt) | ❌ Non | ❌ Non |
| `PER_TEL_NTEL` | PYCOORDONNEE | 🔴 Conservé dans `slv_interimaires/coordonnees` uniquement | ❌ Non | ❌ Non |
| `PER_NOM`, `PER_PRENOM` | PYPERSONNE | 🟡 Conservés → `dim_interimaires.nom/prenom` | ✅ `gld_shared.dim_interimaires` | ✅ Dataset Activité (filtrage OK) |
| `PER_NAISSANCE` | PYPERSONNE | 🟡 Conservé → Silver seulement | ❌ Non | ❌ Non |

**Règle Gold :** aucune donnée `_rgpd_flag=SENSIBLE` ne transite vers PostgreSQL ou Superset.

---

## 10. Anomalies & Watchlist

| ID | Sévérité | Table | Colonne | Description | Contournement |
|---|---|---|---|---|---|
| B-01 | 🟡 WARN | WTEFAC | EFAC_MONTANTHT | Absent du DDL Evolia — NULL en Bronze et Silver | `ca_ht = SUM(LFAC_BASE × LFAC_TAUX)` via WTLFAC |
| B-02 | 🟡 WARN | WTTIESERV | NAF / NAF2008 | Deux colonnes NAF, NAF2008 plus récent mais parfois vide | `COALESCE(NAF2008, NAF)` dans Silver |
| B-03 | ℹ️ INFO | WTCOEF | (toutes) | Table vide (count=0 probe 2026-03-12) | LEFT JOIN sans impact |
| B-04 | ℹ️ INFO | WTRHDON | (volume) | 49.6M lignes total — FALLBACK_SINCE = 2024-01-01 | Delta sur RHD_DATED |
| B-05 | 🟡 WARN | WTEFAC | EFAC_TAUXTVA | Stocké comme numérique brut (ex: 20 = 20%) | `× 0.01` dans Silver si calcul TTC |
| B-06 | ℹ️ INFO | WTUGPINT | UGPINT_DATEMODIF | Absent du DDL — full-load (pas de delta) | Full-load accepté (table petite) |
| B-07 | 🟡 WARN | WTPHAB | PHAB_EXPIR | NULL fréquent → date_expiration calculée via THAB_NBMOIS | `MAKE_INTERVAL(months:=THAB_NBMOIS)` |
| B-08 | ℹ️ INFO | dim_metiers | naf_libelle | NULL (référentiel NAF non extrait d'Evolia) | Enrichissement SIRENE possible |
| B-09 | ℹ️ INFO | dim_clients | effectif_tranche | NULL (WTCLPT.CLPT_EFFT non mappé dans dim_clients Gold) | À mapper si besoin analytique |
