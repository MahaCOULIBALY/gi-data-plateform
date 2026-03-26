# DATA MAPPING — GI Data Lakehouse · Evolia → Superset

> **Manifeste v3.1 · Phases 0-4 · Dernière mise à jour : 2026-03-26**
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

```text
EVOLIA SQL Server (on-prem · SRV-SQL2\cegi · DB: INTERACTION)
│
│  pymssql · lecture seule · TDS 7.4
│
├── bronze_agences.py ──────────────────────────────────────────────────────┐
├── bronze_clients.py ──────────────────────────────────────────────────────┤
├── bronze_clients_external.py (SIRENE API + Salesforce) ──────────────────┤
├── bronze_missions.py ─────────────────────────────────────────────────────┤ → S3 Bronze
├── bronze_interimaires.py ─────────────────────────────────────────────────┤   gi-data-prod-bronze
                                                                            │   raw_{table}/YYYY/MM/DD/
                                                                            │   JSON newline-delimited
                                                                            │
                              DuckDB + httpfs · read_json_auto              │
                                                                            ▼
                        ┌── silver_agences_light.py ──────────────────────────────┐
                        ├── silver_clients.py (SCD2) ─────────────────────────────┤
                        ├── silver_clients_detail.py ─────────────────────────────┤ → S3 Silver
                        ├── silver_missions.py ───────────────────────────────────┤   gi-data-prod-silver
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
                        ├── gold_clients_detail.py ─ gld_clients.* ─────────────┤
                        └── gold_qualite_missions.py ─ gld_performance.* ───────┘  ⚠️ hors DAG
                                                                            │
                              SQL direct / REST API                         │
                                                                            ▼
                                              Superset (frdc1dataweb01)
                                              Datasets → Charts → Dashboards
```

**Module partagé Gold :** `gold_helpers.py` — 3 CTE réutilisables :

- `cte_montants_factures` — reconstitution montant HT depuis WTLFAC (contournement B-01)
- `cte_heures_par_contrat` — pré-agrégation heures (per\_id, cnt\_id) pour éviter fan-out
- `cte_missions_distinct` — missions dédupliquées pour JOINs factures (tie\_id, rgpcnt\_id)

---

## 2. Domaine Agences

### 2.1 Mapping Evolia → Bronze

| Table Evolia | S3 Bronze path | Colonnes extraites | Mode | PK / Delta col |
| --- | --- | --- | --- | --- |
| `PYREGROUPECNT` | `raw_pyregroupecnt/…` | RGPCNT\_ID, RGPCNT\_CODE, RGPCNT\_LIBELLE, DOS\_ID, ENT\_ID, DATE\_CLOTURE | FULL | RGPCNT\_ID |
| `PYENTREPRISE` | `raw_pyentreprise/…` | ENT\_ID, ENT\_SIREN, ENT\_RAISON, ENT\_APE, ENT\_ETT, RGPCNT\_ID, ENT\_MONOETAB | FULL | ENT\_ID |
| `PYETABLISSEMENT` | `raw_pyetablissement/…` | ETA\_ID, ENT\_ID, ETA\_ACTIVITE, ETA\_COMMUNE, ETA\_ADR2\_COMP, ETA\_ADR2\_VOIE, ETA\_ADR2\_CP, ETA\_ADR2\_VILLE, ETA\_PSEUDO\_SIRET, ETA\_DATE\_CESACT | FULL | ETA\_ID |
| `WTUG` | `raw_wtug/…` | RGPCNT\_ID, ETA\_ID, UG\_GPS, UG\_CLOTURE\_DATE, UG\_CLOTURE\_USER, PIL\_ID, UG\_EMAIL | FULL | RGPCNT\_ID, ETA\_ID |
| `PYDOSPETA` | `raw_pydospeta/…` | RGPCNT\_ID, ETA\_ID | FULL | RGPCNT\_ID, ETA\_ID |
| `AGENCE_GESTION` | `raw_agence_gestion/…` | DATE\_UG\_GEST, NOM\_COMMERCIAL, ID\_UG, NOM\_UG, CODE\_COMM, MARQUE, BRANCHE, COMMERCIAL\_GESTION, TAUX | DELTA | DATE\_UG\_GEST (depuis 2022-01-01) |
| `Secteurs` (vue Evolia) | `raw_secteurs/…` | [currentTime], [Agence de gestion], [Secteur], [Périmètre], [Zone Géographique] | FULL | — |

### 2.2 Bronze → Silver

| Script | Lire depuis Bronze | Écrire vers Silver | Colonnes Silver |
| --- | --- | --- | --- |
| `silver_agences_light.py` | raw\_pyregroupecnt, raw\_wtug, raw\_agence\_gestion (optionnel) | `slv_agences/dim_agences` | agence\_sk (MD5), rgpcnt\_id, nom, code, marque, branche, nom\_commercial, code\_comm, ville, email, latitude, longitude, is\_cloture, is\_active, cloture\_date, \_loaded\_at |
| `silver_agences_light.py` | raw\_secteurs (optionnel) | `slv_agences/hierarchie_territoriale` | rgpcnt\_id, secteur, perimetre, zone\_geo, \_loaded\_at |

### 2.3 Silver → Gold

| Script | Lire depuis Silver | Table Gold | Colonnes Gold |
| --- | --- | --- | --- |
| `gold_dimensions.py` | slv\_agences/dim\_agences, slv\_agences/hierarchie\_territoriale | `gld_shared.dim_agences` | agence\_sk, rgpcnt\_id, nom\_agence, marque, branche, secteur, perimetre, zone\_geo, ville, is\_active |
| `gold_scorecard_agence.py` | slv\_missions, slv\_facturation, slv\_temps | `gld_performance.scorecard_agence` | agence\_id, mois, ca\_net\_ht, taux\_marge, marge\_brute, nb\_clients\_actifs, nb\_int\_actifs, nb\_missions, taux\_transformation, nb\_commandes, nb\_pourvues |
| `gold_scorecard_agence.py` | idem | `gld_performance.ranking_agence` | agence\_id, mois, ca\_net\_ht, taux\_marge, nb\_int\_actifs, taux\_transformation, rang\_ca, rang\_marge, rang\_placement, rang\_transfo, score\_global (35% CA + 25% marge + 25% placement + 15% transfo) |
| `gold_scorecard_agence.py` | idem | `gld_performance.tendances_agence` | agence\_id, mois, trend\_ca, trend\_marge, trend\_placement\_yoy, trend\_transformation\_qoq |
| `gold_operationnel.py` | slv\_temps/releves\_heures, slv\_missions/commandes | `gld_operationnel.fact_heures_hebdo` | agence\_id, tie\_id, semaine\_debut, heures\_paye, heures\_fact, nb\_releves, \_loaded\_at |
| `gold_operationnel.py` | slv\_missions/commandes | `gld_operationnel.fact_commandes_pipeline` | agence\_id, semaine\_debut, nb\_commandes, nb\_pourvues, nb\_ouvertes, taux\_satisfaction, \_loaded\_at |

---

## 3. Domaine Clients

### 3.1 Mapping Evolia → Bronze

| Table Evolia | S3 Bronze path | Colonnes extraites | Mode | PK |
| --- | --- | --- | --- | --- |
| `CMTIERS` | `raw_cmtiers/…` | TIE\_ID, TIE\_NOM, TIE\_NOMC, TIE\_SIRET, TIE\_CODE | FULL | TIE\_ID |
| `WTTIESERV` | `raw_wttieserv/…` | TIE\_ID, TIES\_SERV, TIES\_DESIGNATION, TIES\_NOM, TIES\_ADR1, TIES\_ADR2, TIES\_CODEP, TIES\_VILLE, PAYS\_CODE, TIES\_SIREN, TIES\_NIC, NAF, NAF2008, TIES\_EMAIL, CLOT\_DAT, TIES\_CDMODIFDATE, RGPCNT\_ID | FULL | TIE\_ID, TIES\_SERV |
| `WTCLPT` | `raw_wtclpt/…` | TIE\_ID, CLPT\_PROS, CLPT\_CAPT, CLPT\_DCREA, CLPT\_EFFT, CLPT\_MODIFDATE | FULL | TIE\_ID |
| `WTTIEINT` | `raw_wttieint/…` | TIE\_ID, TIEI\_ORDRE, TIEI\_NOM, TIEI\_PRENOM, TIEI\_EMAIL, TIEI\_BUREAU, FCTI\_CODE | FULL | TIE\_ID, TIEI\_ORDRE |
| `WTENCOURSG` | `raw_wtencoursg/…` | ENCGRP\_ID, ENC\_SIREN, ENCG\_DECISIONLIB | FULL | ENCGRP\_ID |
| `WTUGCLI` | `raw_wtugcli/…` | RGPCNT\_ID, TIE\_ID, UGCLI\_ORIG | FULL | RGPCNT\_ID, TIE\_ID |

> ⚠️ `WTCOEF` retiré du mapping actif — table vide (count=0, probe 2026-03-12). La fonction `process_coefficients` est commentée dans `silver_clients_detail.py`.

**Sources externes :**

| Source | S3 Bronze path | Colonnes extraites |
| --- | --- | --- |
| SIRENE API | `raw_sirene/…` | siret, raison\_sociale\_officielle, date\_creation, tranche\_effectif, naf, adresse\_siege, \_loaded\_at |
| Salesforce (stub) | `raw_salesforce_accounts/…` | Account\_Id, Name, Industry, SIRET\_\_c, BillingCity, CreatedDate, \_loaded\_at |

### 3.2 Bronze → Silver

| Script | Lire depuis Bronze | Écrire vers Silver | Colonnes Silver |
| --- | --- | --- | --- |
| `silver_clients.py` (SCD2) | raw\_wttieserv, raw\_wtclpt | `slv_clients/dim_clients` | client\_sk (MD5), tie\_id, change\_hash, raison\_sociale, siren, nic, naf\_code, adresse\_complete, ville, code\_postal, statut\_client, ca\_potentiel, date\_creation\_fiche, effectif\_tranche, is\_current, valid\_from, valid\_to, \_source\_raw\_id, \_loaded\_at |
| `silver_clients_detail.py` | raw\_wttieserv | `slv_clients/sites_mission` | site\_id, tie\_id, nom\_site, siren, nic, adresse, ville, code\_postal, pays\_code, siret\_site, email, agence\_id, is\_active, clot\_at, row\_hash, \_loaded\_at |
| `silver_clients_detail.py` | raw\_wttieint | `slv_clients/contacts` 🟡 | contact\_id (MD5), tie\_id, nom, prenom, email, telephone, fonction\_code, \_loaded\_at |
| `silver_clients_detail.py` | raw\_wtencoursg | `slv_clients/encours_credit` | encours\_id, siren, montant\_encours (NULL — DDL absent), limite\_credit (NULL), date\_decision (NULL), decision\_libelle, \_loaded\_at |

### 3.3 Silver → Gold

| Script | Lire depuis Silver | Table Gold | Colonnes Gold |
| --- | --- | --- | --- |
| `gold_dimensions.py` | slv\_clients/dim\_clients | `gld_shared.dim_clients` | client\_sk, tie\_id, raison\_sociale, siren, nic, siret (calculé), naf\_code, naf\_libelle (NULL), ville, code\_postal, statut\_client, effectif\_tranche |
| `gold_clients_detail.py` | slv\_clients, gld\_commercial.fact\_ca\_mensuel\_client | `gld_clients.vue_360_client` | client\_sk, tie\_id, raison\_sociale, siren, ville, secteur\_activite, ca\_ytd, ca\_n1, delta\_ca\_pct, montant\_encours, limite\_credit, risque\_credit\_score |
| `gold_clients_detail.py` | idem | `gld_clients.fact_retention_client` | client\_sk, tie\_id, trimestre, ca\_net, delta\_ca\_qoq, nb\_missions, risque\_churn |
| `gold_vue360_client.py` | slv\_clients, slv\_missions, slv\_facturation, slv\_clients/sites\_mission, slv\_clients/encours\_credit + Gold PG | `gld_clients.vue_360_client` | client\_sk, tie\_id, raison\_sociale, siren, nic, siret, email, ville, secteur\_activite, ca\_ytd, ca\_n1, delta\_ca\_pct, nb\_missions\_actives, nb\_missions\_total, nb\_int\_actifs, nb\_int\_historique, nb\_agences\_partenaires, premiere\_mission, marge\_moyenne\_pct, nb\_sites\_actifs, encours\_credit, limite\_credit, risque\_credit, top\_3\_metiers (JSON), derniere\_facture\_date, \_computed\_at |
| `gold_retention_client.py` | slv\_facturation, slv\_clients/dim\_clients, slv\_missions | `gld_clients.fact_retention_client` | client\_sk, tie\_id, trimestre, ca\_net, delta\_ca\_qoq, delta\_ca\_qoq\_pct, delta\_ca\_yoy, delta\_ca\_yoy\_pct, nb\_missions, nb\_factures, frequence\_4\_trimestres, derniere\_facture, jours\_inactivite, risque\_churn |
| `gold_retention_client.py` | idem | `gld_clients.fact_rentabilite_client` | tie\_id, annee, ca\_annee, cout\_missions\_annee, marge\_brute\_annee, taux\_rentabilite\_annee, top\_3\_metiers, nb\_clients\_concurrents |
| `gold_ca_mensuel.py` | slv\_facturation, slv\_missions, slv\_clients | `gld_commercial.fact_ca_mensuel_client` | client\_sk, tie\_id, mois, ca\_ht (reconstitué via lignes\_factures), avoir\_ht, ca\_net\_ht, nb\_factures, nb\_missions\_facturees, agence\_principale |
| `gold_ca_mensuel.py` | slv\_facturation (Pareto top-20% CA) | `gld_clients.fact_concentration_client` | agence\_id, mois, nb\_clients, nb\_clients\_top20, ca\_net\_total, ca\_net\_top20, taux\_concentration |

---

## 4. Domaine Missions & Facturation

### 4.1 Mapping Evolia → Bronze

| Table Evolia | S3 Bronze path | Colonnes extraites | Mode | Delta col |
| --- | --- | --- | --- | --- |
| `WTMISS` | `raw_wtmiss/…` | PER\_ID, CNT\_ID, TIE\_ID, MISS\_TIEID, TIES\_SERV, MISS\_CODE, MISS\_JUSTIFICATION, MISS\_DPAE, MISS\_ETRANGER, MISS\_QUAL, MISS\_PERFERM, RGPCNT\_ID, MISS\_NDPAE, CNTI\_CREATE, FINMISS\_CODE, MISS\_SAISIE\_DTFIN, MISS\_TRANSDATE, MISS\_MODIFDATE, **MISS\_FLAGDPAE** ⚠️datetime2, MISS\_BTP, MISS\_TYPCOEF, MISS\_LOGIN, CMD\_ID, **MISS\_ANNULE** | DELTA | CNTI\_CREATE |
| `WTCNTI` | `raw_wtcnti/…` | PER\_ID, CNT\_ID, CNTI\_ORDRE, TIE\_ID, MET\_ID, CNTI\_CREATE, CNTI\_DATEFFET, CNTI\_DATEFINCNTI, CNTI\_SOUPDEB, CNTI\_SOUPFIN, CNTI\_HPART, CNTI\_RETCT, CNTI\_RETINT, CNTI\_THPAYE, CNTI\_THFACT, CNTI\_SOUPMODIF, CNTI\_POSTE, LOTFAC\_CODE, CNTI\_SALREF1-4, CNTI\_DESCRIPT1-2, CNTI\_PROTEC1-2, CNTI\_DURHEBDO, PCS\_CODE\_2003 | DELTA | CNTI\_CREATE |
| `WTEFAC` | `raw_wtefac/…` | EFAC\_NUM, EFAC\_LIB, RGPCNT\_ID, TIE\_ID, TIES\_SERV, EFAC\_DTEEDI, EFAC\_DTEECH, EFAC\_TYPF, EFAC\_TYPG, WTE\_EFAC\_NUM, EFAC\_MATR, EFAC\_TRANS, EFAC\_TAUXTVA, EFAC\_DTEREGLF | DELTA | EFAC\_DTEEDI |
| `WTLFAC` | `raw_wtlfac/…` | FAC\_NUM, LFAC\_ORD, LFAC\_LIB, LFAC\_BASE, LFAC\_TAUX, LFAC\_MNT | FULL (filtré via WTEFAC depuis 2024-01-01) | — |
| `WTFACINFO` | `raw_wtfacinfo/…` | CNT\_ID, FAC\_NUM, PER\_ID, TIE\_ID | FULL | — |
| `WTCMD` | `raw_wtcmd/…` | CMD\_ID, RGPCNT\_ID, CMD\_DTE, CMD\_NBSALS, CMD\_CODE, STAT\_CODE, STAT\_TYPE | FULL | — |
| `WTPLAC` | `raw_wtplac/…` | PLAC\_ID, RGPCNT\_ID, TIE\_ID, MET\_ID, PLAC\_DTEEDI | FULL | — |
| `PYCONTRAT` | `raw_pycontrat/…` | PER\_ID, CNT\_ID, ETA\_ID, RGPCNT\_ID, CNT\_DATEDEB, CNT\_DATEFIN, CNT\_FINPREVU, LOTPAYE\_CODE, TYPCOT\_CODE, CNT\_AVT\_ORDRE, CNT\_INI\_ORDRE | DELTA | CNT\_DATEFIN (allow\_null) |
| `WTFINMISS` | `raw_wtfinmiss/…` | FINMISS\_CODE, FINMISS\_LIBELLE, FINMISS\_IFM, FINMISS\_CP, **MTFCNT\_ID** | FULL | — |
| `PYMTFCNT` | `raw_pymtfcnt/…` | MTFCNT\_ID, MTFCNT\_CODE, MTFCNT\_LIBELLE, MTFCNT\_FINCNT, MTFCNT\_DADS | FULL | — |

### 4.2 Bronze → Silver

| Script | Lire depuis Bronze | Écrire vers Silver | Colonnes Silver |
| --- | --- | --- | --- |
| `silver_missions.py` | raw\_wtmiss + raw\_wtcnti + raw\_wtcmd | `slv_missions/missions` | per\_id, cnt\_id, tie\_id, ties\_serv, rgpcnt\_id, date\_debut, date\_fin, motif, code\_fin, prh\_bts, **statut\_dpae** (MISS\_FLAGDPAE datetime2), **ecart\_heures** (DPAE vs début contrat en heures), **delai\_placement\_heures** (CMD\_DTE→CNTI\_DATEFFET), **categorie\_delai** (urgent/court/standard/long), \_batch\_id, \_loaded\_at |
| `silver_missions.py` | raw\_wtcnti | `slv_missions/contrats` | per\_id, cnt\_id, ordre, met\_id, tpci\_code, date\_debut, date\_fin, taux\_paye, taux\_fact, nb\_heures, poste, \_batch\_id, \_loaded\_at |
| `silver_missions.py` | raw\_wtcmd | `slv_missions/commandes` | cmd\_id, rgpcnt\_id, cmd\_date, nb\_sal, stat\_code, stat\_type, \_batch\_id, \_loaded\_at |
| `silver_missions.py` | raw\_wtplac | `slv_missions/placements` | plac\_id, rgpcnt\_id, tie\_id, met\_id, statut, plac\_date, \_batch\_id, \_loaded\_at |
| `silver_missions.py` | raw\_wtmiss + raw\_wtfinmiss + raw\_pymtfcnt | `slv_missions/fin_mission` | per\_id, cnt\_id, rgpcnt\_id, tie\_id, date\_debut, date\_fin\_reelle, date\_fin\_saisie, finmiss\_code, finmiss\_libelle, mtfcnt\_code, mtfcnt\_libelle, mtfcnt\_fincnt, miss\_annule, duree\_reelle\_jours, **statut\_fin\_mission** (ANNULEE/EN\_COURS/TERME\_NORMAL/RUPTURE), \_batch\_id, \_loaded\_at |
| `silver_missions.py` | raw\_pycontrat | `slv_missions/contrats_paie` | per\_id, cnt\_id, eta\_id, rgpcnt\_id, date\_debut, date\_fin, date\_fin\_prevue, lot\_paye\_code, typ\_cotisation\_code, avt\_ordre, ini\_ordre, \_batch\_id, \_loaded\_at |
| `silver_factures.py` | raw\_wtefac | `slv_facturation/factures` | efac\_num, rgpcnt\_id, tie\_id, ties\_serv, type\_facture, date\_facture, date\_echeance, montant\_ht ⚠️NULL, montant\_ttc, taux\_tva, \_batch\_id, \_loaded\_at |
| `silver_factures.py` | raw\_wtlfac | `slv_facturation/lignes_factures` | fac\_num, lfac\_ord, libelle, base, taux, montant (LFAC\_MNT), \_batch\_id, \_loaded\_at |

> ⚠️ **Bug B-01** : `WTEFAC.EFAC_MONTANTHT` absent du DDL Evolia → `montant_ht` NULL en Silver.
> Contournement Gold : `ca_ht = SUM(lfac_base × lfac_taux)` via `lignes_factures` (helper `cte_montants_factures`).

### 4.3 Silver → Gold

| Script | Lire depuis Silver | Table Gold | Colonnes Gold |
| --- | --- | --- | --- |
| `gold_ca_mensuel.py` | slv\_facturation/factures, slv\_facturation/lignes\_factures, slv\_missions/missions, slv\_clients/dim\_clients | `gld_commercial.fact_ca_mensuel_client` | client\_sk, tie\_id, mois, ca\_ht (reconstitué), avoir\_ht, ca\_net\_ht, nb\_factures, nb\_missions\_facturees, agence\_principale |
| `gold_ca_mensuel.py` | slv\_facturation (Pareto top-20%) | `gld_clients.fact_concentration_client` | agence\_id, mois, nb\_clients, nb\_clients\_top20, ca\_net\_total, ca\_net\_top20, taux\_concentration |
| `gold_staffing.py` | slv\_missions, slv\_missions/contrats, slv\_temps, slv\_interimaires | `gld_staffing.fact_missions_detail` | mission\_sk, per\_id, cnt\_id, tie\_id, agence\_id, metier\_id, date\_debut, date\_fin, duree\_jours, taux\_horaire\_paye, taux\_horaire\_fact, marge\_horaire, heures\_totales, ca\_mission, cout\_mission, marge\_mission, taux\_marge |
| `gold_scorecard_agence.py` | slv\_missions, slv\_facturation, slv\_temps | `gld_performance.scorecard_agence` | agence\_id, mois, ca\_net\_ht, taux\_marge, marge\_brute, nb\_clients\_actifs, nb\_int\_actifs, nb\_missions, taux\_transformation, nb\_commandes, nb\_pourvues |
| `gold_operationnel.py` | slv\_missions/missions (statut\_dpae, delai\_placement\_heures, categorie\_delai) | `gld_operationnel.fact_delai_placement` | agence\_id, semaine\_debut, categorie\_delai, nb\_missions, delai\_moyen\_heures, delai\_median\_heures |
| `gold_operationnel.py` | slv\_missions/missions (statut\_dpae, ecart\_heures) | `gld_operationnel.fact_conformite_dpae` | agence\_id, mois, nb\_missions, nb\_dpae\_transmises, nb\_dpae\_manquantes, taux\_conformite\_dpae, ecart\_moyen\_heures |
| `gold_operationnel.py` | slv\_missions/fin\_mission | `gld_operationnel.fact_ruptures_early_term` | agence\_id, semaine\_debut, nb\_ruptures, taux\_rupture\_pct, nb\_annulations, taux\_annulation\_pct |
| `gold_qualite_missions.py` ⚠️ | slv\_missions/fin\_mission + slv\_missions/missions | `gld_performance.fact_rupture_contrat` | agence\_id, tie\_id, mois, nb\_missions\_total, nb\_ruptures, nb\_annulations, nb\_terme\_normal, taux\_rupture\_pct, taux\_fin\_anticipee\_pct, duree\_moy\_avant\_rupture |

> ⚠️ **`gold_qualite_missions.py` n'est pas intégré dans `dag_gi_pipeline.py`** (Phase 4 — à ajouter dans `gold_facts_group`).
> Prérequis : `slv_missions/fin_mission` produit par `silver_missions.py` depuis 2026-03-15.

---

## 5. Domaine Intérimaires & Compétences

### 5.1 Mapping Evolia → Bronze

**Tables personne / dossier :**

| Table Evolia | S3 Bronze path | Colonnes extraites | Mode | RGPD |
| --- | --- | --- | --- | --- |
| `PYPERSONNE` | `raw_pypersonne/…` | PER\_ID, PER\_NOM, PER\_PRENOM, PER\_NAISSANCE, **PER\_NIR** 🔴, NAT\_CODE, PAYS\_CODE, PER\_BISVOIE, PER\_COMPVOIE, PER\_CP, PER\_VILLE, PER\_COMMUNE | FULL | SENSIBLE |
| `PYSALARIE` | `raw_pysalarie/…` | PER\_ID, SAL\_MATRICULE, SAL\_DATEENTREE, SAL\_ACTIF | FULL | PERSONNEL |
| `WTPINT` | `raw_wtpint/…` | PER\_ID, PINT\_CANDIDAT, PINT\_DOSSIER, PINT\_PERMANENT, PINT\_PREVENDTE, PINT\_DERVENDTE, PINT\_MODIFDATE, PINT\_CREATDTE | FULL | PERSONNEL |
| `PYCOORDONNEE` | `raw_pycoordonnee/…` | PER\_ID, TYPTEL\_CODE, **PER\_TEL\_NTEL** 🔴, PER\_TEL\_POSTE | FULL | SENSIBLE |
| `WTPEVAL` | `raw_wtpeval/…` | PER\_ID, PEVAL\_DU, PEVAL\_EVALUATION, PEVAL\_UTL | DELTA | PEVAL\_DU |
| `WTUGPINT` | `raw_wtugpint/…` | PER\_ID, RGPCNT\_ID, AUG\_ORI | FULL | — |

**Tables compétences (liens personne-référentiel) :**

| Table Evolia | S3 Bronze path | Colonnes extraites | Mode |
| --- | --- | --- | --- |
| `WTPMET` | `raw_wtpmet/…` | PER\_ID, PMET\_ORDRE, MET\_ID | FULL |
| `WTPHAB` | `raw_wtphab/…` | PER\_ID, THAB\_ID, PHAB\_DELIVR, PHAB\_EXPIR, PHAB\_ORDRE | FULL |
| `WTPDIP` | `raw_wtpdip/…` | PER\_ID, TDIP\_ID, PDIP\_DATE | FULL |
| `WTEXP` | `raw_wtexp/…` | PER\_ID, EXP\_ORDRE, EXP\_NOM, EXP\_DEBUT, EXP\_FIN, EXP\_INTERNE | FULL |

**Tables référentiels compétences :**

| Table Evolia | S3 Bronze path | Colonnes extraites | Mode | Note |
| --- | --- | --- | --- | --- |
| `WTMET` | `raw_wtmet/…` | MET\_ID, MET\_CODE, MET\_LIBELLE, TQUA\_ID, NIVQ\_ID, SPE\_ID, **PCS\_CODE\_2003**, DFS\_ID, **MET\_DELETE** | FULL | Classification INSEE |
| `WTTHAB` | `raw_wtthab/…` | THAB\_ID, THAB\_CDE, THAB\_LIBELLE, **THAB\_NBMOIS** | FULL | Durée validité standard |
| `WTTDIP` | `raw_wttdip/…` | TDIP\_ID, TDIP\_CODE, TDIP\_LIB, **TDIP\_REF** | FULL | Catégorie diplôme |
| `WTQUA` | `raw_wtqua/…` | TQUA\_ID, TQUA\_CODE, **TQUA\_LIBELLE** | FULL | Libellé type qualification |

### 5.2 Bronze → Silver

| Script | Lire depuis Bronze | Écrire vers Silver | Colonnes Silver | Logique métier |
| --- | --- | --- | --- | --- |
| `silver_interimaires.py` (SCD2) | raw\_pypersonne, raw\_pysalarie, raw\_wtpint | `slv_interimaires/dim_interimaires` | interimaire\_sk, per\_id, change\_hash, matricule, nom, prenom, date\_naissance, **nir\_pseudo** 🟡, nationalite, pays, adresse, ville, code\_postal, date\_entree, is\_actif, is\_candidat, is\_permanent, agence\_rattachement, is\_current, valid\_from, valid\_to, \_source\_raw\_id, \_loaded\_at | NIR → SHA-256+salt |
| `silver_interimaires_detail.py` | raw\_wtpeval | `slv_interimaires/evaluations` | eval\_id, per\_id, date\_eval, note, commentaire, evaluateur\_id, \_loaded\_at | — |
| `silver_interimaires_detail.py` | raw\_pycoordonnee | `slv_interimaires/coordonnees` 🟡 | coord\_id, per\_id, type\_coord, valeur, poste, is\_principal, \_loaded\_at | Silver uniquement — jamais en Gold |
| `silver_interimaires_detail.py` | raw\_wtugpint | `slv_interimaires/portefeuille_agences` | per\_id, rgpcnt\_id, \_loaded\_at | — |
| `silver_interimaires_detail.py` | raw\_wtpint (PINT\_DERVENDTE proxy) | `slv_interimaires/fidelisation` | per\_id, date\_premiere\_vente, date\_avant\_derniere\_vente, date\_derniere\_vente, anciennete\_jours, jours\_depuis\_derniere\_vente, **categorie\_fidelisation** (actif\_recent/actif\_annee/inactif\_long/inactif), \_loaded\_at | SAL\_DATESORTIE absent DDL |
| `silver_competences.py` | raw\_wtpmet+raw\_wtmet, raw\_wtphab+raw\_wtthab, raw\_wtpdip+raw\_wttdip, raw\_wtexp | `slv_interimaires/competences` | competence\_id, per\_id, type\_competence (METIER/HABILITATION/DIPLOME/EXPERIENCE), code, libelle, niveau (TDIP\_REF), date\_obtention, **date\_expiration** (COALESCE PHAB\_EXPIR, PHAB\_DELIVR+THAB\_NBMOIS), **is\_active** (MET\_DELETE+date\_expir), **pcs\_code** (PCS\_CODE\_2003), \_source\_table, \_loaded\_at | Calcul date\_expir théorique |

**Logique date\_expiration habilitations :**

```sql
date_expiration = COALESCE(
  PHAB_EXPIR::DATE,
  PHAB_DELIVR::DATE + MAKE_INTERVAL(months := THAB_NBMOIS)  -- si PHAB_EXPIR absent
)
```

### 5.3 Silver → Gold

| Script | Lire depuis Silver | Table Gold | Colonnes Gold |
| --- | --- | --- | --- |
| `gold_dimensions.py` | slv\_interimaires/dim\_interimaires | `gld_shared.dim_interimaires` | interimaire\_sk, per\_id, matricule, nom, prenom, date\_naissance, nationalite, is\_actif, is\_candidat, is\_permanent, agence\_rattachement |
| `gold_dimensions.py` | raw\_wtmet + raw\_wtqua (Bronze direct) | `gld_shared.dim_metiers` | metier\_sk, met\_id, code\_metier, libelle\_metier, qualification\_code (TQUA\_ID), qualification\_libelle (TQUA\_LIBELLE), niveau (NIVQ\_ID), **pcs\_code** (PCS\_CODE\_2003), **is\_active** (MET\_DELETE=0) |
| `gold_competences.py` | slv\_interimaires/competences, slv\_interimaires/dim\_interimaires, slv\_agences/dim\_agences, slv\_missions/missions | `gld_staffing.fact_competences_dispo` | metier\_sk, agence\_sk, met\_id, rgpcnt\_id, nb\_qualifies, nb\_disponibles, nb\_en\_mission, taux\_couverture, \_computed\_at |
| `gold_staffing.py` | slv\_interimaires/dim\_interimaires, slv\_missions, slv\_temps | `gld_staffing.fact_activite_int` | interimaire\_sk, per\_id, mois, nb\_missions, nb\_agences, nb\_clients, heures\_travaillees, heures\_disponibles, taux\_occupation, ca\_genere |
| `gold_staffing.py` | slv\_missions, slv\_missions/contrats, slv\_temps | `gld_staffing.fact_missions_detail` | mission\_sk, per\_id, cnt\_id, tie\_id, agence\_id, metier\_id, date\_debut, date\_fin, duree\_jours, taux\_horaire\_paye, taux\_horaire\_fact, marge\_horaire, heures\_totales, ca\_mission, cout\_mission, marge\_mission, taux\_marge |
| `gold_staffing.py` | slv\_interimaires/fidelisation, slv\_interimaires/portefeuille\_agences | `gld_staffing.fact_fidelisation` | per\_id, anciennete\_jours, jours\_depuis\_derniere\_vente, categorie (actif\_recent/actif\_annee/inactif), \_loaded\_at |

> **Prérequis `fact_fidelisation`** : nécessite un run `silver_interimaires_detail` après le 2026-03-13 (slv\_interimaires/fidelisation absent des partitions antérieures).

---

## 6. Domaine Temps (Heures)

### 6.1 Mapping Evolia → Bronze

| Table Evolia | S3 Bronze path | Colonnes extraites | Mode | Delta col | Note |
| --- | --- | --- | --- | --- | --- |
| `WTPRH` | `raw_wtprh/…` | PRH\_BTS, PER\_ID, CNT\_ID, TIE\_ID, PRH\_DTEDEBSEM, LOTPAYE\_CODE, CAL\_AN, CAL\_NPERIODE, LOTFAC\_CODE, CALF\_AN, CALF\_NPERIODE, CNTI\_ORDRE, PRH\_DTEFINSEM, PRH\_IFM, PRH\_CP, PRH\_FLAG\_RH, PRH\_MODIFDATE | DELTA | PRH\_MODIFDATE | — |
| `WTRHDON` | `raw_wtrhdon/…` | RINT\_ID, RHD\_LIGNE, RHD\_BASEP, RHD\_TAUXP, RHD\_BASEF, RHD\_TAUXF, PRH\_BTS, FAC\_NUM, BUL\_ID, RHD\_RAPPEL, RHD\_ORIRUB, RHD\_PORTEE, RHD\_LIBRUB, RHD\_EXCLDEP, RHD\_SEUILP, RHD\_SEUILF, RHD\_BASEPROV, RHD\_TAUXPROV, RHD\_DATED, RHD\_DATEF | DELTA | RHD\_DATED | FALLBACK 2024-01-01 (49.6M total rows) |

### 6.2 Bronze → Silver

| Script | Lire depuis Bronze | Écrire vers Silver | Colonnes Silver |
| --- | --- | --- | --- |
| `silver_temps.py` | raw\_wtprh | `slv_temps/releves_heures` | prh\_bts, per\_id, cnt\_id, tie\_id, date\_modif, valide (PRH\_FLAG\_RH), \_batch\_id, \_loaded\_at |
| `silver_temps.py` | raw\_wtrhdon | `slv_temps/heures_detail` (PER\_ID absent — RGPD) | prh\_bts, rhd\_ligne, rubrique, base\_paye, taux\_paye, base\_fact, taux\_fact, libelle, \_batch\_id, \_loaded\_at |

### 6.3 Silver → Gold

| Script | Lire depuis Silver | Table Gold | Colonnes Gold |
| --- | --- | --- | --- |
| `gold_etp.py` | slv\_temps/releves\_heures (valide=true), slv\_temps/heures\_detail | `gld_operationnel.fact_etp_hebdo` | agence\_id, semaine\_debut (DATE\_TRUNC week), nb\_releves, nb\_interimaires, heures\_totales, etp (SUM(base\_paye)/35) |
| `gold_operationnel.py` | slv\_temps/releves\_heures, slv\_temps/heures\_detail | `gld_operationnel.fact_heures_hebdo` | agence\_id, tie\_id, semaine\_debut, heures\_paye, heures\_fact, nb\_releves, \_loaded\_at |
| `gold_ca_mensuel.py` | slv\_temps/heures\_detail | Agrégation dans `fact_ca_mensuel_client` | nb\_missions\_facturees (proxy) |
| `gold_staffing.py` | slv\_temps/releves\_heures, slv\_temps/heures\_detail | `gld_staffing.fact_activite_int` | heures\_travaillees, taux\_occupation |
| `gold_staffing.py` | idem | `gld_staffing.fact_missions_detail` | heures\_totales, ca\_mission, cout\_mission, marge\_mission, taux\_marge |

---

## 7. Dimensions Partagées (Gold)

Toutes produites par `gold_dimensions.py` → schéma `gld_shared` (PostgreSQL frdc1datahub01).

| Dimension | Source principale | Colonnes | Jointure type |
| --- | --- | --- | --- |
| `dim_calendrier` | Génération DuckDB 2020-2035 | date\_id, dow, nom\_jour, semaine\_iso, mois, nom\_mois, trimestre, annee, is\_ouvre, is\_ferie | `ON d.mois = DATE_TRUNC('month', f.mois)` |
| `dim_agences` | Silver slv\_agences | agence\_sk, rgpcnt\_id, nom\_agence, marque, branche, secteur, perimetre, zone\_geo, ville, is\_active | `ON f.agence_sk = d.agence_sk` |
| `dim_clients` | Silver slv\_clients | client\_sk, tie\_id, raison\_sociale, siren, nic, siret, naf\_code, naf\_libelle (NULL), ville, code\_postal, statut\_client, effectif\_tranche | `ON f.client_sk = d.client_sk` |
| `dim_interimaires` | Silver slv\_interimaires | interimaire\_sk, per\_id, matricule, nom, prenom, date\_naissance, nationalite, is\_actif, is\_candidat, is\_permanent, agence\_rattachement | `ON f.interimaire_sk = d.interimaire_sk` |
| `dim_metiers` | Bronze raw\_wtmet + raw\_wtqua | metier\_sk, met\_id, code\_metier, libelle\_metier, qualification\_code, qualification\_libelle, niveau, pcs\_code, is\_active | `ON f.met_id = d.met_id` |

---

## 8. Superset — Datasets & Dashboards

> Superset est déployé sur **frdc1dataweb01.siege.interaction-interim.com**.
> Il se connecte directement au PostgreSQL Gold sur **frdc1datahub01** (connexion `gi_poc_ddi_gold`).

### 8.1 Datasets Superset recommandés

#### Dataset : Scorecard Agences

| Propriété | Valeur |
| --- | --- |
| Schéma PG | `gld_performance` |
| Tables | `scorecard_agence` JOIN `gld_shared.dim_agences` |
| Métriques clés | ca\_net\_ht, taux\_marge, nb\_missions, taux\_transformation, nb\_int\_actifs |
| Dimensions | agence\_id, mois, marque, branche, zone\_geo |
| Filtres suggérés | mois (date range), branche, marque |

#### Dataset : CA Mensuel Client

| Propriété | Valeur |
| --- | --- |
| Schéma PG | `gld_commercial` |
| Tables | `fact_ca_mensuel_client` JOIN `gld_shared.dim_clients` JOIN `gld_shared.dim_agences` |
| Métriques clés | ca\_net\_ht, nb\_factures, nb\_missions\_facturees, agence\_principale |
| Dimensions | mois, tie\_id, raison\_sociale, naf\_code, agence\_principale |
| Filtres suggérés | mois (date range), agence, secteur NAF |

#### Dataset : Pool Compétences Disponibles

| Propriété | Valeur |
| --- | --- |
| Schéma PG | `gld_staffing` |
| Tables | `fact_competences_dispo` JOIN `gld_shared.dim_metiers` JOIN `gld_shared.dim_agences` |
| Métriques clés | nb\_qualifies, nb\_disponibles, nb\_en\_mission, taux\_couverture |
| Dimensions | met\_id, libelle\_metier, pcs\_code, qualification\_libelle, agence\_id, zone\_geo |
| Filtres suggérés | agence, pcs\_code, qualification\_libelle |

#### Dataset : Activité Intérimaires

| Propriété | Valeur |
| --- | --- |
| Schéma PG | `gld_staffing` |
| Tables | `fact_activite_int` JOIN `gld_shared.dim_interimaires` |
| Métriques clés | nb\_missions, heures\_travaillees, taux\_occupation, ca\_genere |
| Dimensions | per\_id, mois, agence\_rattachement, is\_actif |

#### Dataset : Vue 360° Client

| Propriété | Valeur |
| --- | --- |
| Schéma PG | `gld_clients` |
| Tables | `vue_360_client` |
| Métriques clés | ca\_ytd, ca\_n1, delta\_ca\_pct, nb\_missions\_actives, nb\_int\_actifs, risque\_churn, risque\_credit |
| Dimensions | siren, raison\_sociale, naf\_code, ville, secteur\_activite |
| Colonnes calculées Superset | `delta_ca_pct_color` (rouge si <0, vert si >10%) |

#### Dataset : Rétention & Churn Client

| Propriété | Valeur |
| --- | --- |
| Schéma PG | `gld_clients` |
| Tables | `fact_retention_client` JOIN `gld_shared.dim_clients` |
| Métriques clés | ca\_net, delta\_ca\_qoq\_pct, jours\_inactivite, risque\_churn |
| Dimensions | trimestre, risque\_churn |

#### Dataset : Heures Opérationnelles

| Propriété | Valeur |
| --- | --- |
| Schéma PG | `gld_operationnel` |
| Tables | `fact_heures_hebdo` JOIN `gld_shared.dim_agences` JOIN `gld_shared.dim_clients` |
| Métriques clés | heures\_paye, heures\_fact, nb\_releves |
| Dimensions | semaine\_debut, agence\_id, tie\_id |

#### Dataset : Missions Détail

| Propriété | Valeur |
| --- | --- |
| Schéma PG | `gld_staffing` |
| Tables | `fact_missions_detail` JOIN `gld_shared.dim_metiers` JOIN `gld_shared.dim_agences` JOIN `gld_shared.dim_clients` |
| Métriques clés | taux\_marge, marge\_mission, heures\_totales, duree\_jours |
| Dimensions | date\_debut, metier\_id, agence\_id, tie\_id |

#### Dataset : ETP Hebdomadaire

| Propriété | Valeur |
| --- | --- |
| Schéma PG | `gld_operationnel` |
| Tables | `fact_etp_hebdo` JOIN `gld_shared.dim_agences` |
| Métriques clés | etp, heures\_totales, nb\_interimaires, nb\_releves |
| Dimensions | semaine\_debut, agence\_id, marque, branche, zone\_geo |
| Filtres suggérés | semaine (date range), agence, branche |

#### Dataset : Fidélisation Intérimaires

| Propriété | Valeur |
| --- | --- |
| Schéma PG | `gld_staffing` |
| Tables | `fact_fidelisation` JOIN `gld_shared.dim_interimaires` |
| Métriques clés | nb\_interimaires, anciennete\_jours, jours\_depuis\_derniere\_vente |
| Dimensions | per\_id, agence\_rattachement, categorie (actif\_recent/actif\_annee/inactif\_long/inactif) |
| Filtres suggérés | agence, categorie |

#### Dataset : Concentration Client (Pareto)

| Propriété | Valeur |
| --- | --- |
| Schéma PG | `gld_clients` |
| Tables | `fact_concentration_client` JOIN `gld_shared.dim_agences` |
| Métriques clés | taux\_concentration, ca\_net\_top20, ca\_net\_total, nb\_clients\_top20 |
| Dimensions | agence\_id, mois |
| Colonnes calculées Superset | `risque_concentration` (rouge si taux\_concentration > 0.80) |

#### Dataset : Qualité Missions — Ruptures CTT ⚠️ (hors DAG)

| Propriété | Valeur |
| --- | --- |
| Schéma PG | `gld_performance` |
| Tables | `fact_rupture_contrat` JOIN `gld_shared.dim_agences` JOIN `gld_shared.dim_clients` |
| Métriques clés | taux\_rupture\_pct, taux\_fin\_anticipee\_pct, duree\_moy\_avant\_rupture, nb\_ruptures |
| Dimensions | agence\_id, tie\_id, mois |
| Filtres suggérés | mois, agence, client |

### 8.2 Dashboards proposés

| Dashboard | Datasets utilisés | Public cible |
| --- | --- | --- |
| **Tableau de bord Direction** | scorecard\_agence, fact\_ca\_mensuel\_client, ranking\_agence | Direction, DG |
| **Performance Agences** | scorecard\_agence, ranking\_agence, tendances\_agence, fact\_commandes\_pipeline | Directeurs régionaux |
| **Portefeuille Clients** | vue\_360\_client, fact\_retention\_client, fact\_ca\_mensuel\_client | Commerciaux, KAM |
| **Pilotage Staffing** | fact\_competences\_dispo, fact\_activite\_int, fact\_missions\_detail | Recruteurs, chefs d'agence |
| **Opérationnel Hebdo** | fact\_heures\_hebdo, fact\_commandes\_pipeline, fact\_etp\_hebdo | Chefs d'agence, ops |
| **Risques & Recouvrement** | vue\_360\_client (risque\_credit, encours), fact\_retention\_client (churn) | Finance, crédit |
| **Qualité Missions** | fact\_rupture\_contrat, fact\_ruptures\_early\_term, fact\_delai\_placement | DRH, Direction opérationnelle |

---

## 9. Index colonnes RGPD

| Colonne | Table(s) Bronze | Traitement Silver | Présent en Gold | Présent Superset |
| --- | --- | --- | --- | --- |
| `PER_NIR` | PYPERSONNE | 🔴 Pseudonymisé → `nir_pseudo` (SHA-256+salt) | ❌ Non | ❌ Non |
| `PER_TEL_NTEL` | PYCOORDONNEE | 🔴 Conservé dans `slv_interimaires/coordonnees` uniquement | ❌ Non | ❌ Non |
| `PER_NOM`, `PER_PRENOM` | PYPERSONNE | 🟡 Conservés → `dim_interimaires.nom/prenom` | ✅ `gld_shared.dim_interimaires` | ✅ Dataset Activité (filtrage OK) |
| `PER_NAISSANCE` | PYPERSONNE | 🟡 Conservé Silver + dim\_interimaires Gold (sans sélection analytique directe) | ✅ `gld_shared.dim_interimaires` | ❌ Non |
| Contacts clients (nom, email, tel) | WTTIEINT | 🟡 Silver only — `slv_clients/contacts` | ❌ Non | ❌ Non |

**Règle Gold :** aucune donnée `_rgpd_flag=SENSIBLE` ne transite vers PostgreSQL ou Superset.

**Jointure PG possible :** `slv_interimaires/coordonnees` et `slv_clients/contacts` ne sont jamais chargés dans PostgreSQL.

---

## 10. Anomalies & Watchlist

| ID | Sévérité | Script / Table | Colonne | Description | Contournement |
| --- | --- | --- | --- | --- | --- |
| B-01 | 🟡 WARN | WTEFAC | EFAC\_MONTANTHT | Absent du DDL Evolia → NULL en Bronze et Silver | `ca_ht = SUM(LFAC_BASE × LFAC_TAUX)` via WTLFAC (helper `cte_montants_factures`) |
| B-02 | 🟡 WARN | WTTIESERV | NAF / NAF2008 | Deux colonnes NAF, NAF2008 plus récent mais parfois vide | `COALESCE(NAF2008, NAF)` dans Silver |
| B-03 | ℹ️ INFO | WTCOEF | (toutes) | Table vide (count=0 probe 2026-03-12) — `process_coefficients` commenté dans silver\_clients\_detail | LEFT JOIN sans impact, réévaluer si données apparaissent |
| B-04 | ℹ️ INFO | WTRHDON | (volume) | 49.6M lignes total — FALLBACK\_SINCE = 2024-01-01 | Delta sur RHD\_DATED |
| B-05 | 🟡 WARN | WTEFAC | EFAC\_TAUXTVA | Stocké comme numérique brut (ex: 20 = 20%) | `× 0.01` dans Silver si calcul TTC |
| B-06 | ℹ️ INFO | WTUGPINT | UGPINT\_DATEMODIF | Absent du DDL — full-load (pas de delta) | Full-load accepté (table petite) |
| B-07 | 🟡 WARN | WTPHAB | PHAB\_EXPIR | NULL fréquent → date\_expiration calculée via THAB\_NBMOIS | `MAKE_INTERVAL(months:=THAB_NBMOIS)` |
| B-08 | ℹ️ INFO | dim\_metiers | naf\_libelle | NULL (référentiel NAF non extrait d'Evolia) | Enrichissement SIRENE possible |
| B-09 | ℹ️ INFO | dim\_clients | effectif\_tranche | WTCLPT.CLPT\_EFFT mappé en Silver, non exploité dans Gold actuellement | À connecter si besoin analytique |
| B-10 | 🔴 CRIT | gold\_qualite\_missions.py | — | Script Phase 4 existant (produit `gld_performance.fact_rupture_contrat`) absent du DAG Airflow | Ajouter dans `gold_facts_group` de `dag_gi_pipeline.py` |
| B-11 | ℹ️ INFO | silver\_temps | releves\_heures.tie\_id | Jointure tie\_id via releves\_heures à valider par PROBE (G-OP-M03) — fallback JOIN missions | PROBE avant activation `fact_heures_hebdo` avec tie\_id |
