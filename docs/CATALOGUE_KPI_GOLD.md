# Catalogue KPI — Couche Gold GI Data Lakehouse

> **Version 1.0 · 2026-03-26**
> Périmètre : ensemble des indicateurs exposés ou planifiés dans la couche Gold du pipeline GI Data Lakehouse.
> Sources : scripts `gold_*.py`, `docs/ANALYSE_KPI_MANQUANTS.md`, `docs/ANALYSE_KPI_COMPLEMENTAIRES.md`,
> guides Superset (`docs/superset_*_setup.md`).

---

## Légende des statuts

| Statut | Signification |
|--------|--------------|
| ✅ **Implémenté** | Table Gold produite par un script, données disponibles |
| ⚠️ **Partiel** | Table créée mais colonnes clés à NULL ou dépendant d'une Silver/Bronze non encore enrichie |
| 📋 **Planifié** | KPI conçu (SQL + schéma documentés), script non encore développé |
| 💡 **Potentiel** | KPI identifié, données disponibles, pas encore spécifié en détail |

---

## Sommaire

1. [Vue d'ensemble par domaine](#1-vue-densemble-par-domaine)
2. [Domaine Commercial — `gld_commercial`](#2-domaine-commercial--gld_commercial)
3. [Domaine Clients — `gld_clients`](#3-domaine-clients--gld_clients)
4. [Domaine Opérationnel — `gld_operationnel`](#4-domaine-opérationnel--gld_operationnel)
5. [Domaine Performance Agences — `gld_performance`](#5-domaine-performance-agences--gld_performance)
6. [Domaine Staffing — `gld_staffing`](#6-domaine-staffing--gld_staffing)
7. [Dimensions partagées — `gld_shared`](#7-dimensions-partagées--gld_shared)
8. [Récapitulatif des dépendances manquantes](#8-récapitulatif-des-dépendances-manquantes)
9. [Roadmap priorisation](#9-roadmap-priorisation)

---

## 1. Vue d'ensemble par domaine

| Domaine | Tables implémentées | Tables planifiées | KPIs disponibles | KPIs planifiés |
|---------|--------------------:|------------------:|-----------------:|---------------:|
| `gld_commercial` | 2 | 4 | 11 | 24 |
| `gld_clients` | 3 | 0 | 17 | 0 |
| `gld_operationnel` | 5 | 0 | 14 | 0 |
| `gld_performance` | 4 | 3 | 22 | 20 |
| `gld_staffing` | 4 | 2 | 19 | 8 |
| `gld_shared` | 5 | 1 | 0 (dims) | 0 |
| **Total** | **23** | **10** | **83** | **52** |

> Les KPIs « potentiels » non encore documentés ne sont pas comptabilisés.

---

## 2. Domaine Commercial — `gld_commercial`

### 2.1 `fact_ca_mensuel_client` ✅ Implémenté
**Script :** `gold_ca_mensuel.py` | **Grain :** mois × client × agence

| Colonne KPI | Type | Description | Statut |
|-------------|------|-------------|--------|
| `ca_ht` | decimal | CA brut HT (hors avoirs) | ✅ |
| `avoir_ht` | decimal | Total avoirs émis | ✅ |
| `ca_net_ht` | decimal | CA net HT (ca_ht − avoir_ht) — **KPI principal** | ✅ |
| `nb_factures` | int | Nombre de factures émises | ✅ |
| `nb_missions_facturees` | int | Nombre de missions facturées | ✅ |
| `nb_heures_facturees` | float | Heures facturées (via WTLFAC) | ⚠️ NULL — reconstruction via `SUM(lfac_base × lfac_taux)` planifiée |
| `taux_moyen_fact` | float | Taux horaire moyen facturé | ⚠️ NULL — dépend de `nb_heures_facturees` |

---

### 2.2 `fact_concentration_client` ✅ Implémenté
**Script :** `gold_ca_mensuel.py` | **Grain :** mois × agence

| Colonne KPI | Type | Description | Statut |
|-------------|------|-------------|--------|
| `nb_clients` | int | Nombre total de clients actifs | ✅ |
| `nb_clients_top20` | int | Clients représentant 80 % du CA (Pareto) | ✅ |
| `ca_net_total` | decimal | CA net total de l'agence | ✅ |
| `ca_net_top20` | decimal | CA des clients top 20 % | ✅ |
| `taux_concentration` | float | Part des top clients dans le CA total (%) | ✅ |

> **Note :** une version enrichie avec `pct_top1 / pct_top3 / pct_top5 / risque_concentration` est documentée dans `ANALYSE_KPI_MANQUANTS.md §6` — elle peut remplacer cette table ou la compléter.

---

### 2.3 `fact_dso_client` 📋 Planifié
**Script cible :** `gold_recouvrement.py` (à créer) | **Dépendance :** `slv_clients/facturation_detail`
**Référence :** `ANALYSE_KPI_COMPLEMENTAIRES.md §4`

| Colonne KPI | Type | Description |
|-------------|------|-------------|
| `nb_factures` | int | Total factures émises |
| `nb_reglees` | int | Factures réglées |
| `nb_en_retard` | int | Factures non réglées après échéance |
| `dso_moyen_jours` | int | **KPI principal** : délai moyen de règlement (émission → règlement) |
| `dso_median_jours` | int | Médiane DSO (résistante aux gros retardataires) |
| `retard_moyen_jours` | int | Retard moyen sur les factures en retard |
| `niveau_dso` | varchar | BON (≤ 30j) / MOYEN (≤ 45j) / CRITIQUE (> 45j) |

> **Données disponibles :** `WTEFAC` entièrement ingérée dans `bronze_missions.py`.
> **Blocage :** Silver `slv_clients/facturation_detail` à créer dans `silver_clients_detail.py`.

---

### 2.4 `fact_balance_agee` 📋 Planifié
**Script cible :** `gold_recouvrement.py` | **Dépendance :** `slv_clients/facturation_detail`
**Référence :** `ANALYSE_KPI_COMPLEMENTAIRES.md §5`

| Colonne KPI | Type | Description |
|-------------|------|-------------|
| `montant_non_echu` | decimal | Encours non encore échu |
| `montant_0_30j` | decimal | Retard 0-30 jours |
| `montant_30_60j` | decimal | Retard 31-60 jours |
| `montant_60_90j` | decimal | Retard 61-90 jours |
| `montant_plus_90j` | decimal | Retard > 90 jours (litigieux) |
| `encours_total` | decimal | **KPI principal** : total encours non réglés |
| `taux_vetusite_pct` | float | % encours > 60 jours |
| `niveau_risque` | varchar | LITIGIEUX / A_RISQUE / NORMAL |
| `date_snapshot` | date | Date du calcul (snapshot journalier) |

> **Données disponibles :** même source WTEFAC que DSO. Dépend de `fact_dso_client`.

---

### 2.5 `fact_renouvellement_mission` 📋 Planifié
**Script cible :** `gold_qualite_missions.py` | **Dépendance :** `gld_staffing.fact_missions_detail` (disponible)
**Référence :** `ANALYSE_KPI_COMPLEMENTAIRES.md §6`

| Colonne KPI | Type | Description |
|-------------|------|-------------|
| `nb_missions` | int | Total missions |
| `nb_renouvellements` | int | Missions reconduites (même client, écart ≤ 7j) |
| `taux_renouvellement_pct` | float | **KPI principal** (%) |
| `ecart_moyen_jours` | float | Écart moyen entre fin de mission et reprise |

> **Données disponibles immédiatement** — implémentation pure Gold.

---

### 2.6 `fact_ca_secteur_naf` 📋 Planifié
**Script cible :** enrichissement `gold_ca_mensuel.py` | **Dépendance :** `gld_shared.ref_naf_sections` (seed statique à créer)
**Référence :** `ANALYSE_KPI_COMPLEMENTAIRES.md §9`

| Colonne KPI | Type | Description |
|-------------|------|-------------|
| `naf_code` | varchar | Code NAF complet (ex : 7820Z) |
| `naf_section` | varchar | Section NAF (lettre A-U) |
| `secteur_libelle` | varchar | Libellé secteur (ex : Industrie manufacturière) |
| `nb_clients` | int | Clients actifs dans ce secteur |
| `ca_net_ht` | decimal | **KPI principal** : CA net du secteur |
| `nb_missions` | int | Missions facturées |
| `heures_facturees` | float | Heures facturées |
| `part_ca_agence_pct` | float | Part du secteur dans le CA total agence (%) |

> **Dépendance unique :** table statique `gld_shared.ref_naf_sections` (seed `scripts/seed_ref_naf.py` à créer).

---

## 3. Domaine Clients — `gld_clients`

### 3.1 `vue_360_client` ✅ Implémenté
**Script :** `gold_vue360_client.py` | **Grain :** 1 ligne par client (vue snapshot)

| Colonne KPI | Type | Description | Statut |
|-------------|------|-------------|--------|
| `ca_ytd` | decimal | CA net HT depuis le 1er janvier de l'année en cours | ✅ |
| `ca_n1` | decimal | CA net HT sur l'année N-1 (référence) | ✅ |
| `delta_ca_pct` | float | Variation CA YTD vs même période N-1 (%) | ✅ |
| `ca_12_mois_glissants` | decimal | CA net HT sur les 12 derniers mois | ✅ |
| `nb_missions_actives` | int | Missions en cours (contrats ouverts) | ✅ |
| `nb_missions_total` | int | Total missions historiques | ✅ |
| `nb_int_actifs` | int | Intérimaires en mission sur le mois courant | ✅ |
| `nb_int_historique` | int | Total intérimaires ayant travaillé chez ce client | ✅ |
| `anciennete_jours` | int | Ancienneté de la relation client (jours depuis 1re mission) | ✅ |
| `marge_moyenne_pct` | float | Taux de marge moyen sur les missions récentes | ✅ |
| `montant_encours` | decimal | Encours factures non réglées | ✅ |
| `limite_credit` | decimal | Plafond de crédit accordé | ✅ |
| `risque_credit_score` | varchar | Niveau de risque crédit : LOW / MEDIUM / HIGH | ✅ |
| `nb_agences_partenaires` | int | Nombre d'agences GI ayant travaillé pour ce client | ✅ |
| `jours_depuis_derniere` | int | Nombre de jours depuis la dernière mission | ✅ |
| `risque_churn` | varchar | Risque de désengagement : LOW / MEDIUM / HIGH | ✅ |

---

### 3.2 `fact_retention_client` ✅ Implémenté
**Script :** `gold_retention_client.py` | **Grain :** trimestre × client

| Colonne KPI | Type | Description | Statut |
|-------------|------|-------------|--------|
| `ca_net` | decimal | CA net HT du trimestre | ✅ |
| `delta_ca_qoq` | decimal | Variation CA vs trimestre précédent (€) | ✅ |
| `delta_ca_qoq_pct` | float | Variation CA QoQ (%) | ✅ |
| `delta_ca_yoy` | decimal | Variation CA vs même trimestre N-1 (€) | ✅ |
| `delta_ca_yoy_pct` | float | Variation CA YoY (%) | ✅ |
| `nb_missions` | int | Nombre de missions du trimestre | ✅ |
| `nb_factures` | int | Nombre de factures émises | ✅ |
| `frequence_4_trimestres` | int | Présence du client sur les 4 derniers trimestres (0-4) | ✅ |
| `jours_inactivite` | int | Jours depuis la dernière mission | ✅ |
| `risque_churn` | varchar | Évaluation du risque de départ | ✅ |
| `churn_score_ml` | float | Score ML de probabilité de churn (0-1) | ✅ |

---

### 3.3 `fact_rentabilite_client` ✅ Implémenté
**Script :** `gold_retention_client.py` | **Grain :** trimestre × client

| Colonne KPI | Type | Description | Statut |
|-------------|------|-------------|--------|
| `ca_net` | decimal | CA net facturable du trimestre | ✅ |
| `ca_missions` | decimal | CA calculé depuis les lignes de mission | ✅ |
| `cout_paye` | decimal | Coût total salarial payé aux intérimaires | ✅ |
| `marge_brute` | decimal | ca_net − cout_paye | ✅ |
| `taux_marge` | float | **KPI principal** : marge brute / CA net (%) | ✅ |
| `cout_gestion_estime` | decimal | Estimation des coûts de gestion (forfait) | ✅ |
| `rentabilite_nette` | decimal | marge_brute − cout_gestion_estime | ✅ |
| `taux_rentabilite_nette` | float | rentabilite_nette / CA net (%) | ✅ |
| `nb_interimaires` | int | Nombre d'intérimaires distincts sur la période | ✅ |

---

## 4. Domaine Opérationnel — `gld_operationnel`

### 4.1 `fact_etp_hebdo` ✅ Implémenté
**Script :** `gold_etp.py` | **Grain :** semaine × agence

| Colonne KPI | Type | Description | Statut |
|-------------|------|-------------|--------|
| `nb_releves` | int | Nombre de relevés d'heures de la semaine | ✅ |
| `nb_interimaires` | int | Intérimaires distincts avec heures sur la semaine | ✅ |
| `heures_totales` | float | Total heures payées | ✅ |
| `etp` | float | **KPI principal** : ETP = Σ(heures payées) / 35 | ✅ |

> **Formule sectorielle :** ETP hebdomadaire = Σ heures payées / 35 | ETP mensuel = Σ heures / 151,67

---

### 4.2 `fact_heures_hebdo` ✅ Implémenté
**Script :** `gold_operationnel.py` | **Grain :** semaine × agence

| Colonne KPI | Type | Description | Statut |
|-------------|------|-------------|--------|
| `heures_paye` | float | Heures payées aux intérimaires | ✅ |
| `heures_fact` | float | Heures facturées aux clients | ✅ |
| `nb_releves` | int | Nombre de relevés traités | ✅ |

---

### 4.3 `fact_commandes_pipeline` ✅ Implémenté
**Script :** `gold_operationnel.py` | **Grain :** mois × agence

| Colonne KPI | Type | Description | Statut |
|-------------|------|-------------|--------|
| `nb_commandes` | int | Total commandes reçues | ✅ |
| `nb_pourvues` | int | Commandes satisfaites (placement effectué) | ✅ |
| `nb_ouvertes` | int | Commandes en attente de placement | ✅ |
| `taux_satisfaction` | float | **KPI principal** : nb_pourvues / nb_commandes (%) | ✅ |

---

### 4.4 `fact_delai_placement` ⚠️ Partiel
**Script :** `gold_operationnel.py` | **Grain :** mois × agence
**Dépendance Silver non confirmée :** `categorie_delai` et `delai_placement_heures` dans `slv_missions`

| Colonne KPI | Type | Description | Statut |
|-------------|------|-------------|--------|
| `nb_missions` | int | Missions avec commande associée | ⚠️ |
| `delai_moyen_heures` | float | **KPI principal** : délai moyen entre commande et placement (h) | ⚠️ |
| `delai_median_heures` | float | Médiane du délai (moins sensible aux outliers) | ⚠️ |
| `pct_moins_4h` | float | % de placements en < 4h (réactivité excellente) | 📋 À compléter |
| `pct_meme_jour` | float | % de placements le jour même | 📋 À compléter |
| `pct_plus_3j` | float | % de délai > 3 jours (alerte opérationnelle) | 📋 À compléter |

> **Blocage :** Silver `silver_missions.py` doit être enrichi avec `delai_placement_heures` (JOIN `WTCMD.CMD_DTE` → `WTMISS.CNTI_CREATE`). `CAST(CMD_ID AS INT)` requis (anomalie DDL A-01).

---

### 4.5 `fact_conformite_dpae` ⚠️ Partiel
**Script :** `gold_operationnel.py` | **Grain :** mois × agence
**Dépendance Bronze non confirmée :** `MISS_FLAGDPAE` dans `bronze_interimaires.py`

| Colonne KPI | Type | Description | Statut |
|-------------|------|-------------|--------|
| `nb_missions` | int | Total missions du mois | ⚠️ |
| `nb_dpae_transmises` | int | DPAE envoyées (MISS_FLAGDPAE NOT NULL) | ⚠️ |
| `nb_dpae_manquantes` | int | DPAE absentes (NULL) | ⚠️ |
| `taux_conformite_dpae` | float | **KPI légal** : % DPAE envoyées avant début mission | ⚠️ |
| `ecart_moyen_heures` | float | Retard moyen en heures (missions en retard) | ⚠️ |
| `nb_dpae_retard` | int | DPAE envoyées après le début de mission | 📋 À compléter |

> ⚠️ **Indicateur légal critique.** Taux < 100 % = risque d'amende URSSAF.
> **Blocage Bronze :** `MISS_FLAGDPAE` (datetime2, date de transmission), `MISS_DPAE`, `MISS_NDPAE` à ajouter dans `bronze_interimaires.py`.

---

### 4.6 `fact_echus_hebdo` ✅ Implémenté
**Script :** `gold_operationnel.py` | **Grain :** semaine × agence

| Colonne KPI | Type | Description | Statut |
|-------------|------|-------------|--------|
| `montant_factures` | decimal | Total factures émises sur la semaine | ✅ |
| `montant_avoirs` | decimal | Total avoirs émis | ✅ |
| `montant_echu_ht` | decimal | **KPI principal** : encours échu non réglé | ✅ |
| `nb_factures` | int | Nombre de factures concernées | ✅ |

---

## 5. Domaine Performance Agences — `gld_performance`

### 5.1 `scorecard_agence` ✅ Implémenté
**Script :** `gold_scorecard_agence.py` | **Grain :** mois × agence

| Colonne KPI | Type | Description | Statut |
|-------------|------|-------------|--------|
| `ca_net_ht` | decimal | CA net HT du mois | ✅ |
| `marge_brute` | decimal | Marge brute (CA − coût salarial) | ✅ |
| `taux_marge` | float | **KPI central** : taux de marge brute (%) | ✅ |
| `nb_clients_actifs` | int | Clients avec au moins 1 mission dans le mois | ✅ |
| `nb_int_actifs` | int | Intérimaires en mission sur le mois | ✅ |
| `nb_missions` | int | Nombre total de missions | ✅ |
| `nb_commandes` | int | Commandes reçues | ✅ |
| `nb_pourvues` | int | Commandes satisfaites | ✅ |
| `taux_transformation` | float | Commandes transformées en mission (%) | ✅ |

---

### 5.2 `ranking_agences` ✅ Implémenté
**Script :** `gold_scorecard_agence.py` | **Grain :** mois × agence (classement)

| Colonne KPI | Type | Description | Statut |
|-------------|------|-------------|--------|
| `rang_ca` | int | Rang de l'agence par CA net | ✅ |
| `rang_marge` | int | Rang par taux de marge | ✅ |
| `rang_placement` | int | Rang par nombre d'intérimaires actifs | ✅ |
| `rang_transfo` | int | Rang par taux de transformation | ✅ |
| `score_global` | float | **Score composite 0-1** (moyenne des rangs normalisés) | ✅ |

---

### 5.3 `tendances_agence` ✅ Implémenté
**Script :** `gold_scorecard_agence.py` | **Grain :** mois × agence (variation temporelle)

| Colonne KPI | Type | Description | Statut |
|-------------|------|-------------|--------|
| `delta_ca_mom` | float | Variation CA mois/mois précédent (%) | ✅ |
| `delta_marge_mom` | float | Variation taux de marge M/M-1 (pts) | ✅ |
| `delta_int_mom` | float | Variation nb intérimaires actifs M/M-1 (%) | ✅ |
| `delta_ca_yoy` | float | Variation CA année/année précédente (%) | ✅ |
| `delta_marge_yoy` | float | Variation marge N/N-1 (pts) | ✅ |
| `tendance` | varchar | Tendance globale : HAUSSE / STABLE / BAISSE | ✅ |

---

### 5.4 `fact_rupture_contrat` ⚠️ Partiel
**Script :** `gold_qualite_missions.py` | **Grain :** mois × agence × client
**Dépendance Bronze :** `MISS_ANNULE` à ajouter + `WTFINMISS` à ingérer dans `bronze_missions.py`

| Colonne KPI | Type | Description | Statut |
|-------------|------|-------------|--------|
| `nb_missions_total` | int | Total missions clôturées | ⚠️ |
| `nb_ruptures` | int | Missions rompues avant terme | ⚠️ |
| `nb_annulations` | int | Missions annulées | ⚠️ |
| `nb_terme_normal` | int | Fins naturelles à échéance | ⚠️ |
| `taux_rupture_pct` | float | **KPI principal** : taux de rupture (%) | ⚠️ |
| `taux_fin_anticipee_pct` | float | Taux ruptures + annulations (%) | ⚠️ |
| `duree_moy_avant_rupture` | float | Durée moyenne des missions avant rupture (jours) | ⚠️ |

> **Blocage Bronze :** ajouter `MISS_ANNULE` dans `_COLS["WTMISS"]` + ingérer `WTFINMISS` (référentiel ~20 lignes).
> **Blocage Silver :** créer `slv_missions/fin_mission` avec `statut_fin_mission` dans `silver_missions.py`.

---

### 5.5 `fact_duree_mission` 📋 Planifié
**Script cible :** `gold_qualite_missions.py` | **Dépendance :** `gld_staffing.fact_missions_detail` (disponible)
**Référence :** `ANALYSE_KPI_COMPLEMENTAIRES.md §2`

| Colonne KPI | Type | Description |
|-------------|------|-------------|
| `nb_missions` | int | Nombre de missions |
| `dmm_jours` | float | **KPI principal** : Durée Moyenne des Missions (jours) |
| `dmm_semaines` | float | DMM en semaines |
| `duree_mediane_jours` | float | Médiane de durée (résistante aux outliers) |
| `nb_missions_1j` | int | Missions ≤ 1 jour (micro-missions) |
| `nb_missions_1semaine` | int | Missions ≤ 7 jours |
| `nb_missions_1mois` | int | Missions ≤ 30 jours |
| `nb_missions_long` | int | Missions > 30 jours |
| `profil_duree` | varchar | MICRO / COURTE / MOYENNE / LONGUE |

> **Données disponibles immédiatement** — `duree_jours` calculé dans `fact_missions_detail`.

---

### 5.6 `fact_coeff_facturation` 📋 Planifié
**Script cible :** `gold_qualite_missions.py` | **Dépendance :** `gld_staffing.fact_missions_detail` (disponible)
**Référence :** `ANALYSE_KPI_COMPLEMENTAIRES.md §8`

| Colonne KPI | Type | Description |
|-------------|------|-------------|
| `nb_missions` | int | Missions avec taux horaire non nul |
| `coeff_moyen_pondere` | float | **KPI principal** : coeff pondéré par les heures (THF/THP) |
| `coeff_moyen_simple` | float | Moyenne arithmétique des coefficients individuels |
| `thf_moyen` | float | Taux horaire facturé moyen |
| `thp_moyen` | float | Taux horaire payé moyen |
| `marge_coeff_pct` | float | Marge en % : (coeff − 1) × 100 |
| `niveau_coeff` | varchar | BON (≥ 1.35) / MOYEN (≥ 1.25) / A_REVOIR (< 1.25) |

> **Données disponibles immédiatement** — `taux_horaire_paye` et `taux_horaire_fact` dans `fact_missions_detail`.

---

### 5.7 `fact_delai_placement` (détaillé) 📋 Planifié (enrichissement)
Voir §4.4 — colonnes de distribution `pct_moins_4h`, `pct_meme_jour`, `pct_plus_3j` à compléter
une fois la Silver enrichie.

---

## 6. Domaine Staffing — `gld_staffing`

### 6.1 `fact_activite_int` ✅ Implémenté
**Script :** `gold_staffing.py` | **Grain :** mois × intérimaire

| Colonne KPI | Type | Description | Statut |
|-------------|------|-------------|--------|
| `nb_missions` | int | Nombre de missions sur la période | ✅ |
| `nb_agences` | int | Nombre d'agences différentes travaillées | ✅ |
| `nb_clients` | int | Nombre de clients différents | ✅ |
| `heures_travaillees` | float | Total heures effectuées | ✅ |
| `heures_disponibles` | float | Heures théoriquement disponibles | ✅ |
| `taux_occupation` | float | **KPI principal** : heures travaillées / disponibles (%) | ✅ |
| `ca_genere` | decimal | CA généré par l'intérimaire | ✅ |

---

### 6.2 `fact_missions_detail` ✅ Implémenté
**Script :** `gold_staffing.py` | **Grain :** 1 ligne par mission

| Colonne KPI | Type | Description | Statut |
|-------------|------|-------------|--------|
| `duree_jours` | int | Durée de la mission (jours) | ✅ |
| `taux_horaire_paye` | float | Taux horaire payé à l'intérimaire | ✅ |
| `taux_horaire_fact` | float | Taux horaire facturé au client | ✅ |
| `marge_horaire` | float | taux_fact − taux_paye | ✅ |
| `heures_totales` | float | Heures totales de la mission | ✅ |
| `ca_mission` | decimal | CA HT de la mission | ✅ |
| `cout_mission` | decimal | Coût salarial de la mission | ✅ |
| `marge_mission` | decimal | Marge brute de la mission | ✅ |
| `taux_marge` | float | Marge / CA (%) | ✅ |

---

### 6.3 `fact_fidelisation_interimaires` ✅ Implémenté (version initiale)
**Script :** `gold_staffing.py` | **Grain :** mois × agence

| Colonne KPI | Type | Description | Statut |
|-------------|------|-------------|--------|
| `nb_interimaires` | int | Intérimaires du vivier | ✅ |
| `anciennete_moy_jours` | int | Ancienneté moyenne dans le vivier (jours) | ✅ |
| `jours_inactivite_moyen` | int | Inactivité moyenne depuis la dernière mission | ✅ |
| `nb_actifs_debut` | int | Intérimaires actifs en début de période | 📋 À compléter |
| `nb_actifs_fin` | int | Intérimaires actifs en fin de période | 📋 À compléter |
| `nb_perdus` | int | Intérimaires devenus inactifs | 📋 À compléter |
| `nb_nouveaux` | int | Nouveaux intérimaires actifs | 📋 À compléter |
| `taux_fidelisation_pct` | float | **KPI principal** : % intérimaires retenus (N ∩ N-1 / N-1) | 📋 À compléter |

> **Dépendance Bronze :** `PINT_PREVENDTE`, `PINT_DERVENDTE` à ajouter dans `bronze_interimaires.py` (WTPINT).
> **Référence :** `ANALYSE_KPI_MANQUANTS.md §5`.

---

### 6.4 `fact_competences_dispo` ✅ Implémenté
**Script :** `gold_competences.py` | **Grain :** métier × agence

| Colonne KPI | Type | Description | Statut |
|-------------|------|-------------|--------|
| `nb_qualifies` | int | Intérimaires qualifiés pour ce métier | ✅ |
| `nb_disponibles` | int | Qualifiés non actuellement en mission | ✅ |
| `nb_en_mission` | int | Qualifiés actuellement placés | ✅ |
| `taux_couverture` | float | **KPI principal** : nb_disponibles / nb_qualifies (%) | ✅ |

---

### 6.5 `fact_dynamique_vivier` 📋 Planifié
**Script cible :** enrichissement `gold_staffing.py` | **Dépendance :** `fact_fidelisation_interimaires` (disponible)
**Référence :** `ANALYSE_KPI_COMPLEMENTAIRES.md §7`

| Colonne KPI | Type | Description |
|-------------|------|-------------|
| `nb_nouveaux` | int | Nouveaux intérimaires inscrits/actifs sur le mois |
| `nb_perdus` | int | Intérimaires passés inactifs |
| `croissance_nette` | int | **KPI principal** : nb_nouveaux − nb_perdus |
| `pool_actif` | int | Pool actif courant |
| `pool_total` | int | Pool total (actifs + dormants) |
| `taux_renouvellement_vivier_pct` | float | % de turn-over du vivier |

> **Données disponibles** — dépend uniquement de `fact_fidelisation_interimaires` (table Gold existante).

---

### 6.6 `fact_fidelisation_interimaires` (enrichie) 📋 Planifié
Voir §6.3 — colonnes `taux_fidelisation_pct`, `nb_actifs_debut/fin`, `nb_perdus`, `nb_nouveaux` à compléter une fois `PINT_DERVENDTE` ajouté en Bronze.

---

## 7. Dimensions partagées — `gld_shared`

### Dimensions implémentées

| Table | Grain | Colonnes clés | Statut |
|-------|-------|---------------|--------|
| `dim_calendrier` | 1 ligne/jour | jour_semaine, semaine_iso, mois, trimestre, annee, is_jour_ouvre, is_jour_ferie | ✅ |
| `dim_agences` | 1 ligne/agence | nom_agence, marque, branche, secteur, perimetre, zone_geo, ville, is_active | ✅ |
| `dim_clients` | 1 ligne/client | raison_sociale, siren, siret, naf_code, naf_libelle, ville, statut_client, effectif_tranche | ✅ |
| `dim_interimaires` | 1 ligne/intérimaire | matricule, nom, prenom, ville, date_entree, is_actif, is_candidat, agence_rattachement | ✅ |
| `dim_metiers` | 1 ligne/métier | code_metier, libelle_metier, qualification, specialite, niveau, pcs_code | ✅ |

### Référentiel à créer

| Table | Usage | Statut |
|-------|-------|--------|
| `ref_naf_sections` | Libellés NAF (21 sections A-U) — prérequis de `fact_ca_secteur_naf` | 📋 Seed statique (`seed_ref_naf.py`) |

> **Note :** `dim_clients.naf_libelle` est actuellement NULL — sera renseigné par jointure sur `ref_naf_sections`.

---

## 8. Récapitulatif des dépendances manquantes

Les tables planifiées ou partiellement implémentées nécessitent les actions suivantes par couche.

### Bronze — actions requises (`bronze_interimaires.py`)

| Priorité | Table source | Colonnes à ajouter | KPIs débloqués |
|----------|-------------|---------------------|----------------|
| 🔴 Haute | `WTMISS` | `MISS_FLAGDPAE`, `MISS_DPAE`, `MISS_NDPAE` | `fact_conformite_dpae` |
| 🔴 Haute | `WTPINT` | `PINT_PREVENDTE`, `PINT_DERVENDTE`, `PINT_MODIFDATE`, `PINT_CREATDTE` | `fact_fidelisation_interimaires` (complet) |

### Bronze — actions requises (`bronze_missions.py`)

| Priorité | Table source | Action | KPIs débloqués |
|----------|-------------|--------|----------------|
| 🔴 Haute | `WTMISS` | Ajouter `MISS_ANNULE` (1 colonne) | `fact_rupture_contrat` |
| 🔴 Haute | `WTFINMISS` | Ingérer le référentiel (~20 lignes, `TABLES_FULL`) | `fact_rupture_contrat` |

### Silver — enrichissements requis

| Priorité | Script | Action | KPIs débloqués |
|----------|--------|--------|----------------|
| 🔴 Haute | `silver_missions.py` | Ajouter `delai_placement_heures` (JOIN WTCMD via `CAST(CMD_ID AS INT)`) | `fact_delai_placement` (complet) |
| 🔴 Haute | `silver_missions.py` | Créer bloc `slv_missions/fin_mission` avec `statut_fin_mission` | `fact_rupture_contrat` |
| 🔴 Haute | `silver_missions.py` | Ajouter `statut_dpae` + `ecart_heures` (depuis `MISS_FLAGDPAE`) | `fact_conformite_dpae` |
| 🟡 Moyenne | `silver_clients_detail.py` | Créer `slv_clients/facturation_detail` (depuis `raw_wtefac`) | `fact_dso_client`, `fact_balance_agee` |

### Gold — nouveaux scripts à créer

| Priorité | Script | Tables Gold produites |
|----------|--------|----------------------|
| 🔴 Haute | `gold_qualite_missions.py` (enrichir) | `fact_rupture_contrat` (complet), `fact_duree_mission`, `fact_coeff_facturation`, `fact_renouvellement_mission` |
| 🟡 Moyenne | `gold_recouvrement.py` (NOUVEAU) | `fact_dso_client`, `fact_balance_agee` |
| 🟡 Moyenne | `gold_staffing.py` (enrichir) | `fact_dynamique_vivier`, `fact_fidelisation_interimaires` (complet) |
| 🟡 Moyenne | `gold_ca_mensuel.py` (enrichir) | `fact_ca_secteur_naf` |
| 🟢 Basse | `scripts/seed_ref_naf.py` (NOUVEAU) | `gld_shared.ref_naf_sections` |

---

## 9. Roadmap priorisation

### Phase 1 — Compléter les KPIs partiels existants (effort faible, impact fort)

```
Priorité : conformité légale + opérationnel quotidien

1. Bronze  → bronze_interimaires.py : MISS_FLAGDPAE + PINT_DERVENDTE     [< 30 min]
2. Bronze  → bronze_missions.py : MISS_ANNULE + ingestion WTFINMISS      [< 30 min]
3. Silver  → silver_missions.py : enrichir delai_placement + statut_dpae + fin_mission
4. Gold    → gold_operationnel.py : valider fact_delai_placement + fact_conformite_dpae
5. Gold    → gold_qualite_missions.py : compléter fact_rupture_contrat
```

### Phase 2 — Nouveaux KPIs sans modification Bronze (effort faible, données disponibles)

```
Données déjà présentes en Gold → agrégations pures

6. Gold  → gold_qualite_missions.py : fact_duree_mission (DMM)
7. Gold  → gold_qualite_missions.py : fact_coeff_facturation
8. Gold  → gold_qualite_missions.py : fact_renouvellement_mission
9. Gold  → gold_staffing.py : fact_dynamique_vivier
10. Gold  → gold_ca_mensuel.py : fact_ca_secteur_naf + seed_ref_naf.py
```

### Phase 3 — Recouvrement & DSO (effort moyen, Silver à créer)

```
Requiert Silver facturation_detail

11. Silver → silver_clients_detail.py : slv_clients/facturation_detail
12. Gold   → gold_recouvrement.py (NOUVEAU) : fact_dso_client + fact_balance_agee
```

### Phase 4 — Fidélisation intérimaires (effort moyen)

```
Requiert Bronze PINT + enrichissement Silver

13. Silver → silver_interimaires_detail.py : enrichir avec pint_dervendte
14. Gold   → gold_staffing.py : enrichir fact_fidelisation_interimaires (taux_fidelisation_pct)
```

---

## Annexe — Anomalies DDL à connaître

Ces anomalies DDL Evolia ont un impact direct sur les transformations et doivent être gérées dans les scripts Silver.

| ID | Table Evolia | Colonne | Anomalie | Traitement requis |
|----|-------------|---------|----------|-------------------|
| A-01 | `WTMISS` | `CMD_ID` | Stocké en `float` au lieu de `int` | `CAST(CMD_ID AS INT)` avant JOIN avec WTCMD |
| A-02 | `PYSALARIE` | `SAL_DATESORTIE` | Colonne absente du DDL Evolia | Proxy via `PINT_DERVENDTE` (date dernière vente) |
| A-03 | `WTMISS` | `MISS_FLAGDPAE` | Nommé "flag" mais de type `datetime2` | Utiliser comme date de transmission DPAE (NULL = non envoyée) |
| A-04 | `WTCMD` | `TIE_ID` | Absent — lien client via `CCNT_ID` uniquement | Passer par `WTCNTI.CCNT_ID → TIE_ID` |
| A-05 | `WTPLAC` | `CMD_ID` | Absent — pas de lien direct WTPLAC → WTCMD | Privilégier le délai via `WTMISS.CMD_ID` |

---

*Document généré le 2026-03-26 · Prochaine révision recommandée après Phase 1 de déploiement.*
