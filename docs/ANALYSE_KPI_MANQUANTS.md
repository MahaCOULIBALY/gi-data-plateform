# Analyse — KPI Manquants : Sources Evolia & Plan d'Implémentation

> **Version 1.0 · 2026-03-12**
> Périmètre : 5 indicateurs prioritaires identifiés comme manquants dans le pipeline GI Data Lakehouse Phase 2.
> Objectif : valider la disponibilité des données sources, identifier les transformations requises
> et établir un plan d'implémentation par couche (Bronze → Silver → Gold).

---

## Sommaire

1. [Synthèse exécutive](#1-synthèse-exécutive)
2. [ETP — Équivalents Temps Plein](#2-etp--équivalents-temps-plein)
3. [Délai de placement](#3-délai-de-placement)
4. [DPAE en retard](#4-dpae-en-retard)
5. [Taux de fidélisation intérimaires](#5-taux-de-fidélisation-intérimaires)
6. [Concentration client](#6-concentration-client)
7. [Roadmap d'implémentation](#7-roadmap-dimplémentation)
8. [Anomalies DDL détectées](#8-anomalies-ddl-détectées)

---

## 1. Synthèse exécutive

| Indicateur | Priorité | Données disponibles ? | Nouvelles tables Bronze ? | Effort estimé |
| --- | --- | --- | --- | --- |
| ETP (normalisé 35h) | 🔴 Haute | ✅ Déjà en Silver | ❌ Aucune | Faible — calcul Gold uniquement |
| Délai de placement | 🔴 Haute | ✅ Partiellement en Bronze | ❌ Aucune | Moyen — enrichir Bronze WTMISS + logique Silver |
| DPAE en retard | 🔴 Haute | ⚠️ Partiellement (MISS_FLAGDPAE absent du Bronze actuel) | ❌ Aucune | Faible — ajouter 2 colonnes dans bronze_interimaires |
| Fidélisation intérimaires | 🟡 Moyenne | ⚠️ Partielle — SAL_DATESORTIE absent du DDL Evolia | ❌ Aucune | Moyen — proxy par historique missions |
| Concentration client | 🟡 Moyenne | ✅ Déjà en Gold | ❌ Aucune | Faible — agrégation Gold uniquement |

**Conclusion principale :** aucun des 5 indicateurs ne nécessite l'ajout de nouvelles tables Evolia.
Tous les besoins sont couverts par les données déjà ingérées ou par des colonnes manquantes
dans les extractions Bronze existantes.

---

## 2. ETP — Équivalents Temps Plein

### Définition métier

> L'ETP (Équivalent Temps Plein) est la mesure de référence sectorielle dans l'intérim.
> Il représente la somme des heures travaillées ramenée à 35h/semaine, permettant
> de comparer l'activité entre agences, périodes et secteurs indépendamment du volume de contrats.

```
ETP hebdomadaire = Σ(heures payées) / 35
ETP mensuel      = Σ(heures payées sur le mois) / 151,67   (35h × 52/12)
```

### Analyse DDL

**Table source : `WTRHDON`** (déjà ingérée dans `slv_temps/heures_detail`)

| Colonne DDL | Type | Rôle |
| --- | --- | --- |
| `RHD_BASEP` | float | **Base heures payées** ← numérateur ETP |
| `RHD_TAUXP` | float | Taux horaire payé (non requis pour ETP) |
| `RHD_BASEF` | float | Base heures facturées (ETP facturé) |
| `RHD_TAUXF` | float | Taux horaire facturé |
| `RHD_DATED` | datetime2(3) | Date début ligne — clé de période |
| `RHD_DATEF` | datetime2(3) | Date fin ligne |
| `PRH_BTS` | int | Clé de jointure vers `WTPRH` (agence, client, intérimaire) |

**Table de contexte : `WTPRH`** (déjà ingérée dans `slv_temps/releves_heures`)

| Colonne DDL | Type | Rôle |
| --- | --- | --- |
| `PRH_DTEDEBSEM` | datetime2(3) | Semaine de début — axe temporel |
| `PRH_DTEFINSEM` | datetime2(3) | Semaine de fin |
| `RGPCNT_ID` | int | Agence ← dimension pivot |
| `TIE_ID` | int | Client ← dimension pivot |
| `PER_ID` | int | Intérimaire |

### Statut des données

✅ **Toutes les colonnes nécessaires sont déjà présentes dans Silver** (`slv_temps/heures_detail`
et `slv_temps/releves_heures`). Aucune modification Bronze requise.

### Transformation Gold à créer

```sql
-- gold_etp.py → gld_operationnel.fact_etp_hebdo
SELECT
    r.rgpcnt_id                                    AS agence_id,
    r.tie_id                                       AS tie_id,
    DATE_TRUNC('week', r.prh_dtedebsem)            AS semaine_debut,
    DATE_TRUNC('month', r.prh_dtedebsem)           AS mois,
    SUM(d.rhd_basep)                               AS heures_paye,
    SUM(d.rhd_basef)                               AS heures_fact,
    ROUND(SUM(d.rhd_basep) / 35.0, 2)             AS etp_semaine,
    ROUND(SUM(d.rhd_basep) / 151.67, 2)           AS etp_mois_equiv,
    COUNT(DISTINCT r.per_id)                       AS nb_interimaires_actifs
FROM slv_temps.releves_heures r
JOIN slv_temps.heures_detail   d ON d.prh_bts = r.prh_bts
GROUP BY 1, 2, 3, 4
```

**Table Gold cible :** `gld_operationnel.fact_etp_hebdo`

| Colonne | Description |
| --- | --- |
| `agence_id` | Clé agence (rgpcnt_id) |
| `tie_id` | Clé client |
| `semaine_debut` | Lundi de la semaine ISO |
| `mois` | Mois (pour agrégation mensuelle) |
| `heures_paye` | Total heures payées |
| `heures_fact` | Total heures facturées |
| `etp_semaine` | ETP calculé sur la semaine |
| `etp_mois_equiv` | ETP rapporté à un mois entier |
| `nb_interimaires_actifs` | Nombre d'intérimaires distincts |

---

## 3. Délai de placement

### Définition métier

> Le délai de placement mesure le temps entre la réception d'une commande client
> et l'envoi en mission d'un intérimaire qualifié.
> C'est l'indicateur de réactivité opérationnelle des agences.

```
Délai de placement (h) = CNTI_CREATE − CMD_DTE
Délai de placement (j) = (CNTI_CREATE − CMD_DTE) / 24
```

### Analyse DDL

**Table commandes : `WTCMD`** (déjà ingérée en Bronze)

| Colonne DDL | Type | Rôle |
| --- | --- | --- |
| `CMD_ID` | int | **Clé commande** — lien vers WTMISS |
| `CMD_DTE` | datetime2(3) | **Date/heure de saisie de la commande** ← T0 |
| `CMD_NBSALS` | int | Nombre de salariés demandés |
| `CMD_NBSALD` | int | Nombre de salariés disponibles |
| `STAT_CODE` | varchar(5) | Statut commande (OUVERTE, POURVUE, ANNULEE…) |
| `RGPCNT_ID` | int | Agence |
| `CCNT_ID` | int | Compte client (→ TIE_ID via WTCNTI) |

**Table missions : `WTMISS`** (déjà ingérée en Bronze)

| Colonne DDL | Type | Rôle |
| --- | --- | --- |
| `CMD_ID` | float | **Clé commande** ← pont WTCMD → WTMISS |
| `CNTI_CREATE` | datetime2(3) | **Date de création de la mission** ← T1 (proxy placement) |
| `PER_ID` | int | Intérimaire placé |
| `TIE_ID` | int | Client |
| `RGPCNT_ID` | int | Agence |

> **Note DDL :** `CMD_ID` est stocké en `float` dans `WTMISS` (vraisemblablement une
> anomalie de schéma Evolia). Appliquer un `CAST(CMD_ID AS INT)` lors de la jointure.

**Table placements : `WTPLAC`** (déjà ingérée en Bronze)

| Colonne DDL | Type | Rôle |
| --- | --- | --- |
| `PLAC_DTEEDI` | datetime2(3) | Date d'édition/envoi du placement ← T1 alternatif |
| `PLAC_CREELE` | datetime2(3) | Date de création de la fiche placement |
| `TIE_ID` | int | Client |
| `RGPCNT_ID` | int | Agence |
| `STAT_CODE` | varchar(5) | Statut placement |

> **Deux mesures possibles du délai :**
> - **Via WTMISS** : `CNTI_CREATE − CMD_DTE` — plus fiable (lien direct CMD_ID)
> - **Via WTPLAC** : `PLAC_DTEEDI − CMD_DTE` — lien indirect (TIE_ID + RGPCNT_ID + date)

### Statut des données

⚠️ **Lacune Bronze identifiée :** `CMD_ID` est actuellement extrait de `WTMISS` mais
**pas encore exploité comme clé de jointure** vers `WTCMD`. La colonne existe dans les deux
tables Bronze — il faut enrichir la transformation Silver.

Les colonnes `CMD_DTE` (WTCMD) et `CNTI_CREATE` (WTMISS) sont déjà en Bronze.

### Actions requises

**Bronze** : aucune modification (colonnes déjà extraites).

**Silver** — enrichir `silver_missions.py` :

```sql
-- Dans slv_missions/missions, ajouter :
SELECT
    m.per_id,
    m.cnt_id,
    m.tie_id,
    m.rgpcnt_id,
    m.cnti_create                                              AS date_mission_creee,
    c.cmd_dte                                                  AS date_commande,
    DATEDIFF('hour', c.cmd_dte, m.cnti_create)                AS delai_placement_heures,
    DATEDIFF('day',  c.cmd_dte, m.cnti_create)                AS delai_placement_jours,
    CASE
        WHEN DATEDIFF('hour', c.cmd_dte, m.cnti_create) <= 4  THEN 'IMMEDIAT'
        WHEN DATEDIFF('hour', c.cmd_dte, m.cnti_create) <= 24 THEN 'MEME_JOUR'
        WHEN DATEDIFF('day',  c.cmd_dte, m.cnti_create) <= 3  THEN 'COURT'
        ELSE 'LONG'
    END                                                        AS categorie_delai
FROM raw_wtmiss m
LEFT JOIN raw_wtcmd c ON CAST(m.cmd_id AS INT) = c.cmd_id
WHERE m.cmd_id IS NOT NULL
```

**Table Gold cible :** `gld_operationnel.fact_delai_placement`

| Colonne | Description |
| --- | --- |
| `agence_id` | Agence |
| `tie_id` | Client |
| `mois` | Mois de la commande |
| `nb_commandes` | Total commandes avec placement |
| `delai_moyen_heures` | Délai moyen (référence opérationnelle) |
| `delai_median_heures` | Médiane (moins sensible aux outliers) |
| `pct_moins_4h` | % placés en < 4h (excellente réactivité) |
| `pct_meme_jour` | % placés le jour même |
| `pct_plus_3j` | % délai > 3 jours (alerte) |

---

## 4. DPAE en retard

### Définition métier

> La DPAE (Déclaration Préalable à l'Embauche) est une obligation légale : elle doit être
> transmise à l'URSSAF **avant** la prise de poste de l'intérimaire, sous peine de sanctions
> pour travail dissimulé. Une DPAE en retard est une DPAE transmise après le début de mission
> ou non transmise.

```
DPAE en retard  = MISS_FLAGDPAE > CNTI_DATEFFET   (transmission après début mission)
DPAE manquante  = MISS_FLAGDPAE IS NULL             (jamais transmise)
```

### Analyse DDL — Découverte critique

**`MISS_FLAGDPAE` est de type `datetime2(3)` — pas un booléen.**

Contrairement à ce que son nom suggère, `MISS_FLAGDPAE` stocke la **date et heure
de transmission** de la DPAE à l'URSSAF. Cela donne accès à des indicateurs bien plus riches
qu'un simple flag présent/absent.

**Table source : `WTMISS`** (déjà ingérée en Bronze)

| Colonne DDL | Type | Signification réelle |
| --- | --- | --- |
| `MISS_FLAGDPAE` | datetime2(3) | **Date/heure de transmission DPAE** — NULL si non envoyée |
| `MISS_DPAE` | varchar(10) | Numéro ou code DPAE (référence transmise) |
| `MISS_NDPAE` | varchar(15) | Numéro de la DPAE officielle (accusé de réception URSSAF) |
| `CNTI_CREATE` | datetime2(3) | Date de création de la mission (proxy date de début) |

**Table contrats : `WTCNTI`** (déjà ingérée en Bronze)

| Colonne DDL | Type | Signification |
| --- | --- | --- |
| `CNTI_DATEFFET` | datetime2(3) | **Date réelle de début de mission** ← référence légale DPAE |

### Statut des données

⚠️ **Lacune Bronze identifiée :** `MISS_FLAGDPAE`, `MISS_DPAE` et `MISS_NDPAE`
**ne sont pas extraites** dans l'extraction actuelle de `bronze_interimaires.py`.

**Action Bronze requise :** ajouter ces 3 colonnes dans la requête SQL `WTMISS` :

```python
# Dans bronze_interimaires.py — requête WTMISS, ajouter :
"MISS_FLAGDPAE",   # datetime2 — date de transmission DPAE (NULL = non envoyée)
"MISS_DPAE",       # varchar(10) — code DPAE
"MISS_NDPAE",      # varchar(15) — numéro officiel DPAE
```

**Transformation Silver** — enrichir `silver_missions.py` :

```sql
SELECT
    m.per_id,
    m.cnt_id,
    m.tie_id,
    m.rgpcnt_id,
    c.cnti_dateffet                                              AS date_debut_mission,
    m.miss_flagdpae                                              AS dpae_transmise_le,
    m.miss_ndpae                                                 AS dpae_numero,
    CASE
        WHEN m.miss_flagdpae IS NULL                             THEN 'MANQUANTE'
        WHEN m.miss_flagdpae > c.cnti_dateffet                  THEN 'EN_RETARD'
        WHEN m.miss_flagdpae <= c.cnti_dateffet                 THEN 'CONFORME'
    END                                                          AS statut_dpae,
    DATEDIFF('hour', m.miss_flagdpae, c.cnti_dateffet)          AS ecart_heures
FROM raw_wtmiss m
JOIN raw_wtcnti c ON m.per_id = c.per_id AND m.cnt_id = c.cnt_id
```

**Table Gold cible :** `gld_operationnel.fact_conformite_dpae`

| Colonne | Description |
| --- | --- |
| `agence_id` | Agence |
| `mois` | Mois de la mission |
| `nb_missions` | Total missions du mois |
| `nb_dpae_conformes` | DPAE envoyées avant début mission |
| `nb_dpae_retard` | DPAE envoyées après début mission |
| `nb_dpae_manquantes` | DPAE absentes (NULL) |
| `taux_conformite_pct` | % conformes — KPI légal principal |
| `ecart_moyen_heures` | Retard moyen en heures (missions en retard) |

> ⚠️ **Indicateur légal critique.** Un taux de conformité < 100 % expose l'entreprise
> à des amendes URSSAF. Ce tableau doit faire l'objet d'une alerte automatique Airflow.

---

## 5. Taux de fidélisation intérimaires

### Définition métier

> La fidélisation mesure la capacité d'une agence à maintenir ses intérimaires actifs
> d'une période à l'autre. Un taux élevé réduit les coûts de recrutement et améliore
> la qualité de service client.

```
Taux de fidélisation = intérimaires actifs (N) ∩ actifs (N-1) / actifs (N-1)
```

### Analyse DDL — Contrainte identifiée

**`PYSALARIE.SAL_DATESORTIE` n'existe pas dans le DDL Evolia.**

L'unique colonne de statut disponible dans `PYSALARIE` est `SAL_ACTIF` (smallint),
qui indique l'état courant mais ne conserve pas l'historique des sorties.

**Approche alternative par historique de missions :**

| Table | Colonnes | Rôle |
| --- | --- | --- |
| `WTCNTI` | `PER_ID`, `CNTI_DATEFINCNTI` | Dernière date de fin de contrat |
| `WTMISS` | `PER_ID`, `CNTI_CREATE` | Date de la plus récente mission |
| `PYSALARIE` | `PER_ID`, `SAL_ACTIF`, `SAL_DATEENTREE` | Statut courant + ancienneté |
| `WTPINT` | `PER_ID`, `PINT_DERVENDTE` | **Date de dernière vente** ← proxy départ |
| `WTPINT` | `PER_ID`, `PINT_PREVENDTE` | Date de première vente |
| `WTPINT` | `PER_ID`, `PINT_MODIFDATE` | Dernière modification dossier |

> **`PINT_DERVENDTE`** (datetime) est la colonne la plus fiable pour détecter
> l'inactivité d'un intérimaire : si aucune vente depuis N mois, l'intérimaire
> est considéré comme perdu.

### Statut des données

⚠️ **Lacune Bronze identifiée :** `PINT_PREVENDTE`, `PINT_DERVENDTE` et `PINT_MODIFDATE`
ne sont pas extraites dans l'extraction actuelle de `bronze_interimaires.py` (WTPINT).

**Action Bronze requise :** ajouter ces colonnes dans la requête WTPINT :

```python
# Dans bronze_interimaires.py — requête WTPINT, ajouter :
"PINT_PREVENDTE",    # datetime2 — date première vente (ancienneté réelle dans l'intérim)
"PINT_DERVENDTE",    # datetime2 — date dernière vente ← proxy sortie/inactivité
"PINT_MODIFDATE",    # datetime2 — date dernière modification dossier
"PINT_CREATDTE",     # datetime2 — date de création du dossier
```

**Transformation Silver / Gold :**

```sql
-- Statut fidélisation par intérimaire
SELECT
    p.per_id,
    p.pint_prevendte                                            AS premiere_mission,
    p.pint_dervendte                                            AS derniere_mission,
    DATEDIFF('day', p.pint_dervendte, CURRENT_DATE)            AS jours_inactivite,
    CASE
        WHEN DATEDIFF('day', p.pint_dervendte, CURRENT_DATE) <= 90  THEN 'ACTIF'
        WHEN DATEDIFF('day', p.pint_dervendte, CURRENT_DATE) <= 365 THEN 'DORMANT'
        ELSE 'PERDU'
    END                                                         AS statut_fidelisation,
    DATEDIFF('day', p.pint_prevendte, p.pint_dervendte)        AS anciennete_jours
FROM raw_wtpint p
WHERE p.pint_dossier = 1
```

**Table Gold cible :** `gld_staffing.fact_fidelisation_interimaires`

| Colonne | Description |
| --- | --- |
| `agence_id` | Agence de rattachement principal |
| `trimestre` | Période d'analyse |
| `nb_actifs_debut` | Intérimaires actifs en début de période |
| `nb_actifs_fin` | Intérimaires actifs en fin de période |
| `nb_perdus` | Intérimaires actifs début mais inactifs fin |
| `nb_nouveaux` | Nouveaux intérimaires actifs |
| `taux_fidelisation_pct` | KPI principal |
| `anciennete_moyenne_jours` | Durée moyenne dans le vivier |

---

## 6. Concentration client

### Définition métier

> La concentration client mesure la dépendance du CA d'une agence envers ses plus
> gros clients. Une concentration élevée (ex : top 3 = 60% du CA) est un risque
> stratégique majeur.

```
Indice de concentration = CA top N clients / CA total agence × 100
```

### Statut des données

✅ **Aucune donnée supplémentaire requise.** Toutes les données nécessaires sont
déjà présentes dans `gld_commercial.fact_ca_mensuel_client`.

**Transformation Gold pure :**

```sql
-- gold_concentration_client.py → gld_commercial.fact_concentration_client
WITH ranked AS (
    SELECT
        agence_principale                                           AS agence_id,
        DATE_TRUNC('month', mois)                                  AS mois,
        client_sk,
        SUM(ca_net_ht)                                             AS ca_client,
        SUM(SUM(ca_net_ht)) OVER (PARTITION BY agence_principale, DATE_TRUNC('month', mois))
                                                                   AS ca_total_agence,
        ROW_NUMBER() OVER (
            PARTITION BY agence_principale, DATE_TRUNC('month', mois)
            ORDER BY SUM(ca_net_ht) DESC
        )                                                          AS rang
    FROM gld_commercial.fact_ca_mensuel_client
    GROUP BY 1, 2, 3
)
SELECT
    agence_id,
    mois,
    ca_total_agence,
    SUM(CASE WHEN rang <= 1  THEN ca_client ELSE 0 END) / ca_total_agence * 100  AS pct_top1,
    SUM(CASE WHEN rang <= 3  THEN ca_client ELSE 0 END) / ca_total_agence * 100  AS pct_top3,
    SUM(CASE WHEN rang <= 5  THEN ca_client ELSE 0 END) / ca_total_agence * 100  AS pct_top5,
    SUM(CASE WHEN rang <= 10 THEN ca_client ELSE 0 END) / ca_total_agence * 100  AS pct_top10,
    COUNT(DISTINCT client_sk)                                                      AS nb_clients_actifs,
    CASE
        WHEN SUM(CASE WHEN rang <= 3 THEN ca_client ELSE 0 END) / ca_total_agence >= 0.70
             THEN 'CRITIQUE'
        WHEN SUM(CASE WHEN rang <= 3 THEN ca_client ELSE 0 END) / ca_total_agence >= 0.50
             THEN 'ELEVE'
        ELSE 'NORMAL'
    END                                                                            AS risque_concentration
FROM ranked
GROUP BY 1, 2, 3
```

**Table Gold cible :** `gld_commercial.fact_concentration_client`

| Colonne | Description |
| --- | --- |
| `agence_id` | Agence |
| `mois` | Mois |
| `ca_total_agence` | CA total |
| `pct_top1` | Part du 1er client |
| `pct_top3` | Part des 3 premiers clients |
| `pct_top5` | Part des 5 premiers clients |
| `nb_clients_actifs` | Nombre de clients avec CA > 0 |
| `risque_concentration` | CRITIQUE / ELEVE / NORMAL |

---

## 7. Roadmap d'implémentation

### Phase A — Bronze (modifications mineures)

**Fichier : `bronze_interimaires.py`**

| Action | Table | Colonnes à ajouter | Effort |
| --- | --- | --- | --- |
| Ajouter colonnes DPAE | `WTMISS` | `MISS_FLAGDPAE`, `MISS_DPAE`, `MISS_NDPAE` | 15 min |
| Ajouter colonnes fidélisation | `WTPINT` | `PINT_PREVENDTE`, `PINT_DERVENDTE`, `PINT_MODIFDATE`, `PINT_CREATDTE` | 15 min |

> ETP, Délai de placement, Concentration client : **zéro modification Bronze requise**.

### Phase B — Silver (enrichissements)

| Action | Script | Effort |
| --- | --- | --- |
| Ajouter `statut_dpae` + `ecart_heures` | `silver_missions.py` | Moyen |
| Ajouter `delai_placement_heures` + `categorie_delai` via JOIN CMD | `silver_missions.py` | Moyen |
| Créer `slv_interimaires/fidelisation` | `silver_interimaires_detail.py` | Moyen |

### Phase C — Gold (nouveaux scripts)

| Script à créer | Tables Gold produites | Dépendances |
| --- | --- | --- |
| `gold_etp.py` | `gld_operationnel.fact_etp_hebdo` | slv_temps/* (disponible) |
| `gold_operationnel.py` (enrichi) | `gld_operationnel.fact_delai_placement` | slv_missions enrichi |
| `gold_operationnel.py` (enrichi) | `gld_operationnel.fact_conformite_dpae` | slv_missions enrichi |
| `gold_staffing.py` (enrichi) | `gld_staffing.fact_fidelisation_interimaires` | silver_interimaires_detail enrichi |
| `gold_ca_mensuel.py` (enrichi) | `gld_commercial.fact_concentration_client` | gld_commercial.fact_ca_mensuel_client |

### Ordre de déploiement recommandé

```
1. Bronze  → Ajouter MISS_FLAGDPAE / PINT_DERVENDTE   (impact zéro sur l'existant)
2. Silver  → Enrichir silver_missions + silver_interimaires_detail
3. Gold A  → gold_etp.py                               (aucune dépendance Silver nouvelle)
4. Gold B  → Enrichir gold_operationnel.py             (DPAE + délai placement)
5. Gold C  → Enrichir gold_staffing.py                 (fidélisation)
6. Gold D  → Enrichir gold_ca_mensuel.py               (concentration — zéro dépendance)
```

---

## 8. Anomalies DDL détectées

Ces anomalies ont été identifiées lors de l'analyse du DDL Evolia et doivent être
prises en compte dans les transformations.

| ID | Table | Colonne | Anomalie | Impact | Traitement |
| --- | --- | --- | --- | --- | --- |
| A-01 | `WTMISS` | `CMD_ID` | Stocké en `float` au lieu de `int` | JOIN CMD → MISS incorrect sans cast | `CAST(CMD_ID AS INT)` dans Silver |
| A-02 | `PYSALARIE` | `SAL_DATESORTIE` | **Colonne absente du DDL** — n'existe pas dans Evolia | Fidélisation non calculable directement | Proxy via `PINT_DERVENDTE` |
| A-03 | `WTMISS` | `MISS_FLAGDPAE` | Nommé "flag" mais de type `datetime2` | Erreur d'interprétation si traité comme booléen | Utiliser comme date de transmission |
| A-04 | `WTCMD` | `TIE_ID` | Absent — lien client via `CCNT_ID` uniquement | JOIN direct CMD → TIE impossible | Passer par `WTCNTI.CCNT_ID → TIE_ID` |
| A-05 | `WTPLAC` | `CMD_ID` | Absent — pas de lien direct WTPLAC → WTCMD | Délai via WTPLAC uniquement approximatif | Privilégier le délai via WTMISS.CMD_ID |
