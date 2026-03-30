# Dashboard Superset — Indicateurs Complémentaires
**GI Data Lakehouse · Phase 4 · Guide de configuration**

> Complément au dispositif Phase 3.
> Couvre les 8 indicateurs essentiels de l'intérim identifiés comme manquants
> après audit de couverture (cf. `ANALYSE_KPI_COMPLEMENTAIRES.md`).
> Ces charts viennent enrichir les dashboards existants ou alimenter deux nouveaux
> dashboards : "Qualité & Performance Mission" et "Risques & Recouvrement".

---

## Connexion & Datasets à enregistrer

**Database** : PostgreSQL Gold — `gi_data` (`postgresql-161a1420-of9dcfaf6.database.cloud.ovh.net:20184`) — connexion déjà configurée via le dashboard Performance Agences.

| Dataset | Schema | Table | Usage |
|---|---|---|---|
| Durée Mission | gld_performance | fact_duree_mission | DMM par métier/agence/mois |
| Rupture Contrat | gld_performance | fact_rupture_contrat | Taux de rupture CTT |
| DSO Client | **gld_operationnel** | fact_dso_client | Délai moyen de paiement |
| Balance Âgée | **gld_operationnel** | fact_balance_agee | Encours par tranche de retard |
| Renouvellement Mission | gld_commercial | fact_renouvellement_mission | Taux de reconduction |
| Dynamique Vivier | gld_staffing | fact_dynamique_vivier | Nouveaux inscrits, croissance |
| Coeff Facturation | gld_performance | fact_coeff_facturation | Coeff par métier/client |
| CA Secteur NAF | gld_commercial | fact_ca_secteur_naf | CA par secteur d'activité |
| Concentration Client | **gld_clients** | fact_concentration_client | Top-5 et Top-20 par agence/mois |

---

## Dashboard 1 — "Qualité & Performance Mission"

**Audience** : Directeurs d'agence, Directeurs régionaux, Direction
**Schemas Gold** : `gld_performance`, `gld_staffing`, `gld_commercial`

### Chart 1 — KPI Cards (×4)

| KPI | SQL | Format |
|---|---|---|
| DMM globale | `SELECT ROUND(AVG(dmm_jours),1) FROM gld_performance.fact_duree_mission WHERE mois = (SELECT MAX(mois) FROM gld_performance.fact_duree_mission)` | X,X jours |
| Taux de rupture | `SELECT ROUND(AVG(taux_rupture_pct),1) FROM gld_performance.fact_rupture_contrat WHERE mois = (SELECT MAX(mois) FROM gld_performance.fact_rupture_contrat)` | % |
| Taux de renouvellement | `SELECT ROUND(AVG(taux_renouvellement_pct),1) FROM gld_commercial.fact_renouvellement_mission WHERE mois = (SELECT MAX(mois) FROM gld_commercial.fact_renouvellement_mission)` | % |
| Coeff moyen | `SELECT ROUND(AVG(coeff_moyen_pondere),3) FROM gld_performance.fact_coeff_facturation WHERE mois = (SELECT MAX(mois) FROM gld_performance.fact_coeff_facturation)` | X,XXX |

**Type** : Big Number with Trendline
**Comparison** : Mois précédent

---

### Chart 2 — Heatmap : DMM par Métier × Agence

**Dataset** : fact_duree_mission JOIN dim_agences JOIN dim_metiers
**SQL Custom** :
```sql
SELECT
    da.nom                          AS agence,
    dm.libelle_metier               AS metier,
    ROUND(AVG(f.dmm_jours), 1)     AS dmm_jours,
    SUM(f.nb_missions)             AS nb_missions
FROM gld_performance.fact_duree_mission f
JOIN gld_shared.dim_agences da ON da.rgpcnt_id = f.agence_id
JOIN gld_shared.dim_metiers dm ON dm.met_id = f.metier_id
WHERE f.mois >= CURRENT_DATE - INTERVAL '3 months'
GROUP BY 1, 2
ORDER BY SUM(f.nb_missions) DESC
LIMIT 200
```
**Type** : Heatmap
**X** : métier, **Y** : agence, **Valeur** : dmm_jours
**Coloration** : gradient bleu (court) → orange (long), seuil vert = ≤7j, rouge = >30j

---

### Chart 3 — Bar Chart : Taux de Rupture par Agence

**SQL Custom** :
```sql
SELECT
    da.nom                           AS agence,
    r.taux_rupture_pct,
    r.nb_missions_total,
    r.nb_ruptures,
    CASE
        WHEN r.taux_rupture_pct <= 5  THEN 'BON'
        WHEN r.taux_rupture_pct <= 10 THEN 'MOYEN'
        ELSE 'ELEVE'
    END                              AS niveau_risque
FROM gld_performance.fact_rupture_contrat r
JOIN gld_shared.dim_agences da ON da.rgpcnt_id = r.agence_id
WHERE r.mois = (SELECT MAX(mois) FROM gld_performance.fact_rupture_contrat)
ORDER BY r.taux_rupture_pct DESC
LIMIT 30
```
**Type** : Bar Chart horizontal
**X** : taux_rupture_pct, **Y** : agence
**Coloration** : BON=vert (#27AE60), MOYEN=orange (#F39C12), ELEVE=rouge (#E74C3C)
**Annotation** : ligne verticale pointillée à 5% (seuil cible)

---

### Chart 4 — Donut : Distribution Durées de Mission

**SQL Custom** :
```sql
SELECT
    profil_duree,
    SUM(nb_missions)  AS nb_missions,
    ROUND(
        SUM(nb_missions)::FLOAT /
        SUM(SUM(nb_missions)) OVER () * 100
    , 1)              AS pct
FROM gld_performance.fact_duree_mission
WHERE mois = (SELECT MAX(mois) FROM gld_performance.fact_duree_mission)
GROUP BY profil_duree
```
**Type** : Pie Chart (Donut)
**Couleurs** : MICRO=#3498DB, COURTE=#2ECC71, MOYENNE=#F39C12, LONGUE=#9B59B6
**Options** : Show legend, Show percentage labels

---

### Chart 5 — Line Chart : Évolution Taux de Rupture et Renouvellement (12 mois)

**SQL Custom** :
```sql
SELECT
    r.mois,
    ROUND(AVG(r.taux_rupture_pct), 1)          AS taux_rupture,
    ROUND(AVG(rm.taux_renouvellement_pct), 1)  AS taux_renouvellement
FROM gld_performance.fact_rupture_contrat r
LEFT JOIN gld_commercial.fact_renouvellement_mission rm
    ON rm.agence_id = r.agence_id AND rm.mois = r.mois
WHERE r.mois >= CURRENT_DATE - INTERVAL '12 months'
GROUP BY r.mois
ORDER BY r.mois
```
**Type** : Mixed Chart
**Axe Y gauche** : taux_rupture (rouge), **Axe Y droit** : taux_renouvellement (vert)
**Annotation** : zone rouge si taux_rupture > 10%

---

### Chart 6 — Scatter Plot : Coeff Facturation × Métier

**SQL Custom** :
```sql
SELECT
    libelle_metier,
    ROUND(AVG(coeff_moyen_pondere), 3)  AS coeff_moyen,
    SUM(nb_missions)                    AS nb_missions,
    ROUND(AVG(thf_moyen), 2)            AS thf_moyen,
    ROUND(AVG(marge_coeff_pct), 1)      AS marge_pct,
    niveau_coeff
FROM gld_performance.fact_coeff_facturation
WHERE mois >= CURRENT_DATE - INTERVAL '3 months'
GROUP BY libelle_metier, niveau_coeff
ORDER BY SUM(nb_missions) DESC
LIMIT 30
```
**Type** : Scatter Plot (Bubble Chart)
**X** : coeff_moyen, **Y** : marge_pct, **Taille bulle** : nb_missions
**Couleur** : niveau_coeff (BON=vert, MOYEN=orange, A_REVOIR=rouge)
**Annotation** : lignes de référence X=1.25 et X=1.35

---

### Chart 7 — Tableau : Détail Coefficients par Agence × Métier

**SQL Custom** :
```sql
SELECT
    da.nom                                  AS agence,
    f.libelle_metier                        AS metier,
    f.coeff_moyen_pondere,
    f.thf_moyen,
    f.thp_moyen,
    f.marge_coeff_pct,
    f.nb_missions,
    f.niveau_coeff
FROM gld_performance.fact_coeff_facturation f
JOIN gld_shared.dim_agences da ON da.rgpcnt_id = f.agence_id
WHERE f.mois = (SELECT MAX(mois) FROM gld_performance.fact_coeff_facturation)
ORDER BY f.nb_missions DESC
```
**Type** : Table
**Conditional Formatting** :
- `coeff_moyen_pondere` < 1.25 → fond rouge clair
- `coeff_moyen_pondere` >= 1.35 → fond vert clair
- `marge_coeff_pct` < 25 → texte rouge gras

---

### Chart 8 — Filter Box

**Filtres** :
- **Mois** : dropdown (derniers 12 mois)
- **Agence** : multi-select (dim_agences.nom)
- **Marque** : multi-select (dim_agences.marque)
- **Métier** : searchable multi-select (dim_metiers.libelle_metier)
- **Niveau Coeff** : BON / MOYEN / A_REVOIR

---

### Layout Dashboard "Qualité & Performance Mission"

```
┌──────────────────────────────────────────────────────────────────────┐
│  [KPI: DMM]  [KPI: Taux Rupture]  [KPI: Renouvellement]  [KPI: Coeff]│
├──────────────────────────────┬───────────────────────────────────────┤
│  Heatmap DMM Métier×Agence   │  Bar Taux Rupture par Agence          │
│  (Chart 2)                   │  (Chart 3)                            │
├──────────────────────────────┴───────────────────────────────────────┤
│  Line Chart : Rupture vs Renouvellement 12 mois (Chart 5)            │
├──────────────────────────────┬───────────────────────────────────────┤
│  Donut Distribution Durées   │  Scatter Coeff × Métier               │
│  (Chart 4)                   │  (Chart 6)                            │
├──────────────────────────────┴───────────────────────────────────────┤
│  Tableau Coefficients Agence × Métier (Chart 7)                       │
├──────────────────────────────────────────────────────────────────────┤
│  Filter Box (Chart 8)                                                 │
└──────────────────────────────────────────────────────────────────────┘
```

---

## Dashboard 2 — "Risques & Recouvrement"

**Audience** : Finance, Crédit Management, Direction
**Schemas Gold** : `gld_commercial`

> Ce dashboard remplace et enrichit considérablement le dashboard "Risques & Recouvrement"
> mentionné en Phase 3 (qui ne comportait pas encore de charts configurés).

### Chart 1 — KPI Cards (×4)

| KPI | SQL | Format |
|---|---|---|
| DSO global | `SELECT ROUND(AVG(dso_jours), 1) FROM gld_operationnel.fact_dso_client WHERE mois = (SELECT MAX(mois) FROM gld_operationnel.fact_dso_client)` | X,X jours |
| Encours total échu | `SELECT SUM(montant_echu) FROM gld_operationnel.fact_balance_agee WHERE mois = (SELECT MAX(mois) FROM gld_operationnel.fact_balance_agee)` | € |
| Encours > 60j | `SELECT SUM(montant_echu) FROM gld_operationnel.fact_balance_agee WHERE mois = (SELECT MAX(mois) FROM gld_operationnel.fact_balance_agee) AND tranche IN ('61-90j', '>90j')` | € |
| Factures ouvertes | `SELECT SUM(nb_factures_ouvertes) FROM gld_operationnel.fact_dso_client WHERE mois = (SELECT MAX(mois) FROM gld_operationnel.fact_dso_client)` | N factures |

**Type** : Big Number with Trendline
**Alerte** : DSO > 45j → couleur rouge, DSO ≤ 30j → vert

---

### Chart 2 — Stacked Bar : Balance Âgée par Agence

> Note : `fact_balance_agee` stocke les tranches en format narrow (`tranche` VARCHAR + `montant_echu`).
> Le pivot vers le format wide est réalisé via `FILTER`.

**SQL Custom** :
```sql
SELECT
    da.nom_agence                                                    AS agence,
    SUM(b.montant_echu) FILTER (WHERE b.tranche = '0-30j')          AS retard_0_30j,
    SUM(b.montant_echu) FILTER (WHERE b.tranche = '31-60j')         AS retard_31_60j,
    SUM(b.montant_echu) FILTER (WHERE b.tranche = '61-90j')         AS retard_61_90j,
    SUM(b.montant_echu) FILTER (WHERE b.tranche = '>90j')           AS retard_plus_90j,
    SUM(b.montant_echu)                                              AS total_echu
FROM gld_operationnel.fact_balance_agee b
JOIN gld_shared.dim_agences da ON da.rgpcnt_id = b.agence_id
WHERE b.mois = (SELECT MAX(mois) FROM gld_operationnel.fact_balance_agee)
GROUP BY da.nom_agence
ORDER BY SUM(b.montant_echu) DESC
LIMIT 20
```
**Type** : Bar Chart Stacked (horizontal)
**Couleurs** : 0-30j=#F39C12, 31-60j=#E67E22, 61-90j=#E74C3C, >90j=#922B21
**Options** : Show data labels, Sort par total_echu DESC

---

### Chart 3 — Tableau : Encours DSO Détaillé par Client

> Note : `fact_balance_agee` n'a pas de granularité client (`tie_id` absent).
> Ce tableau utilise `fact_dso_client` qui dispose du détail client.

**SQL Custom** :
```sql
SELECT
    dc.raison_sociale               AS client,
    da.nom_agence                   AS agence,
    d.encours_ht,
    d.montant_echu,
    d.dso_jours,
    d.nb_factures_ouvertes
FROM gld_operationnel.fact_dso_client d
JOIN gld_shared.dim_clients dc ON dc.tie_id = d.tie_id
JOIN gld_shared.dim_agences da ON da.rgpcnt_id = d.agence_id
WHERE d.mois = (SELECT MAX(mois) FROM gld_operationnel.fact_dso_client)
  AND d.encours_ht > 0
ORDER BY d.encours_ht DESC
LIMIT 100
```
**Type** : Table
**Options** : Search enabled, Sort par encours_ht DESC, Page size 25
**Conditional Formatting** :
- `dso_jours` > 60 → fond rouge clair
- `dso_jours` > 30 → fond orange clair
- `montant_echu` > 0 → texte rouge gras

---

### Chart 4 — Line Chart : Évolution DSO 12 mois par Agence (Top 10)

**SQL Custom** :
```sql
SELECT
    da.nom_agence       AS agence,
    d.mois,
    ROUND(AVG(d.dso_jours), 1) AS dso_jours
FROM gld_operationnel.fact_dso_client d
JOIN gld_shared.dim_agences da ON da.rgpcnt_id = d.agence_id
WHERE d.mois >= CURRENT_DATE - INTERVAL '12 months'
  AND d.agence_id IN (
    SELECT agence_id FROM gld_operationnel.fact_dso_client
    WHERE mois = (SELECT MAX(mois) FROM gld_operationnel.fact_dso_client)
    ORDER BY AVG(dso_jours) DESC LIMIT 10
  )
GROUP BY da.nom_agence, d.mois
ORDER BY d.mois
```
**Type** : Time-series Line Chart
**X** : mois, **Y** : dso_moyen_jours, **Series** : agence
**Annotations** : ligne horizontale rouge à 45j (seuil critique), jaune à 30j (cible)

---

### Chart 5 — Donut : Répartition Encours par Tranche

**SQL Custom** :
```sql
SELECT
    tranche,
    SUM(montant_echu) AS montant
FROM gld_operationnel.fact_balance_agee
WHERE mois = (SELECT MAX(mois) FROM gld_operationnel.fact_balance_agee)
GROUP BY tranche
ORDER BY CASE tranche
    WHEN '0-30j'  THEN 1
    WHEN '31-60j' THEN 2
    WHEN '61-90j' THEN 3
    WHEN '>90j'   THEN 4
END
```
**Type** : Pie Chart (Donut)
**Couleurs** : 0-30j=#F39C12, 31-60j=#E67E22, 61-90j=#E74C3C, >90j=#922B21
**Options** : Labels = montant €, Tooltips = % + montant

---

### Chart 6 — Bar Chart : Top 15 Clients par Montant Échu

**SQL Custom** :
```sql
SELECT
    dc.raison_sociale           AS client,
    da.nom_agence               AS agence,
    d.montant_echu,
    d.encours_ht,
    d.dso_jours
FROM gld_operationnel.fact_dso_client d
JOIN gld_shared.dim_clients dc ON dc.tie_id = d.tie_id
JOIN gld_shared.dim_agences da ON da.rgpcnt_id = d.agence_id
WHERE d.mois = (SELECT MAX(mois) FROM gld_operationnel.fact_dso_client)
  AND d.montant_echu > 0
ORDER BY d.montant_echu DESC
LIMIT 15
```
**Type** : Bar Chart horizontal
**Couleur** : dso_jours (gradient vert ≤30j → orange ≤60j → rouge >60j)

---

### Chart 7 — Scatter : DSO × Encours (Matrice de Risque Client)

**SQL Custom** :
```sql
SELECT
    dc.raison_sociale           AS client,
    da.nom_agence               AS agence,
    d.dso_jours,
    d.encours_ht,
    d.montant_echu,
    d.nb_factures_ouvertes
FROM gld_operationnel.fact_dso_client d
JOIN gld_shared.dim_clients dc ON dc.tie_id = d.tie_id
JOIN gld_shared.dim_agences da ON da.rgpcnt_id = d.agence_id
WHERE d.mois = (SELECT MAX(mois) FROM gld_operationnel.fact_dso_client)
  AND d.encours_ht > 0
ORDER BY d.encours_ht DESC
LIMIT 200
```
**Type** : Scatter Plot (Bubble Chart)
**X** : dso_jours, **Y** : montant_echu, **Taille bulle** : encours_ht
**Couleur** : dso_jours (gradient vert → rouge)
**Lignes de référence** : X=30j (cible), X=45j (critique)
**Quadrants** :
- Bas gauche = Zone saine (DSO court, peu d'échu)
- Haut droit = Zone rouge (DSO long + montant échu élevé)

---

### Chart 8 — Filter Box

**Filtres** :
- **Agence** : multi-select (dim_agences.nom_agence)
- **Tranche** : multi-select (`fact_balance_agee.tranche` : 0-30j / 31-60j / 61-90j / >90j)
- **DSO seuil** : native filter numérique sur `dso_jours` (≥ X jours)
- **Mois** : dropdown derniers 12 mois

---

### Layout Dashboard "Risques & Recouvrement"

```
┌───────────────────────────────────────────────────────────────────────┐
│  [KPI: DSO Global]  [KPI: Encours Total]  [KPI: >60j]  [KPI: Retards]│
├───────────────────────────────┬───────────────────────────────────────┤
│  Stacked Bar Balance par Agence│  Donut Répartition Encours            │
│  (Chart 2)                    │  (Chart 5)                            │
├───────────────────────────────┴───────────────────────────────────────┤
│  Tableau Balance Âgée Détaillée (Chart 3)                              │
├───────────────────────────────┬───────────────────────────────────────┤
│  Line DSO 12 mois             │  Scatter Matrice Risque Client        │
│  (Chart 4)                    │  (Chart 7)                            │
├───────────────────────────────┴───────────────────────────────────────┤
│  Bar Top 15 Clients Encours Retard (Chart 6)                           │
├───────────────────────────────────────────────────────────────────────┤
│  Filter Box (Chart 8)                                                  │
└───────────────────────────────────────────────────────────────────────┘
```

---

## Charts additionnels — Enrichissement dashboards existants

Ces charts sont à **ajouter aux dashboards existants** (Phase 3) plutôt que de créer
un nouveau dashboard.

---

### A. Enrichissement "360° Client" — Taux de Renouvellement

**Ajouter au dashboard 360° Client, après le Time Series CA Mensuel.**

**Chart A1 — Bar Chart : Top 20 Clients par Taux de Renouvellement**
```sql
SELECT
    dc.raison_sociale                        AS client,
    r.taux_renouvellement_pct,
    r.nb_missions,
    r.nb_renouvellements
FROM gld_commercial.fact_renouvellement_mission r
JOIN gld_shared.dim_clients dc
    ON dc.tie_id = r.tie_id AND dc.is_current
WHERE r.mois >= CURRENT_DATE - INTERVAL '3 months'
GROUP BY dc.raison_sociale, r.taux_renouvellement_pct,
         r.nb_missions, r.nb_renouvellements
ORDER BY r.taux_renouvellement_pct DESC
LIMIT 20
```
**Type** : Bar Chart horizontal
**Interprétation** : taux élevé = client fidèle → à maintenir ; taux faible = instabilité

---

### B. Enrichissement "360° Intérimaire" — Dynamique du Vivier

**Ajouter au dashboard 360° Intérimaire, en bas de page.**

**Chart B1 — Line Chart : Nouveaux inscrits vs Perdus (12 mois)**
```sql
SELECT
    v.mois,
    SUM(v.nb_nouveaux)       AS nouveaux,
    SUM(v.nb_perdus)         AS perdus,
    SUM(v.croissance_nette)  AS croissance_nette,
    SUM(v.pool_actif)        AS pool_actif
FROM gld_staffing.fact_dynamique_vivier v
WHERE v.mois >= CURRENT_DATE - INTERVAL '12 months'
GROUP BY v.mois
ORDER BY v.mois
```
**Type** : Mixed Chart
**Barres** : nouveaux (vert), perdus (rouge)
**Ligne** : pool_actif (bleu, axe Y droit)

**Chart B2 — Big Number : Croissance nette du vivier (dernier mois)**
```sql
SELECT SUM(croissance_nette) AS croissance
FROM gld_staffing.fact_dynamique_vivier
WHERE mois = (SELECT MAX(mois) FROM gld_staffing.fact_dynamique_vivier)
```
**Type** : Big Number with Trendline
**Couleur** : vert si > 0, rouge si < 0

---

### C. Enrichissement "Performance Agences" — CA par Secteur NAF

**Ajouter au dashboard Performance Agences, onglet "Analyse Sectorielle".**

**Chart C1 — Treemap : Répartition CA par Secteur**
```sql
SELECT
    secteur_libelle,
    SUM(ca_net_ht)     AS ca_net_ht,
    SUM(nb_clients)    AS nb_clients,
    SUM(nb_missions)   AS nb_missions
FROM gld_commercial.fact_ca_secteur_naf
WHERE mois = (SELECT MAX(mois) FROM gld_commercial.fact_ca_secteur_naf)
GROUP BY secteur_libelle
ORDER BY ca_net_ht DESC
```
**Type** : Treemap
**Taille** : ca_net_ht, **Couleur** : secteur (palette distincte)
**Options** : Show labels avec % du total

**Chart C2 — Heatmap : CA Secteur × Agence**
```sql
SELECT
    da.nom               AS agence,
    n.secteur_libelle    AS secteur,
    SUM(n.ca_net_ht)     AS ca_net_ht,
    ROUND(AVG(n.part_ca_agence_pct), 1) AS part_pct
FROM gld_commercial.fact_ca_secteur_naf n
JOIN gld_shared.dim_agences da ON da.rgpcnt_id = n.agence_id
WHERE n.mois >= CURRENT_DATE - INTERVAL '3 months'
GROUP BY 1, 2
ORDER BY SUM(n.ca_net_ht) DESC
```
**Type** : Pivot Table / Heatmap
**Lignes** : agence, **Colonnes** : secteur, **Valeur** : part_pct
**Coloration** : gradient blanc → bleu foncé (concentration sectorielle)

**Chart C3 — Stacked Bar : Évolution CA par Secteur (12 mois)**
```sql
SELECT
    mois,
    secteur_libelle,
    SUM(ca_net_ht) AS ca_net_ht
FROM gld_commercial.fact_ca_secteur_naf
WHERE mois >= CURRENT_DATE - INTERVAL '12 months'
  AND secteur_libelle IN (
      SELECT secteur_libelle FROM gld_commercial.fact_ca_secteur_naf
      GROUP BY secteur_libelle ORDER BY SUM(ca_net_ht) DESC LIMIT 8
  )
GROUP BY 1, 2
ORDER BY 1
```
**Type** : Time-series Bar Chart (100% Stacked)
**Options** : Show totals, Time grain = Month

---

## Refresh, Permissions & Alertes

### Refresh

| Dashboard | Cache timeout | DAG Airflow |
|---|---|---|
| Qualité & Performance Mission | 3600s | dag_gold_qualite (quotidien 6h) |
| Risques & Recouvrement | 1800s | dag_gold_recouvrement (quotidien 7h) |
| Enrichissements existants | Hérité du dashboard parent | — |

### Rôles & Permissions

| Rôle | Qualité & Perf | Risques & Recouvrement | Notes |
|---|---|---|---|
| Direction | ✅ Complet | ✅ Complet | Accès total |
| Directeur Régional | ✅ Filtré agences région | ✅ Filtré agences région | RLS par zone_geo |
| Agence | ✅ Filtré agence propre | ❌ Non exposé | Données opérationnelles |
| Finance | ❌ | ✅ Complet | Recouvrement seulement |

### Alertes Superset recommandées

> **Menu** : Alerts & Reports → + Alert

| Alerte | Condition | Destinataires | Fréquence |
|---|---|---|---|
| DSO critique | `AVG(dso_jours) > 45` sur `gld_operationnel.fact_dso_client` | Finance, Direction | Lundi matin |
| Encours > 90j | `SUM(montant_echu) > 50000` sur `fact_balance_agee WHERE tranche = '>90j'` | Finance, Agence concernée | Quotidien |
| Taux rupture élevé | `AVG(taux_rupture_pct) > 10` | Dir. Régional, Agence | Lundi matin |
| Coeff < seuil | `AVG(coeff_moyen_pondere) < 1.25` | Directeur Régional | Hebdomadaire |

---

*Classification : INTERNE — CONFIDENTIEL*
*Version : 1.0 · 2026-03-15*
