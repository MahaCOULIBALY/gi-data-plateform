# 📊 Guide Configuration Superset — Dashboard 360° Client

**GI Data Lakehouse · Phase 3 · OVHcloud Data Platform**

> **Prérequis** : gld_clients.vue_360_client et gld_commercial.fact_ca_mensuel_client créés dans PostgreSQL Gold.

---

## 1. Connexion à la base de données

1. Ouvrir Superset (URL fournie par OVH Data Platform → Dashboard → Superset)
2. Menu **Settings** → **Database Connections** → **+ Database**
3. Choisir **PostgreSQL**
4. Renseigner :
   - Host : `postgresql-161a1420-of9dcfaf6.database.cloud.ovh.net`
   - Port : `20184`
   - Database : `gi_data`
   - Username : `gi_gold_user` / Password : voir `.env` OVH_PG_PASSWORD
   - SSL : `require`
5. **Test Connection** → succès → **Connect**

---

## 2. Création des Datasets

### Dataset 1 : vue_360_client
1. Menu **Datasets** → **+ Dataset**
2. Database : `gi_poc` → Schema : `gld_clients` → Table : `vue_360_client`
3. **Add** → ouvrir le dataset → onglet **Columns**
4. Ajouter les colonnes calculées :

**Colonne calculée "Statut Risque Global"** :
```sql
CASE
  WHEN risque_credit_score = 'HIGH' OR risque_churn = 'HIGH' THEN '🔴 Critique'
  WHEN risque_credit_score = 'MEDIUM' OR risque_churn = 'MEDIUM' THEN '🟠 Attention'
  ELSE '🟢 OK'
END
```

**Colonne calculée "CA Formaté"** :
```sql
TO_CHAR(ca_12_mois_glissants, 'FM999 999 999 €')
```

5. Onglet **Metrics** : ajouter les agrégations utiles (SUM ca_ytd, COUNT *, AVG marge_moyenne_pct)

### Dataset 2 : fact_ca_mensuel_client
1. **Datasets** → **+ Dataset**
2. Schema : `gld_commercial` → Table : `fact_ca_mensuel_client`
3. **Add**

---

## 3. Création des 7 Charts

### Chart 1 — KPI Cards (×4)

Créer 4 charts de type **Big Number with Trendline** :

| KPI | Dataset | Metric | Filtres |
|-----|---------|--------|---------|
| Nb Clients | vue_360_client | COUNT(*) | — |
| CA YTD Total | vue_360_client | SUM(ca_ytd) | — |
| Missions Actives | vue_360_client | SUM(nb_missions_actives) | — |
| Intérimaires Actifs | vue_360_client | SUM(nb_int_actifs) | — |

Configuration : Time Grain = None, couleur = bleu GI (#1B4F72)

### Chart 2 — Top 20 Clients par CA (Bar Chart)

1. Chart type : **Bar Chart** (horizontal)
2. Dataset : `vue_360_client`
3. Dimensions : `raison_sociale`
4. Metrics : `SUM(ca_12_mois_glissants)`
5. Sort descending
6. Row Limit : 20
7. Couleur : gradient bleu

### Chart 3 — CA vs Marge (Scatter Plot)

1. Chart type : **Scatter Plot** (ou Bubble Chart)
2. Dataset : `vue_360_client`
3. X-Axis : `ca_12_mois_glissants`
4. Y-Axis : `marge_moyenne_pct`
5. Bubble Size : `nb_missions_total`
6. Color : `risque_credit_score` (HIGH=rouge, MEDIUM=orange, LOW=vert)
7. Tooltip : `raison_sociale`, `ville`

### Chart 4 — Table Liste Clients

1. Chart type : **Table**
2. Dataset : `vue_360_client`
3. Colonnes : raison_sociale, ville, ca_ytd, delta_ca_pct, nb_missions_actives, risque_churn, risque_credit_score
4. Enable : Search box, Sort columns, Page length = 25
5. Conditional Formatting :
   - delta_ca_pct < 0 → rouge
   - risque_churn = 'HIGH' → fond rouge clair
   - risque_credit_score = 'HIGH' → fond orange

### Chart 5 — Répartition Risque Churn (Pie Chart)

1. Chart type : **Pie Chart**
2. Dataset : `vue_360_client`
3. Dimension : `risque_churn`
4. Metric : `COUNT(*)`
5. Couleurs : HIGH=#E74C3C, MEDIUM=#F39C12, LOW=#27AE60

### Chart 6 — Évolution CA Mensuel (Time Series)

1. Chart type : **Time-series Line Chart**
2. Dataset : `fact_ca_mensuel_client`
3. Time column : `mois`
4. Metric : `SUM(ca_net_ht)`
5. Time range : Last 12 months
6. Time grain : Month

### Chart 7 — Filter Box

1. Chart type : **Filter Box**
2. Dataset : `vue_360_client`
3. Filtres configurés :
   - `ville` (searchable dropdown)
   - `statut` (single select)
   - `secteur_activite` (searchable dropdown)
   - `risque_churn` (multi-select)
   - `risque_credit_score` (multi-select)
4. Enable : Instant filtering (cross-chart)

---

## 4. Assemblage du Dashboard

1. Menu **Dashboards** → **+ Dashboard**
2. Nom : **"360° Client — Groupe Interaction"**
3. Layout (2 colonnes) :

```
┌─────────────────────────────────────────────────────┐
│  [KPI: Clients]  [KPI: CA YTD]  [KPI: Missions]  [KPI: Int.] │
├──────────────────────────┬──────────────────────────┤
│  Bar Chart Top 20        │  Scatter CA vs Marge     │
├──────────────────────────┴──────────────────────────┤
│  Table Liste Clients (full width)                   │
├──────────────────────────┬──────────────────────────┤
│  Pie Risque Churn        │  Time Series CA Mensuel  │
├──────────────────────────┴──────────────────────────┤
│  Filter Box (full width, en haut ou en sidebar)     │
└─────────────────────────────────────────────────────┘
```

4. **Published** : activer
5. Ajouter à la liste des favoris pour accès rapide

---

## 5. Permissions et accès

| Rôle | Accès Dashboard | Accès Datasets | Notes |
|------|-----------------|----------------|-------|
| Direction | ✅ Complet | ✅ Tous | Accès total |
| Agence | ✅ Filtré | ✅ Avec RLS | Voir section 6 |
| Viewer | ✅ Lecture seule | ✅ Colonnes limitées | Pas de nom/prenom |

---

## 6. Row-Level Security (optionnel, Phase 4)

Pour filtrer par agence :
1. Menu **Security** → **Row Level Security**
2. Créer une règle :
   - Tables : `vue_360_client`
   - Clause : `EXISTS (SELECT 1 FROM gld_shared.dim_agences WHERE rgpcnt_id = vue_360_client.nb_agences_partenaires AND ...)`
   - Note : RLS avancé nécessite table de mapping user → agence. À implémenter en Phase 4.

---

*Classification : INTERNE — CONFIDENTIEL*
