# 📊 Guide Configuration Superset — Dashboard 360° Intérimaire

**GI Data Lakehouse · Phase 3 · OVHcloud Data Platform**

> **Prérequis** : gld_staffing.fact_competences_dispo, gld_staffing.fact_activite_int, gld_shared.dim_interimaires et gld_shared.dim_metiers créés dans PostgreSQL Gold.

---

## 1. Datasets à créer

### Dataset 1 : fact_competences_dispo
- Schema : `gld_staffing` → Table : `fact_competences_dispo`
- Jointure enrichie (SQL Lab) :

```sql
SELECT
    fc.metier_sk, fc.agence_sk, fc.met_id, fc.rgpcnt_id,
    fc.nb_qualifies, fc.nb_disponibles, fc.nb_en_mission, fc.taux_couverture,
    dm.libelle_metier, dm.qualification, dm.specialite,
    da.nom_agence, da.marque, da.secteur, da.zone_geo
FROM gld_staffing.fact_competences_dispo fc
LEFT JOIN gld_shared.dim_metiers dm ON dm.metier_sk = fc.metier_sk
LEFT JOIN gld_shared.dim_agences da ON da.agence_sk = fc.agence_sk
```

Enregistrer comme Dataset virtuel : `v_competences_enrichi`

### Dataset 2 : fact_activite_int (existant)
- Schema : `gld_staffing` → Table : `fact_activite_int`

### Dataset 3 : dim_interimaires (pour table détail)
- Schema : `gld_shared` → Table : `dim_interimaires`
- Jointure SQL Lab avec activité :

```sql
SELECT
    di.interimaire_sk, di.per_id, di.matricule, di.nom, di.prenom,
    di.ville, di.code_postal, di.date_entree, di.is_actif,
    di.is_candidat, di.is_permanent, di.agence_rattachement,
    COALESCE(a.nb_missions, 0) AS nb_missions,
    COALESCE(a.taux_occupation, 0) AS taux_occupation,
    COALESCE(a.ca_genere, 0) AS ca_genere,
    da.nom_agence
FROM gld_shared.dim_interimaires di
LEFT JOIN (
    SELECT per_id, SUM(nb_missions) AS nb_missions,
           AVG(taux_occupation) AS taux_occupation, SUM(ca_genere) AS ca_genere
    FROM gld_staffing.fact_activite_int
    GROUP BY per_id
) a ON a.per_id = di.per_id::INT
LEFT JOIN gld_shared.dim_agences da ON da.rgpcnt_id = di.agence_rattachement::INT
WHERE di.is_actif = true
```

Enregistrer comme Dataset virtuel : `v_interimaires_activite`

---

## 2. Création des 6 Charts

### Chart 1 — KPI Cards (×4)

| KPI | Dataset | Metric |
|-----|---------|--------|
| Total Intérimaires Actifs | v_interimaires_activite | COUNT(*) |
| Taux Occupation Moyen | v_interimaires_activite | AVG(taux_occupation) × 100 |
| En Mission | v_competences_enrichi | SUM(nb_en_mission) |
| Disponibles | v_competences_enrichi | SUM(nb_disponibles) |

Type : **Big Number**, format pourcentage pour taux occupation.

### Chart 2 — Heatmap Compétences × Agences

1. Chart type : **Heatmap**
2. Dataset : `v_competences_enrichi`
3. X-Axis : `nom_agence`
4. Y-Axis : `libelle_metier`
5. Metric : `AVG(taux_couverture)`
6. Color scheme : spectral (vert = couvert, rouge = pénurie)
7. Sort : par nb_qualifies descending
8. Row limit : Top 30 métiers × Top 20 agences

### Chart 3 — Top 20 Métiers (Bar Chart empilé)

1. Chart type : **Bar Chart** (vertical, stacked)
2. Dataset : `v_competences_enrichi`
3. X-Axis : `libelle_metier`
4. Metrics :
   - SUM(nb_disponibles) → couleur verte (#27AE60)
   - SUM(nb_en_mission) → couleur bleue (#2E86C1)
5. Sort : SUM(nb_qualifies) descending
6. Row limit : 20
7. Label : afficher les totaux

### Chart 4 — Table Liste Intérimaires

1. Chart type : **Table**
2. Dataset : `v_interimaires_activite`
3. Colonnes visibles :
   - nom, prenom (🔐 RGPD — masqué pour rôle Viewer)
   - ville, date_entree, nom_agence
   - taux_occupation (format %)
   - nb_missions
   - ca_genere (format €)
4. Enable : Search, Sort, Pagination (25 rows)
5. Conditional formatting : taux_occupation < 0.3 → rouge

### Chart 5 — Taux Occupation Mensuel (Line Chart)

1. Chart type : **Time-series Line Chart**
2. Dataset : `fact_activite_int`
3. Time column : `mois`
4. Metric : `AVG(taux_occupation)`
5. Time range : Last 12 months
6. Time grain : Month
7. Y-axis format : pourcentage

### Chart 6 — Filter Box

1. Chart type : **Filter Box**
2. Filtres :
   - `nom_agence` (searchable dropdown)
   - `ville` (searchable dropdown)
   - `is_actif` (boolean toggle)
   - `qualification` (multi-select, from dim_metiers)
   - `libelle_metier` (searchable dropdown)
3. Cross-filter : activé

---

## 3. Assemblage du Dashboard

1. **Dashboards** → **+ Dashboard**
2. Nom : **"360° Intérimaire — Groupe Interaction"**
3. Layout :

```
┌───────────────────────────────────────────────────────┐
│ [KPI: Actifs] [KPI: Taux Occup.] [KPI: Mission] [KPI: Dispo] │
├────────────────────────────┬──────────────────────────┤
│  Heatmap Compétences       │  Bar Top 20 Métiers      │
├────────────────────────────┴──────────────────────────┤
│  Table Intérimaires (full width)                      │
├────────────────────────────┬──────────────────────────┤
│  Line Taux Occupation      │  Filter Box              │
└────────────────────────────┴──────────────────────────┘
```

4. Published : ✅

---

## 4. Permissions RGPD Superset

### Rôle "Direction"
- Accès : tous les dashboards, tous les datasets
- Colonnes : toutes visibles (nom, prenom inclus)

### Rôle "Agence"
- Accès : dashboards 360° Intérimaire et Client
- **Row-Level Security** :
  1. **Security** → **Row Level Security** → **+ Rule**
  2. Tables : `v_interimaires_activite`, `v_competences_enrichi`
  3. Clause : `agence_rattachement = {{ current_user_agence_id }}`
  4. Note : nécessite table mapping `superset_user_agences(username, rgpcnt_id)` — à créer en Phase 4

### Rôle "Viewer"
- Accès : dashboards en lecture seule
- **Masquage colonnes** :
  1. Créer un Dataset dédié `v_interimaires_anonyme` :
  ```sql
  SELECT interimaire_sk, per_id, ville, date_entree,
         is_actif, nom_agence, nb_missions, taux_occupation, ca_genere
  -- nom et prenom EXCLUS
  FROM v_interimaires_activite
  ```
  2. Assigner ce dataset au rôle Viewer uniquement
  3. Les charts Table et KPI pointent vers ce dataset pour les Viewers

---

## 5. Checklist de validation pré-démo

- [ ] Les 4 KPI Cards affichent des valeurs cohérentes
- [ ] La Heatmap montre au moins 10 métiers × 10 agences
- [ ] Le Top 20 affiche les barres empilées correctement
- [ ] La table intérimaires est filtrable et triable
- [ ] Le taux d'occupation mensuel montre 12 mois
- [ ] Les filtres cross-chart fonctionnent (sélection agence → mise à jour tous les charts)
- [ ] Le rôle Viewer ne voit pas les colonnes nom/prenom
- [ ] Temps de chargement dashboard < 3 secondes

---

*Classification : INTERNE — CONFIDENTIEL*
