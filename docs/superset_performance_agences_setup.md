# Dashboard Superset — Performance Agences
**GI Data Lakehouse · Phase 3 · Guide de configuration**

---

## Connexion

- **Database** : PostgreSQL Gold (`gi-poc-warehouse`)
- **Schemas** : `gld_performance`, `gld_shared`
- Superset → Settings → Database Connections → PostgreSQL déjà configurée (cf. POC Phase 0)

---

## Datasets à enregistrer

| Dataset | Schema | Table | Usage |
|---|---|---|---|
| Scorecard Agence | gld_performance | scorecard_agence | KPIs mensuels 360° |
| Ranking Agences | gld_performance | ranking_agences | Classement multi-critères |
| Tendances Agence | gld_performance | tendances_agence | Variations M/M-1, N/N-1 |
| Dim Agences | gld_shared | dim_agences | Lookup nom, marque, secteur |

**SQL Lab → Dataset → Add Dataset** pour chacun.

---

## Dashboard : "Performance Agences GI"

### Chart 1 — KPI Cards (Big Number)
4 indicateurs en haut du dashboard :

| KPI | SQL | Format |
|---|---|---|
| Nb agences actives | `COUNT(DISTINCT agence_id) WHERE mois = (SELECT MAX(mois) FROM scorecard_agence)` | Entier |
| CA net total | `SUM(ca_net_ht) WHERE mois = MAX(mois)` | € avec séparateur milliers |
| Marge moyenne | `AVG(taux_marge) WHERE mois = MAX(mois)` | % (×100) |
| Taux transfo moyen | `AVG(taux_transformation) WHERE mois = MAX(mois)` | % (×100) |

**Type** : Big Number with Trendline  
**Comparison** : Mois précédent (SUBQUERY mois = MAX(mois) - INTERVAL '1 month')

### Chart 2 — Bar Chart : Top 20 Agences par CA
**Dataset** : scorecard_agence JOIN dim_agences  
**SQL Custom** :
```sql
SELECT da.nom AS agence, s.ca_net_ht, s.taux_marge, s.mois
FROM gld_performance.scorecard_agence s
JOIN gld_shared.dim_agences da ON da.rgpcnt_id = s.agence_id
WHERE s.mois = (SELECT MAX(mois) FROM gld_performance.scorecard_agence)
ORDER BY s.ca_net_ht DESC LIMIT 20
```
**Config** : X=agence, Y=ca_net_ht, Color=taux_marge (gradient vert→rouge)

### Chart 3 — Heatmap : Scorecard Multi-KPI
**Dataset** : scorecard_agence + dim_agences  
**SQL Custom** :
```sql
SELECT da.nom AS agence,
       s.ca_net_ht, s.taux_marge, s.nb_clients_actifs,
       s.nb_int_actifs, s.taux_transformation
FROM gld_performance.scorecard_agence s
JOIN gld_shared.dim_agences da ON da.rgpcnt_id = s.agence_id
WHERE s.mois = (SELECT MAX(mois) FROM gld_performance.scorecard_agence)
ORDER BY s.ca_net_ht DESC LIMIT 30
```
**Type** : Pivot Table ou Heatmap  
**Colonnes** : ca_net_ht, taux_marge, nb_int_actifs, taux_transformation  
**Lignes** : agence  
**Coloration** : Conditional formatting (vert=top quartile, rouge=bottom)

### Chart 4 — Ranking Table
**Dataset** : ranking_agences + dim_agences  
**SQL Custom** :
```sql
SELECT da.nom AS agence, da.marque, da.secteur,
       r.rang_ca, r.rang_marge, r.rang_placement, r.rang_transfo,
       r.score_global,
       CASE WHEN r.score_global >= 0.75 THEN '🟢'
            WHEN r.score_global >= 0.50 THEN '🟡'
            WHEN r.score_global >= 0.25 THEN '🟠'
            ELSE '🔴' END AS performance
FROM gld_performance.ranking_agences r
JOIN gld_shared.dim_agences da ON da.rgpcnt_id = r.agence_id
WHERE r.mois = (SELECT MAX(mois) FROM gld_performance.ranking_agences)
ORDER BY r.score_global DESC
```
**Type** : Table  
**Options** : Search enabled, Sort par score_global DESC, Page size 50  
**Colonnes calculées** :
- "Rang Global" = ROW_NUMBER() OVER (ORDER BY score_global DESC)
- "Performance" = emoji indicateur (🟢🟡🟠🔴)

### Chart 5 — Line Chart : Tendances CA 12 mois
**Dataset** : tendances_agence + dim_agences  
**SQL Custom** :
```sql
SELECT da.nom AS agence, t.mois, t.ca_net_ht, t.tendance
FROM gld_performance.tendances_agence t
JOIN gld_shared.dim_agences da ON da.rgpcnt_id = t.agence_id
WHERE t.mois >= CURRENT_DATE - INTERVAL '12 months'
  AND t.agence_id IN (
    SELECT agence_id FROM gld_performance.scorecard_agence
    WHERE mois = (SELECT MAX(mois) FROM gld_performance.scorecard_agence)
    ORDER BY ca_net_ht DESC LIMIT 10
  )
ORDER BY t.mois
```
**Type** : Line Chart  
**X** : mois, **Y** : ca_net_ht, **Series** : agence  
**Options** : Show markers, Y-axis format €

### Chart 6 — Dual Axis : Delta CA M/M-1 vs N/N-1
**Dataset** : tendances_agence  
**SQL Custom** :
```sql
SELECT da.nom AS agence, t.delta_ca_mom, t.delta_ca_yoy, t.tendance
FROM gld_performance.tendances_agence t
JOIN gld_shared.dim_agences da ON da.rgpcnt_id = t.agence_id
WHERE t.mois = (SELECT MAX(mois) FROM gld_performance.tendances_agence)
ORDER BY t.ca_net_ht DESC LIMIT 20
```
**Type** : Mixed Chart (Bars + Line)  
**Bars** : delta_ca_mom (bleu), **Line** : delta_ca_yoy (orange)  
**Coloration** : Positive=vert, Négatif=rouge

### Chart 7 — Pie Chart : Distribution Tendances
**Dataset** : tendances_agence  
```sql
SELECT tendance, COUNT(*) AS nb_agences
FROM gld_performance.tendances_agence
WHERE mois = (SELECT MAX(mois) FROM gld_performance.tendances_agence)
GROUP BY tendance
```
**Colors** : HAUSSE=vert, STABLE=gris, BAISSE=rouge

### Chart 8 — Filter Box
**Filtres** :
- **Mois** : dropdown (derniers 12 mois)
- **Marque** : multi-select (dim_agences.marque)
- **Secteur** : multi-select (dim_agences.secteur)
- **Zone Géo** : multi-select (dim_agences.zone_geo)
- **Performance** : 🟢🟡🟠🔴

---

## Layout Dashboard

```
┌─────────────────────────────────────────────────────────────────┐
│  [KPI: Nb Agences] [KPI: CA Total] [KPI: Marge] [KPI: Transfo]│
├───────────────────────────────┬─────────────────────────────────┤
│  Bar Chart Top 20 CA          │  Ranking Table (score global)   │
│  (Chart 2)                    │  (Chart 4)                      │
├───────────────────────────────┴─────────────────────────────────┤
│  Heatmap Scorecard Multi-KPI (Chart 3)                          │
├───────────────────────────────┬─────────────────────────────────┤
│  Line Chart Tendances 12m     │  Dual Axis Delta M/M-1 N/N-1   │
│  (Chart 5)                    │  (Chart 6)                      │
├───────────────────────────────┴─────────────────────────────────┤
│  [Pie Tendances] (Chart 7)        [Filter Box] (Chart 8)       │
└─────────────────────────────────────────────────────────────────┘
```

## Refresh & Permissions

- **Cache timeout** : 3600s (1h) — données rafraîchies quotidiennement par dag_phase3
- **Roles** : "Direction" (full access), "Agence" (filtré par RLS sur agence_id)
- **RLS** : `agence_id = {{ current_user_agence_id() }}` pour role "Agence"
