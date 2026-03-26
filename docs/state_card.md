# State Card — Pipeline GI Data Platform

> Dernière mise à jour : **2026-03-26** · Auteur : M. COULIBALY (Phase 3 — tâches #15-#16)
> Architecture : Bronze (Evolia/SQL Server) → Silver (Parquet S3) → Gold (PostgreSQL)

---

## Statut global

| Couche | Statut | Notes |
| --- | --- | --- |
| Bronze | ✅ Stable | 30+ tables · Celery Worker frdc1pipeline01 |
| Silver | ✅ Stable | Guard source vide déployé sur tous les scripts |
| Gold | 🟡 Partiel | fact_competences_dispo corrigé · CA validation activée |

---

## Bugs résolus — session 2026-03-26

### ✅ T1 — Bug silver_temps : source bronze vide non gérée

**Fichiers :** [scripts/shared.py](../scripts/shared.py) · [scripts/silver_temps.py](../scripts/silver_temps.py)

**Cause :** `read_json_auto(…/*.json)` levait `IOException` quand aucun fichier JSON n'existait
sur S3 pour la partition du jour → `stats.errors` pollué → statut pipeline `partial`.

**Fix :**

- Ajout de `s3_has_files(cfg, bucket, prefix)` dans `shared.py` (MaxKeys=1, < 5ms)
- Guard appliqué dans `silver_temps._process()` → `logger.info({…, "status": "empty"})` + `return`

**Étendu :** Guard généralisé à **tous les scripts Silver** (voir section dédiée ci-dessous).

---

### ✅ T2 — gold_competences : fact_competences_dispo retournait 0 lignes

**Fichier :** [scripts/gold_competences.py](../scripts/gold_competences.py)

**Cause racine :** `silver_interimaires.py:67` force `agence_rattachement = NULL::INT`
→ `dim_int.rgpcnt_id` toujours NULL → `WHERE b.rgpcnt_id IS NOT NULL` filtrait 100% des lignes.

**Fix :** Suppression de `AND b.rgpcnt_id IS NOT NULL` dans la clause WHERE finale.
Les compétences remontent avec `rgpcnt_id = NULL` et `agence_sk = MD5(NULL)` (fallback déjà en place).
Quand `agence_rattachement` sera alimenté en Silver, la ventilation par agence sera automatique.

**Blocker résiduel :** `agence_rattachement` toujours NULL en Silver (source DDL Evolia absente). Voir B1.

---

### ✅ T3 — Validation CA mensuel systématiquement skippée

**Fichiers :** [scripts/gold_ca_mensuel.py](../scripts/gold_ca_mensuel.py) · [data/validation/pyramid_ca_mensuel.csv](../data/validation/pyramid_ca_mensuel.csv)

**Cause :** `data/validation/pyramid_ca_mensuel.csv` absent → warning à chaque run Gold.

**Fix :**

- Fichier CSV créé avec 12 mois (2025-04 → 2026-03), valeurs `ca_net_ht = 0` (non calibrées)
- Logique de validation ajustée : mois avec `ca_net_ht = 0` → statut `UNCALIBRATED` (ignorés)
  au lieu de générer de faux `FAIL` (ancien diviseur `max(abs(py), 1)` remplacé par `abs(py)`)

**Action requise :** Calibrer les valeurs de référence depuis le système legacy ou les premières
semaines de données Gold réelles. Remplacer les `0` mois par mois dans le CSV.

---

### ✅ T4 — SIRENE_API_TOKEN non documenté

**Fichiers :** [.env.example](../.env.example) · [docs/DEPLOY.md](DEPLOY.md)

**Fix :**

- Variable `SIRENE_API_TOKEN` ajoutée dans `.env.example` (section "Enrichissement externe")
- Section dédiée ajoutée dans `DEPLOY.md §8` : procédure INSEE complète (création app, OAuth2,
  durée de vie 7 jours, prérequis `data/sirets_clients.json`)

---

## Refactor — Guard source vide · Tous les pipelines Silver

**Fichier central :** [scripts/shared.py](../scripts/shared.py) — `s3_has_files(cfg, bucket, prefix)`

Comportement uniforme : si Bronze est vide pour la partition du jour → skip propre,
sans `stats.errors`, sans casser le statut du pipeline.

| Script | Source gardée | Niveau log | Particularité |
| --- | --- | --- | --- |
| `silver_temps.py` | `raw_{bronze}/{dp}/` par table | INFO | Refactoré depuis guard inline |
| `silver_agences_light.py` | `raw_pyregroupecnt/{dp}/` | INFO | Skip pipeline entier |
| `silver_clients.py` | `raw_wttieserv/{dp}/` | INFO | Skip early — SCD2 conserve Silver intact |
| `silver_clients_detail.py` | 3 × source spécifique | INFO | Guard indépendant par fonction |
| `silver_competences.py` | `raw_wtpmet/{dp}/` (ancre) | INFO | WTMET/WTTHAB en `/**/` non concernés |
| `silver_interimaires.py` | `raw_pypersonne/{dp}/` | INFO | Skip early — SCD2 conserve Silver intact |
| `silver_interimaires_detail.py` | 4 × source spécifique | INFO | Guard indépendant par fonction |
| `silver_factures.py` | `raw_{bronze}/{dp}/` par table | **WARNING** | Table critique CA |
| `silver_missions.py` | `raw_{bronze}/{dp}/` par table | **WARNING** | Table critique, includes FIN_MISSION |

---

## Blockers ouverts

### 🔴 B1 — agence_rattachement NULL dans silver_interimaires

**Impact :** `gold_competences.fact_competences_dispo` remonte les données mais sans ventilation
par agence (`rgpcnt_id = NULL`). `gold_staffing` et `gold_scorecard_agence` potentiellement affectés.

**Cause :** `RGPCNT_ID` absent du DDL Evolia `WTPINT` → `NULL::INT` hardcodé en Silver.
Source alternative probable : `WTUGPINT` (table `slv_interimaires/portefeuille_agences`).

**Mitigation :** Utiliser `WTUGPINT.RGPCNT_ID` comme source de `agence_rattachement` dans
`silver_interimaires.py` (jointure PER_ID). À tester en probe.

---

### 🟡 B2 — pyramid_ca_mensuel.csv non calibré

**Impact :** Validation Gold CA exécutée mais tous les mois en statut `UNCALIBRATED`.
Aucune vraie vérification de cohérence du CA jusqu'à calibration.

**Action :** Extraire les CA nets mensuels réels (12 derniers mois) depuis Gold PostgreSQL
et mettre à jour `data/validation/pyramid_ca_mensuel.csv`. Tolérance actuelle : 0.5%.

```sql
SELECT TO_CHAR(mois, 'YYYY-MM') AS mois, SUM(ca_net_ht::NUMERIC) AS ca_net_ht
FROM gld_commercial.fact_ca_mensuel_client
GROUP BY 1 ORDER BY 1 DESC LIMIT 12;
```

---

### 🟡 B3 — nb_heures_facturees et taux_moyen_fact NULL dans fact_ca_mensuel_client

**Fichier :** [scripts/gold_ca_mensuel.py](../scripts/gold_ca_mensuel.py) — TODO B-02 existant

**Cause :** `montant_ht` NULL en Silver factures → reconstitution via `SUM(lfac_base * lfac_taux)`
non encore implémentée. Colonnes `nb_heures_facturees` et `taux_moyen_fact` déclarées `NULL`.

---

### 🟡 B4 — data/sirets_clients.json absent

**Impact :** `bronze_clients_external.run_sirene()` skip silencieusement (warning non-bloquant).
L'enrichissement SIRENE ne s'exécute jamais même si `SIRENE_API_TOKEN` est configuré.

**Action :** Créer `data/sirets_clients.json` avec la liste des SIRETs clients actifs à enrichir
(tableau JSON de chaînes). Source probable : export `gld_commercial.fact_ca_mensuel_client`.

---

### 🟡 B5 — montant_regle NULL dans slv_clients/facturation_detail

**Impact :** `gold_recouvrement.fact_dso_client` — `encours_ht` surestimé (aucune déduction
des paiements). DSO potentiellement > réalité. `fact_balance_agee` non affecté (basé sur date_echeance).

**Cause :** Colonnes `EFAC_MNTPAI` / `EFAC_TYPEPAI` absentes du DDL Evolia WTEFAC (probe 2026-03-26).
`EFAC_TIERS` et `EFAC_AGENCE` également absents — remplacés par `TIE_ID` / `RGPCNT_ID`.

**Action :** `SELECT TOP 1 EFAC_MNTPAI, EFAC_TYPEPAI, EFAC_TIERS, EFAC_AGENCE FROM WTEFAC` en probe.
Si confirmées, ajouter dans `_COLS["WTEFAC"]` de `bronze_missions.py` et mettre à jour
`process_facturation_detail()` dans `silver_clients_detail.py`.

---

## Décisions d'architecture actives

| ID | Décision | Raison |
| --- | --- | --- |
| D01 | Silver → Parquet S3 (migré depuis Iceberg REST) | Iceberg REST OVH instable en prod ; Parquet + read_parquet DuckDB fiable |
| D02 | `s3_has_files` guard centralisé dans `shared.py` | DRY — single source of truth, < 5ms par appel (MaxKeys=1) |
| D03 | WARNING pour missions/factures, INFO pour le reste | Missions/factures = impact direct CA ; autres = données de référence |
| D04 | SCD2 skip early si Bronze vide (clients, interimaires) | Préserve le Silver existant intact — sémantique SCD2 correcte |
| D05 | `agence_sk` fallback = `MD5(rgpcnt_id)` si dim_agences inconnue | Évite les NULLs en Gold ; jointures Gold toujours possibles |
| D06 | `UNCALIBRATED` au lieu de `FAIL` pour mois sans référence | Évite faux positifs validation CA ; permet activation progressive |
| D07 | WTRHDON delta via `RHD_DATED` (proxy, sémantique date métier) | Pas de DATEMODIF en DDL ; limitation documentée |
| D08 | `WTUG.UG_GPS` : `CAST(… AS NVARCHAR(MAX))` | pyodbc ne supporte pas SQL Server type -151 (geography) |

---

## Prochaines actions recommandées

1. **[B1 — Priorité haute]** Alimenter `agence_rattachement` depuis `WTUGPINT` dans
   `silver_interimaires.py` → débloque la ventilation par agence dans tout le Gold.

2. **[B2 — Priorité moyenne]** Calibrer `pyramid_ca_mensuel.csv` avec les vraies valeurs
   Gold (requête SQL ci-dessus) → active la validation de cohérence CA.

3. **[B3 — Priorité moyenne]** Implémenter TODO B-02 dans `gold_ca_mensuel.py` :
   reconstitution `montant_ht = SUM(lfac_base * lfac_taux)` depuis `silver_factures/lignes_factures`.

4. **[B4 — Priorité basse]** Créer `data/sirets_clients.json` + configurer `SIRENE_API_TOKEN`
   → active l'enrichissement SIRENE en production.

5. **[Qualité]** Corriger les 2 warnings pyright dans `silver_competences.py` (lignes 153, 164) :
   `fetchone()` peut retourner `None` → utiliser `(fetchone() or (0,))[0]`.

---

## Roadmap KPI — Phases 3-5 (feature/kpi-completion)

> Phases 1 et 2 terminées — commit `6256a76` sur `feature/kpi-completion`.
> Catalogue KPI complet : [docs/CATALOGUE_KPI_GOLD.md](CATALOGUE_KPI_GOLD.md)

### ✅ Phase 3 — Recouvrement & DSO (tâches #15-#16) — commit `feature/kpi-completion`

#### ✅ Tâche #15 — Silver `slv_clients/facturation_detail`

**Fichier modifié** : [scripts/silver_clients_detail.py](../scripts/silver_clients_detail.py)
**Fonction ajoutée** : `process_facturation_detail()` — stats.tables_processed = 4

**Source Bronze** : `raw_wtefac` (ingéré par `bronze_missions.py` — non dupliqué)

**Colonnes produites** :

| Colonne | Source Bronze réelle | Note |
| ------- | -------------------- | ---- |
| `efac_num` | `EFAC_NUM` | VARCHAR |
| `tie_id` | `TIE_ID` | TRY_CAST AS INT (EFAC_TIERS absent DDL) |
| `rgpcnt_id` | `RGPCNT_ID` | TRY_CAST AS INT (EFAC_AGENCE absent DDL) |
| `date_paiement` | `EFAC_DTEREGLF` | TRY_CAST AS DATE (EFAC_DATPAI absent DDL) |
| `montant_regle` | `NULL` | EFAC_MNTPAI absent DDL — probe requis |
| `type_reglement` | `NULL` | EFAC_TYPEPAI absent DDL — probe requis |
| `retard_jours` | calculé | LEFT JOIN slv_facturation/factures sur efac_num |

> **Blocker B5** : `montant_regle` NULL — colonnes `EFAC_MNTPAI` / `EFAC_TYPEPAI` non confirmées
> dans le DDL Evolia WTEFAC (probe 2026-03-26). DSO surestimé jusqu'à confirmation.
> Action : probe `SELECT TOP 1 EFAC_MNTPAI FROM WTEFAC` sur Evolia pour valider.

#### ✅ Tâche #16 — Gold `gold_recouvrement.py`

**Fichier créé** : [scripts/gold_recouvrement.py](../scripts/gold_recouvrement.py)
**DDL ajouté** : section Phase 3 dans [scripts/ddl_gold_tables.sql](../scripts/ddl_gold_tables.sql)

**Tables Gold** : `gld_operationnel.fact_dso_client` + `gld_operationnel.fact_balance_agee`

**Logique DSO** : `encours_ht / (ca_mensuel / nb_jours_mois)` — snapshot = premier jour du mois courant.
**Balance âgée** : tranches `0-30j` / `31-60j` / `61-90j` / `>90j` sur `DATEDIFF(date_echeance, snapshot)`.

---

### 🔵 Phase 4 — Fidélisation intérimaires (tâches #17-#18) — À FAIRE

**Tâche #17** — `silver_interimaires_detail.py` : enrichir `slv_interimaires/fidelisation` avec
`taux_fidelisation_pct` = nb_missions_12m / NULLIF(anciennete_mois, 0).

**Tâche #18** — `gold_staffing.py` : compléter `fact_fidelisation_interimaires` avec
`taux_fidelisation_pct` (ajouter colonne en DDL via `ALTER TABLE … ADD COLUMN IF NOT EXISTS`).

---

### Phase 5 — Finitions (tâches #19-#21)

**Tâche #19** — `gold_ca_mensuel.py` : remplir `nb_heures_facturees` / `taux_moyen_fact`
via `WTLFAC` (voir TODO B-02 ligne 18 — actuellement `NULL::DECIMAL`).

**Tâche #20** — `fact_concentration_client` : ajouter `ca_net_top5` + `taux_concentration_top5`
(PERCENT_RANK <= 0.05).

**Tâche #21** — Finaliser DDL : index manquants, `COMMENT ON COLUMN`, nettoyage migrations.
