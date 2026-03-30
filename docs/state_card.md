# State Card — Pipeline GI Data Platform

> Dernière mise à jour : **2026-03-30** · Auteur : M. COULIBALY (session B6b — JSON type accumulation SCD2 + Gold Green 12/12)
> Architecture : Bronze (Evolia/SQL Server) → Silver (Parquet S3) → Gold (PostgreSQL)

---

## Statut global

| Couche | Statut | Notes |
| --- | --- | --- |
| Bronze | ✅ Stable | 30+ tables · FreeTDS/pymssql · frdc1pipeline01 |
| Silver | ✅ Stable | `agence_rattachement` BIGINT propagé · `effectif_tranche` corrigé · 1.24M dim_interimaires · 1.56M dim_clients |
| Gold | ✅ Green | **12/12 pipelines · 0 erreur · 0 rows_rejected** · dernière run 2026-03-30 |
| Airflow | ✅ Actif | `gi_pipeline` · LocalExecutor · 05h00 UTC |
| Git | ✅ Synchro | `main` @ 6816630 · remote GitHub à jour |

---

## Session 2026-03-30 — B6b + Gold Green 12/12

### ✅ B6b — JSON type accumulation dans SCD2 JSONL (RÉSOLU 2026-03-30)

**Cause :** DuckDB `read_json_auto` inférait `JSON` (pas `BIGINT`/`VARCHAR`) quand la fenêtre de sampling ne voyait que des `null` — `new_records` étaient placés en fin de fichier JSONL après 1.1M+ lignes existantes. Chaque re-run SCD2 ajoutait un niveau d'encodage JSON supplémentaire (5 niveaux détectés sur `agence_rattachement`).

**Fix :**

| Fichier | Correctif |
| --- | --- |
| `silver_interimaires.py` | Décode `agence_rattachement` JSON imbriqué→int + `new_records` en tête de `all_records` |
| `silver_clients.py` | Idem pour `effectif_tranche` JSON imbriqué→str |
| `gold_dimensions.py` | `TRY_CAST(agence_rattachement AS INTEGER)` + `LEFT(effectif_tranche, 50)` |
| `gold_competences.py` | `TRY_CAST(agence_rattachement::VARCHAR AS INTEGER)` dans CTE `dim_int` |
| `gold_vue360_client.py` | `LEFT(effectif_tranche, 50)` colonne `effectif` |

**Résultat :** `agence_rattachement` = `BIGINT` · 519 267 valeurs renseignées sur 522 278 actifs. Gold 12/12 Green.

---

### ✅ B6 — Guard s3_has_files + `SILVER_DATE_PARTITION='**'` (RÉSOLU 2026-03-30)

**Fix :** `shared.py` — bypass `if "**" in prefix: return True`.
Silver full-history relancé avec succès.

---

## Session 2026-03-27 — B1 + B3 + Phase 4 + Phase 5

### ✅ DDL appliqué (avnadmin)

| Table | Résultat |
| --- | --- |
| `gld_shared.dim_habilitations` | **CREATE TABLE** (nouvelle) |
| `gld_shared.dim_diplomes` | **CREATE TABLE** (nouvelle) |
| `gld_staffing.fact_fidelisation_interimaires` | **ALTER TABLE** — colonne `taux_fidelisation_pct DECIMAL(10,4)` ajoutée |
| Toutes les autres | NOTICE "already exists" — safe |

---

### ✅ Phase 5 — dim_habilitations + dim_diplomes en Gold (#22-#23)

**Fichiers :** [scripts/gold_dimensions.py](../scripts/gold_dimensions.py) · [scripts/ddl_gold_tables.sql](../scripts/ddl_gold_tables.sql)

**Source :** `slv_interimaires/competences` filtré par `type_competence` (déjà peuplé par `silver_competences.py`).

| Dimension | PK | Colonnes | Source Silver |
| --- | --- | --- | --- |
| `gld_shared.dim_habilitations` | `thab_id` | habilitation_sk, thab_id, libelle, is_active | type=HABILITATION · WTTHAB |
| `gld_shared.dim_diplomes` | `tdip_id` | diplome_sk, tdip_id, libelle, niveau, is_active | type=DIPLOME · WTTDIP |

**Actif dans le DAG :** `gold_dimensions` les inclut dans `DIMENSIONS` + `builders`. Elles seront alimentées au prochain run Gold.

---

### ✅ Phase 4 — taux_fidelisation_pct (#17-#18)

**Fichiers :** [scripts/silver_interimaires_detail.py](../scripts/silver_interimaires_detail.py) · [scripts/gold_staffing.py](../scripts/gold_staffing.py)

**Silver :** `process_fidelisation` enrichi avec CTE `missions_12m` → `nb_missions_12m` + `taux_fidelisation_pct = nb_missions_12m / NULLIF(anciennete_jours/30, 0)`.

**Gold :** `build_fidelisation_query` agrège `AVG(taux_fidelisation_pct)` par `(agence_id, categorie_fidelisation)`.

**DDL :** `ALTER TABLE fact_fidelisation_interimaires ADD COLUMN IF NOT EXISTS taux_fidelisation_pct` — appliqué ✅.

**Action requise :** relancer `silver_interimaires_detail.py` (voir section "Prochaines actions").

---

### ✅ B3 — nb_heures_facturees + taux_moyen_fact (TODO B-02)

**Fichier :** [scripts/gold_ca_mensuel.py](../scripts/gold_ca_mensuel.py)

**Fix :** CTE `heures_fac` — `SUM(base)` depuis `slv_facturation/lignes_factures` (WTLFAC.LFAC_BASE = quantité heures).

| Colonne | Avant | Après |
| --- | --- | --- |
| `nb_heures_facturees` | `NULL::DECIMAL(10,2)` | `SUM(heures_ht) DECIMAL(12,2)` |
| `taux_moyen_fact` | `NULL::DECIMAL(10,2)` | `ca_ht / NULLIF(heures_ht, 0) DECIMAL(10,4)` |

**Action requise :** relancer `./run_pipeline.sh gold` (pas de DDL nécessaire).

---

### ✅ B1 — agence_rattachement depuis WTUGPINT

**Fichier :** [scripts/silver_interimaires.py](../scripts/silver_interimaires.py)

**Fix :** CTE `raw_ugpint` lit `raw_wtugpint/**/*.json` (full-history Bronze) → `RGPCNT_ID` le plus récent par `per_id`. Boucle SCD2 : `None` → `rd.get("agence_rattachement")`.

**Run tenté :** `SILVER_DATE_PARTITION='**' python scripts/silver_interimaires.py` → `status: empty`

**Cause :** Guard `s3_has_files(cfg, bucket, f"raw_pypersonne/{cfg.date_partition}/")` avec `date_partition='**'` cherche le prefix S3 littéral `raw_pypersonne/**/` — inexistant (S3 ne comprend pas les globs dans les préfixes de listing). Voir Blocker B6.

**Action requise :** relancer avec une date réelle (voir B6).

---

### ✅ Airflow — déploiement complet

| Action | Résultat |
| --- | --- |
| `dags_folder` → `/opt/groupe-interaction/etl/gi-data-plateform/dags` | ✅ airflow.cfg |
| Symlink `dag_gi_pipeline.py` corrigé | ✅ `gi-data-plateform` (sans typo) |
| 72 DAGs exemple supprimés de la DB | ✅ |
| `schedule_interval` → `schedule` (Airflow 3.x) | ✅ |
| `sla=` retiré du BashOperator | ✅ |
| Python `.venv/bin/python` explicite | ✅ |
| `gold_qualite_missions` + `gold_recouvrement` ajoutés au DAG | ✅ |
| Scheduler + Webserver redémarrés | ✅ |
| `airflow dags reserialize` → `Sync 1 DAGs : gi_pipeline` | ✅ |

---

## Blockers ouverts

### ✅ B6 — Guard s3_has_files incompatible avec SILVER_DATE_PARTITION='**' (RÉSOLU 2026-03-27)

**Fix :** `shared.py` — `s3_has_files` retourne `True` si `"**" in prefix` (bypass guard, S3 ne supporte pas les globs dans les préfixes de listing).

---

### ✅ B1 — agence_rattachement propagé en Gold (RÉSOLU 2026-03-30)

Silver régénéré (B6b fix) · `agence_rattachement` = BIGINT · 519 267 valeurs renseignées dans `dim_interimaires`.

---

### 🟡 B2 — pyramid_ca_mensuel.csv non calibré

**Action :** Extraire les CA nets mensuels réels depuis Gold PostgreSQL :

```sql
SELECT TO_CHAR(mois, 'YYYY-MM') AS mois, SUM(ca_net_ht::NUMERIC) AS ca_net_ht
FROM gld_commercial.fact_ca_mensuel_client
GROUP BY 1 ORDER BY 1 DESC LIMIT 12;
```

---

### ✅ B3 — nb_heures_facturees + taux_moyen_fact (PROPAGÉ 2026-03-30)

`gold_ca_mensuel` actif · 291 lignes `fact_ca_mensuel_client`.

---

### ✅ B4 — Enrichissement clients SIRENE (FERMÉ — non pertinent)

**Décision 2026-03-30 :** Salesforce est la référence client. SIRENE supprimé de la roadmap.

- `SALESFORCE_ENABLED=1` configuré dans `.env` · SF tourne quotidiennement
- `naf_libelle` ← `Code_NAF__r.name` (SF) · 103 K/150 K actifs couverts (68%)
- 32% gap = clients Evolia sans `Id_Evolia__c` dans SF → sujet gouvernance métier, pas technique

---

### ✅ B5 — montant_regle NULL (RÉSOLU 2026-03-26)

Colonnes `EFAC_MNTPAI` / `EFAC_TYPEPAI` définitivement absentes du DDL Evolia. Décision finale.

---

## Décisions d'architecture actives

| ID | Décision | Raison |
| --- | --- | --- |
| D01 | Silver → Parquet S3 (migré depuis Iceberg REST) | Iceberg REST OVH instable en prod |
| D02 | `s3_has_files` guard centralisé dans `shared.py` | DRY — < 5ms par appel (MaxKeys=1) |
| D03 | WARNING pour missions/factures, INFO pour le reste | Missions/factures = impact direct CA |
| D04 | SCD2 skip early si Bronze vide (clients, interimaires) | Préserve le Silver existant intact |
| D05 | `agence_sk` fallback = `MD5(rgpcnt_id)` si dim_agences inconnue | Évite les NULLs en Gold |
| D06 | `UNCALIBRATED` au lieu de `FAIL` pour mois sans référence | Évite faux positifs validation CA |
| D07 | WTRHDON delta via `RHD_DATED` (proxy, sémantique date métier) | Pas de DATEMODIF en DDL |
| D08 | `WTUG.UG_GPS` : `CAST(… AS NVARCHAR(MAX))` | pyodbc ne supporte pas SQL Server type -151 |
| D09 | Airflow standalone sur frdc1pipeline01 (LocalExecutor) | Bronze nécessite accès LAN → SRV-SQL2:1433 |
| D10 | Python venv pipeline explicite dans BashOperator | Évite collision Python 3.11 Airflow / 3.12 pipeline |
| D11 | `raw_wtugpint/**/*.json` full-history dans silver_interimaires | WTUGPINT full-load, pas de delta DDL |

---

## Roadmap KPI

### ✅ Phase 1 — Opérationnel (tâches #1-#8) — TERMINÉ

### ✅ Phase 2 — Qualité missions (tâches #9-#14) — TERMINÉ

### ✅ Phase 3 — Recouvrement DSO (tâches #15-#16) — TERMINÉ

### ✅ Phase 4 — Fidélisation intérimaires (tâches #17-#18) — PROPAGÉ EN PROD (2026-03-30)

`taux_fidelisation_pct` alimenté · `fact_fidelisation_interimaires` : 450 lignes.

### ✅ Phase 5 — Référentiels partagés (tâches #22-#23) — PROPAGÉ EN PROD (2026-03-30)

`dim_habilitations` : 266 · `dim_diplomes` : 36.

### 🔵 Phase 6 — FastAPI serving layer (tâche #24)

Serving layer complet sur Gold PostgreSQL — toutes les dimensions + KPIs.

| Endpoint | Périmètre |
| --- | --- |
| `/clients/{tie_id}` | vue_360_client + fact_retention_client + fact_ca_mensuel |
| `/interimaires/{per_id}` | identité + lieu + qualification + missions + habilitations + diplômes |
| `/agences/{agence_id}` | scorecard_agence + ranking + tendances |
| `/metiers/{metier_id}` | dim_metiers + fact_competences_dispo |
| `/referentiels/` | dim_habilitations + dim_diplomes + dim_metiers |
| `/kpis/agence/{agence_id}` | CA, DSO, fidélisation, conformité DPAE |

### 🔵 Phase 7 — Finitions (tâches #19-#21)

- **#19** : ~~`nb_heures_facturees` via WTLFAC~~ → **RÉSOLU en B3**
- **#20** : `fact_concentration_client` — ajouter `ca_net_top5` + `taux_concentration_top5`
- **#21** : DDL — index manquants, `COMMENT ON COLUMN`, nettoyage migrations

---

## Prochaines actions recommandées

| Priorité | Jalon | Action | Statut |
| --- | --- | --- | --- |
| 🔴 Haute | Jalon 1 | **B2** — Calibrer `pyramid_ca_mensuel.csv` (requête SQL Gold PG) | 🟡 En cours |
| 🟡 Moyenne | Jalon 1 | **Phase 7 #20** — `fact_concentration_client` : `ca_net_top5` + `taux_concentration_top5` | ⏳ Pending |
| 🟡 Moyenne | Jalon 1 | **Phase 7 #21** — DDL finitions : index manquants + `COMMENT ON COLUMN` | ⏳ Pending |
| 🟡 Moyenne | Jalon 1 | **Superset** — Connexion datasource PostgreSQL Gold + dashboards KPI | ⏳ Pending |
| 🔵 Basse | Jalon 2 | **Phase 6** — FastAPI serving layer : toutes dimensions + KPIs | ⏳ Pending |
| 🔵 Phase 2+ | Architecture | Migration Silver Parquet → Iceberg · K8s · Kafka · MDM | — |
