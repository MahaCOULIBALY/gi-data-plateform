# GI Data Platform

> Pipeline de données **Evolia → Bronze → Silver → Gold** sur OVHcloud.
> Architecture Medallion · DuckDB · S3 Object Storage · PostgreSQL 16 · Airflow managé.

---

## Table des matières

1. [Vue d'ensemble](#vue-densemble)
2. [Architecture](#architecture)
3. [Stack technique](#stack-technique)
4. [Structure du projet](#structure-du-projet)
5. [Démarrage rapide](#démarrage-rapide)
6. [Configuration](#configuration)
7. [Run Modes](#run-modes)
8. [Scripts — inventaire](#scripts--inventaire)
9. [Chemins S3](#chemins-s3)
10. [Watermarks & delta ingestion](#watermarks--delta-ingestion)
11. [RGPD](#rgpd)
12. [FinOps](#finops)
13. [Tests](#tests)
14. [Dette technique](#dette-technique)
15. [Décisions d'architecture](#décisions-darchitecture)

---

## Vue d'ensemble

Ce projet implémente un **Data Lakehouse** pour le Groupe Interaction (GI) qui extrait les données de l'ERP **Evolia** (SQL Server on-prem), les transforme en couches progressives et les expose dans un entrepôt PostgreSQL pour Superset et les équipes métier.

**Domaines couverts :**

| Domaine | Tables source Evolia | Destination Gold |
| --- | --- | --- |
| Agences | PYREGROUPECNT, PYSECTEUR | `gld_shared.dim_agences` |
| Clients | CMTIERS, WTTIESERV, WTCLPT, WTCOEF… | `gld_clients.*` |
| Intérimaires | PYPERSONNE, PYSALARIE, WTPINT… | `gld_shared.dim_interimaires` |
| Missions | WTMISS, WTCNTI, WTCMD, WTPLAC, PYCONTRAT | `gld_operationnel.*` |
| Temps | WTPRH, WTRHDON | `gld_operationnel.*` |
| Facturation | WTEFAC, WTLFAC | `gld_commercial.*` |
| Compétences | WTPMET, WTPHAB, WTPDIP, WTEXP | `gld_shared.dim_metiers` |

---

## Architecture

```text
┌─────────────────────────────────────────────────────────────────┐
│  SOURCE  │  Evolia ERP — SQL Server on-prem (VPN GI Siège)      │
└────────────────────────────┬────────────────────────────────────┘
                             │ pyodbc · ODBC Driver 18
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│  BRONZE  │  s3://gi-poc-bronze/raw_{table}/{YYYY/MM/DD}/*.json  │
│          │  Format : NDJSON · Enrichi : _loaded_at, _batch_id   │
│          │  Stratégie : DELTA (col *_DATEMODIF) ou FULL-LOAD     │
│          │  Chunking : 100 000 lignes/fichier (limite OVH S3)    │
└────────────────────────────┬────────────────────────────────────┘
                             │ DuckDB · read_json_auto()
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│  SILVER  │  s3://gi-poc-silver/slv_{domaine}/{table}/**/*.parquet│
│          │  Format : Parquet ZSTD · Dédup ROW_NUMBER() · SCD2    │
│          │  Partition lue : /{date_partition}/ (FinOps)          │
│          │  RGPD : NIR pseudonymisé SHA-256 · coords Silver-only │
└────────────────────────────┬────────────────────────────────────┘
                             │ DuckDB · read_parquet()
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│   GOLD   │  PostgreSQL 16 OVHcloud (Essential-4 GRA)            │
│          │  Schémas : gld_commercial · gld_staffing              │
│          │            gld_performance · gld_clients              │
│          │            gld_operationnel · gld_shared              │
│          │  Chargement : execute_values batch 500 + upsert       │
└─────────────────────────────────────────────────────────────────┘
                             │ Superset · Équipes métier
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│ OBSERV.  │  ops.pipeline_watermarks (PostgreSQL)                 │
│          │  WatermarkStore · pipeline/table · last_success · rows│
└─────────────────────────────────────────────────────────────────┘
```

**Orchestration :** Managed Airflow OVHcloud — DAG `gi_poc_pipeline`
**Exécution Bronze :** On-prem ou laptop + VPN (accès SQL Server)
**Exécution Silver/Gold :** Workers Airflow OVH (accès S3 + PostgreSQL)

---

## Stack technique

| Couche | Technologie | Version |
| --- | --- | --- |
| Python | CPython | ≥ 3.12 |
| Extraction ERP | pyodbc | ≥ 5.0 |
| Object Storage | boto3 (OVH S3) | ≥ 1.34 |
| Transformation | DuckDB | ≥ 1.1 |
| Entrepôt Gold | psycopg2 (PostgreSQL 16) | ≥ 2.9.9 |
| Env management | python-dotenv | ≥ 1.0 |
| Packaging | uv + hatchling | — |
| Lint / Format | Ruff | ≥ 0.4 |
| Type checking | mypy | ≥ 1.10 |
| Tests | pytest + pytest-cov | ≥ 8.0 |
| Qualité données | Great Expectations *(optionnel)* | ≥ 0.18 |
| Orchestration | Managed Airflow OVHcloud | — |

---

## Structure du projet

```
gi-data-platform/
├── scripts/
│   ├── shared.py                    # Config · RunMode · Stats · connexions · helpers RGPD
│   ├── pipeline_utils.py            # WatermarkStore · with_retry (ParamSpec générique)
│   │
│   ├── bronze_agences.py            # PYREGROUPECNT · PYSECTEUR · WTUG → S3
│   ├── bronze_clients.py            # CMTIERS · WTTIESERV · WTCLPT · WTCOEF… → S3
│   ├── bronze_interimaires.py       # PYPERSONNE · PYSALARIE · WTPEVAL… → S3
│   ├── bronze_missions.py           # WTMISS · WTCNTI · WTCMD · PYCONTRAT… → S3
│   ├── bronze_clients_external.py   # Sources externes (Excel Agence Gestion…)
│   │
│   ├── silver_agences_light.py      # → slv_agences/dim_agences + hierarchie_territoriale
│   ├── silver_clients.py            # → slv_clients/dim_clients (SCD Type 2)
│   ├── silver_clients_detail.py     # → sites_mission · contacts · encours · coefficients
│   ├── silver_competences.py        # → slv_interimaires/competences
│   ├── silver_factures.py           # → slv_facturation/factures + lignes_factures
│   ├── silver_interimaires.py       # → slv_interimaires/dim_interimaires (SCD Type 2)
│   ├── silver_interimaires_detail.py# → evaluations · coordonnees · portefeuille_agences
│   ├── silver_missions.py           # → slv_missions/missions · contrats · commandes · placements
│   ├── silver_temps.py              # → slv_temps/releves_heures · heures_detail
│   │
│   ├── gold_dimensions.py           # → gld_shared : 5 dimensions conformed
│   ├── gold_ca_mensuel.py           # → gld_commercial.fact_ca_mensuel_client
│   ├── gold_clients_detail.py       # → gld_clients.vue_360_client + fact_retention_client
│   ├── gold_competences.py          # → gld_shared.fact_competences_actives
│   ├── gold_operationnel.py         # → gld_operationnel.fact_missions_detail
│   ├── gold_retention_client.py     # → gld_clients.fact_retention_client
│   ├── gold_scorecard_agence.py     # → gld_performance.scorecard_agence
│   ├── gold_staffing.py             # → gld_staffing.fact_heures_mission
│   ├── gold_vue360_client.py        # → gld_clients.vue_360_client
│   │
│   ├── probe_ddl.py                 # Sonde DDL Evolia → ddl_probe_result.json
│   ├── probe_ddl_schema.py          # Schéma complet table par table
│   ├── enrich_ban_geocode.py        # Enrichissement géocodage BAN API
│   ├── rgpd_audit.py                # Audit champs sensibles par regex
│   ├── migrate_watermarks.py        # Migration table ops.pipeline_watermarks
│   └── purge_bronze.py              # Purge partitions Bronze > rétention
│
├── docs/
│   ├── state_card.md                # Journal des décisions architecture (JSON)
│   ├── TESTING.md                   # Guide de tests complet (9 sections)
│   ├── DDL_EVOLIA_FILTERED.sql      # DDL réel Evolia (post-probe 2026-03-05)
│   ├── superset_360_client_setup.md
│   ├── superset_360_interimaire_setup.md
│   └── superset_performance_agences_setup.md
│
├── pyproject.toml                   # Dépendances · Ruff · pytest · mypy · coverage
├── uv.lock
└── README.md
```

---

## Démarrage rapide

### Prérequis

- Python 3.12+
- [uv](https://docs.astral.sh/uv/) (gestionnaire de paquets recommandé)
- ODBC Driver 18 for SQL Server (extraction Bronze on-prem)
- Accès VPN GI Siège (Bronze uniquement)

### Installation

```bash
# Cloner
git clone <repo-url> && cd gi-data-platform

# Installer les dépendances (prod)
uv sync

# Avec les outils dev (lint, tests, mypy)
uv sync --group dev

# Groupes optionnels
uv sync --extra quality   # Great Expectations
uv sync --extra dbt       # dbt-postgres (Phase 2+)
uv sync --extra geo       # Géocodage BAN
```

### Configuration des secrets

```bash
# Copier le template
cp .env.example .env.local

# Renseigner les variables (voir section Configuration ci-dessous)
$EDITOR .env.local
```

### Premier test (sans connexion)

```bash
RUN_MODE=offline python scripts/bronze_missions.py
# Attendu : logs JSON {"mode":"offline","table":"WTMISS"} pour chaque table
```

---

## Configuration

Toutes les variables sont chargées depuis `.env.local` (dev) ou `.env` (CI/prod) via `python-dotenv`.
La priorité est : **variable d'environnement > fichier .env**.

### Variables obligatoires

| Variable | Description |
|---|---|
| `EVOLIA_SERVER` | Hôte SQL Server Evolia (`serveur,port`) |
| `EVOLIA_DB` | Base de données Evolia |
| `EVOLIA_USER` | Compte SQL Server |
| `EVOLIA_PASSWORD` | Mot de passe SQL Server |
| `OVH_S3_ENDPOINT` | Endpoint OVH S3 (`https://s3.gra.perf.cloud.ovh.net`) |
| `OVH_S3_ACCESS_KEY` | Clé d'accès S3 |
| `OVH_S3_SECRET_KEY` | Clé secrète S3 |

### Variables optionnelles

| Variable | Défaut | Description |
| --- | --- | --- |
| `RUN_MODE` | `live` | `offline` \| `probe` \| `live` |
| `OVH_PG_HOST` | *(défini dans Config)* | Hôte PostgreSQL OVHcloud |
| `OVH_PG_PORT` | `20184` | Port PostgreSQL |
| `OVH_PG_DATABASE` | `gi_poc_ddi_gold` | Base Gold |
| `OVH_PG_USER` | — | Utilisateur PostgreSQL |
| `OVH_PG_PASSWORD` | — | Mot de passe PostgreSQL |
| `RGPD_SALT` | **obligatoire en LIVE** | Salt SHA-256 NIR (min 32 chars) |
| `SILVER_DATE_PARTITION` | Date du jour `YYYY/MM/DD` | Override partition Bronze lue par Silver (backfill) |
| `TABLE_FILTER` | *(toutes)* | `WTMISS,WTCNTI` — restreint les tables traitées |
| `ALERT_EMAIL` | `data-team@groupe-interaction.fr` | Email alertes pipeline |

> **Sécurité** : Le pipeline refuse de démarrer en mode `LIVE` si `RGPD_SALT` est absent ou égal à la valeur sentinelle par défaut.

---

## Run Modes

Le pipeline supporte trois niveaux d'exécution contrôlés par `RUN_MODE` :

| Mode | Connexions | Lectures | Écritures | Usage |
| --- | --- | --- | --- | --- |
| `offline` | Aucune | Aucune | Aucune | CI/CD · tests unitaires · dev hors réseau |
| `probe` | Actives | Oui (Evolia + S3) | **Non** | Validation DDL · comptages · dry-run Silver |
| `live` | Actives | Oui | **Oui** | Production |

```bash
# Offline — zéro dépendance externe
RUN_MODE=offline python scripts/bronze_missions.py

# Probe — compter les lignes sans écrire
RUN_MODE=probe python scripts/bronze_interimaires.py

# Live — exécution complète
RUN_MODE=live python scripts/silver_clients.py
```

**Backfill / Rejeu d'une partition :**

```bash
SILVER_DATE_PARTITION=2026/03/01 RUN_MODE=probe python scripts/silver_factures.py
```

---

## Scripts — inventaire

### Bronze — extraction Evolia → S3

Chaque script Bronze charge ses tables vers `s3://gi-poc-bronze/raw_{table}/{YYYY/MM/DD}/`.
La stratégie **DELTA** (colonne `*_DATEMODIF` confirmée par probe DDL) ou **FULL-LOAD** est indiquée.

| Script | Tables | Stratégie |
| --- | --- | --- |
| `bronze_agences.py` | PYREGROUPECNT, PYENTREPRISE | DELTA |
| | PYSECTEUR, WTUG, WTUGPINT | FULL |
| `bronze_clients.py` | CMTIERS, WTTIESERV, WTCLPT, WTTIEINT, WTCOEF, WTENCOURSG, WTUGCLI, WTUGAG | FULL |
| `bronze_interimaires.py` | WTPEVAL | DELTA |
| | PYPERSONNE, PYSALARIE, WTPINT, PYCOORDONNEE, WTPMET, WTPHAB, WTPDIP, WTEXP, WTUGPINT | FULL |
| `bronze_missions.py` | WTMISS, WTCNTI, WTCMD, WTPRH | DELTA |
| | WTPLAC, PYCONTRAT, WTRHDON | FULL |

> **Chunking** : les extractions volumineuses sont découpées en fichiers de 100 000 lignes pour respecter la limite `EntityTooLarge` d'OVH S3.

### Silver — transformation → Parquet

| Script | Tables Silver produites |
|---|---|
| `silver_agences_light.py` | `slv_agences/dim_agences` · `slv_agences/hierarchie_territoriale` |
| `silver_clients.py` | `slv_clients/dim_clients` (SCD Type 2) |
| `silver_clients_detail.py` | `sites_mission` · `contacts` · `encours_credit` · `coefficients` |
| `silver_competences.py` | `slv_interimaires/metiers` · `habilitations` · `diplomes` · `experiences` |
| `silver_factures.py` | `slv_facturation/factures` · `lignes_factures` |
| `silver_interimaires.py` | `slv_interimaires/dim_interimaires` (SCD Type 2) |
| `silver_interimaires_detail.py` | `evaluations` · `coordonnees` · `portefeuille_agences` |
| `silver_missions.py` | `slv_missions/missions` · `contrats` · `commandes` · `placements` · `contrats_paie` |
| `silver_temps.py` | `slv_temps/releves_heures` · `heures_detail` |

### Gold — agrégation → PostgreSQL

| Script | Tables Gold produites | Schéma |
| --- | --- | --- |
| `gold_dimensions.py` | `dim_calendrier` · `dim_agences` · `dim_clients` · `dim_interimaires` · `dim_metiers` | `gld_shared` |
| `gold_ca_mensuel.py` | `fact_ca_mensuel_client` | `gld_commercial` |
| `gold_clients_detail.py` | `vue_360_client` · `fact_retention_client` | `gld_clients` |
| `gold_scorecard_agence.py` | `scorecard_agence` | `gld_performance` |
| `gold_staffing.py` | `fact_heures_mission` | `gld_staffing` |
| `gold_operationnel.py` | `fact_missions_detail` | `gld_operationnel` |
| `gold_retention_client.py` | `fact_retention_client` | `gld_clients` |
| `gold_vue360_client.py` | `vue_360_client` | `gld_clients` |
| `gold_competences.py` | `fact_competences_actives` | `gld_shared` |

### Utilitaires

| Script | Rôle |
|---|---|
| `probe_ddl.py` | Sonde toutes les colonnes DDL Evolia — produit `ddl_probe_result.json` |
| `probe_ddl_schema.py` | Dump schéma complet par table |
| `enrich_ban_geocode.py` | Géocodage adresses via API BAN (lat/lon → Silver) |
| `rgpd_audit.py` | Scan regex champs sensibles dans S3 Bronze/Silver |
| `migrate_watermarks.py` | Migration DDL `ops.pipeline_watermarks` |
| `purge_bronze.py` | Purge partitions Bronze hors rétention |

---

## Chemins S3

```text
Bronze  : s3://gi-poc-bronze/raw_{table}/{YYYY}/{MM}/{DD}/batch_{id}_{chunk:04d}.json
Silver  : s3://gi-poc-silver/slv_{domaine}/{table}/**/*.parquet
Gold    : PostgreSQL — schemas gld_*
Ops     : ops.pipeline_watermarks (PostgreSQL)
```

**Override de partition (backfill) :**

```bash
# Silver lit Bronze du 1er mars au lieu d'aujourd'hui
SILVER_DATE_PARTITION=2026/03/01 python scripts/silver_missions.py
```

**Alias Silver canoniques** (utilisés par Gold) :

| Table Silver | Colonnes clés |
|---|---|
| `slv_facturation/factures` | `efac_num, tie_id, ties_serv, rgpcnt_id, type_facture, date_facture, montant_ht` (NULL — voir dette DT-02) |
| `slv_facturation/lignes_factures` | `fac_num, lfac_ord, libelle, lfac_base, lfac_taux, lfac_mnt` |
| `slv_temps/releves_heures` | `prh_bts, per_id, cnt_id, tie_id, date_debut, date_fin, valide` |
| `slv_missions/missions` | `per_id, cnt_id, tie_id, ties_serv, rgpcnt_id, date_debut, date_fin, code_fin` |
| `slv_missions/contrats` | `per_id, cnt_id, ordre, met_id, taux_horaire_paye, taux_horaire_fact, duree_hebdo` |
| `slv_clients/dim_clients` | `tie_id, client_sk, raison_sociale, siren, naf_code, ville, is_current` |
| `slv_agences/dim_agences` | `rgpcnt_id, agence_sk, nom, is_active` |

---

## Watermarks & delta ingestion

La table `ops.pipeline_watermarks` (PostgreSQL) enregistre l'état de chaque extraction :

```sql
SELECT pipeline, table_name, last_success, last_status, rows_ingested
FROM ops.pipeline_watermarks
ORDER BY updated_at DESC;
```

**Comportement :**

- `WatermarkStore.get(table)` → retourne `None` si jamais exécuté (→ `FALLBACK_SINCE = 2023-01-01`)
- `WatermarkStore.set(table, ts, rows)` → upsert succès, incrémente `run_count` + `rows_ingested`
- `WatermarkStore.mark_failed(table, msg)` → enregistre l'échec **sans modifier** `last_success` (borne delta préservée)
- Sentinel epoch `1970-01-01` utilisé pour `last_success` lors du premier échec (empêche un delta à `NOW()` après un run raté)

**Retry automatique** (`@with_retry`) :

- 3 tentatives par défaut · backoff exponentiel (2s, 4s)
- Exceptions non-transientes (`KeyError`, `ValueError`, `TypeError`…) remontent immédiatement sans retry

---

## RGPD

| Niveau | Données | Traitement |
| --- | --- | --- |
| Bronze | NIR (PER_NIR) · Téléphones (PER_TEL_NTEL) | Stockés bruts, flag `_rgpd_flag=SENSIBLE` |
| Silver | NIR | Pseudonymisé SHA-256 + `RGPD_SALT` (256 bits, sans troncature) |
| Silver | Coordonnées (PYCOORDONNEE) · Contacts clients (WTTIEINT) | **Silver-only** — jamais exposés en Gold |
| Gold | Tout champ `SENSIBLE` ou `PERSONNEL` | **Exclu** |

```python
# Pseudonymisation NIR — shared.py
pseudonymize_nir(nir="1234567890123", salt=cfg.rgpd_salt)
# → SHA-256 hex 64 chars (résistant collision)
```

**Garde-fou LIVE :** la `Config.__post_init__` lève une `ValueError` si `RGPD_SALT` est absent ou égal au sentinel en mode LIVE. Le pipeline ne démarre pas.

---

## FinOps

La lecture Bronze par Silver est **partitionnée par date** pour éviter les full-scans S3 coûteux :

```python
# shared.py — s3_bronze()
f"s3://{cfg.bucket_bronze}/raw_{table}/{cfg.date_partition}/*.json"
# → lit uniquement la partition du jour, pas raw_wtmiss/**/*.json
```

| Optimisation | Économie |
|---|---|
| Partition date Silver → Bronze | Évite full-scan S3 quotidien (~80% requêtes en moins) |
| DuckDB au lieu de Spark | ~80 €/mois économisés sur le volume Phase 0 (3 Go/j delta) |
| Chunking 100 000 lignes | Évite EntityTooLarge OVH · pas de retransmission |
| boto3 singleton `@lru_cache` | Une seule connexion S3 par processus |
| Credentials DuckDB via `CREATE SECRET` | Évite les credentials en clair dans les logs |

---

## Tests

Voir **[docs/TESTING.md](docs/TESTING.md)** pour le guide complet (9 sections).

### Commandes essentielles

```bash
# Lint + format (CI-equivalent)
uv run ruff check scripts/ && uv run ruff format --check scripts/

# Type checking
uv run mypy scripts/ --ignore-missing-imports

# Tests unitaires (zéro I/O)
RUN_MODE=offline uv run pytest tests/ -m unit -v --cov=scripts

# Probe DDL Evolia (nécessite VPN + .env.local)
RUN_MODE=probe python scripts/probe_ddl.py

# Dry-run Silver (nécessite Bronze S3 non vide)
RUN_MODE=probe python scripts/silver_missions.py

# Test backfill
SILVER_DATE_PARTITION=2026/03/01 RUN_MODE=probe python scripts/silver_factures.py
```

### Ordre d'exécution recommandé (pipeline complet)

```bash
# 1. Bronze (on-prem ou laptop + VPN)
RUN_MODE=live python scripts/bronze_agences.py
RUN_MODE=live python scripts/bronze_interimaires.py
RUN_MODE=live python scripts/bronze_clients.py
RUN_MODE=live python scripts/bronze_missions.py

# 2. Silver (OVH workers — accès S3)
RUN_MODE=live python scripts/silver_agences_light.py
RUN_MODE=live python scripts/silver_interimaires.py
RUN_MODE=live python scripts/silver_interimaires_detail.py
RUN_MODE=live python scripts/silver_competences.py
RUN_MODE=live python scripts/silver_clients.py
RUN_MODE=live python scripts/silver_clients_detail.py
RUN_MODE=live python scripts/silver_factures.py
RUN_MODE=live python scripts/silver_missions.py
RUN_MODE=live python scripts/silver_temps.py

# 3. Gold — dimensions d'abord, faits ensuite
RUN_MODE=live python scripts/gold_dimensions.py
RUN_MODE=live python scripts/gold_ca_mensuel.py
RUN_MODE=live python scripts/gold_clients_detail.py
RUN_MODE=live python scripts/gold_scorecard_agence.py
RUN_MODE=live python scripts/gold_staffing.py
RUN_MODE=live python scripts/gold_operationnel.py
```

---

## Dette technique

| ID | Criticité | Description | Action |
| --- | --- | --- | --- |
| DT-01 | HIGH | `WTPRH.PRH_DATEDEB/FIN` et `PYCONTRAT.CNT_DATEDEB/FIN` — colonnes UNCERTAIN (non confirmées par probe). `silver_temps.py` et `silver_missions.py` utilisent des noms supposés. | `SELECT TOP 1 * FROM WTPRH; SELECT TOP 1 * FROM PYCONTRAT;` avant premier run LIVE |
| DT-02 | HIGH | `WTEFAC.EFAC_MONTANTHT/TTC` NULL en DDL réel. Gold CA doit reconstituer le HT via `SUM(WTLFAC.LFAC_BASE * LFAC_TAUX)`. | Pattern B-02 déjà implémenté dans 4 scripts Gold — vérifier que `silver_factures.py` produit `lignes_factures` |
| DT-03 | MEDIUM | `SIREN/SIRET/NAF` NULL en `dim_clients` Silver (absent DDL `WTTIESERV`). | Probe `WTCLPT` extra (67 cols) ou `CMTIER.TIE_SIREN` |
| DT-04 | MEDIUM | `marque/branche` NULL dans `dim_agences` (absent DDL `WTUG`). | Investiguer table `[Agence Gestion]` — `bronze_clients_external.py` existe |
| DT-05 | MEDIUM | DAG Airflow non aligné : pas de `TaskGroup` DELTA/FULL, `SILVER_DATE_PARTITION` non injecté. | Réécrire `dag_poc_pipeline.py` avec TaskGroups |
| DT-08 | HIGH | CTEs B-02 (montants via `lignes_factures`) dupliquées dans 4 scripts Gold. Maintenance fragile. | Extraire dans `gold_helpers.py` |
| DT-09 | HIGH | Jointure `releves (per_id+cnt_id)` peut produire des doublons si plusieurs relevés par contrat. | Pré-agréger `releves` ou ajouter `QUALIFY` avant JOIN dans `gold_staffing/scorecard` |
| DT-07 | LOW | Coverage tests = 0%. | Minimum : 3 tests OFFLINE par script Bronze |

**Blockers actifs :**

| ID | Bloque | Résolution |
| --- | --- | --- |
| B-03 | Tout Silver + Gold | Premier run Bronze LIVE (laptop + VPN) sur 5 tables pilotes |
| B-04 | Exactitude agrégats staffing/scorecard | Fixer DT-09 (pré-agrégation releves) avant premier run Gold LIVE |
| B-05 | CTEs B-02 dans 4 scripts Gold | Vérifier que `silver_factures.py` produit `slv_facturation/lignes_factures` |

---

## Décisions d'architecture

| Décision | Raison |
|---|---|
| **Full-load ~80% tables Bronze** | Aucune colonne `*_DATEMODIF` confirmée par probe DDL réel — non-négociable |
| **DuckDB au lieu de Spark** | Volume Phase 0 : ~3 Go/j delta Evolia. DuckDB couvre largement, économie ~80 €/mois |
| **Architecture hybride on-prem / cloud** | Bronze requiert accès SQL Server (VPN). Silver/Gold s'exécutent sur workers OVH |
| **Parquet ZSTD pour Silver** | Ratio compression ~4:1 vs JSON · format natif DuckDB · pushdown prédicats |
| **`ops.pipeline_watermarks` séparé des schémas `gld_*`** | Infrastructure vs. métier — schéma dédié `ops` pour les tables de contrôle |
| **Gold lit Silver S3, jamais Bronze** | Isolation des couches — Gold ne dépend pas du format JSON Bronze |
| **`CREATE SECRET` DuckDB pour credentials S3** | Les credentials n'apparaissent pas en clair dans les logs de requêtes |
| **`RGPD_SALT` obligatoire en LIVE** | Guard-rail technique non contournable — `Config.__post_init__` lève `ValueError` |
| **SCD Type 2 sur `dim_clients` et `dim_interimaires`** | Historisation des changements business (raison sociale, statut, compétences) |
| **`execute_values` psycopg2 batch 500 pour Gold** | Élimine les N round-trips TCP — critique sur tables volumineuses |
| **`SILVER_DATE_PARTITION` overridable via env** | Backfill et rejeu sans modification de code — opérationnel en 30 secondes |

---

Manifeste v2.0 · GI Data Lakehouse · Auteur : Mahamadou COULIBALY (Data Manager)
