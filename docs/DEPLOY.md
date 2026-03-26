# DEPLOY — Procédure de déploiement GI Data Platform
> Manifeste v4.0 · Architecture Medallion · Airflow CeleryExecutor multi-site
> Dernière mise à jour : 2026-03-25 · Auteur : M. COULIBALY

---

## Sommaire

1. [Architecture cible](#1-architecture-cible)
2. [Prérequis réseau — point critique](#2-prérequis-réseau)
3. [Installation initiale](#3-installation-initiale)
4. [Déploiement production — procédure répétable](#4-déploiement-production)
5. [Validation post-déploiement](#5-validation-post-déploiement)
6. [Rollback](#6-rollback)
7. [Backfill / Rejeu](#7-backfill--rejeu)
8. [Référence — Variables d'environnement](#8-variables-denvironnement)
9. [Checklist déploiement](#9-checklist-déploiement)

---

## 1. Architecture cible

```
┌─────────────────────────────────────────────────────────────┐
│                        OVH Cloud                            │
│                                                             │
│  ┌──────────────────────────────────────────────────────┐  │
│  │  OVH Managed Airflow (Dataflow)                      │  │
│  │  · Webserver + Scheduler (managed, HA)               │  │
│  │  · Broker Redis (managed par OVH)                    │  │
│  │  · Metadata DB PostgreSQL (managed par OVH)          │  │
│  │                                                      │  │
│  │  Workers OVH (Celery)                                │  │
│  │  · queue="silver"  → Silver DuckDB/Parquet/Iceberg   │  │
│  │  · queue="gold"    → Gold DuckDB → PostgreSQL        │  │
│  └──────────────────────────────────────────────────────┘  │
│                                                             │
│  S3 gi-data-prod-bronze  (JSON brut partitionné /YYYY/MM/DD)│
│  S3 gi-data-prod-silver  (Parquet Silver)                   │
│  S3 gi-data-prod-iceberg (Silver Iceberg REST)              │
│  PostgreSQL 18 gi_data   (Gold Warehouse — port 20184)      │
└─────────────────────────────────────────────────────────────┘
          ▲
          │  VPN IPSec ou tunnel TLS
          │
┌─────────────────────────────────────────────────────────────┐
│  frdc1pipeline01  [ datapipeline01p ]                       │
│  /opt/groupe-interaction/etl/gi-data-plateform/             │
│                                                             │
│  Celery Worker  queue="bronze"                              │
│  · Accès ERP Evolia (SQL Server on-prem · pymssql)          │
│  · Rejoint broker Redis OVH via VPN                         │
│  · run_pipeline.sh (orchestration locale / debug)           │
└─────────────────────────────────────────────────────────────┘
```

**DAG unifié `gi_pipeline` (05:00 UTC · max_active_runs=1) :**
```
bronze (×5 parallèle — queue bronze — frdc1pipeline01)
  └── silver (×10 — queue silver — workers OVH)
        ├── [séquentiel] silver_clients → enrich_ban_geocode → silver_clients_detail
        └── [parallèle]  silver_agences | silver_missions | silver_interimaires | …
              └── gold_dimensions (queue gold)
                    └── gold_facts (×7 parallèle — queue gold)
                          └── gold_views (×2 parallèle — queue gold)
                                └── rgpd_audit
```

**Table de suivi des exécutions :** `ops.pipeline_runs` (PostgreSQL Gold)
— Créée automatiquement au premier run. Voir §5.3.

---

## 2. Prérequis réseau

Le Celery Worker bronze sur **frdc1pipeline01** doit rejoindre le broker Redis hébergé sur OVH Managed Airflow.

| Option | Mise en œuvre | Complexité |
| --- | --- | --- |
| **VPN IPSec OVH ↔ frdc1pipeline01** | Tunnel chiffré permanent, standard entreprise | Moyenne (à monter une fois) |
| **TLS direct** | Redis OVH exposé avec endpoint public TLS | Faible (si OVH expose l'endpoint) |

> **À confirmer avec le support OVH** au moment du provisionnement de Managed Airflow (Dataflow) :
> OVH expose-t-il le broker Redis sur un endpoint public ou uniquement en réseau privé vRack ?

---

## 3. Installation initiale

> **⚠ Section applicable UNIQUEMENT au premier déploiement ou réinstallation complète.**
> Pour une mise à jour de code → §4.

### 3.1 Prérequis système (frdc1pipeline01)

```bash
sudo apt update && sudo apt upgrade -y
sudo apt install -y git curl

# Python 3.12
sudo add-apt-repository ppa:deadsnakes/ppa -y
sudo apt install -y python3.12 python3.12-dev python3.12-venv

# uv — gestionnaire de dépendances
curl -LsSf https://astral.sh/uv/install.sh | sh
source ~/.bashrc
python3.12 --version && uv --version
```

### 3.2 Déploiement du projet

```bash
sudo mkdir -p /opt/groupe-interaction/etl
sudo git clone <repo-url> /opt/groupe-interaction/etl/gi-data-plateform
sudo chown -R datapipeline01p:datapipeline01p /opt/groupe-interaction/etl/gi-data-plateform

cd /opt/groupe-interaction/etl/gi-data-plateform
uv sync               # Dépendances prod
uv sync --group dev   # + lint, tests

# Vérification imports critiques
.venv/bin/python -c "import pymssql, boto3, duckdb, psycopg2; print('OK')"
```

### 3.3 Configuration des secrets (.env)

```bash
cd /opt/groupe-interaction/etl/gi-data-plateform
cp .env.example .env
chmod 600 .env
nano .env   # Remplir toutes les variables obligatoires (voir §8)
```

> **⚠ JAMAIS committer `.env`** — il est dans `.gitignore`.
> Le `RGPD_SALT` doit être différent de la valeur dev (min. 32 chars).

### 3.4 Installation du Celery Worker bronze (frdc1pipeline01)

```bash
# Le venv contient déjà apache-airflow[celery] via uv sync
source /opt/groupe-interaction/etl/gi-data-plateform/.venv/bin/activate

# Configurer la connexion au broker OVH (récupérer l'URL dans l'UI OVH Dataflow)
export AIRFLOW__CELERY__BROKER_URL="redis://:PASSWORD@REDIS_OVH_HOST:6379/0"
export AIRFLOW__CELERY__RESULT_BACKEND="db+postgresql://USER:PWD@PG_META_HOST/airflow_meta"
export AIRFLOW__CORE__EXECUTOR="CeleryExecutor"

# Démarrer le worker en écoute sur la queue bronze uniquement
airflow celery worker --queues bronze --concurrency 2 &

# Activer au démarrage (systemd)
# → Créer /etc/systemd/system/airflow-worker-bronze.service (cf. template ci-dessous)
```

**Template systemd `/etc/systemd/system/airflow-worker-bronze.service` :**

```ini
[Unit]
Description=Airflow Celery Worker — queue bronze
After=network.target

[Service]
User=datapipeline01p
WorkingDirectory=/opt/groupe-interaction/etl/gi-data-plateform
EnvironmentFile=/opt/groupe-interaction/etl/gi-data-plateform/.env
ExecStart=/opt/groupe-interaction/etl/gi-data-plateform/.venv/bin/airflow celery worker \
          --queues bronze --concurrency 2 --loglevel info
Restart=on-failure
RestartSec=10

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now airflow-worker-bronze
sudo systemctl status airflow-worker-bronze
```

### 3.5 Provisionnement OVH Managed Airflow (Dataflow)

1. OVH Console → Data Platform → Dataflow → Créer un service (region `GRA`)
2. Configurer Git Sync vers le dépôt (branche `main`, dossier `dags/`)
3. Ajouter les variables d'environnement depuis §8 dans les Variables Airflow (UI ou CLI)
4. Vérifier que le DAG `gi_pipeline` apparaît sans Import Error
5. Configurer les queues workers OVH : `silver` et `gold`

### 3.6 Validation de l'installation

```bash
cd /opt/groupe-interaction/etl/gi-data-plateform

# Probe complet Bronze → Silver → Gold (zéro écriture)
./run_pipeline.sh full-probe

# Vérifier le DAG dans l'UI OVH Dataflow
# → gi_pipeline → aucun Import Error
# → airflow tasks list gi_pipeline | wc -l  → 26 tâches
```

---

## 4. Déploiement production — Procédure répétable

> **Durée estimée : 10-15 min** · Ne pas déployer pendant 04:00-06:00 UTC (fenêtre run DAG)

### 4.1 Pré-checks (depuis laptop — avant SSH)

```bash
# Tests unitaires verts
uv run pytest tests/ -m unit -v

# Lint sans erreur
uv run ruff check scripts/

# Tag sémantique créé AVANT push
git tag -a v4.0.0 -m "Migration CeleryExecutor multi-site"
git push origin main --tags

# Vérifier qu'aucun run DAG n'est en cours
# UI Airflow → gi_pipeline → aucun run en état "running"
```

### 4.2 Connexion serveur et pré-déploiement

```bash
ssh datapipeline01p@frdc1pipeline01.siege.interaction-interim.com
cd /opt/groupe-interaction/etl/gi-data-plateform

# [1] État Git propre
git status       # Doit afficher "nothing to commit, working tree clean"

# [2] Version en cours
git describe --tags --abbrev=0

# [3] Worker bronze actif
sudo systemctl status airflow-worker-bronze
```

### 4.3 Pause du DAG

```bash
# Via CLI (OVH Dataflow expose le CLI Airflow)
airflow dags pause gi_pipeline

# Attendre la fin des runs en cours
airflow dags list-runs --dag-id gi_pipeline --state running
# Doit retourner 0 ligne avant de continuer
```

### 4.4 Mise à jour du code

```bash
cd /opt/groupe-interaction/etl/gi-data-plateform

# Inspecter les changements entrants
git fetch origin main
git diff HEAD origin/main --stat

# Appliquer
git pull origin main

# Synchroniser les dépendances (uv.lock versionné)
uv sync

# Redémarrer le worker bronze pour charger le nouveau code
sudo systemctl restart airflow-worker-bronze
sudo systemctl status airflow-worker-bronze
```

### 4.5 Validation DAG (Git Sync OVH)

Le DAG se synchronise automatiquement depuis Git. Attendre ~30s puis vérifier :

```bash
# Aucun Import Error
airflow dags list 2>&1 | grep -E "gi_pipeline|ERROR"

# 26 tâches
airflow tasks list gi_pipeline | wc -l
```

### 4.6 Probe des scripts critiques

```bash
cd /opt/groupe-interaction/etl/gi-data-plateform

./run_pipeline.sh gold-probe
# ⛔ Arrêt immédiat si l'un des probes échoue → rollback §6
```

### 4.7 Réactivation et premier run

```bash
airflow dags unpause gi_pipeline

# Déclencher manuellement le premier run de validation
airflow dags trigger gi_pipeline --conf '{"triggered_by": "deploy_v4.0.0"}'

# Surveiller
watch -n 10 "airflow dags list-runs --dag-id gi_pipeline --state running"
```

---

## 5. Validation post-déploiement

### 5.1 Bronze (S3)

```bash
source /opt/groupe-interaction/etl/gi-data-plateform/.venv/bin/activate
python - <<'EOF'
import boto3, os
from dotenv import load_dotenv
from datetime import date
load_dotenv("/opt/groupe-interaction/etl/gi-data-plateform/.env")
s3 = boto3.client("s3",
    endpoint_url=os.environ["OVH_S3_ENDPOINT"],
    aws_access_key_id=os.environ["OVH_S3_ACCESS_KEY"],
    aws_secret_access_key=os.environ["OVH_S3_SECRET_KEY"],
)
bucket = os.environ.get("OVH_S3_BUCKET_BRONZE", "gi-data-prod-bronze")
for table in ["raw_wtmet", "raw_wtqua", "raw_wtmiss", "raw_wtpint"]:
    prefix = f"{table}/{date.today().strftime('%Y/%m/%d')}/"
    resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    total = sum(o["Size"] for o in resp.get("Contents", []))
    status = "✓" if resp["KeyCount"] > 0 else "✗"
    print(f"{status} {table}: {resp['KeyCount']} fichier(s), {total:,} bytes")
EOF
```

### 5.2 Gold PostgreSQL — santé globale

```bash
psql "host=${OVH_PG_HOST} port=${OVH_PG_PORT} dbname=${OVH_PG_DATABASE} \
      user=${OVH_PG_USER} sslmode=require" <<'EOF'
SELECT schemaname || '.' || tablename AS table,
       n_live_tup                     AS lignes
FROM   pg_stat_user_tables
WHERE  schemaname LIKE 'gld_%'
ORDER  BY n_live_tup DESC;
EOF
```

### 5.3 Monitoring — ops.pipeline_runs

```bash
psql "host=${OVH_PG_HOST} port=${OVH_PG_PORT} dbname=${OVH_PG_DATABASE} \
      user=${OVH_PG_USER} sslmode=require" <<'EOF'
-- Dernier run de chaque pipeline (toutes couches)
SELECT pipeline, layer, mode, status,
       started_at::date   AS date,
       duration_s         AS duree_s,
       rows_ingested       AS lignes
FROM   ops.v_pipeline_last_run
ORDER  BY layer, pipeline;

-- Échecs éventuels sur les dernières 24h
SELECT pipeline, status, started_at, error_count, errors
FROM   ops.pipeline_runs
WHERE  status != 'success'
  AND  started_at > NOW() - INTERVAL '24 hours'
ORDER  BY started_at DESC;
EOF
```

### 5.4 Tables critiques Gold

```bash
psql "host=${OVH_PG_HOST} port=${OVH_PG_PORT} dbname=${OVH_PG_DATABASE} \
      user=${OVH_PG_USER} sslmode=require" <<'EOF'
SELECT 'fact_commandes_pipeline'        AS table, COUNT(*) AS lignes FROM gld_operationnel.fact_commandes_pipeline
UNION ALL
SELECT 'fact_delai_placement',                    COUNT(*) FROM gld_operationnel.fact_delai_placement
UNION ALL
SELECT 'fact_fidelisation_interimaires',          COUNT(*) FROM gld_staffing.fact_fidelisation_interimaires
UNION ALL
SELECT 'fact_ca_mensuel_client',                  COUNT(*) FROM gld_commercial.fact_ca_mensuel_client
UNION ALL
SELECT 'fact_retention_client',                   COUNT(*) FROM gld_clients.fact_retention_client
UNION ALL
SELECT 'scorecard_agence',                        COUNT(*) FROM gld_performance.scorecard_agence;
-- Toutes les lignes doivent retourner count > 0
EOF
```

---

## 6. Rollback

### 6.1 Rollback rapide — code uniquement

```bash
cd /opt/groupe-interaction/etl/gi-data-plateform

git tag --sort=-version:refname | head -5
TARGET_TAG="v3.2.0"
git reset --hard ${TARGET_TAG}
uv sync

# Redémarrer le worker bronze
sudo systemctl restart airflow-worker-bronze

# Re-probe pour valider
./run_pipeline.sh gold-probe

# Redéclencher le DAG
airflow dags trigger gi_pipeline
```

### 6.2 Rollback schéma PostgreSQL

```sql
-- Les tables Gold sont recréées à chaque run (TRUNCATE + INSERT)
-- Un rollback code + re-trigger suffit dans 99% des cas.
-- Rollback manuel d'une colonne uniquement si nécessaire :
ALTER TABLE gld_shared.dim_metiers DROP COLUMN IF EXISTS nouvelle_colonne;
```

### 6.3 Rollback données S3

```bash
# Rejouer Silver sur une partition Bronze antérieure
SILVER_DATE_PARTITION=2026/03/10 ./run_pipeline.sh silver
```

---

## 7. Backfill / Rejeu

### 7.1 Re-ingestion Bronze (table spécifique)

```bash
TABLE_FILTER=WTQUA,WTMET ./run_pipeline.sh bronze
```

### 7.2 Re-transformation Silver (date passée)

```bash
SILVER_DATE_PARTITION=2026/03/10 ./run_pipeline.sh silver
```

### 7.3 Re-transformation Silver full-history (après purge Bronze)

```bash
SILVER_DATE_PARTITION='**' ./run_pipeline.sh silver
```

### 7.4 Backfill via Airflow CLI

```bash
airflow dags backfill gi_pipeline \
  --start-date 2026-03-10 --end-date 2026-03-11
```

---

## 8. Variables d'environnement

> **Source de vérité : `scripts/shared.py` classe `Config`.**
> Template à jour : `.env.example`

### Obligatoires

| Variable | Description | Exemple |
|---|---|---|
| `EVOLIA_SERVER` | Serveur SQL pymssql (`host,port`) | `SRV-SQL2,1433` |
| `EVOLIA_DB` | Base de données Evolia | `INTERACTION` |
| `EVOLIA_USER` | Compte lecture seule | `srv_data_read_analyst` |
| `EVOLIA_PASSWORD` | Mot de passe | — |
| `OVH_S3_ENDPOINT` | Endpoint S3 OVHcloud | `https://s3.gra.perf.cloud.ovh.net` |
| `OVH_S3_ACCESS_KEY` | Clé d'accès S3 | — |
| `OVH_S3_SECRET_KEY` | Clé secrète S3 | — |
| `BUCKET_ICEBERG` | Bucket Iceberg REST | `gi-data-prod-iceberg` |
| `OVH_PG_HOST` | Hôte PostgreSQL OVH | `postgresql-161a1420-...database.cloud.ovh.net` |
| `RGPD_SALT` | Salt pseudonymisation NIR (≥ 32 chars) | — |

### Enrichissement externe

#### `SIRENE_API_TOKEN`

Utilisé par `bronze_clients_external.py` pour enrichir les clients via l'**API SIRENE INSEE**.
Si absent, l'enrichissement est silencieusement skippé (warning dans les logs, pipeline non bloqué).

**Procédure d'obtention :**

1. Créer un compte sur le portail INSEE : [api.insee.fr](https://api.insee.fr)
2. Dans l'espace développeur → "Mes applications" → Créer une application
3. Souscrire à l'API **"Sirene V3"** (accès gratuit, quota 30 req/min)
4. Récupérer le **Consumer Key** et **Consumer Secret**
5. Obtenir le token Bearer OAuth2 :

   ```bash
   curl -s -X POST https://api.insee.fr/token \
     -H "Authorization: Basic $(echo -n 'CONSUMER_KEY:CONSUMER_SECRET' | base64)" \
     -d "grant_type=client_credentials" | jq -r '.access_token'
   ```

6. Ajouter dans `.env` :

   ```text
   SIRENE_API_TOKEN=<token obtenu>
   ```

> **Note :** Le token Bearer INSEE a une durée de vie limitée (7 jours). Il faut le renouveler périodiquement ou implémenter un refresh automatique via les credentials Consumer Key/Secret.
> **Prérequis** : le fichier `data/sirets_clients.json` doit exister et contenir la liste des SIRETs à enrichir (tableau JSON de chaînes). Override possible via `SIRETS_INPUT=chemin/vers/fichier.json`.

### Optionnelles (valeurs par défaut dans shared.py)

| Variable | Défaut | Description |
|---|---|---|
| `OVH_S3_BUCKET_BRONZE` | `gi-data-prod-bronze` | Bucket Bronze S3 |
| `OVH_S3_BUCKET_SILVER` | `gi-data-prod-silver` | Bucket Silver S3 |
| `OVH_S3_BUCKET_GOLD` | `gi-data-prod-gold` | Bucket Gold S3 |
| `OVH_PG_PORT` | `20184` | Port PostgreSQL |
| `OVH_PG_DATABASE` | `gi_poc_ddi_gold` | Base Gold (override si différent) |
| `OVH_PG_USER` | — | Utilisateur PostgreSQL |
| `OVH_PG_PASSWORD` | — | Mot de passe PostgreSQL |
| `OVH_ICEBERG_URI` | — | URI catalog Iceberg REST |
| `OVH_ICEBERG_CATALOG` | `silver` | Nom du catalog Iceberg |
| `ALERT_EMAIL` | `data-team@groupe-interaction.fr` | Alertes Airflow |
| `RUN_MODE` | `live` | `offline` \| `probe` \| `live` |
| `TABLE_FILTER` | — | Filtrage tables debug/backfill |
| `SILVER_DATE_PARTITION` | Date du jour | Override partition backfill |

### Fichiers `.env` par environnement

| Fichier | Contexte | Git |
| --- | --- | --- |
| `.env.example` | Template de référence | ✅ versionné |
| `.env` | frdc1pipeline01 prod | ❌ gitignore |
| `.env.local` | Laptop développeur | ❌ gitignore |

---

## 9. Checklist déploiement

### ✅ Avant de déployer (laptop)

- [ ] `uv run pytest tests/ -m unit` — 100% vert
- [ ] `uv run ruff check scripts/` — zéro erreur
- [ ] `.env.example` mis à jour si nouvelles variables ajoutées
- [ ] Tag sémantique créé : `git tag -a vX.Y.Z`
- [ ] Push main + tags : `git push origin main --tags`
- [ ] Aucun run DAG en cours (UI OVH Dataflow)
- [ ] Heure de déploiement hors fenêtre 04:00-06:00 UTC

### ✅ Sur frdc1pipeline01

- [ ] `git status` — working tree clean
- [ ] DAG `gi_pipeline` mis en pause : `airflow dags pause gi_pipeline`
- [ ] `git pull origin main` — succès sans conflits
- [ ] `uv sync` — succès
- [ ] `sudo systemctl restart airflow-worker-bronze` — status active
- [ ] Probe complet : `./run_pipeline.sh gold-probe` → **zéro erreur**
- [ ] `airflow dags list` → `gi_pipeline` visible, aucun Import Error
- [ ] `airflow tasks list gi_pipeline | wc -l` → **26 tâches**
- [ ] Anciens DAGs (`dag_poc_pipeline`, `dag_phase*`) mis en pause dans l'UI
- [ ] DAG `gi_pipeline` dépausé : `airflow dags unpause gi_pipeline`
- [ ] Run manuel déclenché : `airflow dags trigger gi_pipeline`

### ✅ Après le premier run LIVE

- [ ] Tous les tasks `gi_pipeline` au vert dans l'UI OVH Dataflow
- [ ] Bronze S3 : fichiers présents pour la date du jour (§5.1)
- [ ] `ops.pipeline_runs` : au moins 26 lignes `status='success'` (§5.3)
- [ ] `gld_operationnel.fact_commandes_pipeline` : count > 0
- [ ] `gld_operationnel.fact_delai_placement` : count > 0
- [ ] `gld_staffing.fact_fidelisation_interimaires` : count > 0
- [ ] `gld_clients.fact_retention_client` : count > 0
- [ ] Aucune alerte email d'échec reçue
