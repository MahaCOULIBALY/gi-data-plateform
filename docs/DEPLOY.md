# DEPLOY — Procédure de déploiement GI Data Platform
> Manifeste v3.2 · Architecture Medallion · Airflow LocalExecutor
> Dernière mise à jour : 2026-03-13 · Auteur : M. COULIBALY

---

## Sommaire

1. [Architecture de déploiement](#1-architecture)
2. [Installation initiale (première fois uniquement)](#2-installation-initiale)
3. [Déploiement local — laptop](#3-déploiement-local)
4. [Déploiement production — procédure répétable](#4-déploiement-production)
5. [Validation post-déploiement](#5-validation-post-déploiement)
6. [Rollback](#6-rollback)
7. [Backfill / Rejeu](#7-backfill--rejeu)
8. [Référence — Variables d'environnement](#8-variables-denvironnement)
9. [Checklist déploiement](#9-checklist-déploiement)

---

## 1. Architecture

```
ERP Evolia (SQL Server on-prem · SRV-SQL2\cegi · DB: INTERACTION)
│  pyodbc · lecture seule · ODBC Driver 17
▼
FRDC1PIPELINE01  [ datapipeline01p ]
│  /opt/groupe-interaction/etl/gi-data-platform/   ← code Python
│  /opt/groupe-interaction/airflow/dags/            ← DAGs (symlinks)
│  Airflow LocalExecutor — http://frdc1pipeline01:8080
│
├── boto3 ──────────────────────▶ OVHcloud S3 (managé)
│                                  gi-poc-bronze/  gi-poc-silver/
└── psycopg2 ───────────────────▶ OVHcloud PostgreSQL (managé)
                                   gld_shared / gld_staffing /
                                   gld_commercial / gld_clients /
                                   gld_performance / gld_operationnel
```

**DAG unifié `gi_pipeline` (05:00 UTC · max_active_runs=1) :**
```
bronze (×5 parallèle)
  └── silver (×10)
        └── gold_dimensions
              ├── gold_facts (×7 parallèle)
              └── gold_views (×2)
                    └── rgpd_audit
```

---

## 2. Installation initiale

> **⚠ Section applicable UNIQUEMENT au premier déploiement ou réinstallation complète.**
> Pour une mise à jour de code → §4.

### 2.1 Prérequis système

```bash
sudo apt update && sudo apt upgrade -y
sudo apt install -y git curl unixodbc unixodbc-dev

# Python 3.12
sudo add-apt-repository ppa:deadsnakes/ppa -y
sudo apt install -y python3.12 python3.12-dev python3.12-venv python3.12-distutils

# uv — version fixée pour reproductibilité
export UV_VERSION="0.5.x"   # Remplacer par la version exacte en production
curl -LsSf https://astral.sh/uv/${UV_VERSION}/install.sh | sh
source ~/.bashrc

# Vérifications
python3.12 --version && uv --version
```

### 2.2 ODBC Driver 17 for SQL Server

```bash
curl -fsSL https://packages.microsoft.com/keys/microsoft.asc \
  | sudo gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg
curl https://packages.microsoft.com/config/ubuntu/$(lsb_release -rs)/prod.list \
  | sudo tee /etc/apt/sources.list.d/mssql-release.list
sudo apt update && sudo ACCEPT_EULA=Y apt install -y msodbcsql17
odbcinst -q -d   # Doit afficher "ODBC Driver 17 for SQL Server"
```

### 2.3 Déploiement initial du projet

```bash
# Créer l'arborescence sous /opt/groupe-interaction/etl/
sudo mkdir -p /opt/groupe-interaction/etl
sudo git clone <repo-url> /opt/groupe-interaction/etl/gi-data-platform
sudo chown -R datapipeline01p:datapipeline01p /opt/groupe-interaction/etl/gi-data-platform

cd /opt/groupe-interaction/etl/gi-data-platform
uv sync               # Dépendances prod
uv sync --group dev   # + lint, tests

# Vérification imports critiques
.venv/bin/python -c "import pyodbc, boto3, duckdb, psycopg2; print('OK')"
```

### 2.4 Configuration des secrets (.env)

```bash
cp .env.example .env
chmod 600 .env      # Lecture réservée au propriétaire uniquement
nano .env           # Remplir toutes les variables obligatoires (voir §8)
```

> **⚠ JAMAIS committer `.env`** — il est dans `.gitignore`.
> Le `RGPD_SALT` doit être différent de la valeur dev (min. 32 chars).

### 2.5 Intégration Airflow — Symlink DAG

```bash
# Créer le dossier centralisé si nécessaire
sudo mkdir -p /opt/groupe-interaction/airflow/dags
sudo chown datapipeline01p:datapipeline01p /opt/groupe-interaction/airflow/dags

# Créer le symlink vers le DAG unifié
ln -sf /opt/groupe-interaction/etl/gi-data-platform/dags/dag_gi_pipeline.py \
       /opt/groupe-interaction/airflow/dags/dag_gi_pipeline.py

# Mettre à jour airflow.cfg
# dags_folder = /opt/groupe-interaction/airflow/dags
```

### 2.6 Validation de l'installation

```bash
cd /opt/groupe-interaction/etl/gi-data-platform

# Probe Evolia (comptage, zéro écriture)
env RUN_MODE=probe .venv/bin/python scripts/bronze_interimaires.py
# Attendu : {"mode": "probe", "table": "WTMET", "count": >0}

# Probe S3 + DuckDB
env RUN_MODE=probe .venv/bin/python scripts/silver_competences.py

# Probe PostgreSQL Gold
env RUN_MODE=probe .venv/bin/python scripts/gold_dimensions.py

# Vérifier le DAG dans l'UI
# http://frdc1pipeline01:8080 → gi_pipeline → pas d'Import Error
```

---

## 3. Déploiement local — laptop

### 3.1 Prérequis

| Composant | Version | Vérification |
|---|---|---|
| Python | ≥ 3.12 | `python --version` |
| uv | même que prod | `uv --version` |
| ODBC Driver 17 | 17.x | `odbcinst -q -d` |
| Git | ≥ 2.x | `git --version` |
| VPN Interaction SI | actif | `ping SRV-SQL2` |

### 3.2 Installation

```bash
git clone <repo-url> gi-data-platform && cd gi-data-platform
uv sync && uv sync --group dev
cp .env.example .env.local
# Éditer .env.local — RUN_MODE=probe par défaut en dev
```

### 3.3 Vérification connectivité (PROBE)

```bash
env RUN_MODE=probe uv run python scripts/bronze_interimaires.py
env RUN_MODE=probe uv run python scripts/silver_competences.py
env RUN_MODE=probe uv run python scripts/gold_dimensions.py
```

### 3.4 Tests locaux obligatoires avant push

```bash
uv run ruff check scripts/           # Zéro erreur
uv run ruff format --check scripts/
uv run mypy scripts/
uv run pytest tests/ -m unit -v      # Coverage ≥ 60%
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
git tag -a v3.2.0 -m "Phase 2 complete + DAG prod-ready"
git push origin main --tags

# Vérifier qu'aucun run DAG n'est en cours
# UI Airflow → gi_pipeline → aucun run en état "running"
```

### 4.2 Connexion serveur et pré-déploiement

```bash
ssh datapipeline01p@frdc1pipeline01.siege.interaction-interim.com

cd /opt/groupe-interaction/etl/gi-data-platform

# [1] Vérifier l'état Git (aucun fichier modifié manuellement)
git status       # Doit être "nothing to commit, working tree clean"

# [2] Sauvegarder la version actuelle (rollback rapide)
CURRENT_TAG=$(git describe --tags --abbrev=0)
echo "Version actuelle : ${CURRENT_TAG}"

# [3] Vérifier l'état du scheduler Airflow
airflow scheduler --help > /dev/null 2>&1 && echo "Airflow OK"
```

### 4.3 Pause du DAG (obligatoire avant toute mise à jour de code)

```bash
# Via CLI (préféré — traçable)
airflow dags pause gi_pipeline

# Attendre la fin des runs en cours (max 60 min)
airflow dags list-runs --dag-id gi_pipeline --state running
# Doit retourner 0 ligne avant de continuer
```

### 4.4 Mise à jour du code

```bash
cd /opt/groupe-interaction/etl/gi-data-platform

# Inspecter les changements entrants
git fetch origin main
git diff HEAD origin/main --stat

# Appliquer la mise à jour
git pull origin main

# Synchroniser les dépendances (uv.lock versionné — reproductible)
uv sync
```

### 4.5 Vérification du symlink DAG

```bash
ls -la /opt/groupe-interaction/airflow/dags/dag_gi_pipeline.py
# Attendu :
# lrwxrwxrwx ... dag_gi_pipeline.py -> /opt/groupe-interaction/etl/gi-data-platform/dags/dag_gi_pipeline.py

# Si absent, créer :
ln -sf /opt/groupe-interaction/etl/gi-data-platform/dags/dag_gi_pipeline.py \
       /opt/groupe-interaction/airflow/dags/dag_gi_pipeline.py
```

### 4.6 Probe des scripts critiques

```bash
cd /opt/groupe-interaction/etl/gi-data-platform

# Bronze — connexion Evolia
env RUN_MODE=probe .venv/bin/python scripts/bronze_missions.py
env RUN_MODE=probe .venv/bin/python scripts/bronze_interimaires.py

# Silver — DuckDB + S3
env RUN_MODE=probe .venv/bin/python scripts/silver_missions.py

# Gold — PostgreSQL OVH
env RUN_MODE=probe .venv/bin/python scripts/gold_operationnel.py

# ⛔ Arrêt immédiat si l'un de ces probes échoue → rollback §6
```

### 4.7 Validation DAG Airflow

```bash
# Attendre le rechargement automatique (refresh_interval=30s)
sleep 35

# Vérifier la liste des DAGs (aucun Import Error)
airflow dags list 2>&1 | grep -E "gi_pipeline|ERROR"

# Vérifier la structure du DAG (25 tâches attendues)
airflow tasks list gi_pipeline | wc -l   # Doit retourner 25
```

### 4.8 Réactivation et premier run

```bash
# Dépause le DAG
airflow dags unpause gi_pipeline

# Déclencher manuellement le premier run de validation
airflow dags trigger gi_pipeline --conf '{"triggered_by": "deploy_v3.2.0"}'

# Surveiller l'avancement
watch -n 10 "airflow dags list-runs --dag-id gi_pipeline --state running"
```

---

## 5. Validation post-déploiement

### 5.1 Bronze (S3)

```bash
.venv/bin/python - <<'EOF'
import boto3, os
from dotenv import load_dotenv
from datetime import date
load_dotenv("/opt/groupe-interaction/etl/gi-data-platform/.env")
s3 = boto3.client("s3",
    endpoint_url=os.environ["OVH_S3_ENDPOINT"],
    aws_access_key_id=os.environ["OVH_S3_ACCESS_KEY"],
    aws_secret_access_key=os.environ["OVH_S3_SECRET_KEY"],
)
for table in ["raw_wtmet", "raw_wtqua", "raw_wtmiss", "raw_wtpint"]:
    prefix = f"{table}/{date.today().strftime('%Y/%m/%d')}/"
    resp = s3.list_objects_v2(Bucket="gi-poc-bronze", Prefix=prefix)
    total = sum(o["Size"] for o in resp.get("Contents", []))
    status = "✅" if resp["KeyCount"] > 0 else "❌"
    print(f"{status} {table}: {resp['KeyCount']} fichier(s), {total:,} bytes")
EOF
```

### 5.2 Gold PostgreSQL — santé globale

```bash
psql "host=${OVH_PG_HOST} port=20184 dbname=gi_poc_ddi_gold \
      user=${OVH_PG_USER} sslmode=require" <<'EOF'
SELECT schemaname || '.' || tablename AS table, n_live_tup AS lignes
FROM pg_stat_user_tables
WHERE schemaname LIKE 'gld_%'
ORDER BY n_live_tup DESC;
EOF
```

### 5.3 Tables critiques v3.2

```bash
psql "host=${OVH_PG_HOST} port=20184 dbname=gi_poc_ddi_gold \
      user=${OVH_PG_USER} sslmode=require" <<'EOF'
SELECT 'fact_etp_hebdo'                AS table, COUNT(*) AS lignes
  FROM gld_operationnel.fact_etp_hebdo
UNION ALL
SELECT 'fact_delai_placement',                   COUNT(*)
  FROM gld_operationnel.fact_delai_placement
UNION ALL
SELECT 'fact_fidelisation_interimaires',          COUNT(*)
  FROM gld_staffing.fact_fidelisation_interimaires
UNION ALL
SELECT 'fact_concentration_client',              COUNT(*)
  FROM gld_clients.fact_concentration_client;
-- Toutes les lignes doivent retourner count > 0
EOF
```

---

## 6. Rollback

### 6.1 Rollback rapide — code uniquement

```bash
cd /opt/groupe-interaction/etl/gi-data-platform

# Identifier les tags disponibles
git tag --sort=-version:refname | head -5

# Rollback vers la version stable
TARGET_TAG="v3.1.0"   # Remplacer par le tag cible
git reset --hard ${TARGET_TAG}
uv sync

# Re-probe pour valider
env RUN_MODE=probe .venv/bin/python scripts/bronze_missions.py
env RUN_MODE=probe .venv/bin/python scripts/gold_operationnel.py

# Redéclencher le DAG
airflow dags trigger gi_pipeline
```

### 6.2 Rollback schéma PostgreSQL

```sql
-- Les tables Gold sont recréées à chaque run (CREATE TABLE IF NOT EXISTS + TRUNCATE)
-- Un rollback code + re-trigger suffit dans 99% des cas.
-- Rollback manuel d'une colonne uniquement si nécessaire :
ALTER TABLE gld_shared.dim_metiers DROP COLUMN IF EXISTS pcs_code;
```

### 6.3 Rollback données S3

```bash
# Rejouer Silver sur une partition Bronze antérieure
env SILVER_DATE_PARTITION=2026/03/11 RUN_MODE=live \
  .venv/bin/python scripts/silver_missions.py
```

---

## 7. Backfill / Rejeu

### 7.1 Re-ingestion Bronze (table spécifique)

```bash
env TABLE_FILTER=WTQUA,WTMET RUN_MODE=live \
  .venv/bin/python scripts/bronze_interimaires.py
```

### 7.2 Re-transformation Silver (date passée)

```bash
env SILVER_DATE_PARTITION=2026/03/10 RUN_MODE=live \
  .venv/bin/python scripts/silver_competences.py
```

### 7.3 Backfill via Airflow CLI

```bash
airflow dags backfill gi_pipeline \
  --start-date 2026-03-10 --end-date 2026-03-11
```

---

## 8. Variables d'environnement

| Variable | Obligatoire | Description | Exemple |
|---|---|---|---|
| `EVOLIA_SERVER` | ✅ | Serveur SQL Evolia | `SRV-SQL2\cegi` |
| `EVOLIA_DB` | ✅ | Base de données | `INTERACTION` |
| `EVOLIA_USER` | ✅ | Compte lecture seule | `srv_data_read_analyst` |
| `EVOLIA_PASSWORD` | ✅ | Mot de passe | — |
| `OVH_S3_ENDPOINT` | ✅ | Endpoint S3 OVHcloud | `https://s3.gra.perf.cloud.ovh.net` |
| `OVH_S3_ACCESS_KEY` | ✅ | Clé d'accès S3 | — |
| `OVH_S3_SECRET_KEY` | ✅ | Clé secrète S3 | — |
| `OVH_PG_HOST` | ✅ | Hôte PostgreSQL OVH | `xxx.database.cloud.ovh.net` |
| `OVH_PG_PORT` | ✅ | Port PostgreSQL | `20184` |
| `OVH_PG_USER` | ✅ | Utilisateur PostgreSQL | — |
| `OVH_PG_PASSWORD` | ✅ | Mot de passe PostgreSQL | — |
| `OVH_PG_DATABASE` | ✅ | Base Gold | `gi_poc_ddi_gold` |
| `RGPD_SALT` | ✅ LIVE | Salt pseudonymisation NIR (≥ 32 chars) | — |
| `RUN_MODE` | ✅ | `offline` / `probe` / `live` | `live` |
| `TABLE_FILTER` | ❌ | Filtrage tables debug/backfill | `WTMET,WTQUA` |
| `SILVER_DATE_PARTITION` | ❌ | Override partition backfill | `2026/03/10` |
| `ALERT_EMAIL` | ❌ | Destinataire alertes Airflow | `data-team@...` |

### Fichiers `.env` par environnement

| Fichier | Contexte | Git |
|---|---|---|
| `.env.local` | Laptop développeur | ❌ gitignore |
| `.env` | FRDC1PIPELINE01 prod | ❌ gitignore |
| `.env.example` | Template de référence | ✅ versionné |

---

## 9. Checklist déploiement

### ✅ Avant de déployer (laptop)

- [ ] `uv run pytest tests/ -m unit` — 100% vert, coverage ≥ 60%
- [ ] `uv run ruff check scripts/` — zéro erreur
- [ ] `.env.example` mis à jour si nouvelles variables ajoutées
- [ ] Tag sémantique créé localement : `git tag -a vX.Y.Z`
- [ ] Push main + tags : `git push origin main --tags`
- [ ] Aucun run DAG en cours (vérifier dans l'UI)
- [ ] Heure de déploiement hors fenêtre 04:00-06:00 UTC

### ✅ Sur FRDC1PIPELINE01

- [ ] `git status` — working tree clean
- [ ] DAG `gi_pipeline` mis en pause : `airflow dags pause gi_pipeline`
- [ ] `git pull origin main` — succès sans conflits
- [ ] `uv sync` — succès
- [ ] Symlink DAG présent et valide (`ls -la /opt/groupe-interaction/airflow/dags/`)
- [ ] Probe Bronze : `env RUN_MODE=probe .venv/bin/python scripts/bronze_missions.py` → OK
- [ ] Probe Silver : `env RUN_MODE=probe .venv/bin/python scripts/silver_missions.py` → OK
- [ ] Probe Gold : `env RUN_MODE=probe .venv/bin/python scripts/gold_operationnel.py` → OK
- [ ] `airflow dags list` → `gi_pipeline` visible, aucun Import Error
- [ ] `airflow tasks list gi_pipeline | wc -l` → 25 tâches
- [ ] Anciens DAGs (`gi_phase*`) mis en pause dans l'UI
- [ ] DAG `gi_pipeline` dépausé : `airflow dags unpause gi_pipeline`
- [ ] Run manuel déclenché : `airflow dags trigger gi_pipeline`

### ✅ Après le premier run LIVE

- [ ] Tous les tasks `gi_pipeline` au vert dans l'UI Airflow
- [ ] Bronze S3 : fichiers présents pour la date du jour (§5.1)
- [ ] `gld_operationnel.fact_etp_hebdo` : count > 0
- [ ] `gld_operationnel.fact_delai_placement` : count > 0
- [ ] `gld_staffing.fact_fidelisation_interimaires` : count > 0
- [ ] `gld_clients.fact_concentration_client` : count > 0
- [ ] Aucune alerte email d'échec reçue
