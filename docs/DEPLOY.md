# DEPLOY — Procédure de déploiement GI Data Lakehouse

> **Manifeste v3.2 · Phase 0-3 · Dernière mise à jour : 2026-03-13**

---

## Sommaire

1. [Architecture de déploiement](#1-architecture-de-déploiement)
2. [Installation FRDC1PIPELINE01](#2-installation-frdc1pipeline01)
3. [Déploiement local (laptop)](#3-déploiement-local-laptop)
4. [Tests — laptop](#4-tests--laptop)
5. [Déploiement production](#5-déploiement-production)
6. [Tests — production](#6-tests--production)
7. [Rollback](#7-rollback)
8. [Backfill / rejeu](#8-backfill--rejeu)
9. [Variables d'environnement de référence](#9-variables-denvironnement-de-référence)

---

## 1. Architecture de déploiement

```
ERP Evolia (SQL Server on-prem · SRV-SQL2\cegi · DB: INTERACTION)
│
│  pyodbc · lecture seule · ODBC Driver 17 for SQL Server
│
▼
FRDC1PIPELINE01.siege.interaction-interim.com
│  /opt/gi-data-platform/    ← code Python
│  Airflow  http://frdc1pipeline01:8080
│
├── boto3 ──────────────────────────────────────────────▶ OVHcloud S3 (managé)
│                                                          gi-poc-bronze/  (JSON brut)
│                                                          gi-poc-silver/  (Parquet)
│
└── psycopg2 ───────────────────────────────────────────▶ OVHcloud PostgreSQL (managé)
                                                           gld_shared / gld_staffing /
                                                           gld_commercial / gld_clients /
                                                           gld_performance / gld_operationnel
```

**Flux pipeline quotidien (05:00 UTC) — DAG unifié `gi_pipeline` :**
```
bronze (×5 parallèle)
  └── silver (×10 ; clients SCD2 → enrich_ban → detail)
        └── gold_dimensions
              └── gold_facts (×7 parallèle : ca_mensuel, staffing, operationnel, etp,
              │               scorecard_agence, competences, retention_client)
              └── gold_views (×2 : vue360_client, clients_detail)
                    └── rgpd_audit
```

> **Note :** OVH S3 et PostgreSQL sont des **services managés OVHcloud** — aucune installation
> de serveur de base de données ni de stockage objet n'est requise côté infrastructure interne.

---

## 2. Installation FRDC1PIPELINE01

Section applicable uniquement au **premier déploiement** ou à une réinstallation complète.
Pour une mise à jour de code, aller directement au [§5](#5-déploiement-production).

### 2.1 Prérequis système

```bash
# Mise à jour des paquets
sudo apt update && sudo apt upgrade -y

# Git
sudo apt install -y git curl

# Python 3.12
sudo add-apt-repository ppa:deadsnakes/ppa -y
sudo apt install -y python3.12 python3.12-dev python3.12-venv python3.12-distutils

# uv — gestionnaire de dépendances Python
curl -LsSf https://astral.sh/uv/install.sh | sh
source ~/.bashrc   # ou se reconnecter

# Vérification
python3.12 --version   # Python 3.12.x
uv --version           # uv 0.x.x
```

### 2.2 ODBC Driver 17 for SQL Server

Requis par `pyodbc` pour se connecter à Evolia (SQL Server on-prem).

```bash
# Dépendances unixODBC
sudo apt install -y unixodbc unixodbc-dev

# Clé et repo Microsoft
curl -fsSL https://packages.microsoft.com/keys/microsoft.asc \
  | sudo gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg

curl https://packages.microsoft.com/config/ubuntu/$(lsb_release -rs)/prod.list \
  | sudo tee /etc/apt/sources.list.d/mssql-release.list

sudo apt update
sudo ACCEPT_EULA=Y apt install -y msodbcsql17

# Vérification — doit lister "ODBC Driver 17 for SQL Server"
odbcinst -q -d
```

### 2.3 Déploiement initial du projet

```bash
# Cloner le dépôt sous l'utilisateur airflow
sudo git clone <repo-url> /opt/gi-data-platform
sudo chown -R airflow:airflow /opt/gi-data-platform

# Installer les dépendances Python dans un venv géré par uv
cd /opt/gi-data-platform
sudo -u airflow uv sync

# Vérification
sudo -u airflow .venv/bin/python -c "import pyodbc, boto3, duckdb, psycopg2; print('OK')"
```

### 2.4 Configuration de l'environnement (.env)

```bash
sudo -u airflow cp /opt/gi-data-platform/.env.example /opt/gi-data-platform/.env
sudo -u airflow nano /opt/gi-data-platform/.env
```

Valeurs à renseigner (voir [§9](#9-variables-denvironnement-de-référence)) :

```ini
# Evolia SQL Server (on-prem)
EVOLIA_SERVER=SRV-SQL2\cegi
EVOLIA_DB=INTERACTION
EVOLIA_USER=srv_data_read_analyst
EVOLIA_PASSWORD=<mot_de_passe>

# OVHcloud S3
OVH_S3_ENDPOINT=https://s3.gra.perf.cloud.ovh.net
OVH_S3_ACCESS_KEY=<clé_accès>
OVH_S3_SECRET_KEY=<clé_secrète>

# OVHcloud PostgreSQL (managé)
OVH_PG_HOST=<host.database.cloud.ovh.net>
OVH_PG_PORT=20184
OVH_PG_USER=<user>
OVH_PG_PASSWORD=<password>
OVH_PG_DATABASE=gi_poc_ddi_gold

# RGPD — salt unique prod (≥ 32 chars, JAMAIS identique au dev)
RGPD_SALT=<generé_aléatoirement_min32chars>

RUN_MODE=live
```

```bash
# Droits restreints — le .env ne doit être lisible que par airflow
sudo chmod 600 /opt/gi-data-platform/.env
```

### 2.5 Intégration Airflow

```bash
# Option A — lien symbolique (les DAGs sont auto-rechargés à chaque git pull)
ln -sf /opt/gi-data-platform/dags/dag_gi_pipeline.py ~/airflow/dags/

# Option B — configurer dags_folder dans airflow.cfg
# dags_folder = /opt/gi-data-platform/dags
```

> **Note :** les anciens fichiers `dag_poc_pipeline.py`, `dag_phase1_clients.py`,
> `dag_phase2_interimaires.py`, `dag_phase3_performance.py` ne contiennent plus de code DAG.
> Si des liens symboliques existent vers eux, les supprimer ou les re-pointer vers `dag_gi_pipeline.py`.

### 2.6 Validation de l'installation

```bash
cd /opt/gi-data-platform

# Test Evolia SQL Server (comptes lignes, zéro écriture)
RUN_MODE=probe sudo -u airflow .venv/bin/python scripts/bronze_interimaires.py

# Test OVH S3 + DuckDB (Silver)
RUN_MODE=probe sudo -u airflow .venv/bin/python scripts/silver_competences.py

# Test OVH PostgreSQL (Gold)
RUN_MODE=probe sudo -u airflow .venv/bin/python scripts/gold_dimensions.py
```

Sortie attendue :

```json
{"mode": "probe", "table": "WTMET",  "count": 412, "load": "full"}
{"mode": "probe", "table": "WTQUA",  "count": 8,   "load": "full"}
{"mode": "probe", "table": "competences", "rows": 12500}
```

---

## 3. Déploiement local (laptop)

### 3.1 Prérequis laptop

| Composant | Version minimale | Vérification |
| --------- | --------------- | ------------ |
| Python | 3.12 | `python --version` |
| uv | dernière | `uv --version` |
| ODBC Driver 17 for SQL Server | 17.x | `odbcinst -q -d` (Linux/Mac) |
| Git | 2.x | `git --version` |
| VPN Interaction SI | — | `ping SRV-SQL2` |

### 3.2 Installer et configurer

```bash
git clone <repo-url> gi-data-platform
cd gi-data-platform

uv sync             # dépendances projet
uv sync --group dev # + tests, linter

cp .env.example .env.local
# Remplir .env.local (RUN_MODE=probe par défaut en dev)
```

> `.env.local` est dans `.gitignore` — ne jamais committer.

### 3.3 Vérifier la connectivité (mode PROBE)

```bash
RUN_MODE=probe uv run python scripts/bronze_interimaires.py
RUN_MODE=probe uv run python scripts/silver_competences.py
RUN_MODE=probe uv run python scripts/gold_dimensions.py
```

### 3.4 Exécution locale complète (LIVE)

> Écrit dans les buckets S3 et le PostgreSQL configurés dans `.env.local`.

```bash
RUN_MODE=live uv run python scripts/bronze_interimaires.py
RUN_MODE=live uv run python scripts/silver_competences.py
RUN_MODE=live uv run python scripts/gold_dimensions.py
RUN_MODE=live uv run python scripts/gold_competences.py
```

---

## 4. Tests — laptop

### 4.1 Tests unitaires (sans I/O)

```bash
uv run pytest tests/ -m unit -v
uv run pytest tests/ -m unit --cov=scripts --cov-report=html
```

### 4.2 Tests d'intégration (nécessite `.env.local` + VPN)

```bash
RUN_MODE=probe uv run pytest tests/ -m integration -v
```

### 4.3 Linting et typage

```bash
uv run ruff check scripts/
uv run ruff format --check scripts/
uv run mypy scripts/
```

### 4.4 Validation post-déploiement

```bash
# Vérifier les nouvelles colonnes Bronze (référentiels enrichis)
RUN_MODE=probe TABLE_FILTER=WTMET,WTTHAB,WTTDIP,WTQUA \
  uv run python scripts/bronze_interimaires.py

# Silver — compte + présence colonne pcs_code
RUN_MODE=probe uv run python scripts/silver_competences.py

# Gold — dim_metiers avec qualification réelle (TQUA_LIBELLE)
RUN_MODE=probe uv run python scripts/gold_dimensions.py
```

---

## 5. Déploiement production

### 5.1 Préparation (depuis laptop)

```bash
uv run pytest tests/ -m unit -v   # doit passer

git checkout main && git pull origin main
git tag -a v2.1.0 -m "description du changement"
git push origin v2.1.0
```

### 5.2 Mise à jour du code sur FRDC1PIPELINE01

```bash
ssh <user>@frdc1pipeline01.siege.interaction-interim.com

cd /opt/gi-data-platform
sudo -u airflow git pull origin main
sudo -u airflow .venv/bin/uv sync
```

### 5.3 Validation PROBE avant activation

```bash
cd /opt/gi-data-platform
RUN_MODE=probe sudo -u airflow .venv/bin/python scripts/bronze_interimaires.py
RUN_MODE=probe sudo -u airflow .venv/bin/python scripts/silver_competences.py
RUN_MODE=probe sudo -u airflow .venv/bin/python scripts/gold_dimensions.py
```

### 5.4 Vérification DAGs Airflow

Airflow recharge les DAGs automatiquement (polling 30s).

Vérifier dans l'UI (<http://frdc1pipeline01.siege.interaction-interim.com:8080/>) :

- Onglet **DAGs** → `gi_pipeline` visible, pas d'**Import Error**
- `gi_pipeline` → **Graph** → 25 tâches attendues dans 5 TaskGroups
- Les anciens DAGs (`gi_phase0_pipeline`, `gi_phase1_clients`, `gi_phase2_interimaires`, `gi_phase3_performance`) → **Pause** s'ils apparaissent encore

### 5.5 Déclenchement manuel (premier run post-déploiement)

Depuis l'UI : `gi_pipeline` → **Trigger DAG ▶**

Ou en ligne de commande :

```bash
sudo -u airflow airflow dags trigger gi_pipeline
```

Graphe d'exécution attendu :

```text
bronze.* (×5) → silver.* (×10) → gold_dimensions
                                        → gold_facts.* (×7 parallèle)
                                              → gold_views.* (×2)
                                                    → rgpd_audit
```

Durée attendue : ~45-60 min (pipeline complet).

### 5.6 Schéma PostgreSQL OVH

Le script `gold_dimensions.py` crée automatiquement les tables et colonnes via `pg_bulk_insert`
(`CREATE TABLE IF NOT EXISTS` + `TRUNCATE`). **Aucune migration SQL manuelle requise.**

Contrôle post-run :

```bash
# Depuis FRDC1PIPELINE01 avec le client psql
psql "host=<ovh-pg-host> port=20184 dbname=gi_poc_ddi_gold user=<user> sslmode=require" \
  -c "SELECT COUNT(*), COUNT(pcs_code), COUNT(NULLIF(qualification,'')) FROM gld_shared.dim_metiers;"
```

---

## 6. Tests — production

### 6.1 Vérification post-run Bronze (S3)

```bash
sudo -u airflow .venv/bin/python - <<'EOF'
import boto3, os
from dotenv import load_dotenv
from datetime import date
load_dotenv("/opt/gi-data-platform/.env")
s3 = boto3.client("s3",
    endpoint_url=os.environ["OVH_S3_ENDPOINT"],
    aws_access_key_id=os.environ["OVH_S3_ACCESS_KEY"],
    aws_secret_access_key=os.environ["OVH_S3_SECRET_KEY"],
)
for table in ["raw_wtmet", "raw_wtqua", "raw_wtthab", "raw_wttdip"]:
    prefix = f"{table}/{date.today().strftime('%Y/%m/%d')}/"
    resp = s3.list_objects_v2(Bucket="gi-poc-bronze", Prefix=prefix)
    total = sum(o["Size"] for o in resp.get("Contents", []))
    print(f"{table}: {resp['KeyCount']} fichier(s), {total} bytes")
EOF
```

### 6.2 Contrôle de cohérence Silver

```bash
sudo -u airflow .venv/bin/python - <<'EOF'
import duckdb, os
from dotenv import load_dotenv
load_dotenv("/opt/gi-data-platform/.env")
conn = duckdb.connect()
conn.execute("INSTALL httpfs; LOAD httpfs;")
endpoint = os.environ["OVH_S3_ENDPOINT"].replace("https://","").rstrip("/")
conn.execute(f"""
    CREATE OR REPLACE SECRET s3_gi (
        TYPE S3, KEY_ID '{os.environ["OVH_S3_ACCESS_KEY"]}',
        SECRET '{os.environ["OVH_S3_SECRET_KEY"]}',
        ENDPOINT '{endpoint}', REGION 'gra', URL_STYLE 'path', USE_SSL true
    )
""")
for row in conn.execute("""
    SELECT type_competence,
           COUNT(*)                                    AS total,
           COUNT(date_expiration)                      AS avec_date_expir,
           COUNT(CASE WHEN is_active THEN 1 END)       AS actives,
           COUNT(pcs_code)                             AS avec_pcs_code
    FROM read_parquet('s3://gi-poc-silver/slv_interimaires/competences/**/*.parquet')
    GROUP BY 1 ORDER BY 1
""").fetchall():
    print(row)
EOF
```

Résultat attendu :

```text
('DIPLOME',      N,   0,  N,  0)
('EXPERIENCE',   N,   N,  N,  0)
('HABILITATION', N,  >0,  N,  0)   # date_expir calculée via THAB_NBMOIS
('METIER',       N,   0,  N, >0)   # pcs_code depuis PCS_CODE_2003
```

### 6.3 Vérification Gold PostgreSQL OVH

```bash
psql "host=<ovh-pg-host> port=20184 dbname=gi_poc_ddi_gold user=<user> sslmode=require" <<'EOF'
SELECT 'dim_metiers'   AS table, COUNT(*) AS lignes, COUNT(pcs_code) AS avec_pcs,
       COUNT(NULLIF(qualification,'')) AS avec_qualification FROM gld_shared.dim_metiers
UNION ALL
SELECT 'fact_competences_dispo', COUNT(*), NULL, NULL FROM gld_staffing.fact_competences_dispo
UNION ALL
SELECT 'fact_ca_mensuel_client', COUNT(*), NULL, NULL FROM gld_commercial.fact_ca_mensuel_client;
EOF
```

---

## 7. Rollback

### 7.1 Rollback code

```bash
cd /opt/gi-data-platform
sudo -u airflow git log --oneline -5
sudo -u airflow git checkout <commit_précédent>
sudo -u airflow .venv/bin/uv sync
sudo systemctl restart airflow-worker airflow-scheduler
```

### 7.2 Rollback schéma PostgreSQL OVH

Les tables Gold sont recréées à chaque run. Un rollback code + re-trigger du DAG suffit.

Si besoin de retirer `pcs_code` immédiatement :

```sql
-- Via psql sur le PG OVH
ALTER TABLE gld_shared.dim_metiers DROP COLUMN IF EXISTS pcs_code;
```

### 7.3 Rollback données S3

Rejouer Silver sur une partition Bronze antérieure :

```bash
SILVER_DATE_PARTITION=2026/03/11 RUN_MODE=live \
  sudo -u airflow .venv/bin/python scripts/silver_competences.py
```

---

## 8. Backfill / rejeu

### 8.1 Re-ingérer une table spécifique

```bash
TABLE_FILTER=WTQUA,WTMET RUN_MODE=live \
  sudo -u airflow .venv/bin/python scripts/bronze_interimaires.py
```

### 8.2 Re-transformer Silver sur une date passée

```bash
SILVER_DATE_PARTITION=2026/03/10 RUN_MODE=live \
  sudo -u airflow .venv/bin/python scripts/silver_competences.py
```

### 8.3 Backfill via Airflow

```bash
sudo -u airflow airflow dags backfill gi_pipeline \
  --start-date 2026-03-10 --end-date 2026-03-11
```

---

## 9. Variables d'environnement de référence

| Variable | Obligatoire | Description | Exemple |
|----------|-------------|-------------|---------|
| `EVOLIA_SERVER` | ✅ | Serveur SQL Evolia | `SRV-SQL2\cegi` |
| `EVOLIA_DB` | ✅ | Base de données | `INTERACTION` |
| `EVOLIA_USER` | ✅ | Compte lecture seule | `srv_data_read_analyst` |
| `EVOLIA_PASSWORD` | ✅ | Mot de passe | — |
| `OVH_S3_ENDPOINT` | ✅ | Endpoint S3 OVHcloud | `https://s3.gra.perf.cloud.ovh.net` |
| `OVH_S3_ACCESS_KEY` | ✅ | Clé d'accès S3 | — |
| `OVH_S3_SECRET_KEY` | ✅ | Clé secrète S3 | — |
| `OVH_PG_HOST` | ✅ | Hôte PostgreSQL OVH managé | `xxx.database.cloud.ovh.net` |
| `OVH_PG_PORT` | ✅ | Port PostgreSQL | `20184` |
| `OVH_PG_USER` | ✅ | Utilisateur PostgreSQL | — |
| `OVH_PG_PASSWORD` | ✅ | Mot de passe PostgreSQL | — |
| `OVH_PG_DATABASE` | ✅ | Base Gold | `gi_poc_ddi_gold` |
| `RGPD_SALT` | ✅ LIVE | Salt pseudonymisation NIR (≥ 32 chars) | — |
| `RUN_MODE` | ✅ | `offline` / `probe` / `live` | `live` |
| `TABLE_FILTER` | ❌ | Filtrer les tables (debug/backfill) | `WTMET,WTQUA` |
| `SILVER_DATE_PARTITION` | ❌ | Partition Bronze à lire (backfill) | `2026/03/10` |
| `ALERT_EMAIL` | ❌ | Destinataire alertes Airflow | `data-team@...` |

### Fichiers .env par environnement

| Fichier | Chargé par | Usage |
|---------|-----------|-------|
| `.env.local` | prioritaire (présence détectée au démarrage) | Laptop développeur |
| `.env` | fallback | FRDC1PIPELINE01 production |
| `.env.example` | versionné dans git | Template de référence |

> `.env` et `.env.local` sont dans `.gitignore` — ne jamais committer les secrets.

---

## Checklist déploiement

### Avant de déployer

- [ ] `uv run pytest tests/ -m unit` — vert
- [ ] `uv run ruff check scripts/` — aucune erreur
- [ ] `.env.example` mis à jour si nouvelles variables
- [ ] Code review approuvée, merge sur `main`

### Sur FRDC1PIPELINE01

- [ ] `git pull` + `uv sync` réussis
- [ ] Probe Bronze : `RUN_MODE=probe python scripts/bronze_missions.py` → 0 erreur
- [ ] Probe Silver : `RUN_MODE=probe python scripts/silver_missions.py` → colonnes enrichies présentes
- [ ] Probe Gold : `RUN_MODE=probe python scripts/gold_operationnel.py` → 0 erreur (fix _write_pg validé)
- [ ] DAG `gi_pipeline` visible dans Airflow UI — 25 tâches, pas d'Import Error
- [ ] Anciens DAGs mis en pause dans l'UI

### Après le premier run LIVE

- [ ] Tous les tasks `gi_pipeline` au vert dans Airflow
- [ ] `gld_operationnel.fact_etp_hebdo` : count > 0
- [ ] `gld_operationnel.fact_delai_placement` : count > 0 (nécessite Silver missions post-2026-03-13)
- [ ] `gld_staffing.fact_fidelisation_interimaires` : count > 0 (nécessite Silver interimaires_detail post-2026-03-13)
- [ ] `gld_clients.fact_concentration_client` : count > 0
- [ ] Aucune alerte email d'échec
