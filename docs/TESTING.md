# 🧪 Guide de Tests — GI Data Platform

> Tous les tests s'exécutent depuis la racine du projet `gi-data-platform/`.  
> Prérequis : `uv sync` (ou `pip install -e ".[dev]"`) effectué.

---

## 0. Setup rapide

```powershell
# Installer les dépendances dev
uv sync --extra dev

# Vérifier l'environnement
python -c "import duckdb, boto3, pyodbc, psycopg2; print('✓ Dépendances OK')"
```

---

## 1. Linting & formatage (sans I/O)

```powershell
# Lint complet — erreurs bloquantes
uv run ruff check scripts/

# Lint + fix auto (safe)
uv run ruff check scripts/ --fix

# Formatage
uv run ruff format scripts/

# Typage statique
uv run mypy scripts/ --ignore-missing-imports

# Tout d'un coup (CI-equivalent)
uv run ruff check scripts/ && uv run ruff format --check scripts/ && echo "✓ Lint OK"
```

---

## 2. Tests unitaires (RunMode.OFFLINE — zéro I/O)

Aucune connexion requise. Exécutables en CI/CD et hors VPN.

```powershell
# Tous les tests unitaires
$env:RUN_MODE = "offline"
uv run pytest tests/ -m unit -v

# Test d'un module spécifique
uv run pytest tests/test_bronze_interimaires.py -v

# Test avec coverage
uv run pytest tests/ -m unit --cov=scripts --cov-report=term-missing

# Validation Config (dry_run, date_partition)
python -c "
import os
os.environ['RUN_MODE'] = 'offline'
os.environ['EVOLIA_SERVER'] = 'mock'
os.environ['EVOLIA_DB'] = 'mock'
os.environ['EVOLIA_USER'] = 'mock'
os.environ['EVOLIA_PASSWORD'] = 'mock'
os.environ['OVH_S3_ENDPOINT'] = 'mock'
os.environ['OVH_S3_ACCESS_KEY'] = 'mock'
os.environ['OVH_S3_SECRET_KEY'] = 'mock'
from scripts.shared import Config
c = Config()
print('mode:', c.mode)
print('date_partition:', c.date_partition)   # YYYY/MM/DD du jour
print('dry_run:', c.dry_run)
"

# Tester override date_partition (backfill)
$env:SILVER_DATE_PARTITION = "2026/03/01"
python -c "
import os; os.environ.update({'RUN_MODE':'offline','EVOLIA_SERVER':'x','EVOLIA_DB':'x','EVOLIA_USER':'x','EVOLIA_PASSWORD':'x','OVH_S3_ENDPOINT':'x','OVH_S3_ACCESS_KEY':'x','OVH_S3_SECRET_KEY':'x'})
from scripts.shared import Config, s3_bronze
c = Config()
print(s3_bronze(c, 'PYPERSONNE'))
# Attendu : s3://gi-poc-bronze/raw_pypersonne/2026/03/01/*.json
"
$env:SILVER_DATE_PARTITION = ""  # Reset
```

---

## 3. Probe DDL (RunMode.PROBE — lecture Evolia, zéro écriture)

Nécessite VPN + `.env.local` configuré.

```powershell
# Prérequis : .env.local avec EVOLIA_* renseignés
$env:RUN_MODE = "probe"

# Probe complet — vérifie toutes les colonnes DDL attendues
cd scripts/
python probe_ddl.py

# Résultat : ddl_probe_result.json dans le répertoire courant
# Vérifier le résumé
python -c "
import json
with open('ddl_probe_result.json') as f: r = json.load(f)
s = r.get('summary', {})
print(f'PASS:{s.get(\"pass\",0)} WARN:{s.get(\"warn\",0)} FAIL:{s.get(\"fail\",0)}')
"

# Probe Bronze — compte les lignes delta disponibles (5 tables pilotes)
$env:RUN_MODE = "probe"
python scripts/bronze_missions.py
python scripts/bronze_interimaires.py  # Ne requête QUE WTPEVAL + WTUGPINT en probe
python scripts/bronze_clients.py       # Ne requête QUE CMTIER en probe
python scripts/bronze_agences.py       # Ne requête QUE PYREGROUPCNT + PYENTREPRISE
```

---

## 4. Dry-run Silver (RunMode.PROBE — lit Bronze S3, zéro écriture Silver)

Nécessite des données en Bronze S3 (au moins 1 exécution Bronze préalable).

```powershell
$env:RUN_MODE = "probe"

# Tester la partition du jour
python scripts/silver_interimaires.py
python scripts/silver_interimaires_detail.py
python scripts/silver_competences.py
python scripts/silver_clients.py
python scripts/silver_clients_detail.py
python scripts/silver_agences_light.py
python scripts/silver_factures.py
python scripts/silver_missions.py
python scripts/silver_temps.py

# Tester avec une partition spécifique (backfill)
$env:SILVER_DATE_PARTITION = "2026/03/04"
python scripts/silver_factures.py
$env:SILVER_DATE_PARTITION = ""
```

---

## 5. Test de connectivité (avant run live)

```powershell
# Connexion Evolia SQL Server
python -c "
import os, pyodbc
conn_str = (
    'DRIVER={ODBC Driver 18 for SQL Server};'
    f'SERVER={os.environ[\"EVOLIA_SERVER\"]},1433;'
    f'DATABASE={os.environ[\"EVOLIA_DB\"]};'
    f'UID={os.environ[\"EVOLIA_USER\"]};PWD={os.environ[\"EVOLIA_PASSWORD\"]};'
    'TrustServerCertificate=yes;Encrypt=yes;'
)
with pyodbc.connect(conn_str, timeout=10) as conn:
    with conn.cursor() as cur:
        cur.execute('SELECT COUNT(*) FROM WTMISS')
        print('✓ Evolia OK —', cur.fetchone()[0], 'missions')
"

# Connexion S3 OVHcloud
python -c "
import os, boto3
s3 = boto3.client('s3',
    endpoint_url=os.environ['OVH_S3_ENDPOINT'],
    aws_access_key_id=os.environ['OVH_S3_ACCESS_KEY'],
    aws_secret_access_key=os.environ['OVH_S3_SECRET_KEY'],
)
buckets = [b['Name'] for b in s3.list_buckets()['Buckets']]
print('✓ S3 OK — buckets:', buckets)
"

# Connexion PostgreSQL Gold
python -c "
import os, psycopg2
with psycopg2.connect(
    host=os.environ['PG_HOST'], port=os.environ.get('PG_PORT', 5432),
    dbname=os.environ.get('PG_DB', 'gi_poc'),
    user=os.environ['PG_USER'], password=os.environ['PG_PASSWORD'],
    sslmode='require'
) as conn:
    with conn.cursor() as cur:
        cur.execute('SELECT version()')
        print('✓ PostgreSQL OK —', cur.fetchone()[0][:40])
"
```

---

## 6. Test FinOps — vérifier le partitionnement S3

```powershell
# Vérifier qu'un Bronze a bien écrit dans la partition du jour
$date = (Get-Date -Format "yyyy/MM/dd")
aws s3 ls "s3://gi-poc-bronze/raw_wtmiss/$date/" `
    --endpoint-url $env:OVH_S3_ENDPOINT
# Attendu : 1+ fichier batch_XXXXXXXX.json

# Vérifier le Silver lit bien la bonne partition (pas **/*.json)
$env:RUN_MODE = "probe"
python -c "
import os
os.environ['RUN_MODE'] = 'probe'
from scripts.shared import Config, s3_bronze
c = Config()
print('Bronze path Silver utilise:', s3_bronze(c, 'WTMISS'))
# Doit afficher /2026/03/05/*.json et NON /**/*.json
"

# Simuler un backfill J-1
$env:SILVER_DATE_PARTITION = (Get-Date).AddDays(-1).ToString("yyyy/MM/dd")
$env:RUN_MODE = "probe"
python scripts/silver_factures.py
# Log doit afficher : mode=probe, rows=X pour la partition d'hier
$env:SILVER_DATE_PARTITION = ""
```

---

## 7. Pipeline complet (RunMode.LIVE — écriture réelle)

⚠️ Uniquement après validation de tous les tests ci-dessus.

```powershell
$env:RUN_MODE = "live"

# 1. Bronze (depuis serveur on-prem ou laptop + VPN)
python scripts/bronze_missions.py       # Tables avec delta confirmé
python scripts/bronze_interimaires.py   # WTPEVAL delta + 8 tables full-load
python scripts/bronze_clients.py        # CMTIER delta + 7 tables full-load
python scripts/bronze_agences.py        # 2 delta + 3 full-load

# 2. Silver (depuis OVH Worker Airflow ou local avec accès S3)
python scripts/silver_missions.py
python scripts/silver_temps.py
python scripts/silver_factures.py
python scripts/silver_interimaires.py
python scripts/silver_interimaires_detail.py
python scripts/silver_competences.py
python scripts/silver_clients.py
python scripts/silver_clients_detail.py
python scripts/silver_agences_light.py

# 3. Vérifier les stats du dernier run
python -c "
# Derniers logs structurés (JSON) — extraire les stats
import subprocess, json
# Exemple : lire le log Airflow ou stdout redirigé
"
```

---

## 8. Qualité des données (Great Expectations)

```powershell
# Initialiser (première fois)
cd quality/
great_expectations init

# Créer une suite pour Bronze WTMISS
great_expectations suite new

# Lancer les checkpoints
great_expectations checkpoint run bronze_missions_checkpoint
great_expectations checkpoint run silver_factures_checkpoint

# Rapport HTML
great_expectations docs build
start gx/uncommitted/data_docs/local_site/index.html
```

---

## 9. Commandes de diagnostic rapide

```powershell
# Compter les lignes par partition Bronze
$env:RUN_MODE = "probe"
python -c "
import duckdb, os
con = duckdb.connect()
con.execute('INSTALL httpfs; LOAD httpfs;')
# ... configurer S3 ...
# Compter par partition
res = con.execute('''
    SELECT regexp_extract(filename, '(2026/[0-9]{2}/[0-9]{2})', 1) AS date_partition,
           COUNT(*) AS nb_fichiers
    FROM glob('s3://gi-poc-bronze/raw_wtmiss/**/*.json')
    GROUP BY 1 ORDER BY 1 DESC LIMIT 7
''').fetchall()
for row in res: print(row)
"

# Vérifier watermarks Bronze (derniers ingestion timestamps)
python -c "
import os, psycopg2
with psycopg2.connect(host=os.environ['PG_HOST'], ...) as conn:
    with conn.cursor() as cur:
        cur.execute('SELECT pipeline, table_name, last_success, last_error FROM gi_watermarks ORDER BY last_success DESC LIMIT 20')
        for r in cur.fetchall(): print(r)
"
```
