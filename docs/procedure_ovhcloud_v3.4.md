# Procédure de mise en place — Composants OVHcloud & OVHcloud Data Platform (v3.4)
**Plateforme Lakehouse souveraine GI — Architecture cible v3 | POC actif Silver Parquet + Gold PostgreSQL 18**

> **Durée estimée POC** : 1–2 semaines (Silver Parquet + Gold PostgreSQL 18 opérationnel)
> **Durée estimée architecture complète** : 8–10 semaines (Phase 2+)
> **Budget mensuel POC estimé** : ~222 € (S3 + PostgreSQL 18 Business DB1-15)
> **Budget mensuel architecture complète** : 400–600 € (hors coûts réseau et support)
> **Objectif POC** : Provisionner PostgreSQL 18 EU-WEST-PAR, corriger 6 scripts Silver, connecter Superset
> **Auteur** : Architecture Data
> **Version** : 3.4 (mise à jour 2026-03-20)
> **Classification** : INTERNE — CONFIDENTIEL
> **Référence** : GI_Data_Architecture_v3.4.md

---

## ⚠️ CHANGEMENTS v3.4 vs v3.0

| Paramètre | v3.0 | **v3.4 (actif)** |
|---|---|---|
| Région | GRA (Gravelines) | **EU-WEST-PAR (Paris)** |
| Endpoint S3 | `https://s3.gra.perf.cloud.ovh.net` | **`https://s3.eu-west-par.io.cloud.ovh.net/`** |
| PostgreSQL version | 16 | **18** |
| Silver storage | Iceberg via Polaris | **S3 Parquet ZSTD via DuckDB COPY TO** |
| Gold POC | Iceberg marts | **PostgreSQL 18 Business DB1-15** |
| OVH Lakehouse Manager | Phase 1 (immédiat) | **Phase 2+ (post-stabilisation POC)** |
| Schémas Gold | srv_live, srv_analytics, srv_ref | **gld_commercial, gld_staffing, gld_facturation, gld_rh, srv_analytics, srv_ref, slv_mirror** |

---

## TABLE DES MATIÈRES

1. [Vue d'ensemble des composants — POC actif](#1)
2. [Pré-requis](#2)
3. [Phase POC — PostgreSQL 18 Gold (EU-WEST-PAR)](#3)
4. [Phase POC — Correction scripts Silver (6 scripts)](#4)
5. [Phase POC — Connexion Superset OVH Data Platform](#5)
6. [Validation et critères de succès POC](#6)
7. [Estimation des coûts POC](#7)
8. [Phase 2+ — OVH Lakehouse Manager (post-stabilisation)](#8)
9. [Phase 2+ — Infrastructure complète K8s (S3–S5)](#9)
10. [Phase 2+ — Observabilité et monitoring](#10)
11. [Checklist récapitulative](#11)

---

## 1. Vue d'ensemble des composants — POC actif

### Cartographie POC actif

| Composant | Mode | Service OVHcloud | Région | Statut |
|---|---|---|---|---|
| **Object Storage S3 Bronze** | Managé | OVHcloud S3 | **EU-WEST-PAR** | ✅ Opérationnel |
| **Object Storage S3 Silver** | Managé | OVHcloud S3 | **EU-WEST-PAR** | ✅ Opérationnel |
| **Object Storage S3 Gold** | Managé | OVHcloud S3 | **EU-WEST-PAR** | ✅ Opérationnel (vide) |
| **PostgreSQL 18 Gold** | Managé | OVHcloud Managed PostgreSQL 18 | **EU-WEST-PAR** | 🔴 À provisionner |
| **Superset** | Managé | OVHcloud Data Platform | **EU-WEST-PAR** | 🔄 À connecter |
| OVH Lakehouse Manager | Managé | OVHcloud Lakehouse Manager | EU-WEST-PAR | ⏳ Phase 2+ |
| OVH Managed Kubernetes | Managé | OVHcloud Managed K8s | EU-WEST-PAR | ⏳ Phase 2+ |
| Kafka + Debezium | Self-hosted K8s | Strimzi Operator | EU-WEST-PAR | ⏳ Phase 2+ |
| Apache Polaris | Self-hosted K8s | Polaris sur K8s | EU-WEST-PAR | ⏳ Phase 2+ |
| FastAPI | Self-hosted K8s | Deployment K8s | EU-WEST-PAR | ⏳ Phase 3+ |
| VictoriaMetrics + Grafana | Self-hosted K8s | Helm charts | EU-WEST-PAR | ⏳ Phase 2+ |

### Architecture POC déployée

```
OVHcloud Cloud Souverain (EU-WEST-PAR — Paris)
│
├── OVHcloud S3 EU-WEST-PAR ───────────────────────────────────────────────────
│   ├── gi-poc-bronze/   (47.87 Go, JSON brut immutable)
│   ├── gi-poc-silver/   (459 Mo, Parquet ZSTD — DuckDB COPY TO)
│   └── gi-poc-gold/     (vide — réservé migration Phase 2+)
│
├── OVHcloud Managed PostgreSQL 18 (EU-WEST-PAR) ─────────────────────────────
│   └── gi-gold (gi_data database)
│       ├── gld_commercial    (KPIs commerciaux, CA, marges)
│       ├── gld_staffing      (Missions, ETP, taux placement)
│       ├── gld_facturation   (Factures, recouvrement)
│       ├── gld_rh            (Intérimaires, fidélisation, compétences)
│       ├── srv_analytics     (Agrégats pré-calculés Superset)
│       ├── srv_ref           (Référentiels partagés)
│       └── slv_mirror        (Miroir Silver pour consultation)
│
└── OVHcloud Data Platform Superset ─────────────────────────────────────────
    └── Datasource PostgreSQL → gi-gold
        └── Dashboards : commercial, staffing, facturation, RH
```

---

## 2. Pré-requis

### Compte et accès

- [ ] Compte OVHcloud actif avec vérification d'identité validée
- [ ] Projet Public Cloud existant (instance `mly0m4er`)
- [ ] **Région EU-WEST-PAR (Paris) disponible sur le projet** — vérifier dans OVH Manager
- [ ] Buckets `gi-poc-bronze`, `gi-poc-silver`, `gi-poc-gold` déjà provisionnés en EU-WEST-PAR ✅
- [ ] Accès SQL Server Evolia (compte de service dédié, lecture seule)
- [ ] Python 3.12 + environnement virtuel actif

### Outils locaux

```bash
python --version          # >= 3.12
psql --version            # >= 15 (client CLI PostgreSQL)
pip install psycopg2-binary duckdb boto3
```

### Variables d'environnement à préparer — POC

```bash
# Object Storage S3 — RÉGION EU-WEST-PAR
export OVH_S3_ENDPOINT="https://s3.eu-west-par.io.cloud.ovh.net/"
export OVH_S3_REGION="eu-west-par"
export OVH_S3_ACCESS_KEY="<votre_access_key>"
export OVH_S3_SECRET_KEY="<votre_secret_key>"
export BUCKET_BRONZE="gi-poc-bronze"
export BUCKET_SILVER="gi-poc-silver"
export BUCKET_ICEBERG="gi-poc-gold"   # Neutralise Config.__post_init__ sans refactor

# PostgreSQL 18 Gold — renseigné après provisionnement
export OVH_PG_HOST="<host>.database.cloud.ovh.net"
export OVH_PG_PORT="20184"             # Port standard OVHcloud Managed PostgreSQL
export OVH_PG_DATABASE="gi_data"
export OVH_PG_USER="gi_gold_user"
export OVH_PG_PASSWORD="<password>"

# RunMode par défaut
export RUN_MODE="probe"
```

> **Note** : le port OVHcloud Managed PostgreSQL est **20184** (non-standard). Ne pas confondre avec le port PostgreSQL standard 5432.

---

## 3. Phase POC — PostgreSQL 18 Gold (EU-WEST-PAR)

### 3.1 Provisionnement PostgreSQL 18 dans OVH Manager

**Durée estimée : 30 min**

```
1. Aller sur https://www.ovhcloud.com/fr/manager/
2. Public Cloud → Bases de données (menu gauche)
3. Cliquer "Créer une base de données"
4. Sélectionner : PostgreSQL 18
5. Plan : Business DB1-15
   ├── 2 nœuds (haute disponibilité)
   ├── 15 Go RAM
   ├── 4 vCPU
   └── 240 Go SSD NVMe
6. Région : EU-WEST-PAR (Paris) ← IMPÉRATIF
7. Nom : gi-gold
8. Cliquer "Confirmer la commande"
9. Attendre ~10-15 min (statut "Running")
```

> ⚠️ **Vérifier EU-WEST-PAR** : les buckets S3 sont en EU-WEST-PAR. Provisionner PostgreSQL dans une autre région génère des coûts de transfert et de la latence.

### 3.2 Configuration post-provisionnement

#### Étape 1 — Whitelist IP

```
OVH Manager → gi-gold → Sécurité → Autoriser une IP
  Pour développement : 0.0.0.0/0 (à restreindre en prod)
  Pour production    : IP fixe de votre serveur / CI
```

#### Étape 2 — Créer l'utilisateur applicatif

```
OVH Manager → gi-gold → Utilisateurs → Ajouter un utilisateur
  Nom      : gi_gold_user
  Rôle     : Aucun (on assignera les droits via SQL)
  → Copier le mot de passe généré → stocker dans .env
```

#### Étape 3 — Récupérer le DSN de connexion

```
OVH Manager → gi-gold → Informations générales
  → Service URI → Copier le DSN complet :
  postgresql://avnadmin:<password>@<host>.database.cloud.ovh.net:20184/defaultdb?sslmode=require

  → Noter séparément :
  HOST : <host>.database.cloud.ovh.net
  PORT : 20184
  → Mettre à jour le fichier .env
```

#### Étape 4 — Initialisation DDL (ddl_gold_init.sql)

```sql
-- Connexion initiale avec l'utilisateur admin OVH (avnadmin)
-- psql "postgresql://avnadmin:<password>@<host>:20184/defaultdb?sslmode=require"

-- 1. Créer la base de données applicative
CREATE DATABASE gi_data;

-- 2. Se connecter à gi_data
-- \c gi_data

-- 3. Créer les schémas Gold (idempotent)
CREATE SCHEMA IF NOT EXISTS gld_commercial;    -- KPIs commerciaux, CA, marges
CREATE SCHEMA IF NOT EXISTS gld_staffing;      -- Missions, taux placement, ETP
CREATE SCHEMA IF NOT EXISTS gld_facturation;   -- Factures, recouvrement
CREATE SCHEMA IF NOT EXISTS gld_rh;            -- Intérimaires, fidélisation, compétences
CREATE SCHEMA IF NOT EXISTS srv_analytics;     -- Vues Superset (agrégats pré-calculés)
CREATE SCHEMA IF NOT EXISTS srv_ref;           -- Référentiels partagés (agences, SIREN)
CREATE SCHEMA IF NOT EXISTS slv_mirror;        -- Miroir Silver pour consultation Superset

-- 4. Extension pgcrypto (disponible sur tous les plans OVH)
CREATE EXTENSION IF NOT EXISTS pgcrypto;
-- CREATE EXTENSION IF NOT EXISTS vector;  -- Activer en Phase 3+ si plan compatible

-- 5. Créer l'utilisateur applicatif (si pas déjà fait via OVH Manager)
-- CREATE ROLE gi_gold_user LOGIN PASSWORD '<password>';

-- 6. Droits sur les schémas
GRANT ALL ON SCHEMA gld_commercial  TO gi_gold_user;
GRANT ALL ON SCHEMA gld_staffing    TO gi_gold_user;
GRANT ALL ON SCHEMA gld_facturation TO gi_gold_user;
GRANT ALL ON SCHEMA gld_rh          TO gi_gold_user;
GRANT ALL ON SCHEMA srv_analytics   TO gi_gold_user;
GRANT ALL ON SCHEMA srv_ref         TO gi_gold_user;
GRANT ALL ON SCHEMA slv_mirror      TO gi_gold_user;

-- 7. Droits futurs sur les tables
ALTER DEFAULT PRIVILEGES IN SCHEMA gld_commercial  GRANT ALL ON TABLES TO gi_gold_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA gld_staffing    GRANT ALL ON TABLES TO gi_gold_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA gld_facturation GRANT ALL ON TABLES TO gi_gold_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA gld_rh          GRANT ALL ON TABLES TO gi_gold_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA srv_analytics   GRANT ALL ON TABLES TO gi_gold_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA srv_ref         GRANT ALL ON TABLES TO gi_gold_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA slv_mirror      GRANT ALL ON TABLES TO gi_gold_user;

-- 8. Vérification
SELECT schema_name
FROM information_schema.schemata
WHERE schema_name NOT LIKE 'pg_%' AND schema_name != 'information_schema'
ORDER BY schema_name;
-- Doit lister : gld_commercial, gld_facturation, gld_rh, gld_staffing,
--               public, slv_mirror, srv_analytics, srv_ref
```

**Exécution** :

```bash
# Exécuter le script DDL
psql "postgresql://avnadmin:<password>@<host>:20184/defaultdb?sslmode=require" \
  -f ddl_gold_init.sql
```

### 3.3 Validation — probe_pg.py

```python
# probe_pg.py — validation connectivité PostgreSQL 18 Gold
import json
import sys
import psycopg2
from shared import Config, logger

def run():
    cfg = Config()
    try:
        conn = psycopg2.connect(
            host=cfg.pg_host,
            port=cfg.pg_port,
            dbname=cfg.pg_database,
            user=cfg.pg_user,
            password=cfg.pg_password,
            sslmode="require",
            connect_timeout=10
        )
        with conn.cursor() as cur:
            cur.execute("SELECT version();")
            version = cur.fetchone()[0]

            cur.execute("SELECT current_database();")
            db = cur.fetchone()[0]

            cur.execute("SELECT pg_size_pretty(pg_database_size(current_database()));")
            size = cur.fetchone()[0]

            cur.execute("""
                SELECT schema_name FROM information_schema.schemata
                WHERE schema_name NOT LIKE 'pg_%' AND schema_name != 'information_schema'
                ORDER BY schema_name;
            """)
            schemas = [row[0] for row in cur.fetchall()]

        result = {
            "status": "OK",
            "version": version,
            "database": db,
            "size": size,
            "schemas": schemas,
            "expected_schemas": [
                "gld_commercial", "gld_facturation", "gld_rh", "gld_staffing",
                "srv_analytics", "srv_ref", "slv_mirror"
            ]
        }
        missing = [s for s in result["expected_schemas"] if s not in schemas]
        if missing:
            result["warning"] = f"Schémas manquants : {missing}"
        logger.info(json.dumps(result))
        conn.close()
        sys.exit(0)
    except Exception as e:
        logger.error(json.dumps({"status": "ERROR", "error": str(e)}))
        sys.exit(1)

if __name__ == "__main__":
    run()
```

**Exécution** :

```bash
RUN_MODE=probe python probe_pg.py
# Sortie attendue :
# {"status": "OK", "version": "PostgreSQL 18.x ...", "database": "gi_data",
#  "schemas": ["gld_commercial", "gld_facturation", "gld_rh", "gld_staffing",
#              "public", "slv_mirror", "srv_analytics", "srv_ref"]}
```

---

## 4. Phase POC — Correction scripts Silver (6 scripts bloqués)

**Durée estimée : 1h30 via Claude Opus**

### 4.1 Bilan des scripts Silver

| Script | Tables | Pattern | Status |
|---|---|---|---|
| `silver_missions.py` | 6 tables missions | COPY Parquet | ✅ OK |
| `silver_temps.py` | 2 tables heures | COPY Parquet | ✅ OK |
| `silver_factures.py` | 2 tables factures | COPY Parquet | ✅ OK |
| `silver_agences_light.py` | dim_agences, hierarchie_territoriale | `write_silver_iceberg` | ❌ BLOQUÉ |
| `silver_clients.py` | dim_clients (SCD2) | `write_silver_iceberg` | ❌ BLOQUÉ |
| `silver_clients_detail.py` | sites_mission, contacts, encours_credit | `write_silver_iceberg` ×3 | ❌ BLOQUÉ |
| `silver_interimaires.py` | dim_interimaires (SCD2 + RGPD) | `write_silver_iceberg` | ❌ BLOQUÉ |
| `silver_competences.py` | competences (4 types) | `write_silver_iceberg` | ❌ BLOQUÉ |
| `silver_interimaires_detail.py` | evaluations, coordonnees, portefeuille, fidelisation | `write_silver_iceberg` ×3 + 3 bugs | ❌ BLOQUÉ |

### 4.2 Pattern de correction (référence)

```python
# AVANT (bloqué)
write_silver_iceberg(ddb, query, "silver.namespace.table", cfg, stats)

# APRÈS (correct)
silver_path = f"s3://{cfg.bucket_silver}/slv_{domain}/{table}/**/*.parquet"
if cfg.mode in (RunMode.OFFLINE, RunMode.PROBE):
    count = ddb.execute(f"SELECT COUNT(*) FROM ({query})").fetchone()[0]
    logger.info(json.dumps({"mode": cfg.mode.value, "table": table, "rows": count}))
    return count
ddb.execute(f"""
    COPY ({query}) TO '{silver_path}'
    (FORMAT PARQUET, COMPRESSION ZSTD, OVERWRITE_OR_IGNORE TRUE)
""")
count = ddb.execute(f"SELECT COUNT(*) FROM read_parquet('{silver_path}')").fetchone()[0]
```

### 4.3 Bugs cachés à corriger dans silver_interimaires_detail.py

```python
# BUG-1 : RunMode non importé → NameError
# AVANT :
from shared import Config, Stats, get_duckdb_connection, write_silver_iceberg, logger
# APRÈS :
from shared import Config, RunMode, Stats, get_duckdb_connection, logger

# BUG-2 : process_fidelisation() jamais appelée dans run()
# Ajouter dans run() :
c4 = process_fidelisation(ddb, cfg)
stats.tables_processed = 4         # était 3
stats.rows_transformed = c1 + c2 + c3 + c4
stats.extra = {"evaluations": c1, "coordonnees": c2,
               "portefeuille_agences": c3, "fidelisation": c4}

# BUG-3 : COMPRESSION ZSTD manquante dans process_fidelisation()
# AVANT :
COPY ({query}) TO '{silver}' (FORMAT PARQUET, OVERWRITE_OR_IGNORE true)
# APRÈS :
silver = f"s3://{cfg.bucket_silver}/slv_interimaires/fidelisation/**/*.parquet"
COPY ({query}) TO '{silver}' (FORMAT PARQUET, COMPRESSION ZSTD, OVERWRITE_OR_IGNORE TRUE)
```

### 4.4 Validation Silver en mode probe

```bash
# Tester tous les scripts Silver en mode probe (aucune écriture S3)
RUN_MODE=probe python silver_agences_light.py
RUN_MODE=probe python silver_clients.py
RUN_MODE=probe python silver_clients_detail.py
RUN_MODE=probe python silver_interimaires.py
RUN_MODE=probe python silver_competences.py
RUN_MODE=probe python silver_interimaires_detail.py

# Chaque script doit logger :
# {"mode": "probe", "table": "<nom>", "rows": <n>}
# Exit 0 pour tous
```

---

## 5. Phase POC — Connexion Superset OVH Data Platform

**Durée estimée : 15 min**

### 5.1 Ajouter la datasource PostgreSQL Gold dans Superset

```
OVH Data Platform → Analytics Manager → Superset → Ouvrir l'interface Superset
  1. Menu → Settings → Database Connections → + DATABASE
  2. Choisir : PostgreSQL
  3. Renseigner :
     Display Name   : GI Gold PostgreSQL 18
     Host           : <host>.database.cloud.ovh.net
     Port           : 20184
     Database Name  : gi_data
     Username       : gi_gold_user
     Password       : <password>
     ☑ SSL : Require
  4. Cliquer "Test Connection" → doit retourner "Connection looks good!"
  5. Cliquer "Connect"
```

### 5.2 Vérification dans Superset SQL Lab

```sql
-- SQL Lab → connexion "GI Gold PostgreSQL 18"
SELECT
    schema_name,
    COUNT(table_name) AS nb_tables
FROM information_schema.tables
WHERE table_schema IN (
    'gld_commercial', 'gld_staffing', 'gld_facturation', 'gld_rh',
    'srv_analytics', 'srv_ref'
)
GROUP BY schema_name
ORDER BY schema_name;
```

### 5.3 Connexion Airflow (OVH Data Platform)

```
OVH Data Platform → Airflow → Admin → Connections → Add Connection

# Connection PostgreSQL Gold
Conn Id    : gi_postgres_gold
Conn Type  : PostgreSQL
Host       : <host>.database.cloud.ovh.net
Port       : 20184
Schema     : gi_data
Login      : gi_gold_user
Password   : <password>
Extra      : {"sslmode": "require"}

# Connection S3 — REGION EU-WEST-PAR
Conn Id    : gi_s3
Conn Type  : Amazon Web Services
Extra      : {
    "endpoint_url": "https://s3.eu-west-par.io.cloud.ovh.net/",
    "region_name": "eu-west-par",
    "aws_access_key_id": "<access_key>",
    "aws_secret_access_key": "<secret_key>"
}

# Connection Evolia SQL Server
Conn Id    : evolia_sqlserver
Conn Type  : Microsoft SQL Server
Host       : 195.25.27.45
Port       : 1433
Schema     : INTERACTION
Login      : srv_data_read_analyst
Password   : <password>
Extra      : {"TrustServerCertificate": "yes", "Encrypt": "yes",
              "driver": "ODBC Driver 18 for SQL Server"}
```

---

## 6. Validation et critères de succès POC

### 6.1 Checklist de validation POC

| # | Critère | Commande / Vérification | Attendu |
|---|---|---|---|
| 1 | PostgreSQL 18 provisionné | OVH Manager → gi-gold → Running | ✅ Status Running |
| 2 | Région correcte | OVH Manager → gi-gold → Région | EU-WEST-PAR |
| 3 | Connectivité | `RUN_MODE=probe python probe_pg.py` | Exit 0 |
| 4 | Schémas Gold créés | `probe_pg.py` → schemas list | 7 schémas présents |
| 5 | Silver scripts corrigés | `RUN_MODE=probe python silver_*.py` | Tous Exit 0 |
| 6 | Silver LIVE | `RUN_MODE=live python silver_agences_light.py` | Exit 0, count > 0 |
| 7 | Gold LIVE | `RUN_MODE=live python gold_etp.py` | Exit 0, tables peuplées |
| 8 | Superset connecté | SQL Lab → Test Connection | "looks good!" |
| 9 | Premier dashboard | Superset → New Chart → gld_staffing | Données affichées |

### 6.2 Probe de référence — Silver

```bash
# Probe de validation Silver (tous les scripts)
for script in silver_agences_light silver_clients silver_clients_detail \
              silver_interimaires silver_competences silver_interimaires_detail; do
    echo "=== $script ==="
    RUN_MODE=probe python ${script}.py
    echo "Exit: $?"
done
```

### 6.3 Probe de référence — Gold

```bash
# Probe de validation Gold
RUN_MODE=probe python gold_etp.py
RUN_MODE=probe python gold_commercial.py
# Exit 0 attendu pour chaque
```

---

## 7. Estimation des coûts POC

| Composant | Spec | Coût mensuel estimé |
|---|---|---|
| S3 Bronze (47.87 Go) | EU-WEST-PAR, Parquet JSON | ~2€ |
| S3 Silver (459 Mo + croissance) | EU-WEST-PAR, Parquet ZSTD | ~1€ |
| S3 Gold (gi-poc-gold, vide) | EU-WEST-PAR | ~0€ |
| **PostgreSQL 18 Business DB1-15** | 2 nœuds HA, 15 Go RAM, 240 Go SSD | **~200€** |
| OVH Data Platform Superset | Inclus dans le projet | ~19€ |
| **Total POC** | | **~222€/mois** |

**Comparaison** :

| Scénario | Coût mensuel | Délai |
|---|---|---|
| ✅ **POC Silver Parquet + Gold PostgreSQL 18** | **~222€** | **< 1 semaine** |
| Silver PostgreSQL + Gold PostgreSQL | ~982€ | 3-4 semaines |
| Silver Iceberg + Gold Iceberg (Lakehouse Manager) | ~855€ | Indéterminé |

---

## 8. Phase 2+ — OVH Lakehouse Manager (post-stabilisation)

> ⏳ **Cette section s'applique après stabilisation du POC** — une fois Silver + Gold PostgreSQL 18 opérationnels et les premiers domaines Gold validés (≥ 3 domaines actifs).

### 8.1 Déclencheurs de migration vers Iceberg

La migration Silver Parquet → Iceberg sera déclenchée quand :
1. Au moins 3 domaines Gold sont actifs et stables (commercial, staffing, facturation).
2. La documentation OVH Lakehouse Manager est suffisante pour l'intégration du catalog Iceberg.
3. L'équipe dispose de 2 semaines pour la migration.

### 8.2 Migration Silver Parquet → Iceberg

La migration ne modifie **que le sink** dans `shared.py` :

```python
# shared.py — write_silver_iceberg() réactivée (Phase 2+)
# Remplace le pattern COPY TO Parquet dans les 6 scripts corrigés
def write_silver_iceberg(ddb, query, namespace_table, cfg, stats):
    """
    Phase 2+ : écriture vers Iceberg via OVH Lakehouse Manager.
    Le SQL de transformation est identique — seul le sink change.
    """
    # Connexion au REST Catalog OVH Lakehouse Manager
    # catalog_uri = cfg.lakehouse_catalog_uri
    # ddb.execute(f"INSTALL iceberg; LOAD iceberg;")
    # ...
    pass
```

### 8.3 Activation OVH Lakehouse Manager

```
OVH Manager → Data Platform → gi-data-platform
  → Lakehouse Manager → Enable
  Région      : EU-WEST-PAR (IMPÉRATIF — mêmes AZ que les buckets)
  Stockage    : Pointer vers gi-poc-silver et gi-poc-gold existants
  → Récupérer l'URL du REST Catalog : https://lakehouse-xxx.eu-west-par.data.ovh.net/v1
```

**Test de compatibilité REST Catalog** :

```bash
LAKEHOUSE_CATALOG_URL="https://lakehouse-xxx.eu-west-par.data.ovh.net/v1"
curl -s "$LAKEHOUSE_CATALOG_URL/namespaces" \
  -H "Authorization: Bearer $LAKEHOUSE_TOKEN" | python -m json.tool
# Doit retourner les namespaces Iceberg — pas "catalog 'iceberg' not found"
```

### 8.4 Namespaces Iceberg à créer

```bash
POLARIS_URL="http://polaris-catalog.data-platform.svc:8181/api/catalog/v1"

# Namespaces Silver
curl -X POST "$POLARIS_URL/namespaces" -H "Content-Type: application/json" \
  -d '{"namespace": ["silver"], "properties": {"location": "s3://gi-poc-silver/iceberg/"}}'
curl -X POST "$POLARIS_URL/namespaces" -H "Content-Type: application/json" \
  -d '{"namespace": ["silver", "agences"], "properties": {}}'
curl -X POST "$POLARIS_URL/namespaces" -H "Content-Type: application/json" \
  -d '{"namespace": ["silver", "clients"], "properties": {}}'
curl -X POST "$POLARIS_URL/namespaces" -H "Content-Type: application/json" \
  -d '{"namespace": ["silver", "interimaires"], "properties": {}}'

# Namespaces Gold
curl -X POST "$POLARIS_URL/namespaces" -H "Content-Type: application/json" \
  -d '{"namespace": ["gold"], "properties": {"location": "s3://gi-poc-gold/iceberg/"}}'
curl -X POST "$POLARIS_URL/namespaces" -H "Content-Type: application/json" \
  -d '{"namespace": ["gold", "commercial"], "properties": {}}'
curl -X POST "$POLARIS_URL/namespaces" -H "Content-Type: application/json" \
  -d '{"namespace": ["gold", "staffing"], "properties": {}}'
curl -X POST "$POLARIS_URL/namespaces" -H "Content-Type: application/json" \
  -d '{"namespace": ["gold", "facturation"], "properties": {}}'
```

---

## 9. Phase 2+ — Infrastructure complète K8s

> ⏳ **Cette section s'applique après stabilisation du POC.**

### Pré-requis (à vérifier lors du passage Phase 2+)

```bash
terraform --version   # >= 1.5
kubectl version       # >= 1.29
helm version          # >= 3.14
```

### Cluster Kubernetes OVH (EU-WEST-PAR)

```
OVH Manager → Managed Kubernetes → Create Cluster
Version : 1.30
Région  : EU-WEST-PAR
Nom     : gi-data-k8s
```

**Node Pools** :

| Pool | Instances | Type | RAM | Rôle |
|---|---|---|---|---|
| `system` | 2 | B2-15 | 15 Go | Kafka, Polaris, monitoring |
| `compute` | 2–3 | B2-30 | 30 Go | Spark batch + streaming |
| `api` | 2 | B2-7 | 7 Go | FastAPI, Apicurio |

### Namespace K8s

```bash
kubectl create namespace data-platform
kubectl create namespace kafka
kubectl create namespace monitoring
kubectl create namespace api
```

### Secrets K8s (remplacent le .env en Phase 2+)

```bash
kubectl -n data-platform create secret generic s3-credentials \
  --from-literal=endpoint="https://s3.eu-west-par.io.cloud.ovh.net/" \
  --from-literal=region="eu-west-par" \
  --from-literal=access-key="<access_key>" \
  --from-literal=secret-key="<secret_key>"

kubectl -n data-platform create secret generic pg-gold-credentials \
  --from-literal=host="<host>.database.cloud.ovh.net" \
  --from-literal=port="20184" \
  --from-literal=database="gi_data" \
  --from-literal=user="gi_gold_user" \
  --from-literal=password="<password>" \
  --from-literal=url="postgresql://gi_gold_user:<password>@<host>:20184/gi_data?sslmode=require"
```

### Kafka + Debezium + Apicurio (EU-WEST-PAR)

```bash
# Strimzi Kafka Operator
helm repo add strimzi https://strimzi.io/charts
helm install strimzi-operator strimzi/strimzi-kafka-operator --namespace kafka

# Apicurio Registry (Schema Registry)
kubectl -n kafka apply -f apicurio-registry.yaml

# Connecteur CDC Evolia
kubectl apply -f debezium-connector-evolia.yaml
```

> Note : tous les YAML Kafka sont identiques à v3.0 — seuls les endpoints S3 changent
> (`https://s3.gra.perf.cloud.ovh.net` → `https://s3.eu-west-par.io.cloud.ovh.net/`).

---

## 10. Phase 2+ — Observabilité et monitoring

Architecture OTel identique à v3.0 — tous les endpoints S3 doivent utiliser EU-WEST-PAR.

```bash
# OTel Collector
helm install otel-collector open-telemetry/opentelemetry-collector \
  --namespace monitoring \
  --set config.exporters.kafka.brokers="{gi-kafka-kafka-bootstrap.kafka.svc:9092}"

# VictoriaMetrics
helm install victoriametrics vm/victoria-metrics-single \
  --namespace monitoring \
  --set server.retentionPeriod=30d

# Grafana
helm install grafana grafana/grafana \
  --namespace monitoring \
  --set persistence.enabled=true
```

**Dashboards prioritaires Phase 2+** :
1. Pipeline Health : jobs Silver + Gold (succès/échec/durée)
2. Data Freshness : fraîcheur par table PostgreSQL Gold
3. S3 Usage : taille buckets Bronze/Silver par semaine
4. API Performance : latence p50/p95/p99 (Phase 3+)

---

## 11. Checklist récapitulative

### POC — Semaine 1

- [ ] **D01** : Provisionner PostgreSQL 18 Business DB1-15 EU-WEST-PAR (30 min)
- [ ] **D02** : Mettre à jour .env (OVH_PG_* + BUCKET_ICEBERG=gi-poc-gold) (5 min)
- [ ] **D03** : Exécuter ddl_gold_init.sql — 7 schémas créés (10 min)
- [ ] **D04** : Valider probe_pg.py Exit 0 (5 min)
- [ ] **D05** : Corriger 6 scripts Silver via Claude Opus (1h30)
- [ ] **D06** : Valider tous scripts Silver RUN_MODE=probe Exit 0 (20 min)
- [ ] **D07** : Exécuter Silver LIVE complet (2-3h selon réseau)
- [ ] **D08** : Exécuter Gold LIVE scripts gold_*.py (1-2h)
- [ ] **D09** : Connecter Superset OVH Data Platform → PostgreSQL Gold (15 min)
- [ ] **D10** : Premier dashboard Superset opérationnel (30 min)

### Phase 2+ — Post-stabilisation (Q2-Q3 2026)

- [ ] Tester OVH Lakehouse Manager REST Catalog (catalog 'iceberg' accessible ?)
- [ ] Provisionner OVH Managed Kubernetes EU-WEST-PAR
- [ ] Déployer Kafka + Debezium + Apicurio
- [ ] Migrer Silver Parquet → Iceberg (remplacement sink shared.py)
- [ ] Déployer Apache Polaris
- [ ] Déployer observabilité OTel + VictoriaMetrics + Grafana

### Phase 3+ — Serving enrichi

- [ ] Activer pgvector sur PostgreSQL 18
- [ ] Déployer FastAPI
- [ ] Évaluation SQLMesh vs Dagster assets
- [ ] Évaluation Infisical vs HashiCorp Vault (si > 50 secrets ou tokenisation réversible)

---

*Document mis à jour le 20/03/2026 — Architecture Data — Groupe Interaction*
*Classification : INTERNE — CONFIDENTIEL*
*Diff v3.4 : région EU-WEST-PAR, PostgreSQL 18 (pas 16), Silver Parquet DuckDB (pas Iceberg), Lakehouse Manager repositionné Phase 2+*
