# Migration GI Data Platform — POC vers OVH Data Platform (Lakehouse Manager)

> **Version 1.0 · 2026-03-15**
> Auteur : GI Data Team
> Périmètre : migration end-to-end de l'architecture ETL Python/DuckDB/PostgreSQL
> vers OVH Data Platform avec Trino, Apache Iceberg et Spark managé.

---

## Sommaire

1. [Architecture actuelle vs cible](#1-architecture-actuelle-vs-cible)
2. [Pré-requis et durée estimée](#2-pré-requis-et-durée-estimée)
3. [Phase 1 — Provisioning OVH Data Platform](#3-phase-1--provisioning-ovh-data-platform)
4. [Phase 2 — Enregistrement Bronze dans le Metastore](#4-phase-2--enregistrement-bronze-dans-le-metastore)
5. [Phase 3 — Migration Silver Parquet → Iceberg](#5-phase-3--migration-silver-parquet--iceberg)
6. [Phase 4 — Migration transformations DuckDB → Spark](#6-phase-4--migration-transformations-duckdb--spark)
7. [Phase 5 — Gold : PostgreSQL vs Iceberg](#7-phase-5--gold--postgresql-vs-iceberg)
8. [Phase 6 — Superset → connexion Trino](#8-phase-6--superset--connexion-trino)
9. [Phase 7 — Migration Airflow (BashOperator → SparkSubmit)](#9-phase-7--migration-airflow-bashoperator--sparksubmit)
10. [Phase 8 — Validation et go-live](#10-phase-8--validation-et-go-live)
11. [Plan de rollback](#11-plan-de-rollback)
12. [Coûts et dimensionnement](#12-coûts-et-dimensionnement)
13. [Backlog post-migration](#13-backlog-post-migration)

---

## 1. Architecture actuelle vs cible

### Actuelle (POC)

```
Evolia (SQL Server on-prem)
  │  pymssql
  ▼
S3 Bronze  gi-poc-bronze         JSON newline-delimited, partitionné YYYY/MM/DD
  │  DuckDB read_json_auto()
  ▼
S3 Silver  gi-poc-silver         Parquet ZSTD, partitionné par date_partition
  │  DuckDB → psycopg2 pg_bulk_insert (TRUNCATE+COPY)
  ▼
PostgreSQL Gold  gi_poc_ddi_gold  Schémas gld_staffing / gld_commercial / gld_performance…
  │  psycopg2
  ▼
Apache Superset
  │
Airflow 2.8  FRDC1PIPELINE01      BashOperator → uv run scripts/*.py
```

**Limites identifiées à l'échelle prod :**
- DuckDB = single-node, pas de parallélisme distribué (goulot WTRHDON 760K+/j, WTLFAC 26M lignes)
- PostgreSQL Gold = TRUNCATE+COPY, pas de time-travel ni ACID multi-writers
- Parquet Silver = format statique, pas de schema evolution ni de merge incrémental
- Pas de catalogue centralisé (lineage, data quality, discovery)
- Pas de SQL ad-hoc sur Bronze sans script Python

### Cible (OVH Lakehouse Manager)

```
Evolia (SQL Server on-prem)
  │  pymssql (inchangé)
  ▼
S3 Bronze  gi-bronze             JSON → inchangé (Bronze = landing zone, immutable)
  │  Glue/Hive Metastore (table externe, schema-on-read)
  ▼  Spark Structured Streaming ou batch Spark
S3 Silver  gi-silver             Apache Iceberg (ACID, time-travel, schema evolution)
  │  Hive Metastore catalog
  ▼  Spark SQL ou Trino INSERT INTO
S3 Gold    gi-gold               Apache Iceberg (schémas gld_*)
  │  Trino (SQL distribué sur Iceberg)
  ▼
Apache Superset  ──── connexion Trino (trino://...)
  │
Airflow 2.8  (managed OVH ou FRDC1PIPELINE01)
  SparkSubmitOperator + TrinoOperator
```

**Gains attendus :**
| Aspect | POC | Lakehouse |
|---|---|---|
| Transformation Silver | DuckDB single-node | Spark distribué (scale-out) |
| SQL ad-hoc Bronze/Silver | Script Python dédié | `SELECT * FROM bronze.raw_wtmiss` via Trino |
| Schema evolution | Casse le pipeline | `ALTER TABLE … ADD COLUMN` Iceberg ACID |
| Time-travel | Impossible | `SELECT … FOR TIMESTAMP AS OF '...'` Iceberg |
| Lineage | Manuel | DataHub auto-découverte via Trino connector |
| Concurrence écritures | Impossible (TRUNCATE) | Merge/upsert Iceberg |

---

## 2. Pré-requis et durée estimée

### Compétences requises

- Maîtrise SQL (Trino est ANSI SQL, proche DuckDB)
- Notions Spark/PySpark (les transformations DuckDB se traduisent quasi-directement)
- Accès OVH console + droits IAM S3

### Services OVH à provisionner

| Service OVH | Usage | Remarque |
|---|---|---|
| **Lakehouse Manager** | Trino + Hive Metastore | Cœur de la migration |
| **Data Processing** | Spark managé | Remplacement DuckDB |
| **Object Storage** | Buckets S3 (déjà existants) | Renommer gi-poc-* → gi-* |
| **Managed Databases PostgreSQL** | Gold serving layer (option A) | Garder ou migrer |
| **Managed Airflow** (optionnel) | Remplacer FRDC1PIPELINE01 | Optionnel phase 7 |

### Durée estimée

| Phase | Effort | Qui |
|---|---|---|
| Phase 1 : Provisioning | 0.5j | DevOps |
| Phase 2 : Metastore Bronze | 1j | Data Engineer |
| Phase 3 : Silver → Iceberg | 3j | Data Engineer |
| Phase 4 : DuckDB → Spark | 5j | Data Engineer |
| Phase 5 : Gold | 2j | Data Engineer |
| Phase 6 : Superset | 0.5j | Data Engineer |
| Phase 7 : Airflow | 1j | DevOps/Data Engineer |
| Phase 8 : Validation | 2j | Équipe complète |
| **Total** | **~15 jours** | |

### Stratégie de migration

> **Principe : migration en parallèle, pas de cut-over brutal.**
> Les scripts Python existants continuent de tourner pendant toute la migration.
> Chaque couche est migrée et validée indépendamment avant la bascule.
> Rollback possible à n'importe quelle phase.

---

## 3. Phase 1 — Provisioning OVH Data Platform

### 3.1 Créer le Lakehouse Manager

Depuis la console OVH Cloud (manager.ovh.com) :

```
Public Cloud → Data & Analytics → Lakehouse Manager → Créer un service
  Région       : GRA (même région que les buckets S3 existants)
  Version      : dernière stable (Trino 435+ / Hive Metastore 3.1+)
  Connexion    : noter l'endpoint Trino  →  trino://gi-lakehouse.xxx.ovh.com:443
```

### 3.2 Créer les buckets de production

```bash
# Créer les nouveaux buckets (sans le suffixe -poc)
aws s3api create-bucket --bucket gi-bronze --region gra \
  --endpoint-url https://s3.gra.io.cloud.ovh.net

aws s3api create-bucket --bucket gi-silver --region gra \
  --endpoint-url https://s3.gra.io.cloud.ovh.net

aws s3api create-bucket --bucket gi-gold   --region gra \
  --endpoint-url https://s3.gra.io.cloud.ovh.net
```

### 3.3 Configurer les accès IAM S3 → Lakehouse

Dans la console OVH → IAM, créer un user technique `gi-lakehouse-svc` avec :
- Droits `s3:GetObject`, `s3:PutObject`, `s3:DeleteObject`, `s3:ListBucket` sur `gi-bronze`, `gi-silver`, `gi-gold`
- Stocker `AWS_ACCESS_KEY_ID` et `AWS_SECRET_ACCESS_KEY` dans le vault Airflow

### 3.4 Provisionner Data Processing (Spark)

```
Public Cloud → Data & Analytics → Data Processing → Créer un job
  Environnement : Python 3.12 + PySpark 3.5
  Région        : GRA
```

> **Note :** OVH Data Processing est serverless. Pas de cluster permanent —
> chaque job Spark démarre un cluster éphémère, facturé à la seconde.

### 3.5 Mettre à jour .env.local

```bash
# Ajouter dans .env.local
BRONZE_BUCKET=gi-bronze
SILVER_BUCKET=gi-silver
GOLD_BUCKET=gi-gold

TRINO_HOST=gi-lakehouse.xxx.ovh.com
TRINO_PORT=443
TRINO_USER=gi-lakehouse-svc
TRINO_PASSWORD=<token_OVH>
TRINO_CATALOG=gi_catalog

SPARK_ENDPOINT=https://gra.data-processing.cloud.ovh.net
SPARK_TOKEN=<token_OVH_data_processing>
```

---

## 4. Phase 2 — Enregistrement Bronze dans le Metastore

> **Objectif :** rendre les données Bronze existantes (JSON S3) requêtables via Trino
> sans modifier les scripts Bronze ni re-ingérer les données.

### 4.1 Principe : tables externes Hive (schema-on-read)

Le Hive Metastore stocke le schéma et le path S3. Trino lit le JSON à la volée.
Les scripts `bronze_*.py` continuent d'écrire exactement comme avant.

### 4.2 Créer le catalogue Bronze dans le Lakehouse Manager

Via la console OVH Lakehouse → Catalogs :

```
Catalog name : bronze
Type         : Hive (S3 JSON)
S3 endpoint  : https://s3.gra.io.cloud.ovh.net
Bucket       : gi-bronze
```

### 4.3 Créer les tables externes via Trino CLI

```sql
-- Connexion : trino --server gi-lakehouse.xxx.ovh.com:443 --user gi-lakehouse-svc

CREATE SCHEMA IF NOT EXISTS bronze.raw;

-- Exemple : WTMISS
CREATE TABLE bronze.raw.wtmiss (
    per_id              BIGINT,
    cnt_id              BIGINT,
    tie_id              BIGINT,
    rgpcnt_id           BIGINT,
    finmiss_code        VARCHAR,
    miss_saisie_dtfin   TIMESTAMP,
    miss_annule         INTEGER,
    cnti_create         TIMESTAMP,
    cmd_id              VARCHAR,
    -- ... toutes les colonnes de _COLS["WTMISS"] dans bronze_missions.py
    _loaded_at          TIMESTAMP,
    _batch_id           VARCHAR,
    _source_table       VARCHAR
)
WITH (
    format            = 'JSON',
    external_location = 's3a://gi-bronze/raw_wtmiss/',
    partitioned_by    = ARRAY['year', 'month', 'day']  -- partitionnement YYYY/MM/DD
);

-- Répéter pour chaque table Bronze (WTCNTI, WTEFAC, WTFINMISS, PYMTFCNT, etc.)
-- Script de génération automatique : voir section 4.4
```

### 4.4 Script de génération DDL automatique

Plutôt que de créer chaque table manuellement, générer le DDL depuis les `_COLS` existants :

```python
# scripts/generate_trino_ddl.py
"""Génère les CREATE TABLE Trino Bronze depuis les _COLS des scripts Bronze."""
import ast, sys
from pathlib import Path

# Lire _COLS depuis bronze_missions.py
# Mapper types Python/JSON → types Trino
TYPE_MAP = {
    "id": "BIGINT", "code": "VARCHAR", "date": "TIMESTAMP",
    "create": "TIMESTAMP", "annule": "INTEGER", "nb": "INTEGER",
}

def col_to_trino_type(col_name: str) -> str:
    col = col_name.lower()
    for k, t in TYPE_MAP.items():
        if k in col:
            return t
    return "VARCHAR"

# Pour chaque table dans _COLS, produire le DDL Trino
# ... (implémenter selon les besoins)
```

### 4.5 Validation Phase 2

```sql
-- Vérifier que les données Bronze sont visibles via Trino
SELECT COUNT(*), MAX(_loaded_at)
FROM bronze.raw.wtmiss;

-- Vérifier le contenu WTFINMISS (11 lignes attendues)
SELECT finmiss_code, finmiss_libelle
FROM bronze.raw.wtfinmiss
ORDER BY 1;
```

> **Critère de succès Phase 2 :** toutes les tables Bronze requêtables via Trino,
> counts identiques aux probes Python/DuckDB existants.

---

## 5. Phase 3 — Migration Silver Parquet → Iceberg

> **Objectif :** convertir les tables Silver de Parquet statique vers Apache Iceberg
> pour ACID, schema evolution et time-travel. Migration sans downtime via
> la stratégie shadow-write.

### 5.1 Pourquoi Iceberg plutôt que Parquet brut

| Fonctionnalité | Parquet actuel | Iceberg |
|---|---|---|
| Ajout de colonne | Casse DuckDB read_parquet() | `ALTER TABLE ADD COLUMN` ACID |
| Upsert / merge | Impossible, OVERWRITE_OR_IGNORE | `MERGE INTO` natif |
| Time-travel | Impossible | `SELECT … FOR VERSION AS OF …` |
| Compaction automatique | Manuel | Iceberg maintenance proc |
| Schema evolution | Manuel + re-run complet | Automatique, versionnée |
| Partition evolution | Impossible sans re-écriture | `ALTER TABLE … ADD PARTITION FIELD` |

### 5.2 Créer le catalogue Silver Iceberg

```
Console OVH Lakehouse → Catalogs → Ajouter
  Catalog name : silver
  Type         : Iceberg (REST catalog)
  S3 bucket    : gi-silver
  Warehouse    : s3a://gi-silver/iceberg/
```

```sql
CREATE SCHEMA IF NOT EXISTS silver.missions;
CREATE SCHEMA IF NOT EXISTS silver.interimaires;
CREATE SCHEMA IF NOT EXISTS silver.agences;
CREATE SCHEMA IF NOT EXISTS silver.clients;
CREATE SCHEMA IF NOT EXISTS silver.temps;
CREATE SCHEMA IF NOT EXISTS silver.facturation;
```

### 5.3 Créer les tables Iceberg Silver (exemple : fin_mission)

```sql
-- silver.missions.fin_mission — cible migration de slv_missions/fin_mission Parquet
CREATE TABLE silver.missions.fin_mission (
    per_id              BIGINT,
    cnt_id              BIGINT,
    rgpcnt_id           BIGINT,
    tie_id              BIGINT,
    date_debut          DATE,
    date_fin_reelle     DATE,
    date_fin_saisie     DATE,
    finmiss_code        VARCHAR,
    finmiss_libelle     VARCHAR,
    mtfcnt_code         VARCHAR,
    mtfcnt_libelle      VARCHAR,
    mtfcnt_fincnt       INTEGER,
    miss_annule         INTEGER,
    duree_reelle_jours  INTEGER,
    statut_fin_mission  VARCHAR,
    _batch_id           VARCHAR,
    _loaded_at          TIMESTAMP
)
WITH (
    format             = 'PARQUET',
    location           = 's3a://gi-silver/iceberg/missions/fin_mission/',
    partitioning       = ARRAY['month(date_debut)']
);

-- Répéter pour chaque table Silver (missions, contrats, commandes, placements,
-- contrats_paie, dim_interimaires, dim_agences, dim_clients, etc.)
```

### 5.4 Migration initiale Parquet → Iceberg (copie unique)

La migration initiale lit les Parquet existants depuis S3 et les insère dans Iceberg
via un job Spark (Data Processing OVH) :

```python
# scripts/migrate_silver_to_iceberg.py
# Déposer sur OVH Data Processing comme job Spark
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("gi-migrate-silver-iceberg") \
    .config("spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.silver", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.silver.type", "rest") \
    .config("spark.sql.catalog.silver.uri", "https://gi-lakehouse.xxx.ovh.com/iceberg") \
    .config("spark.sql.catalog.silver.s3.endpoint",
            "https://s3.gra.io.cloud.ovh.net") \
    .getOrCreate()

TABLES = [
    ("slv_missions/missions",          "silver.missions.missions"),
    ("slv_missions/contrats",          "silver.missions.contrats"),
    ("slv_missions/fin_mission",       "silver.missions.fin_mission"),
    ("slv_interimaires/dim_interimaires", "silver.interimaires.dim_interimaires"),
    ("slv_agences/dim_agences",        "silver.agences.dim_agences"),
    ("slv_clients/dim_clients",        "silver.clients.dim_clients"),
    ("slv_temps/releves_heures",       "silver.temps.releves_heures"),
    ("slv_temps/heures_detail",        "silver.temps.heures_detail"),
    # ... compléter avec tous les préfixes Silver
]

for src_prefix, iceberg_table in TABLES:
    src_path = f"s3a://gi-poc-silver/{src_prefix}/**/*.parquet"
    print(f"Migrating {src_path} → {iceberg_table}")
    df = spark.read.parquet(src_path)
    df.writeTo(iceberg_table).append()
    count = spark.table(iceberg_table).count()
    print(f"  → {count} rows in {iceberg_table}")

spark.stop()
```

### 5.5 Stratégie shadow-write pendant la migration

Pendant la durée de la migration, les scripts Silver existants continuent d'écrire
**en parallèle** dans les deux cibles :

```python
# Modification temporaire dans silver_missions.py _process()
# Ajouter après le COPY Parquet existant :
if os.environ.get("ICEBERG_SHADOW_WRITE", "false") == "true":
    _write_to_iceberg(ddb, cfg, t, query)  # nouveau chemin Iceberg
```

Basculer `ICEBERG_SHADOW_WRITE=true` en prod une fois Phase 4 validée.

---

## 6. Phase 4 — Migration transformations DuckDB → Spark

> **Objectif :** remplacer les scripts `silver_*.py` (DuckDB) par des jobs Spark
> pour distribution et scalabilité. Priorité : les tables volumineuses.

### 6.1 Matrice de migration par volume

| Script Silver | Tables source | Volume estimé | Priorité migration |
|---|---|---|---|
| silver_missions.py | WTRHDON 760K+/j, WTMISS | Élevé | 🔴 Spark obligatoire |
| silver_missions.py | WTLFAC 26M lignes | Très élevé | 🔴 Spark obligatoire |
| silver_interimaires.py | PYPERSONNE, WTPINT | Moyen | 🟠 Spark recommandé |
| silver_clients.py | WTTIESERV | Faible | 🟢 DuckDB suffisant |
| silver_agences.py | PYREGROUPECNT, PYENTREPRISE | Très faible | 🟢 DuckDB suffisant |
| silver_missions.py (fin_mission) | JOIN 3 tables | Moyen | 🟠 Spark recommandé |

> **Règle pratique :** tables < 500K lignes → garder DuckDB (plus simple, déjà opérationnel).
> Tables > 500K lignes ou avec JOIN multi-millions → migrer vers Spark.

### 6.2 Traduction DuckDB → Spark SQL

La syntaxe est quasi-identique. Principaux ajustements :

| DuckDB | Spark SQL |
|---|---|
| `read_json_auto('s3://...')` | `spark.read.json('s3a://...')` |
| `read_parquet('s3://...')` | `spark.read.parquet('s3a://...')` |
| `QUALIFY ROW_NUMBER() OVER (…) = 1` | `WHERE rn = 1` (CTE intermédiaire) |
| `TRY_CAST(x AS INTEGER)` | `TRY_CAST(x AS INT)` |
| `DATEDIFF('day', a, b)` | `DATEDIFF(b, a)` *(ordre inversé)* |
| `DATE_TRUNC('month', d)` | `DATE_TRUNC('month', d)` *(identique)* |
| `COPY (…) TO 's3://…' (FORMAT PARQUET)` | `df.writeTo('silver.missions.fin_mission').overwritePartitions()` |

### 6.3 Exemple : migration silver_missions FIN_MISSION → Spark

```python
# scripts/spark/spark_silver_fin_mission.py
# Déployer via OVH Data Processing
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def build_spark_session() -> SparkSession:
    return SparkSession.builder \
        .appName("gi-silver-fin-mission") \
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.bronze", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.silver", "org.apache.iceberg.spark.SparkCatalog") \
        .getOrCreate()


def run(spark: SparkSession, date_partition: str) -> int:
    bronze = f"s3a://gi-bronze"
    dp = date_partition  # ex: 2026/03/15

    wtmiss   = spark.read.json(f"{bronze}/raw_wtmiss/{dp}/*.json")
    wtcnti   = spark.read.json(f"{bronze}/raw_wtcnti/{dp}/*.json")
    wtfinmiss = spark.read.json(f"{bronze}/raw_wtfinmiss/{dp}/*.json")
    pymtfcnt  = spark.read.json(f"{bronze}/raw_pymtfcnt/{dp}/*.json")

    # Agréger WTCNTI : MIN date_debut, MAX date_fin par (per_id, cnt_id)
    cnti_agg = wtcnti.groupBy("PER_ID", "CNT_ID").agg(
        F.min("CNTI_DATEFFET").alias("cnti_dateffet"),
        F.max("CNTI_DATEFINCNTI").alias("cnti_datefincnti"),
    )

    # Préparer WTFINMISS + PYMTFCNT
    finmiss = wtfinmiss.select(
        F.trim("FINMISS_CODE").alias("finmiss_code"),
        F.trim("FINMISS_LIBELLE").alias("finmiss_libelle"),
        F.col("MTFCNT_ID").cast("int").alias("mtfcnt_id"),
    )
    mtfcnt = pymtfcnt.select(
        F.col("MTFCNT_ID").cast("int").alias("mtfcnt_id"),
        F.trim("MTFCNT_CODE").alias("mtfcnt_code"),
        F.trim("MTFCNT_LIBELLE").alias("mtfcnt_libelle"),
        F.col("MTFCNT_FINCNT").cast("short").alias("mtfcnt_fincnt"),
    )

    # Déduplication WTMISS (dernière version par PER_ID+CNT_ID)
    from pyspark.sql.window import Window
    w = Window.partitionBy("PER_ID", "CNT_ID").orderBy(F.desc("_loaded_at"))
    miss_dedup = wtmiss.withColumn("rn", F.row_number().over(w)) \
                       .filter("rn = 1").drop("rn")

    # Jointures
    df = miss_dedup \
        .join(cnti_agg, ["PER_ID", "CNT_ID"], "left") \
        .join(finmiss, miss_dedup["FINMISS_CODE"] == finmiss["finmiss_code"], "left") \
        .join(mtfcnt, "mtfcnt_id", "left")

    # Calcul statut_fin_mission — même logique que le CASE DuckDB
    statut = F.when(F.col("MISS_ANNULE").cast("short") == 1, "ANNULEE") \
              .when(F.col("FINMISS_CODE").isNull(), "EN_COURS") \
              .when(F.col("mtfcnt_fincnt") == 0, "EN_COURS") \
              .when(F.lower("mtfcnt_libelle").contains("fin de mission"), "TERME_NORMAL") \
              .when(F.col("mtfcnt_fincnt") == 1, "RUPTURE") \
              .when(
                  F.lower("finmiss_libelle").contains("terme") |
                  F.lower("finmiss_libelle").contains("normal") |
                  F.lower("finmiss_libelle").contains("échéance"), "TERME_NORMAL") \
              .otherwise("RUPTURE")

    result = df.select(
        F.col("PER_ID").cast("int").alias("per_id"),
        F.col("CNT_ID").cast("int").alias("cnt_id"),
        F.col("RGPCNT_ID").cast("int").alias("rgpcnt_id"),
        F.col("TIE_ID").cast("int").alias("tie_id"),
        F.col("cnti_dateffet").cast("date").alias("date_debut"),
        F.col("cnti_datefincnti").cast("date").alias("date_fin_reelle"),
        F.col("MISS_SAISIE_DTFIN").cast("date").alias("date_fin_saisie"),
        F.trim("FINMISS_CODE").alias("finmiss_code"),
        F.col("finmiss_libelle"),
        F.col("mtfcnt_code"),
        F.col("mtfcnt_libelle"),
        F.col("mtfcnt_fincnt"),
        F.col("MISS_ANNULE").cast("short").alias("miss_annule"),
        F.datediff(
            F.col("cnti_datefincnti").cast("date"),
            F.col("cnti_dateffet").cast("date")
        ).alias("duree_reelle_jours"),
        statut.alias("statut_fin_mission"),
        F.col("_batch_id"),
        F.col("_loaded_at").cast("timestamp"),
    ).filter("per_id IS NOT NULL AND cnt_id IS NOT NULL")

    # Écriture Iceberg avec overwrite de la partition du jour
    result.writeTo("silver.missions.fin_mission") \
          .option("merge-schema", "true") \
          .overwritePartitions()

    count = spark.table("silver.missions.fin_mission").count()
    return count


if __name__ == "__main__":
    spark = build_spark_session()
    dp = os.environ.get("SILVER_DATE_PARTITION",
                        __import__("datetime").date.today().strftime("%Y/%m/%d"))
    n = run(spark, dp)
    print(f"silver.missions.fin_mission : {n} rows total")
    spark.stop()
```

### 6.4 Répertoire cible des jobs Spark

```
scripts/
  spark/
    spark_silver_missions.py        # WTMISS + WTCNTI + WTRHDON
    spark_silver_fin_mission.py     # FIN_MISSION (voir ci-dessus)
    spark_silver_interimaires.py    # PYPERSONNE + WTPINT + compétences
    spark_silver_facturation.py     # WTLFAC (26M lignes — cas le plus critique)
    spark_gold_rupture_contrat.py   # fact_rupture_contrat depuis Silver Iceberg
    migrate_silver_to_iceberg.py    # migration initiale (one-shot)
```

Les scripts DuckDB (`silver_missions.py`, etc.) sont **conservés en parallèle**
jusqu'à validation complète, puis archivés.

---

## 7. Phase 5 — Gold : PostgreSQL vs Iceberg

Deux options, à choisir selon les besoins :

### Option A — Garder PostgreSQL Gold (recommandé pour la continuité)

**Avantages :**
- Superset déjà connecté, pas de reconfiguration
- pg_bulk_insert() fonctionne, modèle TRUNCATE+COPY éprouvé
- Performances suffisantes pour les volumes actuels Gold (agrégations mensuelles)

**Changements uniquement :**
- Les scripts `gold_*.py` lisent depuis Iceberg Silver (Trino ou DuckDB Iceberg)
  au lieu de `read_parquet()` S3 direct
- `get_duckdb_connection()` est remplacé par une connexion Trino pour les Gold
  qui lisent Silver

```python
# shared.py — nouvelle fonction
def get_trino_connection(cfg: Config):
    from trino.dbapi import connect
    return connect(
        host=cfg.trino_host,
        port=cfg.trino_port,
        user=cfg.trino_user,
        http_scheme="https",
        auth=trino.auth.BasicAuthentication(cfg.trino_user, cfg.trino_password),
    )
```

### Option B — Migrer Gold vers Iceberg (recommandé à terme)

**Avantages :**
- Plus de TRUNCATE+COPY : merge ACID, incremental updates
- SQL direct sur Gold via Trino sans PostgreSQL
- Time-travel sur les métriques Gold (audit, comparaison période)

**Changements :**
- Créer les tables Gold en Iceberg : `CREATE TABLE gold.performance.fact_rupture_contrat …`
- Remplacer `pg_bulk_insert()` par `spark.writeTo("gold.performance…").merge()`
- Reconfigurer Superset → connexion Trino catalog `gold.*`
- Supprimer la dépendance PostgreSQL Gold

> **Recommandation :** Option A en migration, Option B comme évolution 6 mois post-go-live.

---

## 8. Phase 6 — Superset → connexion Trino

### 8.1 Ajouter une connexion Trino dans Superset

```
Superset → Settings → Database Connections → + Database
  Database type : Trino
  SQLAlchemy URI : trino://gi-lakehouse-svc:PASSWORD@gi-lakehouse.xxx.ovh.com:443/gi_catalog
  Display name   : GI Lakehouse (Trino)
```

### 8.2 Datasets disponibles via Trino

Une fois Trino connecté, Superset peut explorer directement :
- `bronze.raw.wtmiss` — données brutes Evolia (utile pour debug)
- `silver.missions.fin_mission` — Silver Iceberg
- `silver.missions.missions` — etc.
- `gold.performance.fact_rupture_contrat` — si option B Phase 5

### 8.3 Conserver PostgreSQL Gold en parallèle

La connexion PostgreSQL Gold existante reste active dans Superset.
Les dashboards existants **ne changent pas** pendant la migration.
Ajouter la connexion Trino uniquement pour les nouveaux dashboards.

---

## 9. Phase 7 — Migration Airflow (BashOperator → SparkSubmit)

### 9.1 Architecture DAG actuelle

```python
# dag_gi_pipeline.py — structure actuelle (BashOperator)
bronze_missions = BashOperator(
    task_id="bronze_missions",
    bash_command="cd /opt/gi-data-platform && uv run scripts/bronze_missions.py",
)
```

### 9.2 DAG cible avec SparkSubmitOperator

```python
# dags/dag_gi_pipeline_lakehouse.py
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.trino.operators.trino import TrinoOperator
from airflow import DAG
from datetime import datetime, timedelta

# Bronze : inchangé (BashOperator — scripts Python pymssql restent)
bronze_missions = BashOperator(
    task_id="bronze_missions",
    bash_command="cd /opt/gi-data-platform && uv run scripts/bronze_missions.py",
)

# Silver volumineuse : Spark via OVH Data Processing
silver_missions_spark = SparkSubmitOperator(
    task_id="silver_missions_spark",
    application="scripts/spark/spark_silver_missions.py",
    conn_id="ovh_data_processing",          # connexion Airflow vers OVH Data Processing
    conf={
        "spark.sql.extensions":
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    },
    env_vars={"SILVER_DATE_PARTITION": "{{ ds_nodash[:4] }}/{{ ds_nodash[4:6] }}/{{ ds_nodash[6:] }}"},
)

# Silver légère : DuckDB conservé (agences, clients faible volume)
silver_agences = BashOperator(
    task_id="silver_agences",
    bash_command="cd /opt/gi-data-platform && uv run scripts/silver_agences.py",
)

# Gold : Trino SQL ou BashOperator gold_*.py (selon option A ou B Phase 5)
gold_rupture = BashOperator(
    task_id="gold_rupture_contrat",
    bash_command="cd /opt/gi-data-platform && uv run scripts/gold_qualite_missions.py",
)

# Ordre des dépendances
bronze_missions >> [silver_missions_spark, silver_agences]
silver_missions_spark >> gold_rupture
```

### 9.3 Connexion Airflow → OVH Data Processing

```
Airflow → Admin → Connections → Add
  Conn ID   : ovh_data_processing
  Conn Type : Spark
  Host      : https://gra.data-processing.cloud.ovh.net
  Password  : <OVH_DATA_PROCESSING_TOKEN>
```

---

## 10. Phase 8 — Validation et go-live

### 10.1 Tests de non-régression (à exécuter avant bascule)

```bash
# 1. Bronze — counts identiques entre anciens JSON et tables Trino
python tests/validate_migration.py --layer bronze --compare-trino

# 2. Silver — diff Parquet actuel vs Iceberg migré
python tests/validate_migration.py --layer silver --table missions.fin_mission

# 3. Gold — comparer PostgreSQL Gold actuel vs Gold depuis Iceberg
python tests/validate_migration.py --layer gold --table gld_performance.fact_rupture_contrat
```

```python
# tests/validate_migration.py — exemple pour Silver
def validate_silver_counts(table: str):
    """Compare counts entre Parquet S3 actuel et Iceberg migré."""
    cfg = Config()

    # Parquet actuel via DuckDB
    with get_duckdb_connection(cfg) as ddb:
        parquet_count = ddb.execute(
            f"SELECT COUNT(*) FROM read_parquet('s3://{cfg.bucket_silver}/{table}/**/*.parquet')"
        ).fetchone()[0]

    # Iceberg via Trino
    with get_trino_connection(cfg) as trino:
        schema, tbl = table.replace("/", ".").rsplit(".", 1)
        iceberg_count = trino.cursor().execute(
            f"SELECT COUNT(*) FROM silver.{schema}.{tbl}"
        ).fetchone()[0]

    assert parquet_count == iceberg_count, \
        f"Count mismatch {table}: Parquet={parquet_count}, Iceberg={iceberg_count}"
    print(f"✓ {table}: {iceberg_count} rows — OK")
```

### 10.2 Checklist go-live

```
[ ] Phase 1 : Lakehouse Manager actif, buckets gi-bronze/silver/gold créés
[ ] Phase 2 : toutes les tables Bronze visibles via Trino (SELECT COUNT OK)
[ ] Phase 3 : toutes les tables Silver migrées en Iceberg (counts identiques)
[ ] Phase 4 : jobs Spark validés en probe sur date_partition de test
[ ] Phase 5 : Gold PostgreSQL alimenté depuis Silver Iceberg (comparaison Gold actuel)
[ ] Phase 6 : Superset connecté à Trino, dashboards existants inchangés
[ ] Phase 7 : DAG Airflow lakehouse tourne en parallel run 3 jours sans erreur
[ ] Rollback testé (voir section 11)
[ ] Équipe formée sur requêtage Trino (30 min)
```

### 10.3 Bascule finale

```bash
# 1. Arrêter le DAG actuel (dag_gi_pipeline)
airflow dags pause dag_gi_pipeline

# 2. Activer le nouveau DAG
airflow dags unpause dag_gi_pipeline_lakehouse

# 3. Désactiver shadow-write (plus nécessaire)
# Retirer ICEBERG_SHADOW_WRITE=true des variables Airflow

# 4. Archiver les scripts DuckDB Silver
mv scripts/silver_missions.py scripts/_archive/silver_missions_duckdb.py
# (conserver 30 jours avant suppression définitive)
```

---

## 11. Plan de rollback

### Rollback Phase 3-4 (Silver Iceberg → Parquet DuckDB)

À n'importe quel moment avant la bascule finale :

```bash
# Réactiver le DAG original
airflow dags unpause dag_gi_pipeline
airflow dags pause dag_gi_pipeline_lakehouse

# Les scripts silver_*.py et gold_*.py DuckDB n'ont jamais été supprimés
# → pipeline opérationnel immédiatement
```

### Rollback Phase 5 (Gold Iceberg → PostgreSQL)

Applicable uniquement si option B choisie :

```bash
# Reconfigurer pg_bulk_insert() dans gold_*.py
# Pointer SILVER_BUCKET vers gi-poc-silver (Parquet) au lieu de gi-silver (Iceberg)
# Relancer gold_*.py avec DuckDB read_parquet()
```

### Rollback Phase 6 (Superset)

```
# Supprimer la connexion Trino dans Superset
# Les dashboards pointent toujours sur PostgreSQL Gold → inchangés
```

> **Principe de rollback :** les buckets `gi-poc-bronze` et `gi-poc-silver`
> (Parquet DuckDB) sont conservés **intacts pendant 30 jours** après go-live.
> Pas de suppression avant validation complète en production.

---

## 12. Coûts et dimensionnement

### Estimation coûts OVH (ordre de grandeur)

| Composant | Dimensionnement | Coût mensuel estimé |
|---|---|---|
| Lakehouse Manager (Trino) | XS → M selon usage | ~100–300 €/mois |
| Data Processing (Spark) | Serverless, ~30 min/jour | ~50–150 €/mois |
| Object Storage (3 buckets) | ~50 GB Bronze + 20 GB Silver | ~5–15 €/mois |
| Managed PostgreSQL (Gold) | db1-15 (option A) | ~50 €/mois (inchangé) |
| **Total delta vs POC** | | **+150–450 €/mois** |

> Ces chiffres sont indicatifs. L'essentiel du coût Spark est lié au volume
> traité (WTRHDON + WTLFAC). Optimisation possible : ne lancer Spark que pour
> les tables > 500K lignes, garder DuckDB pour le reste.

### Dimensionnement Spark recommandé

| Pipeline | Fréquence | Worker | Durée estimée |
|---|---|---|---|
| silver_missions (WTRHDON) | quotidien | 2×m3.medium | 8 min |
| silver_missions (WTLFAC 26M) | quotidien | 4×m3.medium | 15 min |
| silver_interimaires | quotidien | 2×m3.small | 5 min |
| silver_fin_mission | quotidien | 2×m3.small | 3 min |
| migrate_silver_to_iceberg | one-shot | 4×m3.large | 45 min |

---

## 13. Backlog post-migration

Une fois la migration stable (J+30), les fonctionnalités suivantes deviennent
accessibles nativement :

### Gouvernance des données (DataHub)

```
# datahub ingestion — déjà dans les dépendances (acryl-datahub>=0.12.0)
datahub ingest -c datahub_trino.yml   # auto-découverte lineage Bronze→Silver→Gold
```

### dbt sur Trino

```
# dbt-trino (remplace dbt-postgres pour les Gold Iceberg)
# profiles.yml
gi_lakehouse:
  type: trino
  host: gi-lakehouse.xxx.ovh.com
  port: 443
  database: gi_catalog
  schema: gold
```

### Time-travel et audit RGPD

```sql
-- Comparer fact_rupture_contrat aujourd'hui vs il y a 30 jours
SELECT * FROM gold.performance.fact_rupture_contrat
FOR TIMESTAMP AS OF TIMESTAMP '2026-02-15 00:00:00'
WHERE agence_id = 42;

-- Prouver qu'une donnée a bien été supprimée (RGPD droit à l'oubli)
SELECT COUNT(*) FROM silver.interimaires.dim_interimaires
FOR VERSION AS OF 1234567890   -- snapshot_id avant suppression
WHERE per_id = 99999;
```

### Requêtes SQL ad-hoc sans script Python

```sql
-- Accessible directement depuis Superset, DBeaver ou CLI Trino :
SELECT
    agence_id,
    DATE_TRUNC('month', date_debut) AS mois,
    statut_fin_mission,
    COUNT(*) AS nb
FROM silver.missions.fin_mission
WHERE date_debut >= DATE '2026-01-01'
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3;
```
