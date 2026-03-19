# Plan de migration Silver → Iceberg sur OVH Cloud Data Platform
## GI Data Platform — Étude, Plan & Procédure end-to-end

> **Version 1.0 · 2026-03-18**
> Scope : couche Silver uniquement (Bronze inchangé, Gold PostgreSQL conservé en serving)
> Prérequis : lire `migration_ovh_lakehouse.md` pour le contexte général
> Principe directeur : **Iceberg = vérité analytique | PostgreSQL = serving layer**

---

## Sommaire

1. [État des lieux — inventaire complet Silver actuel](#1-état-des-lieux--inventaire-complet-silver-actuel)
2. [Ce que change Iceberg concrètement](#2-ce-que-change-iceberg-concrètement)
3. [Composants OVH à provisionner](#3-composants-ovh-à-provisionner)
4. [Procédure de mise en place OVH step-by-step](#4-procédure-de-mise-en-place-ovh-step-by-step)
5. [Mapping Silver actuel → Iceberg cible](#5-mapping-silver-actuel--iceberg-cible)
6. [Décision de migration par script](#6-décision-de-migration-par-script)
7. [Modifications requises dans les silver_*.py](#7-modifications-requises-dans-les-silver_py)
8. [Stratégie de migration sans downtime](#8-stratégie-de-migration-sans-downtime)
9. [Plan de validation et critères de succès](#9-plan-de-validation-et-critères-de-succès)
10. [Rollback](#10-rollback)
11. [Séquencement et dépendances](#11-séquencement-et-dépendances)

---

## 1. État des lieux — inventaire complet Silver actuel

### 9 scripts, 17 tables Silver Parquet sur S3

| Script | Tables Silver produites | Volume estimé | Technologie |
|---|---|---|---|
| `silver_missions.py` | `slv_missions/missions` | ~3M lignes | DuckDB |
| | `slv_missions/contrats` | ~3M lignes | DuckDB |
| | `slv_missions/commandes` | ~500K lignes | DuckDB |
| | `slv_missions/placements` | ~500K lignes | DuckDB |
| | `slv_missions/fin_mission` | ~3M lignes | DuckDB *(à implémenter)* |
| | `slv_missions/contrats_paie` | ~3M lignes | DuckDB |
| `silver_temps.py` | `slv_temps/releves_heures` | ~500K/j | DuckDB |
| | `slv_temps/heures_detail` | **760K+/j** | DuckDB ← **bottleneck** |
| `silver_factures.py` | `slv_facturation/factures` | ~200K | DuckDB |
| | `slv_facturation/lignes_factures` | **26M total** | DuckDB ← **bottleneck** |
| `silver_interimaires.py` | `slv_interimaires/dim_interimaires` | ~100K | DuckDB |
| `silver_interimaires_detail.py` | `slv_interimaires/evaluations` | ~500K | DuckDB |
| | `slv_interimaires/coordonnees` | ~100K | DuckDB |
| | `slv_interimaires/portefeuille_agences` | ~200K | DuckDB |
| `silver_competences.py` | `slv_interimaires/competences` | ~300K | DuckDB |
| `silver_clients.py` | `slv_clients/dim_clients` | ~20K | DuckDB |
| `silver_clients_detail.py` | `slv_clients/sites_mission` | ~50K | DuckDB |
| | `slv_clients/contacts` | ~30K | DuckDB |
| | `slv_clients/encours_credit` | ~20K | DuckDB |
| `silver_agences_light.py` | `slv_agences/dim_agences` | ~300 | DuckDB |
| | `slv_agences/hierarchie_territoriale` | ~100 | DuckDB |

### Pattern d'écriture actuel (commun à tous les scripts)

```python
# Motif universel dans tous les silver_*.py
silver_path = f"s3://{cfg.bucket_silver}/{t.silver}/**/*.parquet"
ddb.execute(f"COPY ({query}) TO '{silver_path}' (FORMAT PARQUET, CODEC ZSTD)")
# puis validation
ddb.execute(f"SELECT COUNT(*) FROM read_parquet('{silver_path}')").fetchone()
```

**Ce motif est le seul point de modification.** La logique SQL des transformations
reste **inchangée**. Seul le sink (destination d'écriture) change.

---

## 2. Ce que change Iceberg concrètement

### Pour les scripts silver_*.py

| Aspect | Aujourd'hui (Parquet DuckDB) | Après (Iceberg) |
|---|---|---|
| Écriture | `COPY TO 's3://...' (FORMAT PARQUET)` | `PyIceberg append()` ou Spark `writeTo()` |
| Lecture validation | `read_parquet('s3://...')` | `SELECT COUNT(*) FROM silver.schema.table` via Trino |
| Schema evolution | Casse le pipeline sur nouvelle colonne | `ALTER TABLE ADD COLUMN` ACID |
| Merge/upsert | Impossible — OVERWRITE complet | `MERGE INTO silver.missions.missions ON pk` |
| Time-travel | Impossible | `FOR TIMESTAMP AS OF '...'` |
| Compaction | Manuelle (fichiers fragmentés) | `CALL iceberg.system.rewrite_data_files(...)` |
| Découverte data | Script Python dédié | `SELECT * FROM bronze.raw.wtmiss` depuis n'importe quel outil SQL |

### Pour les couches Gold et Serving

**Rien ne change en phase 1.** Le Gold PostgreSQL est alimenté par les Silver.
Une fois Silver en Iceberg, les scripts `gold_*.py` liront depuis Iceberg (via DuckDB
ou Trino) au lieu du Parquet S3 brut — **le SQL de transformation reste identique**.

---

## 3. Composants OVH à provisionner

### Vue d'ensemble des services

```
┌─────────────────────────────────────────────────────────────────────────┐
│  OVH Cloud Data Platform — composants nécessaires Silver → Iceberg      │
│                                                                         │
│  ┌──────────────────────┐   ┌────────────────────────────────────────┐  │
│  │  Lakehouse Manager   │   │  Data Processing (Spark serverless)    │  │
│  │  ────────────────    │   │  ───────────────────────────────────   │  │
│  │  • Trino distribué   │   │  • Jobs Spark ephemères (payant/usage) │  │
│  │  • Hive Metastore    │   │  • Python 3.12 + PySpark 3.5          │  │
│  │  • REST Catalog REST │   │  • Accès S3 + Catalog Iceberg          │  │
│  │  • endpoint HTTPS    │   │  • Soumission via API REST ou CLI      │  │
│  └──────────┬───────────┘   └──────────────────┬─────────────────────┘  │
│             │ Trino lit/écrit                   │ Spark lit/écrit        │
│             ▼                                   ▼                        │
│  ┌──────────────────────────────────────────────────────────────────┐    │
│  │  OVH Object Storage S3 (déjà existant)                          │    │
│  │  gi-poc-bronze (inchangé)                                        │    │
│  │  gi-silver (nouveau — Iceberg warehouse)   ← cible migration     │    │
│  │  gi-poc-silver (conservé 30j — rollback)                         │    │
│  └──────────────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────────────┘
```

### Matrice des services OVH

| Service OVH | Rôle dans la migration Silver | Obligatoire | Coût estimé |
|---|---|---|---|
| **Lakehouse Manager** | Trino endpoint + Hive Metastore + REST Catalog Iceberg | ✅ Oui | ~100–300 €/mois |
| **Data Processing** | Spark serverless pour tables volumineuses (WTRHDON, WTLFAC) | ⚠️ Pour vol. > 500K | ~50–150 €/mois |
| **Object Storage** `gi-silver` | Bucket Iceberg warehouse (partagé Trino + Spark) | ✅ Oui | ~5 €/mois |
| **IAM Users** | Compte technique `gi-lakehouse-svc` pour S3 + Trino | ✅ Oui | Gratuit |
| Managed Databases PostgreSQL | Inchangé (Gold serving) | — | Inchangé |
| Managed Kafka | **Hors scope phase 1** (streaming CDC) | ❌ Non | — |

---

## 4. Procédure de mise en place OVH step-by-step

### Étape 1 — Créer le bucket S3 `gi-silver` (production)

```
Console OVH : manager.ovh.com
→ Public Cloud → Object Storage → Créer un conteneur
  Nom     : gi-silver
  Région  : GRA (même région que gi-poc-bronze)
  Type    : S3 API (pas Swift)
  Accès   : Privé
```

Sous-structure attendue après migration :
```
gi-silver/
  iceberg/
    missions/
      missions/         ← données Iceberg (fichiers Parquet + metadata/)
      contrats/
      fin_mission/
      ...
    interimaires/
    clients/
    agences/
    temps/
    facturation/
```

### Étape 2 — Créer l'utilisateur technique IAM S3

```
Console OVH → IAM → Utilisateurs S3 → Créer un utilisateur
  Nom          : gi-lakehouse-svc
  Droits S3    : s3:GetObject, s3:PutObject, s3:DeleteObject,
                 s3:ListBucket, s3:GetBucketLocation
  Buckets      : gi-poc-bronze, gi-silver, gi-gold (futur)
  → Récupérer : Access Key + Secret Key → stocker dans vault Airflow
```

### Étape 3 — Provisionner le Lakehouse Manager

```
Console OVH → Public Cloud → Data & Analytics → Lakehouse Manager → Créer

  Paramètres à renseigner :
  ┌────────────────────────────────────────────────────────────────┐
  │  Nom du service  : gi-lakehouse                               │
  │  Région          : GRA (obligatoire — même région que S3)     │
  │  Version Trino   : 435+ (dernière LTS stable)                 │
  │  Hive Metastore  : 3.1 (inclus dans Lakehouse Manager)       │
  │  Warehouse S3    : s3a://gi-silver/iceberg/                   │
  │  IAM Access Key  : (clé gi-lakehouse-svc créée étape 2)       │
  │  IAM Secret Key  : (secret gi-lakehouse-svc)                  │
  └────────────────────────────────────────────────────────────────┘

  → À récupérer après provisioning :
    - Endpoint Trino   : trino://gi-lakehouse.xxx.ovh.com:443
    - REST Catalog URI : https://gi-lakehouse.xxx.ovh.com/iceberg
    - Token auth JWT   : généré depuis console OVH → IAM → Tokens
```

> **Note OVH :** Le Lakehouse Manager provisionne automatiquement le HMS
> (Hive Metastore Service) et l'expose via REST Catalog compatible Iceberg.
> Pas de configuration HMS manuelle à faire.

### Étape 4 — Créer les catalogues dans le Lakehouse Manager

Via l'interface OVH Lakehouse → Catalogs :

```
Catalogue 1 (Bronze, tables externes JSON) :
  Nom    : bronze
  Type   : Hive (schema-on-read, JSON)
  Bucket : gi-poc-bronze
  Préfixe: /

Catalogue 2 (Silver, tables Iceberg ACID) :
  Nom    : silver
  Type   : Iceberg (REST Catalog)
  Bucket : gi-silver
  Warehouse : s3a://gi-silver/iceberg/
```

### Étape 5 — Créer les schémas Iceberg Silver via Trino CLI

Connexion Trino :
```bash
# Via CLI officiel Trino (ou DBeaver, ou Superset en mode ad-hoc)
trino --server https://gi-lakehouse.xxx.ovh.com:443 \
      --user gi-lakehouse-svc \
      --password \
      --catalog silver
```

Création des schémas (une seule fois, idempotent) :
```sql
CREATE SCHEMA IF NOT EXISTS silver.missions;
CREATE SCHEMA IF NOT EXISTS silver.interimaires;
CREATE SCHEMA IF NOT EXISTS silver.clients;
CREATE SCHEMA IF NOT EXISTS silver.agences;
CREATE SCHEMA IF NOT EXISTS silver.temps;
CREATE SCHEMA IF NOT EXISTS silver.facturation;
```

### Étape 6 — Créer les tables Iceberg Silver via Trino

Principe DDL Iceberg sur Trino OVH :
```sql
CREATE TABLE silver.missions.missions (
    per_id       BIGINT,
    cnt_id       BIGINT,
    tie_id       BIGINT,
    -- ... toutes les colonnes du SELECT dans silver_missions.py
    _batch_id    VARCHAR,
    _loaded_at   TIMESTAMP(6) WITH TIME ZONE
)
WITH (
    format         = 'PARQUET',              -- fichiers physiques = Parquet ZSTD
    location       = 's3a://gi-silver/iceberg/missions/missions/',
    partitioning   = ARRAY['month(date_debut)']  -- partition identique à l'actuel
);
-- Répéter pour chaque table (voir mapping section 5)
```

> **Point clé :** Le DDL Trino ne déclenche aucune écriture S3. Il enregistre
> uniquement les métadonnées dans le HMS. Les données arrivent via les jobs
> d'écriture (étape 7+).

### Étape 7 — Installer PyIceberg dans le projet

```toml
# pyproject.toml — ajouter dans [dependencies]
pyiceberg = ">=0.7.0"        # client Python Iceberg
pyarrow   = ">=15.0"         # sérialisation Arrow → Iceberg
trino     = ">=0.328"        # client Trino Python (déjà dans deps si datahub)
```

PyIceberg est la bibliothèque officielle Apache Iceberg pour Python. Elle permet
aux scripts DuckDB de **continuer à faire le SQL de transformation en DuckDB**
et d'écrire le résultat en Iceberg sans passer par Spark.

### Étape 8 — Configurer Airflow → OVH Data Processing (pour Spark)

```
Airflow → Admin → Connections → Add
  Conn ID   : ovh_data_processing
  Conn Type : HTTP
  Host      : https://gra.data-processing.cloud.ovh.net
  Password  : <OVH_DATA_PROCESSING_TOKEN>

Airflow → Admin → Variables
  TRINO_HOST    = gi-lakehouse.xxx.ovh.com
  TRINO_PORT    = 443
  TRINO_USER    = gi-lakehouse-svc
  ICEBERG_URI   = https://gi-lakehouse.xxx.ovh.com/iceberg
  SILVER_BUCKET = gi-silver
```

### Étape 9 — Mettre à jour .env.local

```bash
# Ajouter dans /opt/groupe-interaction/etc/gi-data-platform/.env
OVH_ICEBERG_URI=https://gi-lakehouse.xxx.ovh.com/iceberg
OVH_TRINO_HOST=gi-lakehouse.xxx.ovh.com
OVH_TRINO_PORT=443
OVH_TRINO_USER=gi-lakehouse-svc
OVH_TRINO_TOKEN=<token_OVH_IAM>
BUCKET_SILVER=gi-silver              # passer de gi-poc-silver à gi-silver
```

---

## 5. Mapping Silver actuel → Iceberg cible

### Correspondance complète des 17 tables

| Chemin S3 actuel (Parquet) | Table Iceberg cible | Partitionnement Iceberg |
|---|---|---|
| `slv_missions/missions` | `silver.missions.missions` | `month(date_debut)` |
| `slv_missions/contrats` | `silver.missions.contrats` | `month(cnti_dateffet)` |
| `slv_missions/commandes` | `silver.missions.commandes` | `month(cmd_date)` |
| `slv_missions/placements` | `silver.missions.placements` | `month(plac_date)` |
| `slv_missions/fin_mission` | `silver.missions.fin_mission` | `month(date_debut)` |
| `slv_missions/contrats_paie` | `silver.missions.contrats_paie` | `month(cnti_dateffet)` |
| `slv_temps/releves_heures` | `silver.temps.releves_heures` | `month(prh_date)` |
| `slv_temps/heures_detail` | `silver.temps.heures_detail` | `month(rhd_dated)` |
| `slv_facturation/factures` | `silver.facturation.factures` | `month(efac_dteedi)` |
| `slv_facturation/lignes_factures` | `silver.facturation.lignes_factures` | `month(efac_dteedi)` |
| `slv_interimaires/dim_interimaires` | `silver.interimaires.dim_interimaires` | *(pas de partition — ~100K)* |
| `slv_interimaires/evaluations` | `silver.interimaires.evaluations` | `year(eval_date)` |
| `slv_interimaires/coordonnees` | `silver.interimaires.coordonnees` | *(pas de partition)* |
| `slv_interimaires/portefeuille_agences` | `silver.interimaires.portefeuille_agences` | *(pas de partition)* |
| `slv_interimaires/competences` | `silver.interimaires.competences` | *(pas de partition)* |
| `slv_clients/dim_clients` | `silver.clients.dim_clients` | *(pas de partition — ~20K)* |
| `slv_clients/sites_mission` | `silver.clients.sites_mission` | *(pas de partition)* |
| `slv_clients/contacts` | `silver.clients.contacts` | *(pas de partition)* |
| `slv_clients/encours_credit` | `silver.clients.encours_credit` | *(pas de partition)* |
| `slv_agences/dim_agences` | `silver.agences.dim_agences` | *(pas de partition — ~300 lignes)* |
| `slv_agences/hierarchie_territoriale` | `silver.agences.hierarchie_territoriale` | *(pas de partition)* |

---

## 6. Décision de migration par script

### Matrice volume × complexité → choix technologique

```
┌──────────────────────────────────────────────────────────────────────────┐
│  RÈGLE DE DÉCISION                                                       │
│                                                                          │
│  Volume < 500K lignes/jour ET logique SQL simple                        │
│    → Conserver DuckDB SQL + écriture PyIceberg (minimal change)          │
│                                                                          │
│  Volume > 500K lignes/jour OU JOIN multi-millions                        │
│    → Migrer vers PySpark (OVH Data Processing serverless)                │
└──────────────────────────────────────────────────────────────────────────┘
```

| Script | Volume | Complexité SQL | Décision | Priorité |
|---|---|---|---|---|
| `silver_temps.py` (heures_detail) | **760K+/j** WTRHDON | Modérée | 🔴 **Spark obligatoire** | P0 |
| `silver_factures.py` (lignes_factures) | **26M total** WTLFAC | Modérée | 🔴 **Spark obligatoire** | P0 |
| `silver_missions.py` (missions, contrats) | ~3M total | Élevée (JOIN 4 tables) | 🟠 Spark recommandé | P1 |
| `silver_missions.py` (fin_mission) | ~3M total | Moyenne (JOIN + CASE) | 🟠 Spark recommandé | P1 |
| `silver_interimaires.py` | ~100K | Élevée (SCD2 + NIR hash) | 🟡 DuckDB + PyIceberg | P2 |
| `silver_interimaires_detail.py` | ~500K | Modérée | 🟡 DuckDB + PyIceberg | P2 |
| `silver_competences.py` | ~300K | Simple (UNION) | 🟢 DuckDB + PyIceberg | P3 |
| `silver_clients.py` | ~20K | Élevée (SCD2) | 🟢 DuckDB + PyIceberg | P3 |
| `silver_clients_detail.py` | ~100K | Simple | 🟢 DuckDB + PyIceberg | P3 |
| `silver_agences_light.py` | ~400 | Simple | 🟢 DuckDB + PyIceberg | P3 |

### Ce que signifie "DuckDB + PyIceberg"

Le SQL de transformation **ne change pas**. DuckDB exécute la requête, retourne
un objet PyArrow, PyIceberg écrit dans la table Iceberg. Changement limité à
la fonction d'écriture dans `shared.py`.

```
AVANT : DuckDB (SQL) → COPY TO S3 Parquet → fichiers statiques
APRÈS : DuckDB (SQL) → PyArrow → PyIceberg → table Iceberg ACID
```

### Ce que signifie "Spark obligatoire"

La transformation SQL est portée par PySpark. Le script silver_*.py DuckDB
est remplacé par un job `scripts/spark/spark_silver_*.py` soumis via
OVH Data Processing (serverless, éphémère, facturé à la seconde).

```
AVANT : DuckDB single-node → mémoire saturée sur 760K lignes/j
APRÈS : Spark distribué (2-4 workers m3.medium) → scale-out automatique
```

---

## 7. Modifications requises dans les silver_*.py

### Vue d'ensemble des points de modification

**Il y a exactement 2 types de modifications dans tous les silver_*.py :**

#### Modification A — La fonction d'écriture (dans `shared.py`)

Ajouter une fonction `write_silver_iceberg()` dans `shared.py`.
Elle remplace le `COPY TO` partout sans toucher au SQL.

```
shared.py — ajouter :
  def write_silver_iceberg(ddb, query, iceberg_table_id, cfg) → None
    1. ddb.execute(query).arrow()   → PyArrow Table
    2. PyIceberg catalog.load_table(iceberg_table_id)
    3. table.overwrite(arrow_table) ou table.append(arrow_table)

  def count_iceberg(iceberg_table_id, cfg) → int
    → remplace read_parquet count validation
```

#### Modification B — Par silver_*.py : remplacer l'appel au sink

Dans chaque script, **un seul endroit change** : la ligne `COPY TO` et la ligne
`read_parquet` de validation.

```python
# AVANT (dans tous les silver_*.py)
silver_path = f"s3://{cfg.bucket_silver}/{t.silver}/**/*.parquet"
ddb.execute(f"COPY ({query}) TO '{silver_path}' (FORMAT PARQUET, CODEC ZSTD)")
row = ddb.execute(f"SELECT COUNT(*) FROM read_parquet('{silver_path}')").fetchone()

# APRÈS
iceberg_id = t.iceberg  # ex: "silver.missions.missions" (nouveau champ sur TableConfig)
write_silver_iceberg(ddb, query, iceberg_id, cfg)
row = (count_iceberg(iceberg_id, cfg),)
```

### Changements par script — détail

#### `silver_missions.py` — 6 tables

Ajouter `iceberg` sur les 6 `TableConfig` :

| Table config actuelle | Champ `iceberg` à ajouter | Décision |
|---|---|---|
| `silver=slv_missions/missions` | `iceberg=silver.missions.missions` | Spark (P1) |
| `silver=slv_missions/contrats` | `iceberg=silver.missions.contrats` | Spark (P1) |
| `silver=slv_missions/commandes` | `iceberg=silver.missions.commandes` | DuckDB+PyIceberg |
| `silver=slv_missions/placements` | `iceberg=silver.missions.placements` | DuckDB+PyIceberg |
| `silver=slv_missions/fin_mission` | `iceberg=silver.missions.fin_mission` | Spark (P1) |
| `silver=slv_missions/contrats_paie` | `iceberg=silver.missions.contrats_paie` | Spark (P1) |

#### `silver_temps.py` — 2 tables → **Spark obligatoire**

La table `heures_detail` (WTRHDON 760K+/j) est le goulot le plus critique.
Ce script sera **remplacé** par `scripts/spark/spark_silver_temps.py`.
Le script DuckDB est archivé (pas supprimé).

#### `silver_factures.py` — 2 tables → **Spark obligatoire**

`lignes_factures` (WTLFAC 26M lignes) est le second goulot.
Ce script sera **remplacé** par `scripts/spark/spark_silver_factures.py`.

#### `silver_interimaires.py` — 1 table

Modification B uniquement. La logique SCD2 et le hash NIR (RGPD) restent
dans DuckDB. PyIceberg écrit le résultat. Le hash NIR est une colonne BINARY
dans le schema Iceberg.

#### `silver_interimaires_detail.py` — 3 tables

Modification B uniquement sur les 3 `silver_path` + 3 counts.

#### `silver_competences.py` — 1 table

Modification B uniquement. UNION simple de 4 tables → DuckDB suffisant.

#### `silver_clients.py` — 1 table

Modification B uniquement. SCD2 simple, ~20K lignes.

#### `silver_clients_detail.py` — 3 tables

Modification B uniquement. Faible volume.

#### `silver_agences_light.py` — 2 tables

Modification B uniquement. ~400 lignes — DuckDB + PyIceberg trivial.

### Résumé des changements de code

| Fichier | Type de changement | Lignes modifiées (estimé) |
|---|---|---|
| `shared.py` | **Ajout** `write_silver_iceberg()` + `count_iceberg()` | +40 lignes |
| `silver_missions.py` | Modification B × 6 tables + champ `iceberg` sur TableConfig | ~30 lignes |
| `silver_interimaires.py` | Modification B × 1 | ~10 lignes |
| `silver_interimaires_detail.py` | Modification B × 3 | ~15 lignes |
| `silver_competences.py` | Modification B × 1 | ~5 lignes |
| `silver_clients.py` | Modification B × 1 | ~5 lignes |
| `silver_clients_detail.py` | Modification B × 3 | ~10 lignes |
| `silver_agences_light.py` | Modification B × 2 | ~10 lignes |
| `silver_temps.py` | **Archivé** → remplacé par Spark | — |
| `silver_factures.py` | **Archivé** → remplacé par Spark | — |
| `scripts/spark/spark_silver_temps.py` | **Nouveau** | ~120 lignes |
| `scripts/spark/spark_silver_factures.py` | **Nouveau** | ~100 lignes |
| `scripts/spark/spark_silver_missions.py` | **Nouveau** | ~150 lignes |
| `scripts/spark/migrate_silver_to_iceberg.py` | **Nouveau** (one-shot) | ~80 lignes |

**Total impact : ~125 lignes modifiées + ~450 lignes ajoutées.**
Le SQL de transformation métier est **100% inchangé**.

---

## 8. Stratégie de migration sans downtime

### Principe : shadow-write parallèle pendant 7 jours

```
Phase A — Préparation (sans impacter la prod)
  ├── Provisionner OVH (étapes 1-9 section 4)
  ├── Créer tables Iceberg vides via Trino
  ├── Modifier shared.py (write_silver_iceberg)
  └── Ajouter variable ENV ICEBERG_SHADOW_WRITE=false

Phase B — Shadow-write (ICEBERG_SHADOW_WRITE=true)
  ├── Chaque silver_*.py écrit dans LES DEUX destinations simultanément
  │     → Parquet S3 (inchangé, pipeline prod opérationnel)
  │     → Iceberg Silver (nouveau, validation)
  ├── Durée : 7 jours (une semaine de données pour valider)
  └── Les scripts gold_*.py lisent toujours le Parquet (inchangé)

Phase C — Migration initiale historique (one-shot Spark)
  ├── Job migrate_silver_to_iceberg.py lit tout le Parquet gi-poc-silver
  ├── Écrit dans toutes les tables Iceberg silver.*
  └── Valide counts (voir section 9)

Phase D — Bascule (ICEBERG_SHADOW_WRITE=false + lecture Iceberg)
  ├── Retirer COPY TO Parquet des silver_*.py (Iceberg devient seul sink)
  ├── Modifier gold_*.py : lire depuis Iceberg au lieu de Parquet
  └── Monitoring 3 jours

Phase E — Archivage (J+30)
  └── Archiver gi-poc-silver (garder 30j, puis supprimer)
```

### Bascule des gold_*.py (lecture)

```python
# gold_qualite_missions.py — AVANT
silver_path = f"s3://{cfg.bucket_silver}/slv_missions/fin_mission/**/*.parquet"
ddb.execute(f"SELECT ... FROM read_parquet('{silver_path}')")

# APRÈS — Option 1 : DuckDB lit Iceberg via extension
ddb.execute("INSTALL iceberg; LOAD iceberg;")
ddb.execute(f"SELECT ... FROM iceberg_scan('s3://gi-silver/iceberg/missions/fin_mission/')")

# APRÈS — Option 2 : Trino comme source (plus robuste)
trino_conn.cursor().execute("SELECT ... FROM silver.missions.fin_mission")
```

> **Recommandation :** Option 1 (DuckDB iceberg_scan) est le chemin de moindre
> résistance. Option 2 (Trino) prépare la migration Gold vers Iceberg.

---

## 9. Plan de validation et critères de succès

### Gate 1 — Infra OVH opérationnelle

```
[ ] Trino endpoint répond : trino://gi-lakehouse.xxx.ovh.com:443
[ ] SELECT 1 depuis Trino CLI → OK
[ ] Schema silver.missions existe dans le catalog
[ ] Bucket gi-silver visible et accessible depuis Trino
[ ] IAM gi-lakehouse-svc : droits S3 vérifiés (put + get + list)
```

### Gate 2 — Shadow-write validé (7 jours)

Pour chaque table Silver :
```sql
-- Comparer count Parquet actuel vs Iceberg shadow
-- (exécuter côté DuckDB + Trino)
SELECT COUNT(*) FROM read_parquet('s3://gi-poc-silver/slv_missions/missions/**/*.parquet')
-- vs
SELECT COUNT(*) FROM silver.missions.missions
-- → delta attendu : 0 (même données, même jour)
```

### Gate 3 — Migration historique complète

```
[ ] migrate_silver_to_iceberg.py terminé sans erreur
[ ] Counts Iceberg ≥ counts Parquet historique (Iceberg peut avoir + de données)
[ ] Snapshot Iceberg le plus ancien = date Bronze la plus ancienne
[ ] Time-travel : SELECT ... FOR TIMESTAMP AS OF 'J-7' retourne des données
```

### Gate 4 — Gold alimenté depuis Iceberg

```
[ ] gold_qualite_missions.py lit silver.missions.fin_mission Iceberg
[ ] Résultats Gold = résultats Gold pré-migration (comparaison fact_rupture_contrat)
[ ] Pas de régression sur les dashboards Superset
[ ] Performance : temps d'exécution gold_*.py ≤ 110% du temps actuel
```

### Gate 5 — Parallel run 7 jours

```
[ ] DAG complet (bronze → silver Iceberg → gold) tourne 7 jours sans erreur
[ ] Pas d'erreur PyIceberg / Trino dans les logs
[ ] Counts croissent correctement jour après jour
[ ] Alertes Airflow : 0 task failure
```

### Critères de rollback automatique

Déclencher le rollback si, lors du parallel run :
- Count Iceberg < Count Parquet − 1% (perte de données)
- Erreur Iceberg non récupérable 2 jours consécutifs
- Gold en erreur depuis Iceberg alors que depuis Parquet OK

---

## 10. Rollback

### Rollback à tout moment avant la bascule finale

```bash
# 1. Passer ICEBERG_SHADOW_WRITE=false dans Airflow Variables
# 2. Les silver_*.py ne font plus que COPY TO Parquet (path original)
# 3. Les gold_*.py lisent toujours read_parquet() (inchangé)
# → Pipeline restauré en < 5 minutes, aucune donnée perdue
```

### Rollback après bascule

```bash
# gi-poc-silver est conservé INTÉGRALEMENT pendant 30 jours
# 1. Restaurer COPY TO Parquet dans silver_*.py (git revert)
# 2. Restaurer read_parquet() dans gold_*.py
# 3. Re-pointer BUCKET_SILVER=gi-poc-silver dans .env
# → Pipeline restauré sans perte de données
```

---

## 11. Séquencement et dépendances

### Diagramme de dépendances

```
W1    ┌─────────────────────────────────────────────────────────────┐
      │  INFRA (section 4 étapes 1-9)                              │
      │  Bucket gi-silver · IAM · Lakehouse Manager · PyIceberg    │
      └─────────────────────────────┬───────────────────────────────┘
                                    │
W2    ┌─────────────────────────────▼───────────────────────────────┐
      │  DDL Iceberg (section 4 étapes 5-6)                        │
      │  CREATE SCHEMA × 6 · CREATE TABLE × 17                     │
      └──────────────────┬──────────────────────────────────────────┘
                         │
      ┌──────────────────▼────────────────┐  ┌─────────────────────┐
W3    │  Modification shared.py (Mod A)   │  │  Jobs Spark (P0)    │
      │  write_silver_iceberg()           │  │  spark_silver_temps  │
      │  count_iceberg()                  │  │  spark_silver_factures│
      └──────────────────┬────────────────┘  └─────────┬───────────┘
                         │                              │
      ┌──────────────────▼────────────────────────────▼─┐
W4    │  Modification silver_*.py (Mod B)               │
      │  6 scripts DuckDB + PyIceberg                   │
      └──────────────────────┬──────────────────────────┘
                             │
      ┌──────────────────────▼──────────────────────────┐
W5    │  Shadow-write 7 jours (Gate 2)                  │
      │  Validation counts quotidiens                   │
      └──────────────────────┬──────────────────────────┘
                             │ ← parallèle possible
      ┌──────────────────────▼──────────────────────────┐
W5    │  Migration historique (migrate_silver_to_iceberg)│
      │  Job Spark one-shot sur gi-poc-silver complet   │
      └──────────────────────┬──────────────────────────┘
                             │
      ┌──────────────────────▼──────────────────────────┐
W6    │  Bascule Gold → lecture Iceberg (Gate 4)        │
      │  Parallel run 7 jours (Gate 5)                  │
      └──────────────────────┬──────────────────────────┘
                             │
      ┌──────────────────────▼──────────────────────────┐
W8    │  Archivage gi-poc-silver (J+30 après go-live)   │
      └─────────────────────────────────────────────────┘
```

### Charge de travail estimée

| Phase | Tâches | Effort |
|---|---|---|
| Infra OVH (étapes 1-9) | Provisioning, IAM, DDL Trino | 1 jour |
| `shared.py` Modification A | `write_silver_iceberg()` + `count_iceberg()` | 0.5 jour |
| Spark P0 (`silver_temps`, `silver_factures`) | 2 nouveaux jobs Spark | 2 jours |
| Spark P1 (`silver_missions`) | 1 job Spark complet | 1.5 jours |
| DuckDB + PyIceberg P2-P3 (6 scripts) | Modification B × 6 | 1.5 jours |
| Shadow-write + validation | 7 jours de monitoring | 7 jours (calendaire) |
| Migration historique | Job one-shot + validation counts | 0.5 jour |
| Bascule Gold + parallel run | Modification gold_*.py + 7 jours | 8 jours (calendaire) |
| **Total effort code** | | **~7 jours** |
| **Total calendaire** | *(parallel run inclus)* | **~4 semaines** |
