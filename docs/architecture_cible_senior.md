# Architecture Cible — Vision Architecte Data Senior
## GI Data Platform : Lakehouse + MDM + API + Streaming

> **Version 1.0 · 2026-03-15**
> Niveau : recommandations top 1% — intègre Gold/Iceberg, MDM, API, CDC/Kafka,
> deux vitesses de traitement et Data Mesh partiel.

---

## Réponse directe aux 4 questions

> **Gold PostgreSQL vs Iceberg ?** → Iceberg pour la vérité analytique, PostgreSQL
> uniquement comme couche de service. Les deux, pas l'un ou l'autre.
>
> **Où positionner les référentiels ?** → Couche MDM dédiée, entre Silver et Gold,
> exposée en Iceberg dans un catalogue `ref.*` partagé par tous les domaines.
>
> **Exposition API ?** → FastAPI sur une Serving DB matérialisée, pas sur Iceberg
> directement. Contrats de données versionnés. Jamais Trino en frontal d'une API.
>
> **Orchestration + Kafka + réplique ERP ?** → Architecture deux vitesses :
> CDC Debezium → Kafka (chemin rapide, API live) +
> Kafka → S3 Iceberg via Spark Streaming (chemin lent, analytique batch).
> L'orchestrateur Airflow gère le batch ; Kafka est le bus d'événements pour le live.

---

## Sommaire

1. [Pourquoi le choix PostgreSQL vs Iceberg est mal posé](#1-pourquoi-le-choix-postgresql-vs-iceberg-est-mal-posé)
2. [Les 5 couches de l'architecture cible](#2-les-5-couches-de-larchitecture-cible)
3. [Couche MDM — positionnement des référentiels](#3-couche-mdm--positionnement-des-référentiels)
4. [Architecture deux vitesses — Batch et Streaming](#4-architecture-deux-vitesses--batch-et-streaming)
5. [CDC Debezium + Kafka — réplique ERP temps réel](#5-cdc-debezium--kafka--réplique-erp-temps-réel)
6. [Exposition API — Data Products et contrats de données](#6-exposition-api--data-products-et-contrats-de-données)
7. [Orchestration unifiée](#7-orchestration-unifiée)
8. [Schéma d'architecture complet](#8-schéma-darchitecture-complet)
9. [Roadmap de mise en œuvre](#9-roadmap-de-mise-en-œuvre)
10. [Ce que cette architecture débloque](#10-ce-que-cette-architecture-débloque)

---

## 1. Pourquoi le choix PostgreSQL vs Iceberg est mal posé

Un architecte senior refuse la dichotomie. **PostgreSQL et Iceberg ne font pas
le même travail** — les opposer est la même erreur que de choisir entre un entrepôt
et un catalogue.

### La vraie séparation : vérité analytique vs couche de service

```
Gold Iceberg (vérité)        PostgreSQL Serving (service)
─────────────────────        ──────────────────────────────
Source of truth analytique   Projection optimisée pour les requêtes fréquentes
Time-travel, ACID, merge     Index B-tree, connexions psycopg2, réponse < 10ms
Trino, Spark, dbt             Superset, FastAPI, dashboards opérationnels
Immuable une fois committé   TRUNCATE+COPY ou upsert selon fréquence
Partitionné par mois/agence  Taille limitée : uniquement les agrégats "chauds"
```

### La règle des architectes seniors

> **"Iceberg est votre source de vérité. PostgreSQL est votre cache intelligent."**

Concrètement sur ce projet :

```
gld_performance (Iceberg)           ← Trino, Spark, dbt, DataHub, time-travel
    │  dbt run --select fact_rupture_contrat+
    ▼
srv_performance (PostgreSQL)        ← Superset, FastAPI /api/v1/rupture-contrat
    │  pg_bulk_insert TRUNCATE+COPY (comme aujourd'hui)
    ▼
Redis (optionnel, hot path)         ← API < 5ms, dashboard widgets temps réel
```

**Pourquoi ne PAS lire Trino directement depuis Superset pour le Gold ?**
Trino est fait pour des requêtes analytiques distribuées (secondes à dizaines de
secondes). Une page de dashboard qui charge 8 charts en parallèle ferait 8 requêtes
Trino simultanées. PostgreSQL répond en < 50ms sur des agrégats pré-calculés.
Trino sur Iceberg reste disponible pour les analyses ad-hoc et l'exploration.

---

## 2. Les 5 couches de l'architecture cible

```
┌─────────────────────────────────────────────────────────────────────────┐
│  COUCHE 0 — SOURCES                                                     │
│  Evolia SQL Server (ERP)  · Salesforce (CRM)  · SIRENE API  · BAN API   │
└────────────────────────────────┬────────────────────────────────────────┘
                                 │  CDC (Debezium) + Bulk Extract (pymssql)
┌────────────────────────────────▼────────────────────────────────────────┐
│  COUCHE 1 — BRONZE  (Landing Zone, immutable)                           │
│  S3 gi-bronze · JSON newline-delimited · partitionné YYYY/MM/DD         │
│  Iceberg Bronze : table externe Hive, schema-on-read · Trino requêtable │
│  Kafka Topics    : evolia.wtmiss · evolia.wtcnti · evolia.wtefac …      │
└──────────────┬────────────────────────────────┬─────────────────────────┘
               │ Spark batch (slow path)        │ Flink/Spark Streaming (fast path)
┌──────────────▼──────────────┐    ┌────────────▼───────────────────────┐
│  COUCHE 2a — SILVER         │    │  COUCHE 2b — SERVING STREAM        │
│  S3 gi-silver · Iceberg ACID│    │  PostgreSQL srv_live               │
│  silver.missions.*          │    │  Tables : wtmiss_live, wtcnti_live │
│  silver.interimaires.*      │    │  Latence : < 30s vs ERP            │
│  silver.clients.*           │    │  Usage : API /live, alertes        │
│  silver.agences.*           │    └────────────────────────────────────┘
│  silver.temps.*             │
└──────────────┬──────────────┘
               │
┌──────────────▼─────────────────────────────────────────────────────────────┐
│  COUCHE 3 — MDM  (Master Data Management)                                  │
│  Iceberg catalog ref.*                                                     │
│  gi_ref.dim_interimaires (golden record : Evolia + BAN + enrichissement)   │
│  gi_ref.dim_clients      (golden record : Evolia + Salesforce + SIRENE)    │
│  gi_ref.dim_agences      (golden record : Evolia + hiérarchie Groupe)      │
│  gi_ref.dim_metiers      ref.dim_codes_naf   ref.dim_calendar              │
└──────────────┬─────────────────────────────────────────────────────────────┘
               │ dbt transformations (Gold = MDM × Silver)
┌──────────────▼──────────────────────────────────────────────────────────┐
│  COUCHE 4 — GOLD ANALYTIQUE  (Source of truth)                          │
│  Iceberg catalog gold.*  · partitionné par mois + agence_id             │
│  gold.performance.*    gold.commercial.*    gold.staffing.*             │
│  Accès : Trino (ad-hoc) · dbt (CI/CD) · DataHub (lineage)               │
└──────────────┬──────────────────────────────────────────────────────────┘
               │ Matérialisation (dbt ou pg_bulk_insert)
┌──────────────▼──────────────────────────────────────────────────────────┐
│  COUCHE 5 — SERVING  (Optimisé pour la consommation)                    │
│  PostgreSQL srv_*   · Redis (hot cache)   · REST API (FastAPI)          │
│  Superset BI · Dashboards opérationnels · Intégrations Salesforce       │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 3. Couche MDM — positionnement des référentiels

### Le problème que l'architecture POC ne résout pas encore

Dans l'architecture actuelle, `dim_interimaires` est calculée dans Silver depuis
Evolia uniquement. Si demain Salesforce ou une saisie manuelle enrichit un
intérimaire, cette donnée n'a aucun chemin vers la dimension Gold. Le MDM résout ça.

### Principe : le Golden Record

```
Sources                         MDM (ref.*)                  Consommateurs
──────────                      ───────────                  ─────────────
Evolia PYPERSONNE ──┐           ref.dim_interimaires         Silver missions
Evolia WTPINT     ──┼──►  Spark  ├── per_id (PK Evolia)     Gold staffing
BAN geocode       ──┤   Merge    ├── identite_hash (RGPD)   API /interimaires
DPAE externe      ──┤   ACID     ├── adresse_normalisee_ban  Superset
Saisie manuelle   ──┘   Iceberg  ├── source_priorite [1-5]  DataHub lineage
                                 └── updated_at, version

Sources                         MDM (ref.*)
──────────                      ───────────
Evolia WTTIESERV  ──┐           ref.dim_clients
Salesforce Account──┼──►  Spark  ├── tie_id (PK Evolia)
SIRENE API        ──┤   Merge    ├── siret (golden, dédupliqué)
BAN geocode       ──┘   ACID     ├── naf_code_normalise
                        Iceberg  ├── ca_potentiel_salesforce
                                 └── source_principale, confidence_score
```

### Règles de priorité (source_priorite)

```python
# Appliquées dans spark_mdm_interimaires.py
SOURCE_PRIORITY = {
    "saisie_manuelle_corrigee": 1,   # correction humaine = vérité absolue
    "evolia_pypersonne":        2,   # ERP = source primaire
    "ban_geocode":              3,   # BAN = référence adresse
    "dpae_externe":             4,   # enrichissement externe
    "inference_algorithm":      5,   # ML/règles = valeur par défaut
}
```

### Où physiquement dans l'architecture

```
S3 gi-ref/  (bucket dédié MDM)
  iceberg/
    dim_interimaires/    ← partitionné par agence_id
    dim_clients/         ← partitionné par agence_id
    dim_agences/
    dim_metiers/
    dim_calendar/        ← table de dates générée une fois (seed)
    dim_codes_naf/       ← seed statique INSEE
    dim_finmiss/         ← WTFINMISS + PYMTFCNT fusionnés (déjà probes 2026-03-15)
```

**Pourquoi un bucket séparé pour le MDM ?**
- Cycle de vie différent : les référentiels évoluent moins souvent que les faits
- Droits IAM distincts : l'équipe MDM peut enrichir sans accéder aux données opérationnelles
- Partage inter-projets : un autre projet GI peut consommer `ref.dim_clients` sans accès à Silver

### Intégration dans le pipeline actuel

```python
# dag_gi_pipeline_lakehouse.py — ajouter le groupe MDM
with TaskGroup("mdm") as mdm_group:
    mdm_interimaires = SparkSubmitOperator(
        task_id="mdm_interimaires",
        application="scripts/spark/spark_mdm_interimaires.py",
        # Lit silver.interimaires.* + ban_cache + evolia_corrections
        # Écrit ref.dim_interimaires via MERGE Iceberg (pas OVERWRITE)
    )
    mdm_clients = SparkSubmitOperator(task_id="mdm_clients", ...)
    mdm_agences  = SparkSubmitOperator(task_id="mdm_agences", ...)

# Ordre : Silver → MDM → Gold
silver_group >> mdm_group >> gold_group
```

---

## 4. Architecture deux vitesses — Batch et Streaming

### Le problème de la batch-only architecture

Le pipeline actuel tourne à 05h00 UTC. À 14h00, les données ont 9 heures de retard.
Pour l'API "live", c'est inacceptable. Pour les alertes (rupture CTT soudaine,
intérimaire DPAE manquante), c'est trop tard.

### Two-Speed Architecture (Lambda simplifiée)

```
                    ┌─── CHEMIN RAPIDE (Kafka + Flink) ──────────────────┐
                    │  Latence : < 30 secondes                           │
Evolia              │  Usage   : API live, alertes, réplique ERP         │
SQL Server          │                                                    │
    │               │  Debezium CDC                                      │
    │  pymssql       │  Kafka topics    Flink job       Serving DB        │
    │  (batch)       │  evolia.*   ──►  filter+enrich ► PostgreSQL srv_* │
    │               └────────────────────────────────────────────────────┘
    │
    │               ┌─── CHEMIN LENT (Spark + Iceberg) ──────────────────┐
    │               │  Latence : quotidien (05h00 UTC)                   │
    │               │  Usage   : analytique, Gold, Superset, dbt         │
    │               │                                                    │
    └──────────────►│  bronze_*.py     Silver Spark    Gold Iceberg      │
       bulk extract │  S3 Bronze  ──►  Iceberg      ►  Iceberg           │
                    └────────────────────────────────────────────────────┘
```

### Quand utiliser chaque chemin

| Cas d'usage | Chemin | Latence cible |
|---|---|---|
| API `/missions/en-cours` (affichage temps réel) | Rapide (Kafka→Serving) | < 30s |
| Alerte DPAE manquante | Rapide (Kafka→Flink→notification) | < 5 min |
| Dashboard taux de rupture mensuel | Lent (Gold Iceberg) | J+1 |
| Rapport CA net agence | Lent (Gold Iceberg) | J+1 |
| Exploration ad-hoc via Trino | Lent (Silver/Gold Iceberg) | Secondes à minutes |
| Synchro Salesforce intérimaires modifiés | Rapide (Kafka→Salesforce connector) | < 1 min |

---

## 5. CDC Debezium + Kafka — réplique ERP temps réel

### Debezium sur SQL Server (Evolia)

Debezium lit le transaction log de SQL Server (CDC natif activé) et publie
chaque INSERT/UPDATE/DELETE sur un topic Kafka.

```yaml
# config/debezium-evolia-connector.json
{
  "name": "evolia-cdc-connector",
  "config": {
    "connector.class": "io.debezium.connector.sqlserver.SqlServerConnector",
    "database.hostname": "evolia-server",
    "database.port": "1433",
    "database.user": "gi_cdc_user",          # compte READ-ONLY dédié
    "database.password": "${EVOLIA_CDC_PWD}",
    "database.dbname": "INTERACTION",
    "database.server.name": "evolia",
    "table.include.list": "dbo.WTMISS,dbo.WTCNTI,dbo.WTEFAC,dbo.PYPERSONNE,dbo.WTTIESERV",
    "snapshot.mode": "initial",              # snapshot complet au démarrage, puis CDC
    "transforms": "unwrap",
    "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
    "transforms.unwrap.delete.handling.mode": "rewrite",
    "transforms.unwrap.add.fields": "op,ts_ms",
    "topic.prefix": "evolia"
  }
}
```

Cela produit des topics Kafka :
```
evolia.dbo.WTMISS     — chaque création/modification de mission
evolia.dbo.WTCNTI     — chaque avenant/contrat
evolia.dbo.WTEFAC     — chaque facture émise
evolia.dbo.PYPERSONNE — chaque création/modification d'intérimaire
evolia.dbo.WTTIESERV  — chaque modification client
```

### Format des messages Kafka (Debezium)

```json
{
  "op": "u",                        // c=create, u=update, d=delete, r=read(snapshot)
  "ts_ms": 1742048424000,
  "before": { "PER_ID": 12345, "FINMISS_CODE": null },
  "after":  { "PER_ID": 12345, "FINMISS_CODE": "TN", "MISS_SAISIE_DTFIN": "2026-03-15" }
}
```

### Kafka → Serving DB (chemin rapide, Flink)

```python
# scripts/flink/flink_missions_live.py
# Flink Stateful Functions ou PyFlink
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment

env = StreamExecutionEnvironment.get_execution_environment()
tenv = StreamTableEnvironment.create(env)

# Source : topic Kafka evolia.dbo.WTMISS
tenv.execute_sql("""
    CREATE TABLE kafka_wtmiss (
        per_id    BIGINT,
        cnt_id    BIGINT,
        tie_id    BIGINT,
        rgpcnt_id BIGINT,
        finmiss_code VARCHAR,
        miss_annule  INT,
        op        VARCHAR,    -- c/u/d
        ts_ms     BIGINT,
        event_time AS TO_TIMESTAMP_LTZ(ts_ms, 3),
        WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
    ) WITH (
        'connector' = 'kafka',
        'topic'     = 'evolia.dbo.WTMISS',
        'properties.bootstrap.servers' = '${KAFKA_BROKERS}',
        'format'    = 'debezium-json'
    )
""")

# Sink : PostgreSQL srv_missions (UPSERT sur PER_ID+CNT_ID)
tenv.execute_sql("""
    CREATE TABLE srv_missions_live (
        per_id    BIGINT,
        cnt_id    BIGINT,
        tie_id    BIGINT,
        rgpcnt_id BIGINT,
        finmiss_code VARCHAR,
        miss_annule  INT,
        updated_at TIMESTAMP,
        PRIMARY KEY (per_id, cnt_id) NOT ENFORCED
    ) WITH (
        'connector'  = 'jdbc',
        'url'        = 'jdbc:postgresql://${PG_SERVING_HOST}/gi_serving',
        'table-name' = 'srv_missions_live',
        'driver'     = 'org.postgresql.Driver'
    )
""")

# Pipeline : Kafka → déduplification → Serving DB
tenv.execute_sql("""
    INSERT INTO srv_missions_live
    SELECT per_id, cnt_id, tie_id, rgpcnt_id, finmiss_code, miss_annule,
           TO_TIMESTAMP_LTZ(ts_ms, 3) AS updated_at
    FROM kafka_wtmiss
    WHERE op IN ('c', 'u', 'r')
""")
```

### Kafka → S3 Bronze Iceberg (chemin lent, sink permanent)

```python
# En parallèle, le même topic Kafka alimente aussi le Bronze S3
# via un Kafka Connect S3 Sink connector (pas besoin de Flink pour ça)
{
  "name": "s3-bronze-sink",
  "config": {
    "connector.class": "io.confluent.connect.s3.S3SinkConnector",
    "tasks.max": "4",
    "topics": "evolia.dbo.WTMISS,evolia.dbo.WTCNTI,evolia.dbo.WTEFAC",
    "s3.region": "gra",
    "s3.bucket.name": "gi-bronze",
    "s3.part.size": "67108864",
    "storage.class": "io.confluent.connect.s3.storage.S3Storage",
    "format.class": "io.confluent.connect.s3.format.json.JsonFormat",
    "flush.size": "10000",
    "rotate.schedule.interval.ms": "3600000",    # 1 fichier/heure max
    "path.format": "'raw_'${topic}'/year='YYYY'/month='MM'/day='dd",
    "locale": "fr_FR",
    "timezone": "Europe/Paris",
    "timestamp.extractor": "RecordField",
    "timestamp.field": "ts_ms"
  }
}
```

Ce connecteur remplace à terme les scripts `bronze_*.py` pour les tables CDC.
Les scripts bulk extract restent pour les tables sans CDC (référentiels, historique).

---

## 6. Exposition API — Data Products et contrats de données

### Principe fondamental : jamais Trino en frontal d'une API

Trino est un moteur analytique. Il démarre des requêtes distribuées sur S3,
consomme des ressources cluster, peut prendre 2-30 secondes. Une API REST
doit répondre en < 200ms. **La Serving DB est le seul frontal acceptable.**

### Architecture API (FastAPI + PostgreSQL Serving)

```
                ┌─────────────────────────────────────────┐
                │  API Gateway (OVH API Management)       │
                │  Rate limiting · Auth JWT · Versioning  │
                └────────────────┬────────────────────────┘
                                 │
        ┌────────────────────────┼────────────────────────┐
        │                        │                        │
┌───────▼───────┐    ┌───────────▼──────┐    ┌───────────▼──────┐
│  /v1/missions │    │  /v1/interimaires│    │  /v1/analytics   │
│  FastAPI      │    │  FastAPI         │    │  FastAPI (async)  │
│               │    │                  │    │                   │
│ srv_missions  │    │ srv_interimaires  │    │ srv_performance  │
│ PostgreSQL    │    │ PostgreSQL        │    │ PostgreSQL       │
│               │    │                  │    │ (Gold matérialisé)│
│ + Redis cache │    │ + Redis cache    │    │                   │
└───────────────┘    └──────────────────┘    └───────────────────┘
        ▲                      ▲                      ▲
        │ UPSERT               │ UPSERT               │ TRUNCATE+COPY
        │ (Flink live)         │ (Flink live)         │ (dbt/Spark batch)
   Kafka evolia.*         Kafka evolia.*          Gold Iceberg
```

### Structure des Data Products

Chaque domaine expose ses données via un **contrat de données versionné** :

```python
# api/v1/missions/schemas.py  — Contrat de données versionné
from pydantic import BaseModel
from datetime import date
from typing import Optional
from enum import Enum

class StatutFinMission(str, Enum):
    EN_COURS      = "EN_COURS"
    TERME_NORMAL  = "TERME_NORMAL"
    RUPTURE       = "RUPTURE"
    ANNULEE       = "ANNULEE"

class MissionResponse(BaseModel):
    """Contrat v1 — stable, rétrocompatible.
    Tout changement breaking → v2 avec période de transition 90 jours.
    """
    per_id:               int
    cnt_id:               int
    agence_id:            int
    tie_id:               int
    date_debut:           date
    date_fin_reelle:      Optional[date]
    statut_fin_mission:   StatutFinMission
    duree_reelle_jours:   Optional[int]
    # Champs RGPD : jamais exposés directement
    # per_id est un identifiant opaque — pas de nom/prénom

    class Config:
        json_schema_extra = {
            "x-data-contract-version": "1.0",
            "x-source": "silver.missions.fin_mission",
            "x-freshness-sla": "T+1j (batch) ou T+30s (live endpoint)",
        }
```

### Endpoints recommandés

```python
# api/v1/missions/router.py
from fastapi import APIRouter, Query
from typing import Optional
import asyncpg  # connexion async PostgreSQL

router = APIRouter(prefix="/v1/missions", tags=["missions"])

@router.get("/rupture-contrat", response_model=list[RuptureContratResponse])
async def get_rupture_contrat(
    agence_id: Optional[int] = Query(None),
    mois_debut: Optional[str] = Query(None, example="2026-01"),
    mois_fin:   Optional[str] = Query(None, example="2026-03"),
    db = Depends(get_serving_db),
):
    """
    KPI taux de rupture de contrat CTT, par agence et mois.
    Source : srv_performance.fact_rupture_contrat (Gold matérialisé).
    Fraîcheur : données J-1 (batch 05h00 UTC).
    Pour données temps réel : utiliser /v1/missions/live.
    """
    ...

@router.get("/live", response_model=list[MissionLiveResponse])
async def get_missions_live(
    agence_id: int,
    db_live = Depends(get_serving_live_db),   # PostgreSQL srv_live alimenté par Flink
):
    """
    Missions en cours, fraîcheur < 30 secondes (CDC Debezium → Kafka → Flink).
    Usage : monitoring temps réel, alertes opérationnelles.
    """
    ...
```

### Kafka comme API événementielle

En plus de l'API REST, exposer Kafka directement pour les consommateurs
qui ont besoin du flux d'événements (pas seulement du snapshot) :

```
Consommateur interne (Salesforce sync) → Subscribe evolia.dbo.PYPERSONNE
Consommateur interne (alertes DPAE)    → Subscribe evolia.dbo.WTMISS (filter miss_flagdpae IS NULL)
Consommateur externe partenaire        → Topic dédié gi.missions.public (données filtrées/RGPD)
DataHub                                → Subscribe tous topics → lineage automatique
```

```yaml
# config/kafka-topics.yml — topics applicatifs (distincts des topics CDC bruts)
topics:
  gi.missions.fin_mission:           # événements Silver fin_mission
    partitions: 6
    retention: 7 days
    schema: avro (schema registry)

  gi.interimaires.modifications:     # golden record modifié (MDM)
    partitions: 3
    retention: 30 days
    schema: avro

  gi.alertes.dpae_manquante:         # alerte métier générée par Flink
    partitions: 1
    retention: 24 hours
```

---

## 7. Orchestration unifiée

### Principe : Airflow orchestre le batch, Kafka est le bus événementiel

```
AIRFLOW (batch, quotidien 05h00 UTC)          KAFKA (événementiel, continu)
────────────────────────────────────          ──────────────────────────────
bronze_bulk_extract (tables sans CDC)         Debezium CDC → topics evolia.*
  ↓                                           Kafka Connect → S3 Bronze (sink)
spark_silver_missions (Spark job)             Flink → PostgreSQL srv_live
  ↓                                           Flink → alertes métier
spark_mdm_interimaires (Spark job)
  ↓
dbt run --select gold.*+ (transformations)
  ↓
materialize_serving (pg_bulk_insert Gold → srv_*)
  ↓
rgpd_audit + datahub_ingest
```

### KafkaSensor Airflow — déclencher le batch depuis Kafka

Pour les tables où le CDC est actif, Airflow peut se déclencher sur un offset
Kafka plutôt que sur un cron :

```python
# dags/dag_gi_pipeline_lakehouse.py
from airflow.providers.apache.kafka.sensors.kafka import AwaitMessageSensor

# Attendre que le CDC ait terminé de syncer la journée (offset lag = 0)
wait_cdc_complete = AwaitMessageSensor(
    task_id="wait_evolia_cdc_sync",
    kafka_config_id="kafka_evolia",
    topics=["evolia.dbo.WTMISS"],
    apply_function=lambda msg: msg.offset() > MIN_EXPECTED_OFFSET,
    poke_interval=60,
    timeout=3600,
)

wait_cdc_complete >> spark_silver_missions
```

### Airflow + dbt Cloud (optionnel mais recommandé)

```python
# Remplacer les gold_*.py par dbt models versionés en CI/CD
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator

gold_dbt = DbtCloudRunJobOperator(
    task_id="dbt_gold_run",
    dbt_cloud_conn_id="dbt_cloud",
    job_id=12345,   # job dbt Cloud "gi_gold_daily"
    # dbt gère les tests de qualité, la documentation, le lineage
)

spark_mdm_group >> gold_dbt >> materialize_serving
```

---

## 8. Schéma d'architecture complet

```
╔══════════════════════════════════════════════════════════════════════════════════╗
║  SOURCES                                                                        ║
║  Evolia SQL Server · Salesforce · SIRENE · BAN · Saisies manuelles            ║
╚══════════════╤════════════════════════════════╤════════════════════════════════╝
               │ pymssql bulk (tables sans CDC)  │ Debezium CDC (tables WTMISS, WTCNTI…)
               ▼                                 ▼
╔══════════════╧══════════════════════════════════════════════════════════════════╗
║  INGESTION                                                                      ║
║  ┌─────────────────────────────┐    ┌────────────────────────────────────────┐ ║
║  │  S3 gi-bronze (JSON)        │    │  Kafka Cluster (OVH Managed Kafka)     │ ║
║  │  Airflow BashOperator       │    │  topics : evolia.dbo.*                 │ ║
║  │  bronze_*.py (inchangés)    │    │  schema registry : Avro               │ ║
║  └─────────────┬───────────────┘    └──────────┬─────────────────────────────┘ ║
╚════════════════╪══════════════════════════════════╪════════════════════════════╝
                 │ Spark batch                       │ Kafka Connect S3 Sink + Flink
                 ▼                                   ▼
╔════════════════╧═══════════════════════════════════╧════════════════════════════╗
║  TRAITEMENT DEUX VITESSES                                                       ║
║  ┌──────────────────────────────────────┐  ┌────────────────────────────────┐  ║
║  │  LENT — Iceberg Lakehouse            │  │  RAPIDE — Serving Live         │  ║
║  │  S3 gi-silver · Silver Iceberg       │  │  PostgreSQL srv_live           │  ║
║  │  silver.missions.fin_mission         │  │  srv_missions_live             │  ║
║  │  silver.interimaires.dim_interimaires│  │  srv_interimaires_live         │  ║
║  │  silver.clients.dim_clients          │  │  Latence < 30s                 │  ║
║  │  (Spark batch quotidien)             │  │  (Flink continu)               │  ║
║  └──────────────────┬───────────────────┘  └───────────────┬────────────────┘  ║
╚═══════════════════════╪═══════════════════════════════════════╪════════════════╝
                        │ Spark MDM                             │
                        ▼                                       │
╔═══════════════════════╧════════════════════════════════════╗  │
║  MDM — S3 gi-ref · Iceberg catalog ref.*                   ║  │
║  ref.dim_interimaires (golden record)                      ║  │
║  ref.dim_clients      (golden record + SIRENE + Salesforce)║  │
║  ref.dim_agences      (golden record + hiérarchie)         ║  │
║  ref.dim_metiers  ref.dim_codes_naf  ref.dim_calendar      ║  │
╚═══════════════════════╤════════════════════════════════════╝  │
                        │ dbt (Gold = MDM × Silver)             │
                        ▼                                       │
╔═══════════════════════╧════════════════════════════════════╗  │
║  GOLD ANALYTIQUE — S3 gi-gold · Iceberg catalog gold.*     ║  │
║  gold.performance.fact_rupture_contrat                     ║  │
║  gold.commercial.fact_ca_mensuel_client                    ║  │
║  gold.staffing.fact_missions_detail                        ║  │
║  gold.staffing.fact_fidelisation_interimaires              ║  │
║  ← Trino (ad-hoc) · dbt CI/CD · DataHub lineage           ║  │
╚═══════════════════════╤════════════════════════════════════╝  │
                        │ pg_bulk_insert / dbt materialize       │
                        ▼                                       ▼
╔═══════════════════════╧════════════════════════════════════════╧════════════════╗
║  SERVING — PostgreSQL srv_* + Redis                                             ║
║  ┌──────────────────────────────┐  ┌────────────────────────────────────────┐  ║
║  │  srv_performance (batch J-1) │  │  srv_live (temps réel < 30s)          │  ║
║  │  srv_commercial              │  │  srv_missions_live                     │  ║
║  │  srv_staffing                │  │  srv_interimaires_live                 │  ║
║  └────────────┬─────────────────┘  └──────────────────┬──────────────────── ┘  ║
╚══════════════════════════════════════════════════════════════════════════════════╝
                        │                                  │
        ┌───────────────┼────────────────┐                 │
        ▼               ▼                ▼                 ▼
   Superset BI    FastAPI /v1/     DataHub         FastAPI /v1/live
   dashboards     analytics        catalogue       missions en cours
                  rupture-contrat  lineage         alertes DPAE
```

---

## 9. Roadmap de mise en œuvre

### Sprint 1 — Fondations (2 semaines)
```
[ ] Provisioner Kafka OVH Managed (ou Confluent Cloud en GRA)
[ ] Activer CDC SQL Server sur INTERACTION.dbo (WTMISS, WTCNTI, WTEFAC, PYPERSONNE)
[ ] Déployer Debezium connector → topics evolia.*
[ ] Vérifier volume messages : probe offset lag quotidien
```

### Sprint 2 — Chemin rapide (2 semaines)
```
[ ] Déployer Flink job : kafka_wtmiss → srv_missions_live (PostgreSQL)
[ ] Déployer FastAPI /v1/missions/live (lecture srv_live)
[ ] Tests : comparer avec Evolia direct (count + freshness)
```

### Sprint 3 — Lakehouse Silver Iceberg (3 semaines)
```
[ ] Migration Silver Parquet → Iceberg (cf. migration_ovh_lakehouse.md §5)
[ ] Spark job spark_silver_fin_mission.py → silver.missions.fin_mission Iceberg
[ ] Validation counts + time-travel test
```

### Sprint 4 — MDM (3 semaines)
```
[ ] spark_mdm_interimaires.py → ref.dim_interimaires Iceberg
[ ] spark_mdm_clients.py → ref.dim_clients (+ SIRENE enrichissement)
[ ] Intégration Salesforce Account → ref.dim_clients (confidence_score)
```

### Sprint 5 — Gold Iceberg + dbt (2 semaines)
```
[ ] Migrer gold_*.py → dbt models (Gold Iceberg)
[ ] gold/performance/fact_rupture_contrat.sql (dbt)
[ ] dbt test + dbt docs → DataHub lineage automatique
```

### Sprint 6 — API complète + Superset Trino (2 semaines)
```
[ ] FastAPI /v1/analytics/* (lecture srv_performance matérialisé)
[ ] API Gateway OVH (auth, rate limiting, versioning)
[ ] Superset → connexion Trino pour exploration ad-hoc
[ ] Kafka topics applicatifs gi.missions.* (Avro + schema registry)
```

### Sprint 7 — Stabilisation go-live (2 semaines)
```
[ ] Parallel run 7 jours (ancien pipeline + nouveau en parallèle)
[ ] Tests non-régression automatisés (cf. migration_ovh_lakehouse.md §10)
[ ] Bascule Airflow + archivage scripts DuckDB
[ ] Documentation runbooks ops
```

---

## 10. Ce que cette architecture débloque

### Fonctionnalités impossibles aujourd'hui, triviales demain

**Time-travel (Iceberg)**
```sql
-- Comparer le taux de rupture cette semaine vs la semaine d'avant
SELECT * FROM gold.performance.fact_rupture_contrat
FOR TIMESTAMP AS OF TIMESTAMP '2026-03-08 05:00:00'
WHERE agence_id = 42;
```

**RGPD droit à l'oubli (MDM Iceberg)**
```sql
-- Supprimer un intérimaire de tous les snapshots Iceberg
CALL iceberg.system.rewrite_data_files('ref.dim_interimaires');
DELETE FROM ref.dim_interimaires WHERE per_id = 99999;
-- Le per_id est pseudonymisé dans Silver/Gold → pas de cascade nécessaire
```

**Schema evolution sans downtime**
```sql
-- Ajouter une colonne sans reconstruire la table
ALTER TABLE silver.missions.fin_mission
ADD COLUMN motif_rupture_detail VARCHAR;
-- Les anciens fichiers Parquet lisent NULL pour cette colonne — aucun job cassé
```

**Lineage automatique (DataHub + Trino)**
```
Evolia.WTMISS
  → bronze.raw.wtmiss
    → silver.missions.fin_mission
      → gold.performance.fact_rupture_contrat
        → srv_performance.fact_rupture_contrat
          → /api/v1/analytics/rupture-contrat
            → Superset dashboard "Qualité & Performance Mission"
```

**Alertes temps réel (Flink sur Kafka)**
```python
# Flink job : détecter DPAE manquante dans les 8h avant début de mission
# Kafka topic gi.alertes.dpae_manquante → notification Slack/email automatique
```

**Multi-tenancy (si Groupe Interaction a plusieurs entités)**
```sql
-- Chaque filiale peut avoir son propre schéma Iceberg
-- avec partage des référentiels MDM (ref.dim_metiers est commun)
CREATE SCHEMA gold_gi_ouest.performance;
CREATE SCHEMA gold_gi_sud.performance;
-- ref.dim_interimaires est partagé entre toutes les filiales
```
