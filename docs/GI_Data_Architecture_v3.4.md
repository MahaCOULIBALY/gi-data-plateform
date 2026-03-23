# GI Data Architecture

> Version 3.4
> Date : 2026-03-20
> Auteur : Architecture Data
> Statut : Référence cible — POC actif (Silver Parquet + Gold PostgreSQL 18) | Architecture cible v3 maintenue
> Diff vs v3.0 : ⚠️ Décision POC 2026-03-20 — Silver S3 Parquet ZSTD (DuckDB, pas Iceberg), Gold PostgreSQL 18 (EU-WEST-PAR, pas GRA), OVH Lakehouse Manager repositionné Phase 2+

---

## AVERTISSEMENT — DÉCISION ARCHITECTURALE POC (2026-03-20)

> ⚠️ **Décision d'accélération** : en attente de documentation suffisante sur OVHcloud Data Platform, l'architecture POC adopte un chemin simplifié :
>
> | Couche | Architecture cible v3 | **Architecture POC active** |
> |---|---|---|
> | Silver | Iceberg via Polaris | **S3 Parquet ZSTD via DuckDB COPY TO** |
> | Gold | Iceberg marts | **PostgreSQL 18 managé OVH (EU-WEST-PAR)** |
> | Région | GRA (Gravelines) | **EU-WEST-PAR (Paris) — aligne sur les buckets existants** |
>
> Cette décision est **réversible** : la migration Silver Parquet → Iceberg ne nécessite que le remplacement du sink dans `shared.py`. L'architecture cible v3 reste la référence long terme.

---

## 1. Objet du document

Ce document constitue l'architecture de référence de la plateforme data GI. Il formalise la cible technique, les principes de conception, les choix structurants, les composants, les flux, la gouvernance, la sécurité, l'exploitation et la roadmap de mise en œuvre.

L'objectif est de construire une plateforme moderne, robuste, souveraine et gouvernable, capable de répondre simultanément à cinq besoins :

- Vérité analytique fiable et historisée.
- Exposition opérationnelle rapide via API et bases de service.
- Intégration temps quasi réel depuis l'ERP.
- Gouvernance des référentiels et des contrats de données.
- Souveraineté des données conforme aux exigences réglementaires françaises et européennes.

---

## 2. Résumé exécutif

L'architecture cible repose sur un lakehouse Iceberg en cloud souverain pour la donnée analytique, une couche MDM dédiée pour les golden records, une serving layer PostgreSQL enrichie pour les usages applicatifs et analytiques légers, et un bus Kafka avec Schema Registry pour les événements et le CDC.

**En phase POC (actif)** : Silver = S3 Parquet ZSTD (DuckDB), Gold = PostgreSQL 18 OVH (EU-WEST-PAR).

### Principes directeurs

| Composant | Rôle | Statut POC |
|---|---|---|
| **S3 Parquet ZSTD** | Silver — couche transformation interne | ✅ Actif (DuckDB COPY TO) |
| **PostgreSQL 18** | Gold — serving analytique + Superset | ✅ Actif (EU-WEST-PAR) |
| **Iceberg** | Source de vérité analytique long terme | 🔄 Phase 2+ |
| **Apache Polaris** | Catalogue Iceberg (TLP Apache, mars 2026) | 🔄 Phase 2+ |
| **Kafka + Apicurio Registry** | Transport événementiel avec contrats de schéma | 🔄 Phase 2+ |
| **Spark** | Moteur unifié batch + streaming | 🔄 Phase 2+ |
| **OpenMetadata / DataHub** | Catalogue, gouvernance, lineage | 🔄 Phase 3+ |
| **FastAPI** | Façade d'exposition contractuelle | 🔄 Phase 3+ |
| **OVHcloud S3** | Object storage souverain | ✅ Actif (EU-WEST-PAR) |
| **K8s Secrets + ESO** (Phase 2+) / **Infisical ou Vault** (Phase 3+) | Gestion des secrets | 🔄 Phase 2+ |

### Évolutions structurantes vs v3.0

| Domaine | v3.0 | **v3.4 (POC actif)** |
|---|---|---|
| Région OVH | GRA (Gravelines) | **EU-WEST-PAR (Paris) — buckets existants** |
| Silver storage | Iceberg via Polaris | **S3 Parquet ZSTD via DuckDB COPY TO** |
| Gold POC | Iceberg marts | **PostgreSQL 18 Business DB1-15 OVH** |
| OVH Lakehouse Manager | Option managée cible Phase 1 | **Repositionné Phase 2+ après stabilisation** |
| PostgreSQL version | 16 | **18 (nouveau sous-système I/O, +3× lecture)** |
| Schémas Gold | srv_live, srv_analytics, srv_ref | **gld_commercial, gld_staffing, gld_facturation, gld_rh, srv_analytics, srv_ref, slv_mirror** |
| Souveraineté cloud | Implicite | OVHcloud S3 explicite, cartographie SecNumCloud |
| Catalogue Iceberg | REST Catalog / HMS / Polaris | Apache Polaris (TLP mars 2026) — Phase 2+ |
| Schema Registry | Absent | Apicurio Registry — Phase 2+ |
| Observabilité | Liste de métriques | Architecture OTel → Kafka → Iceberg — Phase 2+ |
| Serving analytique | PostgreSQL seul | + DuckDB sidecar sur Iceberg REST + pgvector — Phase 3+ |

---

## 3. Souveraineté et conformité cloud

### 3.1 Contexte réglementaire

En 2026, toute plateforme data traitant des données personnelles ou métier sensibles d'un groupe français doit prendre position explicite sur la souveraineté des données. Le CLOUD Act américain expose les données hébergées chez tout provider ayant des filiales US ou des plans de contrôle hors UE à la juridiction américaine, indépendamment de la localisation physique des serveurs.

### 3.2 Exigences de souveraineté

| Exigence | Règle |
|---|---|
| Object storage | OVHcloud S3 High Performance (certifié SecNumCloud) — **région EU-WEST-PAR (Paris)**. MinIO exclu — charge opérationnelle non justifiée |
| Lakehouse managé | OVHcloud Lakehouse Manager (Iceberg natif + Trino + Superset intégrés, SecNumCloud) — **Phase 2+** |
| PostgreSQL Gold (POC) | OVHcloud Managed PostgreSQL 18 — **EU-WEST-PAR (Paris)** — même région que les buckets S3 |
| Compute Kubernetes | OVHcloud Managed Kubernetes — Phase 2+ |
| Plans de contrôle | Hébergés en UE, opérés par entité de droit européen |
| SaaS tiers | Exclusion de tout SaaS US pour le stockage et le traitement de données personnelles ou métier critiques |
| dbt Cloud | Exclu — code et métadonnées hébergés sur serveurs US |
| Chiffrement | Clés de chiffrement gérées par l'organisation (BYOK) ou par le provider souverain, jamais par un tiers US |

### 3.3 OVHcloud Lakehouse Manager — option managée cible Phase 2+

> ⚠️ **Repositionnement v3.4** : le Lakehouse Manager est repositionné en Phase 2+ en raison d'une documentation insuffisante sur l'intégration du catalog Iceberg (`catalog 'iceberg' not found`). La décision sera réévaluée après stabilisation du POC Silver + Gold PostgreSQL 18.

OVHcloud propose un Lakehouse Manager natif Apache Iceberg en cloud souverain avec Trino et Superset intégrés. Cette offre reste la **cible architecturale long terme** pour GI.

**Avantages pour GI (équipe de 5 personnes)** :

- Iceberg, Trino et Superset opérés par OVHcloud — pas de maintenance K8s pour ces composants.
- Object storage S3 souverain intégré.
- Conforme SecNumCloud, RGPD, hors périmètre CLOUD Act.
- Réduit la charge opérationnelle sur les composants analytiques (compaction, snapshots, upgrades Trino).

**Décision de migration** : la migration Silver Parquet → Iceberg se fera après stabilisation (Phase 2+) en remplaçant uniquement le sink dans `shared.py` — le SQL de transformation est inchangé.

### 3.4 Cartographie SecNumCloud

| Composant | Criticité données | SecNumCloud requis | Implémentation POC |
|---|---|---|---|
| Silver S3 Parquet | Haute (données métier transformées) | Oui | OVHcloud S3 EU-WEST-PAR |
| Gold PostgreSQL 18 | Haute (PII tokenisés, données métier) | Oui | OVHcloud Managed PostgreSQL 18 EU-WEST-PAR |
| Lakehouse (Iceberg + Trino + Superset) | Haute | Oui | OVHcloud Lakehouse Manager — Phase 2+ |
| PostgreSQL Serving (Phase 2+) | Haute | Oui | OVHcloud Managed |
| Kubernetes (Spark, Airflow) | Moyenne | Recommandé | OVHcloud Managed K8s — Phase 2+ |
| Secrets (Phase 2+) | Haute | Recommandé | K8s Secrets + External Secrets Operator |
| Secrets + tokenisation PII (Phase 3+) | Critique | Oui | Évaluation Infisical vs HashiCorp Vault |
| Kafka | Moyenne | Recommandé | Self-hosted sur K8s souverain — Phase 2+ |
| Catalogue Polaris | Moyenne | Recommandé | Self-hosted — Phase 2+ |

### 3.5 Composants exclus du périmètre souverain

Les outils suivants peuvent être utilisés en mode SaaS si et seulement si ils ne stockent ni ne traitent de données personnelles ou métier :

- Outils de CI/CD (GitHub Actions, GitLab CI) — code uniquement, pas de données.
- Documentation technique externe.
- Monitoring applicatif non-PII (métriques d'infrastructure anonymisées).

---

## 4. Principes d'architecture

### 4.1 Principes fondamentaux

1. Une donnée analytique ne doit pas être servie directement depuis un moteur de requêtes distribuées à des applications transactionnelles ou à des API.
2. Une API ne lit jamais directement Trino ou Iceberg.
3. Toute donnée exposée à des consommateurs est portée par un contrat versionné.
4. Toute entité métier partagée entre domaines transite par un référentiel gouverné.
5. Le streaming ne doit pas dupliquer inutilement la logique métier du batch.
6. La plateforme doit privilégier la réduction du nombre de moteurs et d'outils à opérer.
7. La traçabilité, la qualité et la gouvernance sont natives, pas ajoutées après coup.
8. Les données personnelles sont pseudonymisées (NIR SHA-256+salt) avant Silver.
9. Tout composant stockant ou traitant des données doit être hébergé sur infrastructure souveraine européenne.
10. L'observabilité est une architecture, pas une liste de métriques.

### 4.2 Conséquences concrètes — POC actif

- **Silver** : S3 Parquet ZSTD via DuckDB — couche interne, jamais exposée directement.
- **Gold** : PostgreSQL 18 OVH (EU-WEST-PAR) — serving Superset et APIs.
- Silver n'est pas exposé à Superset — seul Gold PostgreSQL 18 est la source Superset.
- DuckDB lit Parquet S3 nativement : Bronze JSON + Silver Parquet → Gold PostgreSQL 18.
- Redis uniquement pour le hot cache facultatif (Phase 3+).
- Kafka utilisé pour le transport d'événements, pas comme substitut à l'orchestration (Phase 2+).
- Le MDM est une couche dédiée, entre Silver et Gold (Phase 4+).
- Les référentiels partagés sont exposés dans un namespace `srv_ref`.
- **Région unique** : EU-WEST-PAR pour S3 + PostgreSQL 18 — zéro coût cross-region.

---

## 5. Décisions d'architecture

### 5.1 PostgreSQL 18 vs Iceberg — Positionnement POC

Le choix n'est pas exclusif — les deux coexistent avec des responsabilités différentes.

**Décision POC (2026-03-20)** :

| Moteur | Responsabilité | Phase |
|---|---|---|
| **S3 Parquet ZSTD (DuckDB)** | Silver — transformation interne, batch, scan séquentiel | ✅ Actif |
| **PostgreSQL 18** | Gold — serving Superset, requêtes indexées, SLA dashboards | ✅ Actif |
| **Iceberg** | Historique, time travel, auditabilité, exploration, publications analytiques | 🔄 Phase 2+ |
| **DuckDB sidecar** | Requêtes analytiques ad hoc légères directement sur Iceberg REST | 🔄 Phase 3+ |

**Pourquoi Silver ne va PAS en PostgreSQL** :
- Silver est une couche interne — jamais consommée directement par Superset ou les APIs.
- 760K lignes/jour sur `heures_detail` : Parquet columnar + compression ZSTD = scan analytique optimal vs row-store PostgreSQL.
- Coût : ~3€/mois en Parquet S3 vs ~430€/mois en PostgreSQL managé pour 300 Go.
- PostgreSQL apporte ACID, index B-tree, connexions concurrentes — inutile sur une couche batch séquentielle.

**Pourquoi PostgreSQL 18 pour Gold** :
- PostgreSQL 18 introduit un nouveau sous-système I/O asynchrone (+3× gains en lecture).
- Technologie maîtrisée — déploiement immédiat, zéro courbe d'apprentissage.
- Compatible Superset nativement (datasource PostgreSQL direct).
- pgvector disponible pour les cas RAG/embedding (Phase 3+).

### 5.2 Région OVH — EU-WEST-PAR (Paris)

**Décision irréversible** : tous les services OVH du projet GI Data sont en EU-WEST-PAR.

| Service | Région | Endpoint |
|---|---|---|
| Buckets S3 (Bronze, Silver, Gold) | EU-WEST-PAR | `https://s3.eu-west-par.io.cloud.ovh.net/` |
| PostgreSQL 18 Gold | EU-WEST-PAR | `<host>.database.cloud.ovh.net:20184` |
| OVH Lakehouse Manager (Phase 2+) | EU-WEST-PAR | À provisionner |
| OVH Managed K8s (Phase 2+) | EU-WEST-PAR | À provisionner |

**Justification** : les buckets `gi-poc-bronze`, `gi-poc-silver`, `gi-poc-gold` sont déjà en EU-WEST-PAR. Provisionner PostgreSQL dans une autre région génère des coûts de transfert cross-region et de la latence inutile.

### 5.3 Streaming — simplification stratégique (Phase 2+)

Identique à v3.0 — repositionné en Phase 2+. Spark Structured Streaming reste le moteur cible.

### 5.4 Catalogue Iceberg — Apache Polaris (Phase 2+)

Identique à v3.0 — repositionné en Phase 2+. Polaris reste le catalogue Iceberg cible.

### 5.5 Schema Registry et contrats d'événements Kafka (Phase 2+)

Identique à v3.0 — Apicurio Registry repositionné en Phase 2+.

---

## 6. Architecture cible

### 6.1 Vue d'ensemble — Architecture POC active

```text
┌──────────────────────────────────────────────────────────────────────────────┐
│ Sources                                                                      │
│ Evolia · Wizim · Salesforce · Bluekango · Démat · Digitaléo · Mercii        │
│ SIRH-PAIE · XLSX (SIREN, Réf Org) · OpenData                               │
└──────────────────────────────┬───────────────────────────────────────────────┘
                               │ Extraction batch Python
                               ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│ BRONZE — OVHcloud S3 EU-WEST-PAR                                            │
│ gi-poc-bronze (47.87 Go, JSON brut, immutable)                              │
│ Partitionnement : raw_{source}/{date_partition}/*.json                      │
└──────────────────────────────┬───────────────────────────────────────────────┘
                               │ DuckDB read_json_auto()
                               ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│ SILVER — OVHcloud S3 EU-WEST-PAR                                            │
│ gi-poc-silver (Parquet ZSTD, 459 Mo)                                        │
│ DuckDB COPY TO — FORMAT PARQUET, COMPRESSION ZSTD, OVERWRITE_OR_IGNORE TRUE│
│ Convention : slv_{domaine}/{table}/**/*.parquet                             │
│ SCD2 sur clients + intérimaires · pseudonymisation NIR (SHA-256+salt)       │
│ ⚠️ Couche interne — jamais exposée directement à Superset                   │
└──────────────────────────────┬───────────────────────────────────────────────┘
                               │ Scripts gold_*.py
                               │ DuckDB lit Parquet S3 → matérialise Gold
                               ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│ GOLD — PostgreSQL 18 OVH (EU-WEST-PAR) — gi-gold                           │
│ Plan : Business DB1-15 (15 Go RAM, 2 nœuds HA, 240 Go SSD)                │
│ Port : 20184 · sslmode=require                                              │
│                                                                              │
│  gld_commercial    — KPIs commerciaux, CA, marges                           │
│  gld_staffing      — Missions, taux placement, ETP                          │
│  gld_facturation   — Factures, recouvrement                                 │
│  gld_rh            — Intérimaires, fidélisation, compétences                │
│  srv_analytics     — Vues Superset (agrégats pré-calculés)                 │
│  srv_ref           — Référentiels partagés (agences, SIREN)                │
│  slv_mirror        — Miroir Silver pour Superset (consultation)            │
└──────────────────────────────┬───────────────────────────────────────────────┘
                               │ Datasource PostgreSQL
                               ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│ CONSOMMATION                                                                 │
│ OVH Data Platform Superset ←── PostgreSQL Gold (connexion directe)         │
│ FastAPI (Phase 3+)                                                           │
└──────────────────────────────────────────────────────────────────────────────┘

           ┌───────────────────────────────────────────────────────┐
           │ ML/IA (Phase 3+)                                       │
           │ DuckDB get_duckdb_connection()                         │
           │ Lit Bronze JSON S3 + Silver Parquet S3 nativement     │
           │ Retourne Arrow / pandas / Polars sans copie mémoire    │
           └───────────────────────────────────────────────────────┘
```

### 6.2 Architecture cible long terme (Phase 2+)

```text
Sources → Privacy Gateway → Bronze commun (S3 + Kafka)
  ↓ chemin lent                  ↓ chemin rapide
Silver Iceberg (Polaris)    Spark Structured Streaming
  ↓                              ↓
MDM / Référentiels          srv_live PostgreSQL
  ↓
Gold Iceberg (marts)
  ↓ matérialisation
Serving PostgreSQL + pgvector + DuckDB sidecar
  ↓
FastAPI · Superset (Lakehouse Manager) · Trino ad hoc
```

### 6.3 Les couches

#### Couche 0 — Sources

- Evolia SQL Server (ERP) · Wizim · Salesforce · Bluekango
- Plateforme de Démat · Digitaléo · Mercii · SIRH-PAIE
- XLSX (Base SIREN, Réf Organisationnel) · OpenData

#### Couche 1 — Bronze (OVHcloud S3 EU-WEST-PAR)

- `gi-poc-bronze` — raw immutable, JSON, 47.87 Go, 1541 objets.
- Partitionnement temporel : `raw_{source}/{YYYY-MM-DD}/*.json`.
- Conservation des payloads d'origine.
- Pas de logique métier.
- **POC** : pas de tokenisation PII (NIR pseudonymisé en Silver uniquement).

#### Couche 2 — Silver (OVHcloud S3 EU-WEST-PAR)

- `gi-poc-silver` — Parquet ZSTD, 459 Mo, 44 fichiers.
- **Format** : S3 Parquet ZSTD via DuckDB COPY TO (pas Iceberg en Phase POC).
- Déduplication technique, mapping de schémas sources.
- SCD Type 2 sur `dim_clients` et `dim_interimaires`.
- Pseudonymisation NIR (SHA-256 + salt) dans `silver_interimaires.py`.
- **Migration Iceberg (Phase 2+)** : remplacement du sink uniquement dans `shared.py`.

#### Couche 3 — Gold (PostgreSQL 18 OVH EU-WEST-PAR)

- `gi-gold` — PostgreSQL 18, Business DB1-15, 240 Go SSD, 2 nœuds HA.
- Schémas : `gld_commercial`, `gld_staffing`, `gld_facturation`, `gld_rh`, `srv_analytics`, `srv_ref`, `slv_mirror`.
- Alimentation : scripts `gold_*.py` (DuckDB lit Silver Parquet → écrit PostgreSQL).
- Exposition : Superset OVH Data Platform connecté directement en datasource PostgreSQL.

#### Couche 4 — MDM (Phase 4+)

Identique à v3.0 — `ref.dim_interimaires`, `ref.dim_clients`, etc.

#### Couche 5 — Serving étendu (Phase 3+)

- PostgreSQL + pgvector (recherche sémantique qualifications).
- DuckDB sidecar (requêtes ad hoc sur Iceberg REST).
- FastAPI (façade contractuelle).

---

## 7. Flux de traitement — POC actif

### 7.1 Pipeline batch POC

1. Extraction bulk depuis sources (Evolia SQL Server, XLSX, APIs).
2. Chargement Bronze (`gi-poc-bronze`, JSON, immuable).
3. Transformations Silver : DuckDB `read_json_auto()` → `COPY TO Parquet ZSTD` (`gi-poc-silver`).
4. SCD2 sur clients + intérimaires (JSONL temporaire → COPY TO Parquet).
5. Pseudonymisation NIR (SHA-256+salt) dans `silver_interimaires.py`.
6. Construction Gold : scripts `gold_*.py` (DuckDB lit `gi-poc-silver` → écrit PostgreSQL 18).
7. Exposition Superset via datasource PostgreSQL Gold.

### 7.2 RunModes

| Mode | Comportement |
|---|---|
| `OFFLINE` | No-op total, zéro connexion externe |
| `PROBE` | Compte les lignes, log JSON structuré, aucune écriture S3 ni PostgreSQL |
| `LIVE` | Pipeline complet — COPY TO Parquet puis count validation |

---

## 8. Composants techniques

### 8.1 Ingestion — POC actif

- Scripts Python 3.12 dédiés par source.
- DuckDB `get_duckdb_connection()` configure S3 via `CREATE SECRET` (region=eu-west-par, url_style=path).
- Extraction incrémentale (date_partition) sur les tables volumineuses.
- Fenêtre de partition : `{cfg.date_partition}` (J-1 par défaut, configurable).

### 8.2 Traitement Silver — POC actif

```python
# Pattern Silver canonique (shared.py)
silver_path = f"s3://{cfg.bucket_silver}/slv_{domain}/{table}/**/*.parquet"
ddb.execute(f"""
    COPY ({sql}) TO '{silver_path}'
    (FORMAT PARQUET, COMPRESSION ZSTD, OVERWRITE_OR_IGNORE TRUE)
""")
count = ddb.execute(f"SELECT COUNT(*) FROM read_parquet('{silver_path}')").fetchone()[0]
```

### 8.3 Stockage — POC actif

| Couche | Technologie | Région | Bucket | Volume |
|---|---|---|---|---|
| Bronze | OVHcloud S3 + JSON | EU-WEST-PAR | `gi-poc-bronze` | 47.87 Go |
| Silver | OVHcloud S3 + Parquet ZSTD | EU-WEST-PAR | `gi-poc-silver` | 459 Mo (growing) |
| Gold | PostgreSQL 18 OVH | EU-WEST-PAR | `gi-gold` (DB) | 240 Go SSD |

### 8.4 Stockage — Architecture cible (Phase 2+)

- **OVHcloud S3** — object storage souverain (SecNumCloud, EU-WEST-PAR).
- **Apache Iceberg** — format de table analytique (Silver + Gold Phase 2+).
- **Apache Polaris** — catalogue Iceberg REST (Phase 2+).
- **PostgreSQL 18** — serving + pgvector (Gold POC + serving étendu Phase 3+).
- **DuckDB** — sidecar analytique léger (Silver + lecture Iceberg Phase 3+).
- **Redis** — optionnel, hot cache (Phase 3+).

### 8.5 Connexion DuckDB — Configuration S3 EU-WEST-PAR

```python
# shared.py — get_duckdb_connection()
ddb.execute("""
    CREATE OR REPLACE SECRET s3_ovh (
        TYPE S3,
        KEY_ID '...',
        SECRET '...',
        REGION 'eu-west-par',
        ENDPOINT 's3.eu-west-par.io.cloud.ovh.net',
        URL_STYLE 'path'
    )
""")
```

---

## 9. Sécurité et conformité

### 9.1 Données personnelles — POC actif

| Donnée | Traitement | Couche |
|---|---|---|
| NIR (numéro de sécu) | SHA-256 + salt (non réversible) | Silver (`silver_interimaires.py`) |
| Coordonnées intérimaires | Stockées Silver uniquement (`slv_interimaires/coordonnees`) — RGPD, jamais en Gold | Silver |
| Données métier clients | Non tokenisées en Bronze (dette technique Phase 1) | Bronze |

**Garde-fous POC** :
- Bucket `gi-poc-bronze` : accès restreint (ingestion write, DuckDB read batch uniquement).
- Chiffrement at-rest OVHcloud S3 natif.
- `slv_interimaires/coordonnees` : jamais exposé en Gold ni Superset.
- PostgreSQL 18 : `sslmode=require` systématique.

### 9.2 Droit à l'oubli (Phase 2+)

Identique à v3.0 — opération en 7 étapes sur Iceberg (logique, physique, snapshots, fichiers orphelins, table de résolution, traçabilité).

---

## 10. Roadmap de mise en œuvre

### Phase POC (en cours — mars 2026)

- [x] Bronze S3 Parquet opérationnel (47.87 Go, 1541 objets)
- [x] Silver S3 Parquet ZSTD opérationnel (459 Mo, 44 fichiers, 2.1M lignes validées)
- [ ] **Corriger 6 scripts Silver bloqués** (`write_silver_iceberg` → COPY TO Parquet)
- [ ] **Provisionner PostgreSQL 18 Business DB1-15 EU-WEST-PAR** (OVH Manager)
- [ ] **Créer schémas Gold** (DDL `ddl_gold_init.sql`)
- [ ] **Exécuter scripts `gold_*.py`** (population Gold PostgreSQL 18)
- [ ] **Connecter Superset OVH Data Platform** → datasource PostgreSQL Gold
- [ ] Valider probe_pg.py (Exit 0)

### Phase 2 — Post-stabilisation POC (Q2-Q3 2026)

- Évaluation OVH Lakehouse Manager (Iceberg + Trino + Superset managés)
- Migration Silver Parquet → Iceberg (remplacement sink `shared.py`)
- Migration Gold PostgreSQL → Iceberg marts (si pertinent)
- Déploiement Kafka + Debezium + Apicurio Registry
- Tokenisation PII applicative (Phase 2) — HMAC-SHA256 + salt sur PII Bronze

### Phase 3 — Serving enrichi (Q4 2026)

- pgvector (recherche sémantique qualifications)
- DuckDB sidecar sur Iceberg REST Catalog
- FastAPI — façade contractuelle versionnée
- K8s Secrets + External Secrets Operator
- Évaluation SQLMesh vs Dagster assets

### Phase 4 — MDM industriel

- `ref.dim_interimaires`, `ref.dim_clients`, `ref.dim_agences`
- Interface de stewardship
- File de conflits + KPI MDM

### Phase 5 — Gouvernance et API avancées

- Contrats API versionnés
- Lightdash évaluation BI
- Audits RGPD complets
- Évaluation Infisical vs Vault

### Phase 6 — Streaming avancé (conditionnel)

- RisingWave / Flink si latence sub-200ms requise
- Détection d'anomalies temps réel

---

## 11. Anti-patterns explicitement interdits

1. **Trino en frontal d'API** — toute requête API passe par PostgreSQL serving.
2. **Silver en PostgreSQL** — couche interne batch → Parquet S3 obligatoire (coût, performance).
3. **Superset connecté à Silver directement** — Superset consomme uniquement Gold PostgreSQL 18.
4. **Région OVH différente de EU-WEST-PAR** — tous les services GI Data sont en EU-WEST-PAR.
5. **DuckDB COPY TO sans COMPRESSION ZSTD** — incohérence avec le standard pipeline.
6. **write_silver_iceberg() utilisé** — fonction dépréciée, remplacée par COPY TO Parquet.
7. **Logique métier dupliquée** sans contrôle entre batch et streaming.
8. **Référentiels critiques disséminés** dans plusieurs couches sans ownership clair.
9. **MDM sans stewardship humain** — Phase 4+.
10. **Secrets dans le code** — tous les secrets dans .env (POC) ou K8s Secrets/ESO (Phase 2+).
11. **Object storage hors EU-WEST-PAR** pour les données GI Data.
12. **dbt Cloud ou tout SaaS US** stockant du code ou des métadonnées.
13. **Observabilité réduite à une liste de métriques** sans architecture OTel.
14. **Communication inter-composants sans mTLS** en production (Phase 3+).

---

## 12. Vue des données par responsabilité

| Couche | Rôle | Format / techno | Région | SLA type |
|---|---|---|---|---|
| Bronze | Trace brute rejouable | OVHcloud S3 JSON | EU-WEST-PAR | Minutes à heures |
| Silver | Standardisation (interne) | S3 Parquet ZSTD (DuckDB) | EU-WEST-PAR | Batch quotidien |
| Gold POC | Serving analytique | PostgreSQL 18 OVH | EU-WEST-PAR | < 200 ms côté Superset |
| Gold Phase 2+ | Vérité analytique | Iceberg (Polaris) + PG 18 | EU-WEST-PAR | J+1 ou intra-journalier |
| MDM (Phase 4+) | Golden records | Iceberg (Polaris) | EU-WEST-PAR | J+0 / J+1 |
| Serving étendu (Phase 3+) | API + pgvector | PostgreSQL 18 + pgvector | EU-WEST-PAR | < 200 ms |
| Sidecar analytique (Phase 3+) | Exploration ad hoc | DuckDB sur Iceberg REST | EU-WEST-PAR | Best effort |
| ML/IA (Phase 3+) | Features, embeddings | DuckDB → Arrow/pandas | EU-WEST-PAR | Best effort |

---

*Document mis à jour le 20/03/2026 — Architecture Data — Groupe Interaction*
*Classification : INTERNE — CONFIDENTIEL*
*Diff v3.4 : décision POC 2026-03-20 — Silver Parquet DuckDB, Gold PostgreSQL 18 EU-WEST-PAR, Lakehouse Manager Phase 2+*
