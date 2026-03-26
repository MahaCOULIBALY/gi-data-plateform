# Politiques RGPD — GI Data Lakehouse

> **Dernière mise à jour** : 2026-03-26
> **Périmètre** : Pipeline ETL gi-data-plateform — couches Bronze / Silver / Gold / Superset

---

## Table des matières

1. [Principes généraux](#1-principes-généraux)
2. [Pseudonymisation du NIR](#2-pseudonymisation-du-nir)
3. [Isolation par couche](#3-isolation-par-couche)
4. [Détail par domaine](#4-détail-par-domaine)
   - [4.1 Domaine Intérimaires](#41-domaine-intérimaires)
   - [4.2 Domaine Clients](#42-domaine-clients)
   - [4.3 Domaine Temps & Relevés d'heures](#43-domaine-temps--relevés-dheures)
5. [Matrice de conformité par champ](#5-matrice-de-conformité-par-champ)
6. [Restrictions Superset (couche consommation)](#6-restrictions-superset-couche-consommation)
7. [Droit à l'oubli](#7-droit-à-loubli)
8. [Audit RGPD automatisé](#8-audit-rgpd-automatisé)
9. [Configuration et secrets](#9-configuration-et-secrets)
10. [Tables RGPD métier Evolia](#10-tables-rgpd-métier-evolia)

---

## 1. Principes généraux

Le pipeline applique une **stratégie de minimisation progressive** des données personnelles au fil des couches :

| Couche | Rôle RGPD | Accès |
|--------|-----------|-------|
| **Bronze** | Données brutes Evolia — aucune transformation RGPD | Restreint (ingestion technique) |
| **Silver** | Pseudonymisation du NIR + isolation des coordonnées/contacts | Restreint (équipe data) |
| **Gold** | Données analytiques épurées — aucune donnée directement identifiante sensible | Analytique (Superset) |
| **Superset** | Masquage RLS sur nom/prénom selon le rôle utilisateur | Métier (selon rôle) |

**Règles fondamentales :**

1. Le NIR brut n'est **jamais écrit en Silver** — il est pseudonymisé à la volée avant écriture.
2. Le NIR pseudonymisé (`nir_pseudo`) n'est **jamais chargé en Gold**.
3. Les coordonnées personnelles (téléphone, email) sont **Silver-only**, jamais exposées en Gold.
4. L'adresse complète (numéro + voie) des intérimaires est **Silver-only** ; seuls `ville` et `code_postal` remontent en Gold.
5. Le `PER_ID` est absent de la table `heures_detail` en Silver — la jointure se fait exclusivement via `PRH_BTS`.

---

## 2. Pseudonymisation du NIR

### Algorithme

- **Méthode** : SHA-256 + salt (hash 256 bits, non réversible)
- **Fonction** : `pseudonymize_nir()` dans [scripts/shared.py](../scripts/shared.py)
- **Salt** : variable d'environnement `RGPD_SALT` (minimum 32 caractères, obligatoire en production)

```python
def pseudonymize_nir(nir: str | None, salt: str) -> str | None:
    """SHA-256 + salt — hash complet (256 bits) pour résistance collision RGPD."""
    if not nir:
        return None
    return hashlib.sha256(f"{nir}{salt}".encode()).hexdigest()
```

### Implémentation

| Script | Implémentation |
|--------|---------------|
| [scripts/silver_interimaires.py](../scripts/silver_interimaires.py) | Pseudonymisation Python via `pseudonymize_nir()` lors du SCD2 |

### Règles de validation du salt

- En mode `LIVE` et `PROBE` : le salt par défaut (`CHANGE_ME_32CHARS_MINIMUM!!!!!!!!`) est **refusé** — le pipeline s'arrête.
- En mode `DRY_RUN` : toléré pour les tests locaux uniquement.

---

## 3. Isolation par couche

### Tables Silver-only (jamais exposées en Gold)

| Table Silver | Contenu | Justification | Script source |
|--------------|---------|---------------|---------------|
| `slv_interimaires/coordonnees` | `coord_id`, `per_id`, `type_coord`, `valeur`, `poste`, `is_principal` | Téléphones et emails des intérimaires | [silver_interimaires_detail.py](../scripts/silver_interimaires_detail.py) |
| `slv_clients/contacts` | `contact_id`, `tie_id`, `nom`, `prenom`, `email`, `telephone`, `fonction_code` | Contacts nominatifs clients | [silver_clients_detail.py](../scripts/silver_clients_detail.py) |

Ces tables sont documentées dans le code avec le commentaire explicite :
> `"""RGPD : Silver-only, jamais exposé en Gold."""`

### Champs exclus de Gold malgré leur présence en Silver

| Champ Silver | Table Silver | Motif d'exclusion | Script Gold |
|--------------|-------------|-------------------|-------------|
| `nir_pseudo` | `dim_interimaires` | NIR même pseudonymisé exclu de Gold | [gold_dimensions.py:107](../scripts/gold_dimensions.py) |
| `date_naissance` | `dim_interimaires` | Donnée personnelle sensible | [gold_dimensions.py:107](../scripts/gold_dimensions.py) |
| `adresse` | `dim_interimaires` | Adresse complète (numéro + voie) | [gold_dimensions.py:107](../scripts/gold_dimensions.py) |
| `pays` | `dim_interimaires` | Donnée personnelle | [gold_dimensions.py:107](../scripts/gold_dimensions.py) |
| `PER_ID` | `heures_detail` | Dissociation identité/activité | [silver_temps.py](../scripts/silver_temps.py) |

---

## 4. Détail par domaine

### 4.1 Domaine Intérimaires

#### Table `dim_interimaires` (SCD Type 2)

| Champ | Bronze | Silver | Gold | Traitement |
|-------|--------|--------|------|------------|
| `PER_NIR` | ✅ Brut | ✅ → `nir_pseudo` (SHA-256+salt) | ❌ | Pseudonymisation obligatoire |
| `PER_TEL_NTEL` | ✅ Brut | ✅ `coordonnees.valeur` uniquement | ❌ | Silver-only |
| `nom`, `prenom` | ✅ | ✅ | ✅ | Masquage RLS Superset (rôle Viewer) |
| `date_naissance` | ✅ | ✅ | ❌ | Exclu Gold |
| `adresse` (numéro + voie) | ✅ | ✅ Partielle (`PER_BISVOIE` + `PER_COMPVOIE`) | ❌ | Exclu Gold |
| `ville` | ✅ | ✅ | ✅ | Autorisé |
| `code_postal` | ✅ | ✅ | ✅ | Autorisé |
| `nationalite`, `pays` | ✅ | ✅ | ❌ | Exclu Gold |

> **Note** : l'adresse Silver est une concaténation partielle (`PER_BISVOIE` + `PER_COMPVOIE` + `PER_CP` + `PER_VILLE`), les champs `PER_NUMVOIE` et `TYPVOIE` du DDL Evolia ne sont pas remontés.

#### Table `coordonnees` (Silver-only)

Contient les téléphones et emails des intérimaires issus de `PYCOORDONNEE`. **Jamais chargée en Gold ni exposée dans Superset.**

#### Table `fidelisation` (Silver-only analytique)

Contient `per_id`, dates de vente, ancienneté. Accessible en Gold uniquement via agrégat anonymisé (`fact_fidelisation`), sans exposition nominative.

#### Flags Bronze (`bronze_interimaires.py`)

Les tables sources sont marquées avec un `rgpd_flag` :

| Table Bronze | Flag | Champ sensible détecté |
|-------------|------|----------------------|
| `PYPERSONNE` | `SENSIBLE` | `PER_NIR` |
| `PYSALARIE` | `PERSONNEL` | `SAL_NIR` |
| `WTPINT` | `PERSONNEL` | — |

Un warning est loggué automatiquement si une colonne listée dans `_RGPD_SENSITIVE = {"PER_NIR", "PER_TEL_NTEL"}` est détectée dans les données reçues.

---

### 4.2 Domaine Clients

#### Table `dim_clients` (SCD Type 2)

Les données clients (entreprises) sont considérées moins sensibles que les données des personnes physiques. L'adresse complète des clients est conservée en Gold pour les analyses géographiques.

| Champ | Silver | Gold | Motif |
|-------|--------|------|-------|
| `raison_sociale`, `siren`, `siret` | ✅ | ✅ | Données d'entreprise |
| `adresse`, `ville`, `code_postal` | ✅ | ✅ | Analyse géographique |
| `naf_code`, `naf_libelle` | ✅ | ✅ | Analytique |

#### Table `contacts` (Silver-only)

Les contacts nominatifs des clients (personnes physiques chez le client) sont **strictement Silver-only**.

| Champ | Silver | Gold | Motif |
|-------|--------|------|-------|
| `nom`, `prenom` | ✅ | ❌ | Donnée personnelle |
| `email` | ✅ | ❌ | Donnée personnelle |
| `telephone` | ✅ | ❌ | Donnée personnelle |
| `fonction_code` | ✅ | ❌ | Contexte contact |

---

### 4.3 Domaine Temps & Relevés d'heures

#### Table `heures_detail` (Silver)

Le `PER_ID` est **volontairement absent** de cette table pour dissocier l'identité de l'intérimaire du détail de ses heures travaillées. La jointure avec l'identité se fait uniquement via `PRH_BTS` → `releves_heures`.

```
slv_temps/heures_detail : prh_bts, rhd_ligne, rubrique, base_paye, taux_paye,
                          base_fact, taux_fact, libelle, _batch_id, _loaded_at
                          (PER_ID absent — RGPD)
```

En Gold, la table `fact_etp_hebdo` agrège les heures par agence et semaine, sans jamais exposer le `per_id` au niveau ligne-détail.

---

## 5. Matrice de conformité par champ

| Donnée | Source Evolia | Bronze | Silver | Gold | Superset | Traitement appliqué |
|--------|--------------|--------|--------|------|----------|---------------------|
| NIR | `PYPERSONNE.PER_NIR` | ✅ Brut | ✅ `nir_pseudo` | ❌ | ❌ | SHA-256 + salt |
| NIR salarié | `PYSALARIE.SAL_NIR` | ✅ Brut | — | ❌ | ❌ | Non remonté Silver |
| Téléphone | `PYCOORDONNEE.PER_TEL_NTEL` | ✅ Brut | ✅ `coordonnees` | ❌ | ❌ | Silver-only |
| Email intérimaire | `PYCOORDONNEE` | ✅ Brut | ✅ `coordonnees` | ❌ | ❌ | Silver-only |
| Nom / Prénom intérimaire | `PYPERSONNE` | ✅ | ✅ | ✅ | ✅ masqué Viewer | RLS Superset |
| Date de naissance | `PYPERSONNE` | ✅ | ✅ | ❌ | ❌ | Exclu Gold |
| Adresse complète (voie) | `PYPERSONNE` | ✅ | ✅ Partielle | ❌ | ❌ | Exclu Gold |
| Ville / Code postal intérimaire | `PYPERSONNE` | ✅ | ✅ | ✅ | ✅ | Autorisé |
| Nationalité / Pays | `PYPERSONNE` | ✅ | ✅ | ❌ | ❌ | Exclu Gold |
| Nom / Prénom contact client | `WTTIEINT` | ✅ | ✅ `contacts` | ❌ | ❌ | Silver-only |
| Email contact client | `WTTIEINT` | ✅ | ✅ `contacts` | ❌ | ❌ | Silver-only |
| Téléphone contact client | `WTTIEINT` | ✅ | ✅ `contacts` | ❌ | ❌ | Silver-only |
| PER_ID dans heures_detail | `WTRHDON` | ✅ | ❌ Absent | ❌ | ❌ | Dissociation identité/activité |
| Adresse client (entreprise) | `WTTIESERV` | ✅ | ✅ | ✅ | ✅ | Autorisé (personne morale) |
| Commentaires évaluation | `WTPEVAL` | ✅ | ✅ `evaluations` | ❌ | ❌ | Silver-only |

---

## 6. Restrictions Superset (couche consommation)

### Masquage par rôle (Row-Level Security)

| Champ | Rôle Direction | Rôle Manager | Rôle Viewer |
|-------|---------------|-------------|------------|
| `nom`, `prenom` (intérimaire) | ✅ Visible | ✅ Visible | 🔐 Masqué |
| `ville`, `code_postal` | ✅ | ✅ | ✅ |
| `date_naissance` | Non exposé | Non exposé | Non exposé |
| Données financières (`ca_`, `marge_`) | ✅ | Selon périmètre | ❌ |

### Datasets exposés et données personnelles

| Dataset Superset | Table Gold | Données personnelles exposées |
|-----------------|-----------|------------------------------|
| Activité Intérimaires | `fact_activite_int` JOIN `dim_interimaires` | `nom`, `prenom` (masqué Viewer) |
| Fidélisation Intérimaires | `fact_fidelisation` JOIN `dim_interimaires` | `nom`, `prenom` (masqué Viewer) |
| Performance Agences | `fact_missions_detail`, `dim_agences` | Aucune (agrégats) |
| CA Mensuel | `fait_ca_mensuel_client` | Aucune (agrégats) |

> **Règle** : aucun dataset Superset n'expose de NIR, téléphone, email, adresse complète ou date de naissance.

---

## 7. Droit à l'oubli

La suppression d'un intérimaire du pipeline est réalisée par `per_id` (le NIR étant irréversiblement pseudonymisé, il ne peut pas être utilisé comme critère de recherche).

**Procédure pour la couche Parquet (actuelle) :**

```sql
-- Supprimer toutes les lignes d'un intérimaire dans Silver
DELETE FROM slv_interimaires/dim_interimaires WHERE per_id = <PER_ID>;
-- Idem pour les tables associées
DELETE FROM slv_interimaires/coordonnees WHERE per_id = <PER_ID>;
DELETE FROM slv_interimaires/fidelisation WHERE per_id = <PER_ID>;
```

**Note :** `slv_interimaires/coordonnees` et `slv_clients/contacts` ne sont jamais chargés dans PostgreSQL — la suppression ne concerne que les fichiers S3.

---

## 8. Audit RGPD automatisé

Le script [scripts/rgpd_audit.py](../scripts/rgpd_audit.py) scanne automatiquement les tables Silver et Gold pour détecter les violations.

### Colonnes interdites en Gold

```python
GOLD_FORBIDDEN_SENSIBLE = {"per_nir", "nir", "iban", "per_iban", "rib"}
GOLD_FORBIDDEN_CONTACT  = {"email", "telephone", "tel", "coord_valeur", "valeur"}
GOLD_FORBIDDEN_EVAL     = {"commentaire", "peval_commentaire"}
```

### Patterns de classification automatique

| Pattern | Classification |
|---------|---------------|
| `nir`, `iban`, `rib`, `secu`, `num_secu` | SENSIBLE 🔴 |
| `nom`, `prenom`, `adresse`, `email`, `telephone`, `date_naissance`, `commentaire` | PERSONNEL 🟡 |
| `ca_`, `marge_`, `montant_`, `coef_`, `taux_` | INTERNE |

### Critères PASS / FAIL

| Vérification | Résultat si violation |
|-------------|----------------------|
| Colonne SENSIBLE présente en Gold | ❌ FAIL — critique |
| NIR brut présent en Silver (hors pseudonymisation) | ❌ FAIL — critique |
| Contact ou commentaire présent en Gold | ❌ FAIL — critique |
| Nom/prénom en Gold sans RLS Superset configuré | ⚠️ WARNING |

**Sortie** : rapport JSON `data/validation/rgpd_audit_report.json`

---

## 9. Configuration et secrets

| Variable d'environnement | Obligatoire | Description |
|--------------------------|-------------|-------------|
| `RGPD_SALT` | ✅ En production | Salt SHA-256 pour pseudonymisation NIR (≥ 32 caractères) |

La valeur sentinelle `CHANGE_ME_32CHARS_MINIMUM!!!!!!!!` est **refusée en mode LIVE et PROBE**. Tout déploiement sans `RGPD_SALT` configuré provoque un arrêt immédiat du pipeline.

Voir [.env.example](../.env.example) et [docs/DEPLOY.md](./DEPLOY.md) pour la procédure de configuration.

---

## 10. Tables RGPD métier Evolia

Evolia dispose de ses propres tables de paramétrage RGPD (non transformées par le pipeline, conservées en Bronze pour référence) :

| Table | Rôle |
|-------|------|
| `PYRGPDPARAM` | Paramétrage des traitements RGPD Paie (code, libellé, durée de conservation) |
| `PYRGPDPERELEM` | Mapping personne (`PER_ID`) ↔ politique RGPD Paie |
| `RHRGPDPARAM` | Paramétrage des traitements RGPD RH |
| `RHRGPDPERELEM` | Mapping personne (`PER_ID`) ↔ politique RGPD RH |

Ces tables sont présentes dans le DDL source ([docs/DDL_EVOLIA_FILTERED.sql](./DDL_EVOLIA_FILTERED.sql)) et pourraient être exploitées pour une gestion automatisée du droit à l'oubli ou des durées de conservation.

---

*Document généré à partir de l'analyse du code source et de [DATA_MAPPING.md](./DATA_MAPPING.md).*
