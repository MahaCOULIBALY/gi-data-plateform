-- ddl_gold_tables.sql — GI Data Lakehouse · PostgreSQL 18 Gold
-- Toutes les tables Gold produites par les scripts Python.
-- Idempotent : CREATE TABLE IF NOT EXISTS partout — safe à ré-exécuter.
-- Ordre : gld_shared → gld_commercial → gld_clients → gld_performance
--                     → gld_operationnel → gld_staffing
--
-- Prérequis : ddl_gold_init.sql doit avoir été exécuté (schémas + grants).
-- Exécuter : psql "postgresql://gi_gold_user:<password>@<host>:20184/gi_data?sslmode=require" \
--            -f ddl_gold_tables.sql

-- ─────────────────────────────────────────────────────────────────────────────
-- gld_shared — Dimensions conformed
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS gld_shared.dim_calendrier (
    date_id         DATE            PRIMARY KEY,
    jour_semaine    SMALLINT        NOT NULL,           -- 0=lundi … 6=dimanche (DuckDB DOW)
    nom_jour        VARCHAR(10)     NOT NULL,
    semaine_iso     SMALLINT        NOT NULL,
    mois            SMALLINT        NOT NULL,
    nom_mois        VARCHAR(12)     NOT NULL,
    trimestre       SMALLINT        NOT NULL,
    annee           SMALLINT        NOT NULL,
    is_jour_ouvre   BOOLEAN         NOT NULL DEFAULT false,
    is_jour_ferie   BOOLEAN         NOT NULL DEFAULT false,
    _loaded_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS gld_shared.dim_agences (
    agence_sk       VARCHAR(64)     PRIMARY KEY,        -- MD5(rgpcnt_id)
    rgpcnt_id       INTEGER         NOT NULL,
    nom_agence      VARCHAR(255),
    marque          VARCHAR(100),
    branche         VARCHAR(100),
    secteur         VARCHAR(100),
    perimetre       VARCHAR(100),
    zone_geo        VARCHAR(100),
    ville           VARCHAR(100),
    is_active       BOOLEAN         NOT NULL DEFAULT true,
    _loaded_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);
CREATE UNIQUE INDEX IF NOT EXISTS dim_agences_rgpcnt_idx ON gld_shared.dim_agences (rgpcnt_id);

CREATE TABLE IF NOT EXISTS gld_shared.dim_clients (
    client_sk           VARCHAR(64)     PRIMARY KEY,    -- MD5(tie_id)
    tie_id              INTEGER         NOT NULL,
    raison_sociale      VARCHAR(255),
    siren               VARCHAR(9),
    nic                 VARCHAR(5),
    siret               VARCHAR(14),
    naf_code            VARCHAR(6),
    naf_libelle         VARCHAR(255),
    ville               VARCHAR(100),
    code_postal         VARCHAR(10),
    statut_client       VARCHAR(50),
    effectif_tranche    VARCHAR(50),
    _loaded_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);
CREATE UNIQUE INDEX IF NOT EXISTS dim_clients_tie_id_idx ON gld_shared.dim_clients (tie_id);

CREATE TABLE IF NOT EXISTS gld_shared.dim_interimaires (
    interimaire_sk          VARCHAR(64)     PRIMARY KEY,    -- MD5(per_id)
    per_id                  INTEGER         NOT NULL,
    matricule               VARCHAR(50),
    nom                     VARCHAR(100),
    prenom                  VARCHAR(100),
    ville                   VARCHAR(100),
    code_postal             VARCHAR(10),
    date_entree             DATE,
    is_actif                BOOLEAN,
    is_candidat             BOOLEAN,
    is_permanent            BOOLEAN,
    agence_rattachement     INTEGER,
    _loaded_at              TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);
CREATE UNIQUE INDEX IF NOT EXISTS dim_interimaires_per_id_idx ON gld_shared.dim_interimaires (per_id);

CREATE TABLE IF NOT EXISTS gld_shared.dim_metiers (
    metier_sk       VARCHAR(64)     PRIMARY KEY,        -- MD5(met_id)
    met_id          INTEGER         NOT NULL,
    code_metier     VARCHAR(50),
    libelle_metier  VARCHAR(255),
    qualification   VARCHAR(100),
    specialite      VARCHAR(100),
    niveau          VARCHAR(50),
    pcs_code        VARCHAR(10),
    _loaded_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);
CREATE UNIQUE INDEX IF NOT EXISTS dim_metiers_met_id_idx ON gld_shared.dim_metiers (met_id);

-- ─────────────────────────────────────────────────────────────────────────────
-- gld_commercial — KPIs commerciaux, CA, marges
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS gld_commercial.fact_ca_mensuel_client (
    client_sk               VARCHAR(64),                -- MD5(tie_id) — surrogate key depuis dim_clients
    tie_id                  INTEGER         NOT NULL,
    mois                    DATE            NOT NULL,
    ca_ht                   DECIMAL(18,2)   NOT NULL DEFAULT 0,
    avoir_ht                DECIMAL(18,2)   NOT NULL DEFAULT 0,
    ca_net_ht               DECIMAL(18,2)   NOT NULL DEFAULT 0,
    nb_factures             INTEGER         NOT NULL DEFAULT 0,
    nb_missions_facturees   INTEGER         NOT NULL DEFAULT 0,
    nb_heures_facturees     DECIMAL(12,2),
    taux_moyen_fact         DECIMAL(10,4),
    agence_principale       INTEGER,
    _loaded_at              TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tie_id, mois)
);

-- ─────────────────────────────────────────────────────────────────────────────
-- gld_clients — Rétention, concentration, vue 360°
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS gld_clients.fact_concentration_client (
    agence_id           INTEGER         NOT NULL,
    mois                DATE            NOT NULL,
    nb_clients          INTEGER         NOT NULL DEFAULT 0,
    nb_clients_top20    INTEGER         NOT NULL DEFAULT 0,
    ca_net_total        DECIMAL(18,2)   NOT NULL DEFAULT 0,
    ca_net_top20        DECIMAL(18,2)   NOT NULL DEFAULT 0,
    taux_concentration  DECIMAL(8,4),
    _loaded_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    PRIMARY KEY (agence_id, mois)
);

-- vue_360_client : version complète (gold_vue360_client.py — 27 colonnes)
CREATE TABLE IF NOT EXISTS gld_clients.vue_360_client (
    client_sk               VARCHAR(64)     NOT NULL,
    tie_id                  INTEGER         NOT NULL,
    raison_sociale          VARCHAR(255),
    siren                   VARCHAR(9),
    ville                   VARCHAR(100),
    secteur_activite        VARCHAR(255),
    effectif                VARCHAR(50),
    statut                  VARCHAR(50),
    ca_ytd                  DECIMAL(18,2)   NOT NULL DEFAULT 0,
    ca_n1                   DECIMAL(18,2)   NOT NULL DEFAULT 0,
    delta_ca_pct            DECIMAL(8,2),
    ca_12_mois_glissants    DECIMAL(18,2)   NOT NULL DEFAULT 0,
    nb_missions_actives     INTEGER         NOT NULL DEFAULT 0,
    nb_missions_total       INTEGER         NOT NULL DEFAULT 0,
    nb_int_actifs           INTEGER         NOT NULL DEFAULT 0,
    nb_int_historique       INTEGER         NOT NULL DEFAULT 0,
    top_3_metiers           TEXT            NOT NULL DEFAULT '[]',
    anciennete_jours        INTEGER         NOT NULL DEFAULT 0,
    marge_moyenne_pct       DECIMAL(10,4)   NOT NULL DEFAULT 0,
    montant_encours         DECIMAL(18,2)   NOT NULL DEFAULT 0,
    limite_credit           DECIMAL(18,2)   NOT NULL DEFAULT 0,
    risque_credit_score     VARCHAR(20)     NOT NULL DEFAULT 'LOW',
    nb_agences_partenaires  INTEGER         NOT NULL DEFAULT 0,
    derniere_facture_date   DATE,
    jours_depuis_derniere   INTEGER         NOT NULL DEFAULT 9999,
    risque_churn            VARCHAR(20)     NOT NULL DEFAULT 'HIGH',
    _computed_at            TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tie_id)
);

-- fact_retention_client : version complète (gold_retention_client.py — 15 colonnes)
CREATE TABLE IF NOT EXISTS gld_clients.fact_retention_client (
    client_sk               VARCHAR(64)     NOT NULL,
    tie_id                  INTEGER         NOT NULL,
    trimestre               DATE            NOT NULL,
    ca_net                  DECIMAL(18,2),
    delta_ca_qoq            DECIMAL(18,2)   NOT NULL DEFAULT 0,
    delta_ca_qoq_pct        DECIMAL(10,4),
    delta_ca_yoy            DECIMAL(18,2)   NOT NULL DEFAULT 0,
    delta_ca_yoy_pct        DECIMAL(10,4),
    nb_missions             INTEGER         NOT NULL DEFAULT 0,
    nb_factures             INTEGER         NOT NULL DEFAULT 0,
    frequence_4_trimestres  INTEGER         NOT NULL DEFAULT 0,
    derniere_facture        DATE,
    jours_inactivite        INTEGER,
    risque_churn            VARCHAR(20),
    churn_score_ml          DECIMAL(10,4),
    _loaded_at              TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tie_id, trimestre)
);

CREATE TABLE IF NOT EXISTS gld_clients.fact_rentabilite_client (
    client_sk               VARCHAR(64),
    tie_id                  INTEGER         NOT NULL,
    annee                   SMALLINT        NOT NULL,
    ca_net                  DECIMAL(18,2),
    ca_missions             DECIMAL(18,2)   NOT NULL DEFAULT 0,
    cout_paye               DECIMAL(18,2)   NOT NULL DEFAULT 0,
    marge_brute             DECIMAL(18,2),
    taux_marge              DECIMAL(10,4)   NOT NULL DEFAULT 0,
    cout_gestion_estime     DECIMAL(18,2),
    rentabilite_nette       DECIMAL(18,2),
    taux_rentabilite_nette  DECIMAL(10,4)   NOT NULL DEFAULT 0,
    nb_interimaires         INTEGER         NOT NULL DEFAULT 0,
    _loaded_at              TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tie_id, annee)
);

-- ─────────────────────────────────────────────────────────────────────────────
-- gld_performance — Scorecard agence, qualité missions, ruptures
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS gld_performance.fact_rupture_contrat (
    agence_id               INTEGER         NOT NULL,
    tie_id                  INTEGER         NOT NULL,
    mois                    DATE            NOT NULL,
    nb_missions_total       INTEGER         NOT NULL DEFAULT 0,
    nb_ruptures             INTEGER         NOT NULL DEFAULT 0,
    nb_annulations          INTEGER         NOT NULL DEFAULT 0,
    nb_terme_normal         INTEGER         NOT NULL DEFAULT 0,
    taux_rupture_pct        DECIMAL(6,1),
    taux_fin_anticipee_pct  DECIMAL(6,1),
    duree_moy_avant_rupture DECIMAL(8,1),
    _loaded_at              TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    PRIMARY KEY (agence_id, tie_id, mois)
);

CREATE TABLE IF NOT EXISTS gld_performance.scorecard_agence (
    agence_id               INTEGER         NOT NULL,
    mois                    DATE            NOT NULL,
    ca_net_ht               DECIMAL(18,2)   NOT NULL DEFAULT 0,
    taux_marge              DECIMAL(10,4)   NOT NULL DEFAULT 0,
    marge_brute             DECIMAL(18,2),
    nb_clients_actifs       INTEGER         NOT NULL DEFAULT 0,
    nb_int_actifs           INTEGER         NOT NULL DEFAULT 0,
    nb_missions             INTEGER         NOT NULL DEFAULT 0,
    taux_transformation     DECIMAL(10,4),
    nb_commandes            INTEGER         NOT NULL DEFAULT 0,
    nb_pourvues             INTEGER         NOT NULL DEFAULT 0,
    _loaded_at              TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    PRIMARY KEY (agence_id, mois)
);

CREATE TABLE IF NOT EXISTS gld_performance.ranking_agences (
    agence_id               INTEGER         NOT NULL,
    mois                    DATE            NOT NULL,
    ca_net_ht               DECIMAL(18,2),
    taux_marge              DECIMAL(10,4),
    nb_int_actifs           INTEGER,
    taux_transformation     DECIMAL(10,4),
    rang_ca                 INTEGER,
    rang_marge              INTEGER,
    rang_placement          INTEGER,
    rang_transfo            INTEGER,
    score_global            DECIMAL(10,4),
    _loaded_at              TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    PRIMARY KEY (agence_id, mois)
);

CREATE TABLE IF NOT EXISTS gld_performance.tendances_agence (
    agence_id               INTEGER         NOT NULL,
    mois                    DATE            NOT NULL,
    ca_net_ht               DECIMAL(18,2),
    taux_marge              DECIMAL(10,4),
    nb_int_actifs           INTEGER,
    delta_ca_mom            DECIMAL(18,2),
    delta_marge_mom         DECIMAL(10,4),
    delta_int_mom           INTEGER,
    delta_ca_yoy            DECIMAL(18,2),
    delta_marge_yoy         DECIMAL(10,4),
    tendance                VARCHAR(10),
    _loaded_at              TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    PRIMARY KEY (agence_id, mois)
);

-- ─────────────────────────────────────────────────────────────────────────────
-- gld_operationnel — Heures hebdo, commandes, ETP, DPAE
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS gld_operationnel.fact_etp_hebdo (
    agence_id       INTEGER         NOT NULL,
    semaine_debut   DATE            NOT NULL,
    nb_releves      INTEGER         NOT NULL DEFAULT 0,
    nb_interimaires INTEGER         NOT NULL DEFAULT 0,
    heures_totales  DECIMAL(12,2)   NOT NULL DEFAULT 0,
    etp             DECIMAL(10,4)   NOT NULL DEFAULT 0,
    _loaded_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    PRIMARY KEY (agence_id, semaine_debut)
);

CREATE TABLE IF NOT EXISTS gld_operationnel.fact_heures_hebdo (
    agence_id       INTEGER         NOT NULL,
    tie_id          INTEGER         NOT NULL,
    semaine_debut   DATE            NOT NULL,
    heures_paye     DECIMAL(12,2)   NOT NULL DEFAULT 0,
    heures_fact     DECIMAL(12,2)   NOT NULL DEFAULT 0,
    nb_releves      INTEGER         NOT NULL DEFAULT 0,
    _loaded_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    PRIMARY KEY (agence_id, tie_id, semaine_debut)
);

CREATE TABLE IF NOT EXISTS gld_operationnel.fact_commandes_pipeline (
    agence_id           INTEGER         NOT NULL,
    semaine_debut       DATE            NOT NULL,
    nb_commandes        INTEGER         NOT NULL DEFAULT 0,
    nb_pourvues         INTEGER         NOT NULL DEFAULT 0,
    nb_ouvertes         INTEGER         NOT NULL DEFAULT 0,
    taux_satisfaction   DECIMAL(10,4)   NOT NULL DEFAULT 0,
    _loaded_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    PRIMARY KEY (agence_id, semaine_debut)
);

CREATE TABLE IF NOT EXISTS gld_operationnel.fact_delai_placement (
    agence_id               INTEGER         NOT NULL,
    semaine_debut           DATE            NOT NULL,
    categorie_delai         VARCHAR(50)     NOT NULL,
    nb_missions             INTEGER         NOT NULL DEFAULT 0,
    delai_moyen_heures      DECIMAL(10,2),
    delai_median_heures     DECIMAL(10,2),
    _loaded_at              TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    PRIMARY KEY (agence_id, semaine_debut, categorie_delai)
);

CREATE TABLE IF NOT EXISTS gld_operationnel.fact_echus_hebdo (
    tie_id              INTEGER         NOT NULL,
    agence_id           INTEGER         NOT NULL,
    semaine_debut       DATE            NOT NULL,
    montant_factures    DECIMAL(18,2)   NOT NULL DEFAULT 0,
    montant_avoirs      DECIMAL(18,2)   NOT NULL DEFAULT 0,
    montant_echu_ht     DECIMAL(18,2)   NOT NULL DEFAULT 0,
    nb_factures         INTEGER         NOT NULL DEFAULT 0,
    _loaded_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tie_id, agence_id, semaine_debut)
);
CREATE INDEX IF NOT EXISTS fact_echus_hebdo_tie_id_idx ON gld_operationnel.fact_echus_hebdo (tie_id);

CREATE TABLE IF NOT EXISTS gld_operationnel.fact_conformite_dpae (
    agence_id               INTEGER         NOT NULL,
    mois                    DATE            NOT NULL,
    nb_missions             INTEGER         NOT NULL DEFAULT 0,
    nb_dpae_transmises      INTEGER         NOT NULL DEFAULT 0,
    nb_dpae_manquantes      INTEGER         NOT NULL DEFAULT 0,
    taux_conformite_dpae    DECIMAL(10,4),
    ecart_moyen_heures      DECIMAL(10,2),
    _loaded_at              TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    PRIMARY KEY (agence_id, mois)
);

-- ─────────────────────────────────────────────────────────────────────────────
-- gld_staffing — Missions, intérimaires, fidélisation, compétences
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS gld_staffing.fact_competences_dispo (
    metier_sk       VARCHAR(64)     NOT NULL,
    agence_sk       VARCHAR(64)     NOT NULL,
    met_id          INTEGER         NOT NULL,
    rgpcnt_id       INTEGER         NOT NULL,
    nb_qualifies    INTEGER         NOT NULL DEFAULT 0,
    nb_disponibles  INTEGER         NOT NULL DEFAULT 0,
    nb_en_mission   INTEGER         NOT NULL DEFAULT 0,
    taux_couverture DECIMAL(10,4)   NOT NULL DEFAULT 0,
    _computed_at    TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    PRIMARY KEY (met_id, rgpcnt_id)
);

CREATE TABLE IF NOT EXISTS gld_staffing.fact_activite_int (
    interimaire_sk      VARCHAR(64),
    per_id              INTEGER         NOT NULL,
    mois                DATE            NOT NULL,
    nb_missions         INTEGER         NOT NULL DEFAULT 0,
    nb_agences          INTEGER         NOT NULL DEFAULT 0,
    nb_clients          INTEGER         NOT NULL DEFAULT 0,
    heures_travaillees  DECIMAL(12,2)   NOT NULL DEFAULT 0,
    heures_disponibles  DECIMAL(12,2)   NOT NULL DEFAULT 0,
    taux_occupation     DECIMAL(10,4)   NOT NULL DEFAULT 0,
    ca_genere           DECIMAL(18,2)   NOT NULL DEFAULT 0,
    _loaded_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    PRIMARY KEY (per_id, mois)
);

CREATE TABLE IF NOT EXISTS gld_staffing.fact_missions_detail (
    mission_sk          VARCHAR(64)     PRIMARY KEY,    -- MD5(per_id|cnt_id|tie_id)
    per_id              INTEGER         NOT NULL,
    cnt_id              INTEGER         NOT NULL,
    tie_id              INTEGER         NOT NULL,
    agence_id           INTEGER         NOT NULL,
    metier_id           INTEGER,
    date_debut          DATE,
    date_fin            DATE,
    duree_jours         INTEGER,
    taux_horaire_paye   DECIMAL(10,4),
    taux_horaire_fact   DECIMAL(10,4),
    marge_horaire       DECIMAL(10,4),
    heures_totales      DECIMAL(12,2)   NOT NULL DEFAULT 0,
    ca_mission          DECIMAL(18,2)   NOT NULL DEFAULT 0,
    cout_mission        DECIMAL(18,2)   NOT NULL DEFAULT 0,
    marge_mission       DECIMAL(18,2)   NOT NULL DEFAULT 0,
    taux_marge          DECIMAL(10,4)   NOT NULL DEFAULT 0,
    _loaded_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS fact_missions_detail_tie_id_idx  ON gld_staffing.fact_missions_detail (tie_id);
CREATE INDEX IF NOT EXISTS fact_missions_detail_per_id_idx  ON gld_staffing.fact_missions_detail (per_id);
CREATE INDEX IF NOT EXISTS fact_missions_detail_date_fin_idx ON gld_staffing.fact_missions_detail (date_fin);

CREATE TABLE IF NOT EXISTS gld_staffing.fact_fidelisation_interimaires (
    agence_id               INTEGER         NOT NULL,
    categorie_fidelisation  VARCHAR(50)     NOT NULL,
    nb_interimaires         INTEGER         NOT NULL DEFAULT 0,
    anciennete_moy_jours    DECIMAL(10,1),
    jours_inactivite_moyen  DECIMAL(10,1),
    _loaded_at              TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    PRIMARY KEY (agence_id, categorie_fidelisation)
);

-- ─────────────────────────────────────────────────────────────────────────────
-- ops — Watermarks pipeline (schema existant, table requise par purge_bronze.py)
-- ─────────────────────────────────────────────────────────────────────────────
CREATE SCHEMA IF NOT EXISTS ops;
GRANT ALL ON SCHEMA ops TO gi_gold_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA ops GRANT ALL ON TABLES TO gi_gold_user;

-- ─────────────────────────────────────────────────────────────────────────────
-- Migrations — colonnes ajoutées après création initiale (idempotentes)
-- ─────────────────────────────────────────────────────────────────────────────

-- vue_360_client : ajout adresse complète (2026-03-26)
ALTER TABLE gld_clients.vue_360_client
    ADD COLUMN IF NOT EXISTS code_postal      VARCHAR(10),
    ADD COLUMN IF NOT EXISTS adresse_complete TEXT;

-- ─────────────────────────────────────────────────────────────────────────────
-- Phase 1 KPI completion (2026-03-26)
-- ─────────────────────────────────────────────────────────────────────────────

-- fact_delai_placement : refonte grain + distribution opérationnelle
-- Raison : retrait categorie_delai (dimension) remplacée par KPIs % actionnables.
-- La PK passe de (agence_id, semaine_debut, categorie_delai) à (agence_id, semaine_debut).
-- DROP + CREATE nécessaire car modification de la clé primaire.
DROP TABLE IF EXISTS gld_operationnel.fact_delai_placement;
CREATE TABLE gld_operationnel.fact_delai_placement (
    agence_id               INTEGER         NOT NULL,
    semaine_debut           DATE            NOT NULL,
    nb_missions             INTEGER         NOT NULL DEFAULT 0,
    delai_moyen_heures      DECIMAL(10,2),
    delai_median_heures     DECIMAL(10,2),
    pct_moins_4h            DECIMAL(6,1),   -- % placements en < 4h  (réactivité excellente)
    pct_meme_jour           DECIMAL(6,1),   -- % placements en ≤ 24h (même journée)
    pct_plus_3j             DECIMAL(6,1),   -- % placements > 72h    (alerte opérationnelle)
    _loaded_at              TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    PRIMARY KEY (agence_id, semaine_debut)
);

-- fact_conformite_dpae : ajout colonnes conformes/retard + correction taux (2026-03-26)
-- Raison : taux_conformite_dpae était calculé sur nb_transmises (erroné — inclut les retards).
-- Correction : taux = nb_dpae_conformes (ecart_heures <= 0) / nb_missions.
ALTER TABLE gld_operationnel.fact_conformite_dpae
    ADD COLUMN IF NOT EXISTS nb_dpae_conformes  INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS nb_dpae_retard     INTEGER NOT NULL DEFAULT 0;

CREATE TABLE IF NOT EXISTS ops.pipeline_watermarks (
    pipeline        VARCHAR(100)    NOT NULL,
    table_name      VARCHAR(200)    NOT NULL,
    last_success    TIMESTAMPTZ,
    last_run        TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    rows_loaded     BIGINT,
    _updated_at     TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    PRIMARY KEY (pipeline, table_name)
);
