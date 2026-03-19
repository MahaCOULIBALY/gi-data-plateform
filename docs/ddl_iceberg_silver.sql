-- ddl_iceberg_silver.sql — DDL Trino pour toutes les tables Silver Iceberg
-- GI Data Platform · Silver layer — 23 tables
-- Généré le 2026-03-19
--
-- Usage :
--   trino --server https://<LAKEHOUSE_ENDPOINT>:443 \
--         --user gi-lakehouse-svc --password \
--         --catalog silver \
--         --file docs/ddl_iceberg_silver.sql
--
-- Prérequis : les schémas doivent exister (CREATE SCHEMA ci-dessous).
-- Toutes les instructions sont idempotentes (IF NOT EXISTS).
--
-- Warehouse S3 : s3a://gi-poc-silver/iceberg/  (BUCKET_ICEBERG)
-- Adapter la valeur de location si le bucket change (ex: gi-silver).
-- ─────────────────────────────────────────────────────────────────────────────

-- ══ Schémas ══════════════════════════════════════════════════════════════════

CREATE SCHEMA IF NOT EXISTS silver.missions;
CREATE SCHEMA IF NOT EXISTS silver.temps;
CREATE SCHEMA IF NOT EXISTS silver.facturation;
CREATE SCHEMA IF NOT EXISTS silver.interimaires;
CREATE SCHEMA IF NOT EXISTS silver.clients;
CREATE SCHEMA IF NOT EXISTS silver.agences;

-- ══ MISSIONS (7 tables) ══════════════════════════════════════════════════════

-- silver.missions.missions ← WTMISS + WTCNTI (agg) + WTCMD (~PK : per_id + cnt_id)
CREATE TABLE IF NOT EXISTS silver.missions.missions (
    per_id                      INTEGER,
    cnt_id                      INTEGER,
    tie_id                      INTEGER,
    ties_serv                   INTEGER,
    rgpcnt_id                   INTEGER,
    date_debut                  DATE,
    date_fin                    DATE,
    motif                       VARCHAR,
    code_fin                    VARCHAR,
    prh_bts                     INTEGER,
    statut_dpae                 TIMESTAMP(6) WITH TIME ZONE,
    ecart_heures                BIGINT,
    delai_placement_heures      BIGINT,
    categorie_delai             VARCHAR,
    _batch_id                   VARCHAR,
    _loaded_at                  TIMESTAMP(6) WITH TIME ZONE
)
WITH (
    format       = 'PARQUET',
    location     = 's3a://gi-poc-silver/iceberg/missions/missions/',
    partitioning = ARRAY['month(date_debut)']
);

-- silver.missions.contrats ← WTCNTI (~PK : per_id + cnt_id + ordre)
CREATE TABLE IF NOT EXISTS silver.missions.contrats (
    per_id      INTEGER,
    cnt_id      INTEGER,
    ordre       INTEGER,
    met_id      INTEGER,
    tpci_code   VARCHAR,
    date_debut  DATE,
    date_fin    DATE,
    taux_paye   DECIMAL(10, 4),
    taux_fact   DECIMAL(10, 4),
    nb_heures   DECIMAL(10, 2),
    poste       VARCHAR,
    _batch_id   VARCHAR,
    _loaded_at  TIMESTAMP(6) WITH TIME ZONE
)
WITH (
    format       = 'PARQUET',
    location     = 's3a://gi-poc-silver/iceberg/missions/contrats/',
    partitioning = ARRAY['month(date_debut)']
);

-- silver.missions.commandes ← WTCMD (~PK : cmd_id)
CREATE TABLE IF NOT EXISTS silver.missions.commandes (
    cmd_id      INTEGER,
    rgpcnt_id   INTEGER,
    cmd_date    DATE,
    nb_sal      INTEGER,
    stat_code   VARCHAR,
    stat_type   VARCHAR,
    _batch_id   VARCHAR,
    _loaded_at  TIMESTAMP(6) WITH TIME ZONE
)
WITH (
    format       = 'PARQUET',
    location     = 's3a://gi-poc-silver/iceberg/missions/commandes/',
    partitioning = ARRAY['month(cmd_date)']
);

-- silver.missions.placements ← WTPLAC (~PK : plac_id)
CREATE TABLE IF NOT EXISTS silver.missions.placements (
    plac_id     INTEGER,
    rgpcnt_id   INTEGER,
    tie_id      INTEGER,
    met_id      INTEGER,
    statut      VARCHAR,
    plac_date   DATE,
    _batch_id   VARCHAR,
    _loaded_at  TIMESTAMP(6) WITH TIME ZONE
)
WITH (
    format       = 'PARQUET',
    location     = 's3a://gi-poc-silver/iceberg/missions/placements/',
    partitioning = ARRAY['month(plac_date)']
);

-- silver.missions.fin_mission ← WTMISS + WTCNTI + WTFINMISS + PYMTFCNT (~PK : per_id + cnt_id)
CREATE TABLE IF NOT EXISTS silver.missions.fin_mission (
    per_id                INTEGER,
    cnt_id                INTEGER,
    rgpcnt_id             INTEGER,
    tie_id                INTEGER,
    date_debut            DATE,
    date_fin_reelle       DATE,
    date_fin_saisie       DATE,
    finmiss_code          VARCHAR,
    finmiss_libelle       VARCHAR,
    mtfcnt_code           VARCHAR,
    mtfcnt_libelle        VARCHAR,
    mtfcnt_fincnt         SMALLINT,
    miss_annule           SMALLINT,
    duree_reelle_jours    INTEGER,
    statut_fin_mission    VARCHAR,
    _batch_id             VARCHAR,
    _loaded_at            TIMESTAMP(6) WITH TIME ZONE
)
WITH (
    format       = 'PARQUET',
    location     = 's3a://gi-poc-silver/iceberg/missions/fin_mission/',
    partitioning = ARRAY['month(date_debut)']
);

-- silver.missions.contrats_paie ← PYCONTRAT (~PK : per_id + cnt_id)
CREATE TABLE IF NOT EXISTS silver.missions.contrats_paie (
    per_id              INTEGER,
    cnt_id              INTEGER,
    eta_id              INTEGER,
    rgpcnt_id           INTEGER,
    date_debut          DATE,
    date_fin            DATE,
    date_fin_prevue     DATE,
    lot_paye_code       VARCHAR,
    typ_cotisation_code VARCHAR,
    avt_ordre           INTEGER,
    ini_ordre           INTEGER,
    _batch_id           VARCHAR,
    _loaded_at          TIMESTAMP(6) WITH TIME ZONE
)
WITH (
    format       = 'PARQUET',
    location     = 's3a://gi-poc-silver/iceberg/missions/contrats_paie/',
    partitioning = ARRAY['month(date_debut)']
);

-- silver.missions.facinfo ← WTFACINFO — table de liaison factures ↔ missions
-- (~PK : cnt_id + fac_num) — full-load, FACINFO_DATEMODIF absent DDL
-- Utilisée dans gold_ca_mensuel pour nb_missions_fac par facture
CREATE TABLE IF NOT EXISTS silver.missions.facinfo (
    cnt_id      INTEGER,
    fac_num     VARCHAR,
    per_id      INTEGER,
    tie_id      INTEGER,
    _batch_id   VARCHAR,
    _loaded_at  TIMESTAMP(6) WITH TIME ZONE
)
WITH (
    format   = 'PARQUET',
    location = 's3a://gi-poc-silver/iceberg/missions/facinfo/'
    -- pas de partition : table petite + full-load
);

-- ══ TEMPS (2 tables) ═════════════════════════════════════════════════════════

-- silver.temps.releves_heures ← WTPRH (~PK : prh_bts)
-- rgpcnt_id/periode absents DDL Evolia → NULL
CREATE TABLE IF NOT EXISTS silver.temps.releves_heures (
    prh_bts     INTEGER,
    per_id      INTEGER,
    cnt_id      INTEGER,
    tie_id      INTEGER,
    rgpcnt_id   INTEGER,
    periode     VARCHAR,
    date_modif  TIMESTAMP(6) WITH TIME ZONE,
    valide      BOOLEAN,
    _batch_id   VARCHAR,
    _loaded_at  TIMESTAMP(6) WITH TIME ZONE
)
WITH (
    format       = 'PARQUET',
    location     = 's3a://gi-poc-silver/iceberg/temps/releves_heures/',
    partitioning = ARRAY['month(date_modif)']
);

-- silver.temps.heures_detail ← WTRHDON (~PK : prh_bts + rhd_ligne) — 760K+/j
-- RGPD : per_id absent — jointure via prh_bts ↔ releves_heures
CREATE TABLE IF NOT EXISTS silver.temps.heures_detail (
    prh_bts     INTEGER,
    rhd_ligne   INTEGER,
    rubrique    VARCHAR,
    base_paye   DECIMAL(10, 2),
    taux_paye   DECIMAL(10, 4),
    base_fact   DECIMAL(10, 2),
    taux_fact   DECIMAL(10, 4),
    libelle     VARCHAR,
    rhd_dated   DATE,
    _batch_id   VARCHAR,
    _loaded_at  TIMESTAMP(6) WITH TIME ZONE
)
WITH (
    format       = 'PARQUET',
    location     = 's3a://gi-poc-silver/iceberg/temps/heures_detail/',
    partitioning = ARRAY['month(rhd_dated)']
);

-- ══ FACTURATION (2 tables) ═══════════════════════════════════════════════════

-- silver.facturation.factures ← WTEFAC (~PK : efac_num)
-- montant_ht / montant_ttc / prh_bts absents DDL → NULL
CREATE TABLE IF NOT EXISTS silver.facturation.factures (
    efac_num        VARCHAR,
    rgpcnt_id       INTEGER,
    tie_id          INTEGER,
    ties_serv       INTEGER,
    type_facture    VARCHAR,
    date_facture    DATE,
    date_echeance   DATE,
    montant_ht      DECIMAL(18, 2),
    montant_ttc     DECIMAL(18, 2),
    taux_tva        DECIMAL(6, 4),
    prh_bts         INTEGER,
    _batch_id       VARCHAR,
    _loaded_at      TIMESTAMP(6) WITH TIME ZONE
)
WITH (
    format       = 'PARQUET',
    location     = 's3a://gi-poc-silver/iceberg/facturation/factures/',
    partitioning = ARRAY['month(date_facture)']
);

-- silver.facturation.lignes_factures ← WTLFAC JOIN WTEFAC (~PK : fac_num + lfac_ord)
-- 26M lignes total — filtré horizon 2024-01-01 via JOIN WTEFAC
-- montant = LFAC_MNT (utilisé dans gold via SUM(montant))
-- rubrique absent DDL → NULL
CREATE TABLE IF NOT EXISTS silver.facturation.lignes_factures (
    fac_num     VARCHAR,
    lfac_ord    INTEGER,
    libelle     VARCHAR,
    base        DECIMAL(10, 2),
    taux        DECIMAL(10, 4),
    montant     DECIMAL(18, 2),
    rubrique    VARCHAR,
    efac_dteedi DATE,
    _batch_id   VARCHAR,
    _loaded_at  TIMESTAMP(6) WITH TIME ZONE
)
WITH (
    format       = 'PARQUET',
    location     = 's3a://gi-poc-silver/iceberg/facturation/lignes_factures/',
    partitioning = ARRAY['month(efac_dteedi)']
);

-- ══ INTÉRIMAIRES (6 tables) ══════════════════════════════════════════════════

-- silver.interimaires.dim_interimaires ← PYPERSONNE × PYSALARIE × WTPINT (~PK : per_id)
-- SCD Type 2 — nir_pseudo = SHA-256(NIR || RGPD_SALT)
-- agence_rattachement / date_sortie absents DDL → NULL
CREATE TABLE IF NOT EXISTS silver.interimaires.dim_interimaires (
    interimaire_sk      VARCHAR,
    per_id              INTEGER,
    change_hash         VARCHAR,
    matricule           VARCHAR,
    nom                 VARCHAR,
    prenom              VARCHAR,
    date_naissance      DATE,
    nir_pseudo          VARCHAR,
    nationalite         VARCHAR,
    pays                VARCHAR,
    adresse             VARCHAR,
    ville               VARCHAR,
    code_postal         VARCHAR,
    date_entree         DATE,
    date_sortie         DATE,
    is_actif            BOOLEAN,
    is_candidat         BOOLEAN,
    is_permanent        BOOLEAN,
    agence_rattachement INTEGER,
    is_current          BOOLEAN,
    valid_from          TIMESTAMP(6) WITH TIME ZONE,
    valid_to            TIMESTAMP(6) WITH TIME ZONE,
    _source_raw_id      VARCHAR,
    _loaded_at          TIMESTAMP(6) WITH TIME ZONE
)
WITH (
    format   = 'PARQUET',
    location = 's3a://gi-poc-silver/iceberg/interimaires/dim_interimaires/'
);

-- silver.interimaires.evaluations ← WTPEVAL (~PK : per_id + date_eval)
-- PEVAL_EVALUATEUR / commentaire absents DDL → NULL
CREATE TABLE IF NOT EXISTS silver.interimaires.evaluations (
    eval_id         VARCHAR,
    per_id          INTEGER,
    date_eval       DATE,
    note            DECIMAL(5, 2),
    commentaire     VARCHAR,
    evaluateur_id   INTEGER,
    _loaded_at      TIMESTAMP(6) WITH TIME ZONE
)
WITH (
    format       = 'PARQUET',
    location     = 's3a://gi-poc-silver/iceberg/interimaires/evaluations/',
    partitioning = ARRAY['year(date_eval)']
);

-- silver.interimaires.coordonnees ← PYCOORDONNEE (~PK : per_id + type_coord)
-- RGPD : Silver-only — jamais exposé en Gold
-- COORD_PRINC absent DDL → is_principal hardcodé false
CREATE TABLE IF NOT EXISTS silver.interimaires.coordonnees (
    coord_id    VARCHAR,
    per_id      INTEGER,
    type_coord  VARCHAR,
    valeur      VARCHAR,
    poste       VARCHAR,
    is_principal BOOLEAN,
    _loaded_at  TIMESTAMP(6) WITH TIME ZONE
)
WITH (
    format   = 'PARQUET',
    location = 's3a://gi-poc-silver/iceberg/interimaires/coordonnees/'
);

-- silver.interimaires.portefeuille_agences ← WTUGPINT (~PK : per_id + rgpcnt_id)
-- UGPINT_DATEMODIF absent DDL → full-load
CREATE TABLE IF NOT EXISTS silver.interimaires.portefeuille_agences (
    per_id      INTEGER,
    rgpcnt_id   INTEGER,
    _loaded_at  TIMESTAMP(6) WITH TIME ZONE
)
WITH (
    format   = 'PARQUET',
    location = 's3a://gi-poc-silver/iceberg/interimaires/portefeuille_agences/'
);

-- silver.interimaires.fidelisation ← WTPINT (~PK : per_id)
-- NOUVELLE TABLE — pas de source Parquet Silver à migrer
-- Catégories : actif_recent (≤90j) / actif_annee (≤365j) / inactif_long (>365j) / inactif (jamais)
CREATE TABLE IF NOT EXISTS silver.interimaires.fidelisation (
    per_id                      INTEGER,
    date_premiere_vente         DATE,
    date_avant_derniere_vente   DATE,
    date_derniere_vente         DATE,
    anciennete_jours            INTEGER,
    jours_depuis_derniere_vente INTEGER,
    categorie_fidelisation      VARCHAR,
    _loaded_at                  TIMESTAMP(6) WITH TIME ZONE
)
WITH (
    format   = 'PARQUET',
    location = 's3a://gi-poc-silver/iceberg/interimaires/fidelisation/'
);

-- silver.interimaires.competences ← UNION ALL WTPMET/WTPHAB/WTPDIP/WTEXP (~PK : competence_id)
-- full-scan **/*.json (tables d'enrichissement sans date_partition)
CREATE TABLE IF NOT EXISTS silver.interimaires.competences (
    competence_id   VARCHAR,
    per_id          INTEGER,
    type_competence VARCHAR,
    code            VARCHAR,
    libelle         VARCHAR,
    niveau          VARCHAR,
    date_obtention  DATE,
    date_expiration DATE,
    is_active       BOOLEAN,
    pcs_code        VARCHAR,
    _source_table   VARCHAR,
    _loaded_at      TIMESTAMP(6) WITH TIME ZONE
)
WITH (
    format   = 'PARQUET',
    location = 's3a://gi-poc-silver/iceberg/interimaires/competences/'
);

-- ══ CLIENTS (4 tables) ═══════════════════════════════════════════════════════

-- silver.clients.dim_clients ← WTTIESERV × WTCLPT (~PK : tie_id) — SCD Type 2
CREATE TABLE IF NOT EXISTS silver.clients.dim_clients (
    client_sk           VARCHAR,
    tie_id              INTEGER,
    change_hash         VARCHAR,
    raison_sociale      VARCHAR,
    siren               VARCHAR,
    nic                 VARCHAR,
    naf_code            VARCHAR,
    adresse_complete    VARCHAR,
    ville               VARCHAR,
    code_postal         VARCHAR,
    statut_client       VARCHAR,
    ca_potentiel        DECIMAL(18, 2),
    date_creation_fiche DATE,
    is_current          BOOLEAN,
    valid_from          TIMESTAMP(6) WITH TIME ZONE,
    valid_to            TIMESTAMP(6) WITH TIME ZONE,
    _source_raw_id      VARCHAR,
    _loaded_at          TIMESTAMP(6) WITH TIME ZONE
)
WITH (
    format   = 'PARQUET',
    location = 's3a://gi-poc-silver/iceberg/clients/dim_clients/'
);

-- silver.clients.sites_mission ← WTTIESERV (~PK : site_id + tie_id)
-- siret_site absent DDL → NULL ; is_active = CLOT_DAT IS NULL
CREATE TABLE IF NOT EXISTS silver.clients.sites_mission (
    site_id     INTEGER,
    tie_id      INTEGER,
    nom_site    VARCHAR,
    siren       VARCHAR,
    nic         VARCHAR,
    adresse     VARCHAR,
    ville       VARCHAR,
    code_postal VARCHAR,
    pays_code   VARCHAR,
    siret_site  VARCHAR,
    email       VARCHAR,
    agence_id   INTEGER,
    is_active   BOOLEAN,
    clot_at     DATE,
    row_hash    VARCHAR,
    _loaded_at  TIMESTAMP(6) WITH TIME ZONE
)
WITH (
    format   = 'PARQUET',
    location = 's3a://gi-poc-silver/iceberg/clients/sites_mission/'
);

-- silver.clients.contacts ← WTTIEINT (~PK : contact_id)
-- RGPD : Silver-only — jamais exposé en Gold
CREATE TABLE IF NOT EXISTS silver.clients.contacts (
    contact_id      VARCHAR,
    tie_id          INTEGER,
    nom             VARCHAR,
    prenom          VARCHAR,
    email           VARCHAR,
    telephone       VARCHAR,
    fonction_code   VARCHAR,
    _loaded_at      TIMESTAMP(6) WITH TIME ZONE
)
WITH (
    format   = 'PARQUET',
    location = 's3a://gi-poc-silver/iceberg/clients/contacts/'
);

-- silver.clients.encours_credit ← WTENCOURSG (~PK : encours_id)
-- montant_encours / limite_credit / date_decision absents DDL → NULL
CREATE TABLE IF NOT EXISTS silver.clients.encours_credit (
    encours_id          INTEGER,
    siren               VARCHAR,
    montant_encours     DECIMAL(18, 2),
    limite_credit       DECIMAL(18, 2),
    date_decision       DATE,
    decision_libelle    VARCHAR,
    _loaded_at          TIMESTAMP(6) WITH TIME ZONE
)
WITH (
    format   = 'PARQUET',
    location = 's3a://gi-poc-silver/iceberg/clients/encours_credit/'
);

-- ══ AGENCES (2 tables) ═══════════════════════════════════════════════════════

-- silver.agences.dim_agences ← PYREGROUPECNT × [agence_gestion] (~PK : rgpcnt_id)
-- ville / email / latitude / longitude absents DDL Evolia → NULL
CREATE TABLE IF NOT EXISTS silver.agences.dim_agences (
    agence_sk       VARCHAR,
    rgpcnt_id       INTEGER,
    nom             VARCHAR,
    code            VARCHAR,
    marque          VARCHAR,
    branche         VARCHAR,
    nom_commercial  VARCHAR,
    code_comm       VARCHAR,
    ville           VARCHAR,
    email           VARCHAR,
    latitude        DOUBLE,
    longitude       DOUBLE,
    is_cloture      BOOLEAN,
    is_active       BOOLEAN,
    cloture_date    DATE,
    _loaded_at      TIMESTAMP(6) WITH TIME ZONE
)
WITH (
    format   = 'PARQUET',
    location = 's3a://gi-poc-silver/iceberg/agences/dim_agences/'
);

-- silver.agences.hierarchie_territoriale ← [agence_gestion] × [secteurs]
-- Pont texte NOM_UG ↔ [Agence de gestion] — optionnel (absent si Bronze absent)
CREATE TABLE IF NOT EXISTS silver.agences.hierarchie_territoriale (
    rgpcnt_id   INTEGER,
    secteur     VARCHAR,
    perimetre   VARCHAR,
    zone_geo    VARCHAR,
    _loaded_at  TIMESTAMP(6) WITH TIME ZONE
)
WITH (
    format   = 'PARQUET',
    location = 's3a://gi-poc-silver/iceberg/agences/hierarchie_territoriale/'
);

-- ══ FIN DU SCRIPT ════════════════════════════════════════════════════════════
-- 23 tables créées (6 schémas × n tables)
-- Tables nouvelles (pas de source Parquet Silver à migrer via migrate_silver_to_iceberg.py) :
--   silver.missions.facinfo          ← WTFACINFO (ajouté 2026-03-19)
--   silver.interimaires.fidelisation ← WTPINT    (ajouté 2026-03-19)
