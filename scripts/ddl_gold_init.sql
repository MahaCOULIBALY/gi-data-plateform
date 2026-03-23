-- ddl_gold_init.sql — GI Data Lakehouse · PostgreSQL 18 Gold (EU-WEST-PAR)
-- Idempotent : IF NOT EXISTS partout — safe à ré-exécuter.
-- Exécuter en tant que superuser ou rôle propriétaire de la base gi_data.
-- Étapes : psql "postgresql://gi_gold_user:<password>@<host>:20184/gi_data?sslmode=require" -f ddl_gold_init.sql

-- ── Extensions ────────────────────────────────────────────────────────────────
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- ── Schémas métier ────────────────────────────────────────────────────────────
CREATE SCHEMA IF NOT EXISTS gld_commercial;     -- KPIs commerciaux, CA, marges
CREATE SCHEMA IF NOT EXISTS gld_staffing;       -- Missions, taux placement, ETP
CREATE SCHEMA IF NOT EXISTS gld_facturation;    -- Factures, recouvrement
CREATE SCHEMA IF NOT EXISTS gld_rh;             -- Intérimaires, fidélisation, compétences

-- BUG-1 : schémas manquants ajoutés (utilisés par les scripts Gold)
CREATE SCHEMA IF NOT EXISTS gld_clients;        -- Rétention, vue360, concentration
CREATE SCHEMA IF NOT EXISTS gld_performance;    -- Scorecard agence, qualité, ruptures
CREATE SCHEMA IF NOT EXISTS gld_operationnel;   -- Heures hebdo, commandes, ETP, DPAE
CREATE SCHEMA IF NOT EXISTS gld_shared;         -- Dimensions conformed (dim_agences, dim_clients…)

-- ── Schéma infrastructure pipeline ───────────────────────────────────────────
CREATE SCHEMA IF NOT EXISTS ops;               -- Watermarks, logs pipeline

-- ── Schémas serving ───────────────────────────────────────────────────────────
CREATE SCHEMA IF NOT EXISTS srv_analytics;      -- Vues Superset (agrégats pré-calculés)
CREATE SCHEMA IF NOT EXISTS srv_ref;            -- Référentiels partagés (agences, SIREN)
CREATE SCHEMA IF NOT EXISTS slv_mirror;         -- Miroir Silver pour Superset

-- ── Droit de créer des schémas dans la base (requis par pipeline_utils) ───────
GRANT CREATE ON DATABASE gi_data TO gi_gold_user;

-- ── Grants gi_gold_user ───────────────────────────────────────────────────────
GRANT ALL ON SCHEMA gld_commercial  TO gi_gold_user;
GRANT ALL ON SCHEMA gld_staffing    TO gi_gold_user;
GRANT ALL ON SCHEMA gld_facturation TO gi_gold_user;
GRANT ALL ON SCHEMA gld_rh          TO gi_gold_user;
GRANT ALL ON SCHEMA gld_clients     TO gi_gold_user;
GRANT ALL ON SCHEMA gld_performance TO gi_gold_user;
GRANT ALL ON SCHEMA gld_operationnel TO gi_gold_user;
GRANT ALL ON SCHEMA gld_shared      TO gi_gold_user;
GRANT ALL ON SCHEMA srv_analytics   TO gi_gold_user;
GRANT ALL ON SCHEMA srv_ref         TO gi_gold_user;
GRANT ALL ON SCHEMA slv_mirror      TO gi_gold_user;
GRANT ALL ON SCHEMA ops             TO gi_gold_user;

-- Permettre à gi_gold_user de créer des objets dans ces schémas
ALTER DEFAULT PRIVILEGES IN SCHEMA gld_commercial   GRANT ALL ON TABLES TO gi_gold_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA gld_staffing     GRANT ALL ON TABLES TO gi_gold_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA gld_facturation  GRANT ALL ON TABLES TO gi_gold_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA gld_rh           GRANT ALL ON TABLES TO gi_gold_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA gld_clients      GRANT ALL ON TABLES TO gi_gold_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA gld_performance  GRANT ALL ON TABLES TO gi_gold_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA gld_operationnel GRANT ALL ON TABLES TO gi_gold_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA gld_shared       GRANT ALL ON TABLES TO gi_gold_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA srv_analytics    GRANT ALL ON TABLES TO gi_gold_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA srv_ref          GRANT ALL ON TABLES TO gi_gold_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA slv_mirror       GRANT ALL ON TABLES TO gi_gold_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA ops              GRANT ALL ON TABLES TO gi_gold_user;
