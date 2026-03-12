# State Cards — GI Data Lakehouse

{
  "state_card": {
    "version": "1.0",
    "phase_sortante": "Bronze + Silver (Phase 0–1)",
    "phase_entrante": "Gold + Utilitaires (Phase 1–2)",
    "date": "2026-03-05",
    "auteur": "Maha / CDO"
  },

  "revue_phase": {

    "ce_qui_fonctionne": [
      "shared.py : Config / RunMode (OFFLINE|PROBE|LIVE) / Stats / s3_bronze() / date_partition — référentiel unique",
      "probe_ddl.py + probe_ddl_schema.py : 45 tables sondées, colonne DDL réelle documentée (✓/+/x/~)",
      "Bronze : 4 scripts (interimaires, clients, agences, missions) — stratégie DELTA/FULL-LOAD correcte post-probe",
      "Silver : 9 scripts correctement mappés sur colonnes DDL réelles",
      "Levier FinOps S3 : partitionnement /{date_partition}/ appliqué sur 100% des Silver — plus de full-scan **",
      "RGPD : PER_NIR pseudonymisé SHA-256+salt, coordonnées/contacts marqués Silver-only",
      "SCD2 : change_hash implémenté dans dim_interimaires + dim_clients",
      "Outillage : .gitignore, pyproject.toml (groupes optionnels), terraform.tfvars.example, TESTING.md",
      "kubeconfig-gi-poc.yaml correctement exclu du repo via .gitignore"
    ],

    "ce_qui_ne_fonctionne_pas": [
      "Aucun run LIVE validé — tout resté en PROBE/OFFLINE (pas de données réelles en Bronze S3 confirmées)",
      "DAG Airflow non mis à jour avec la nouvelle stratégie DELTA/FULL-LOAD (cronjob-bronze.yaml obsolète)",
      "silver_agences_light.py : LEFT JOIN raw_wtug non testé — WTUG absent Bronze tant que bronze_agences non exécuté en LIVE",
      "SIREN/SIRET/NAF clients : NULL dans Silver (absent DDL WTTIESERV) — Gold vue_360_client sans ces champs",
      "Montants HT/TTC factures : NULL dans Silver (absent DDL WTEFAC) — Gold commercial doit reconstituer via WTLFAC"
    ],

    "dette_technique_identifiee": [
      {
        "id": "DT-01",
        "criticite": "HIGH",
        "description": "WTPRH + PYCONTRAT : colonnes dates non confirmées par probe (PRH_DATEDEB/FIN, CNT_DATEDEB/FIN = UNCERTAIN). Silver_temps et silver_missions utilisent noms supposés.",
        "action": "Probe SELECT TOP 1 * FROM WTPRH avant premier run LIVE Silver temps/missions"
      },
      {
        "id": "DT-02",
        "criticite": "HIGH",
        "description": "WTEFAC.EFAC_MONTANTHT/TTC absent DDL → NULL Silver. fact_ca_hebdo_agence (Gold) doit agréger via WTLFAC (SUM lignes) et non depuis entête. Architecture Gold à revoir.",
        "action": "Dans gold_ca_mensuel.py : JOIN silver factures + lignes_factures, SUM(base * taux) reconstituant le HT"
      },
      {
        "id": "DT-03",
        "criticite": "MEDIUM",
        "description": "SIREN/SIRET/NAF absent DDL WTTIESERV → NULL dans dim_clients Silver. Vue 360° Client incomplète.",
        "action": "Investiguer WTCLPT (présence CLPT_SIREN ?) ou CMTIER (TIE_SIREN ?) — probe ciblé avant Phase Gold clients"
      },
      {
        "id": "DT-04",
        "criticite": "MEDIUM",
        "description": "Marque/Branche agence NULL (absent DDL WTUG). dim_agences incomplète pour scorecard.",
        "action": "Vérifier table [Agence Gestion] (source Excel ?) — bronze_clients_external.py existe déjà dans le repo"
      },
      {
        "id": "DT-05",
        "criticite": "MEDIUM",
        "description": "DAG Airflow (cronjob-bronze.yaml) non aligné : pas de séparation DELTA/FULL-LOAD, pas de SILVER_DATE_PARTITION en variable DAG.",
        "action": "Réécrire dag_poc_pipeline.py avec TaskGroup Bronze-Delta / Bronze-Full / Silver / Gold"
      },
      {
        "id": "DT-06",
        "criticite": "LOW",
        "description": "gold_*.py existants dans le repo (9 scripts) n'ont pas été revus post-probe DDL. Risque de colonnes Silver inexistantes.",
        "action": "Audit systématique gold_*.py contre le schéma Silver produit — priorité Phase Gold"
      },
      {
        "id": "DT-07",
        "criticite": "LOW",
        "description": "Tests unitaires (tests/) non créés — coverage 0%.",
        "action": "Au moins 3 tests OFFLINE par script Bronze (Config mock, Stats.finish(), delta_col injection)"
      }
    ],

    "decisions_prises": [
      "Full-load accepté pour ~80% des tables Bronze : pas de *_DATEMODIF en DDL réel — conséquence probe, non-négociable",
      "Spark non utilisé Phase 0 : DuckDB couvre le volume (3 Go/j delta Evolia), économie ~80€/mois",
      "Architecture hybride : Bronze on-prem (accès SQL Server), Silver+Gold sur OVH (workers Airflow)",
      "Gold agréger montants HT depuis WTLFAC (lignes factures) et non WTEFAC (entêtes) — EFAC_MONTANTHT NULL",
      "SILVER_DATE_PARTITION overridable via env : backfill sans modification de code",
      "pyproject.toml groupes optionnels : dépendances GX/datahub/dbt hors image prod Docker"
    ]
  },

  "state_card_transition": {

    "decisions_prises": [
      "Gold calcule CA HT via SUM(WTLFAC.LFAC_BASE * LFAC_TAUX) — WTEFAC.EFAC_MONTANTHT est NULL",
      "Probe ciblé WTPRH + PYCONTRAT requis AVANT run LIVE silver_temps.py + silver_missions.py",
      "Probe ciblé WTCLPT.CLPT_SIREN + CMTIER.TIE_SIREN requis AVANT gold_vue360_client.py",
      "DAG Airflow à réécrire — TaskGroup Bronze-Delta | Bronze-Full | Silver | Gold avec SILVER_DATE_PARTITION injecté",
      "gold_*.py (9 scripts repo) à auditer contre schéma Silver actuel avant exécution"
    ],

    "blockers": [
      {
        "id": "B-01",
        "description": "silver_temps.py + silver_missions.py en PROBE non validés — colonnes UNCERTAIN (DT-01)",
        "bloque": "gold_operationnel.py, gold_staffing.py, gold_ca_mensuel.py",
        "resolution": "SELECT TOP 1 * FROM WTPRH; SELECT TOP 1 * FROM PYCONTRAT; sur Evolia"
      },
      {
        "id": "B-02",
        "description": "WTEFAC.EFAC_MONTANTHT NULL — architecture Gold CA à adapter (DT-02)",
        "bloque": "gold_ca_mensuel.py, gold_scorecard_agence.py, gold_vue360_client.py",
        "resolution": "Patch gold_ca_mensuel.py : JOIN slv_facturation/factures + lignes_factures, SUM reconstituant HT"
      },
      {
        "id": "B-03",
        "description": "Aucun run LIVE Bronze exécuté — S3 vide, Silver ne peut pas s'exécuter",
        "bloque": "Tout le pipeline Silver + Gold",
        "resolution": "Premier run Bronze LIVE depuis laptop+VPN sur 5 tables pilotes (WTMISS, WTCNTI, WTEFAC, WTPRH, WTRHDON)"
      }
    ],

    "next_actions": [
      {
        "ordre": 1,
        "action": "Probe WTPRH + PYCONTRAT (SELECT TOP 1 *) — lever DT-01 + B-01",
        "responsable": "Data Engineer",
        "effort": "30 min"
      },
      {
        "ordre": 2,
        "action": "Premier run Bronze LIVE (5 tables pilotes) — lever B-03",
        "responsable": "Data Engineer (laptop + VPN GI Siège)",
        "effort": "1h"
      },
      {
        "ordre": 3,
        "action": "Probe WTCLPT.CLPT_SIREN + CMTIER.TIE_SIREN — lever DT-03",
        "responsable": "Data Engineer",
        "effort": "15 min"
      },
      {
        "ordre": 4,
        "action": "Audit gold_*.py (9 scripts) contre schéma Silver actuel — lever DT-06",
        "responsable": "Maha / Data Engineer",
        "effort": "2h"
      },
      {
        "ordre": 5,
        "action": "Patch gold_ca_mensuel.py : reconstitution HT depuis WTLFAC — lever B-02",
        "responsable": "Data Engineer",
        "effort": "1h"
      },
      {
        "ordre": 6,
        "action": "Réécrire dag_poc_pipeline.py avec TaskGroups + SILVER_DATE_PARTITION — lever DT-05",
        "responsable": "Data Engineer",
        "effort": "2h"
      },
      {
        "ordre": 7,
        "action": "Run Silver LIVE (probe d'abord) après Bronze validé",
        "responsable": "Data Engineer",
        "effort": "2h"
      },
      {
        "ordre": 8,
        "action": "Run Gold LIVE dim_calendrier + dim_agences + dim_clients (Gold dimensions en premier)",
        "responsable": "Data Engineer",
        "effort": "2h"
      }
    ],

    "contexte_minimal": {
      "stack": {
        "ingest": "pyodbc ODBC Driver 18 — TrustServerCertificate=yes, Encrypt=yes",
        "transform": "DuckDB 1.1+ — read_json_auto() + COPY TO Parquet ZSTD",
        "warehouse": "PostgreSQL 16 OVHcloud Essential-4 GRA",
        "orchestration": "Managed Airflow OVHcloud — DAG gi_poc_pipeline",
        "s3": "OVHcloud Object Storage GRA — s3.gra.perf.cloud.ovh.net"
      },
      "chemins_s3": {
        "bronze": "s3://gi-poc-bronze/raw_{table}/{YYYY/MM/DD}/*.json",
        "silver": "s3://gi-poc-silver/slv_{domaine}/{table}.parquet",
        "gold": "PostgreSQL — schemas gld_commercial / gld_staffing / gld_performance / gld_clients / gld_operationnel"
      },
      "run_mode": "RUN_MODE=offline|probe|live — défaut live",
      "backfill": "SILVER_DATE_PARTITION=YYYY/MM/DD — override date_partition dans Config",
      "rgpd": {
        "pseudonymise": "PER_NIR → SHA-256+salt (RGPD_SALT env)",
        "silver_only": "coordonnees, contacts TIEI_* — jamais en Gold",
        "gold_exclus": "Tout champ SENSIBLE ou PERSONNEL"
      },
      "cles_jointure": {
        "PER_ID": "Clé personne physique — PYPERSONNE, WTMISS, WTCNTI, WTPRH",
        "CNT_ID": "Contrat — WTCNTI, WTMISS, WTPRH",
        "TIE_ID": "Tiers/Client — WTTIESERV, WTEFAC, WTMISS",
        "RGPCNT_ID": "Agence UG — WTUG (via PYREGROUPCNT), WTMISS, WTEFAC",
        "EFAC_NUM": "Entête facture — WTEFAC, lien vers WTLFAC via FAC_NUM",
        "PRH_BTS": "Relevé heures — WTPRH, WTRHDON"
      },
      "nulls_connus": {
        "WTEFAC.montant_ht": "NULL — reconstituer via SUM(WTLFAC.LFAC_BASE * LFAC_TAUX)",
        "WTTIESERV.siren_siret_naf": "NULL — investiguer WTCLPT ou CMTIER",
        "WTPINT.agence_rattachement": "NULL — RGPCNT_ID absent DDL WTPINT",
        "PYSAL.date_sortie": "NULL — SAL_DATESORTIE absent DDL",
        "WTUG.marque_branche": "NULL — absent DDL, source [Agence Gestion] externe"
      },
      "incertitudes_ddl": {
        "WTPRH": "PRH_DATEDEB/DATEFIN — UNCERTAIN, probe SELECT TOP 1 requis",
        "PYCONTRAT": "CNT_DATEDEB/DATEFIN — UNCERTAIN, probe SELECT TOP 1 requis"
      }
    },

    "fichiers_modifies": [
      {
        "fichier": "shared.py",
        "changement": "Ajout date_partition dans Config + helpers s3_bronze() / s3_bronze_range()"
      },
      {
        "fichier": "silver_interimaires.py",
        "changement": "Corrections DDL (PER_NAISSANCE, NAT_CODE, NULL agence_rattachement) + partitionnement S3"
      },
      {
        "fichier": "silver_interimaires_detail.py",
        "changement": "Corrections DDL (PEVAL_DU, TYPTEL_CODE, PER_TEL_NTEL) + partitionnement S3"
      },
      {
        "fichier": "silver_competences.py",
        "changement": "Corrections DDL (PMET_ORDRE, PHAB_DELIVR/EXPIR, EXP_NOM/DEBUT/FIN) + partitionnement S3"
      },
      {
        "fichier": "silver_clients.py",
        "changement": "Corrections DDL (TIES_RAISOC, SIREN/SIRET/NAF→NULL) + partitionnement S3"
      },
      {
        "fichier": "silver_clients_detail.py",
        "changement": "Corrections DDL (TIEI_*, ENC_SIREN, ENCG_DECISIONLIB) + partitionnement S3"
      },
      {
        "fichier": "silver_agences_light.py",
        "changement": "Source raw_pyregroupcnt, RGPCNT_LIBELLE, marque/branche→NULL + partitionnement S3"
      },
      {
        "fichier": "silver_factures.py",
        "changement": "Corrections DDL (EFAC_TYPF/DTEEDI/DTEECH, montants→NULL, LFAC_LIB) + partitionnement S3"
      },
      {
        "fichier": "silver_missions.py",
        "changement": "Partitionnement S3 (session précédente : corrections CNTI_ORDRE, FINMISS_CODE)"
      },
      {
        "fichier": "silver_temps.py",
        "changement": "Partitionnement S3 (session précédente : corrections RHD_BASEP, PRH_MODIFDATE)"
      },
      {
        "fichier": "bronze_interimaires.py",
        "changement": "Stratégie TABLES_DELTA (2) / TABLES_FULL (8), colonnes DDL corrigées"
      },
      {
        "fichier": "bronze_clients.py",
        "changement": "Stratégie TABLES_DELTA (1) / TABLES_FULL (7), colonnes DDL corrigées"
      },
      {
        "fichier": "bronze_agences.py",
        "changement": "Stratégie TABLES_DELTA (2) / TABLES_FULL (3), WTUG full-load"
      },
      {
        "fichier": ".gitignore",
        "changement": "Créé — couvre secrets, tfstate, kubeconfig, probe outputs"
      },
      {
        "fichier": "pyproject.toml",
        "changement": "Créé — groupes optionnels quality/dbt/catalogue/dev/geo, ruff+pytest config"
      },
      {
        "fichier": "terraform.tfvars.example",
        "changement": "Créé — template complet sans valeurs sensibles"
      },
      {
        "fichier": "TESTING.md",
        "changement": "Créé — 9 sections de tests (lint→unit→probe→dry-run→live→GX→diagnostic)"
      }
    ]
  }
}

{
  "state_card": {
    "version": "2.0",
    "phase_sortante": "Audit Silver→Gold + Corrections aliases (Session 2)",
    "phase_entrante": "Gold LIVE + DAG Airflow + Tests (Phase 1-2)",
    "date": "2026-03-05",
    "auteur": "Maha / CDO"
  },

  "revue_phase": {

    "ce_qui_fonctionne": [
      "Audit systématique Silver→Gold terminé : 12 fichiers analysés, 7 corrigés, 5 PASS",
      "Pattern B-02 résolu : montant_ht reconstitué via SUM(lfac_mnt) depuis lignes_factures dans 4 Gold scripts",
      "Pattern PRH_BTS résolu : jointure missions→heures via releves (per_id+cnt_id) au lieu de PRH_BTS absent",
      "gold_ca_mensuel.py : aliases Silver canoniques, CTE montants B-02, JOIN fac_num, 174 lignes",
      "gold_staffing.py : aliases corrigés (base_paye, taux_horaire_paye/fact, date_debut/fin), 157 lignes",
      "gold_scorecard_agence.py : aliases + B-02 + releves JOIN, 220 lignes (⚠ >200)",
      "gold_retention_client.py : aliases + B-02 + releves JOIN, 210 lignes (⚠ >200)",
      "gold_vue360_client.py : date_facture alias corrigé, encours OK, fallback PG intact, 204 lignes",
      "gold_competences.py : MISS_DATEFIN → date_fin, 84 lignes",
      "gold_dimensions.py : Bronze DDL refs corrigés (MET_LIBELLE, TQUA_ID, SPE_ID, NIVQ_ID), 169 lignes",
      "gold_operationnel.py : PASS — utilisait déjà aliases lowercase (base_paye, base_fact, valide, date_modif)",
      "gold_clients_detail.py : PASS — aliases Silver corrects, PG CA injection OK",
      "enrich_ban_geocode.py : PASS — lit adresse_complete, code_postal, latitude (Silver canonical)",
      "rgpd_audit.py : PASS — scan regex, pas de référence directe aux colonnes Silver",
      "pipeline_utils.py : PASS — delta_col = noms DDL Bronze confirmés (PRH_MODIFDATE, CNTI_CREATE)"
    ],

    "ce_qui_ne_fonctionne_pas": [
      "Aucun run LIVE Gold exécuté — corrections théoriques, non validées contre données réelles",
      "B-03 persiste : S3 Bronze toujours vide — aucun run Bronze LIVE exécuté",
      "DAG Airflow non mis à jour avec TaskGroups et SILVER_DATE_PARTITION",
      "3 scripts Gold dépassent 200 lignes (scorecard 220, retention 210, vue360 204) — CTEs B-02 dupliquées",
      "CMD_STATUT NULL en Silver (absent DDL) → taux_transformation Gold = 0 systématique",
      "RGPCNT_ID NULL dans releves_heures (absent DDL WTPRH) → jointures agence incomplètes dans gold_operationnel",
      "SIREN/SIRET/NAF NULL en dim_clients → vue_360_client.secteur_activite = NULL"
    ],

    "dette_technique_identifiee": [
      {
        "id": "DT-08",
        "criticite": "HIGH",
        "description": "CTEs B-02 (montants via lignes_factures) dupliquées dans 4 scripts Gold (ca_mensuel, scorecard, retention, rentabilite). Maintenance fragile.",
        "action": "Extraire CTE commune dans gold_ctes_shared.sql ou module Python gold_helpers.py"
      },
      {
        "id": "DT-09",
        "criticite": "HIGH",
        "description": "Jointure missions→heures via releves (per_id+cnt_id) produit potentiellement des doublons si un intérimaire a plusieurs relevés pour le même contrat.",
        "action": "Ajouter QUALIFY ROW_NUMBER() ou agréger releves avant JOIN dans gold_staffing/scorecard"
      },
      {
        "id": "DT-10",
        "criticite": "MEDIUM",
        "description": "3 scripts Gold >200 lignes (Manifeste v2.0 violation). Cause : CTEs B-02 ajoutent ~15 lignes par script.",
        "action": "Factoriser via gold_helpers.py ou accepter dérogation documentée pour Phase 0"
      },
      {
        "id": "DT-05",
        "criticite": "MEDIUM",
        "description": "DAG Airflow (dag_poc_pipeline.py) non aligné avec stratégie DELTA/FULL-LOAD et SILVER_DATE_PARTITION.",
        "action": "Réécrire avec TaskGroup Bronze-Delta | Bronze-Full | Silver | Gold"
      },
      {
        "id": "DT-11",
        "criticite": "MEDIUM",
        "description": "CMD_STATUT absent DDL → taux_transformation = 0. Impact scorecard_agence et commandes_pipeline.",
        "action": "Investiguer WTCMD extra colonnes (CMD_CODE, CMD_PSTQUAL) ou table complémentaire pour statut commande"
      },
      {
        "id": "DT-03",
        "criticite": "MEDIUM",
        "description": "SIREN/SIRET/NAF NULL en dim_clients → vue_360_client incomplète.",
        "action": "Probe WTCLPT extra (67 colonnes disponibles) pour trouver SIREN source"
      },
      {
        "id": "DT-07",
        "criticite": "LOW",
        "description": "Tests unitaires Gold = 0. Aucun test OFFLINE pour vérifier les requêtes SQL.",
        "action": "Au minimum : test parse SQL DuckDB (syntaxe), test columns match DDL PG, test dry-run Stats"
      }
    ],

    "decisions_prises": [
      "B-02 résolu : montant_ht reconstitué via SUM(lfac_mnt) depuis slv_facturation/lignes_factures — appliqué dans 4 scripts",
      "PRH_BTS absent missions Silver → jointure alternative via releves_heures (per_id+cnt_id→prh_bts→heures_detail)",
      "Bronze ref_metiers/ref_qualifications lus directement en Bronze JSON (pas de Silver pour les référentiels)",
      "Dépassement 200 lignes accepté temporairement pour 3 scripts Gold (Phase 0 pragmatisme) — factorisation prévue Phase 1",
      "gold_operationnel.py et gold_clients_detail.py non modifiés — aliases déjà corrects",
      "enrich_ban_geocode.py, rgpd_audit.py, pipeline_utils.py non modifiés — aucune incohérence détectée"
    ]
  },

  "state_card_transition": {

    "decisions_prises": [
      "7 scripts Gold corrigés et livrés : ca_mensuel, staffing, scorecard, retention, vue360, competences, dimensions",
      "5 fichiers PASS confirmés : operationnel, clients_detail, enrich_ban, rgpd_audit, pipeline_utils",
      "B-02 (montant_ht NULL) résolu par CTE montants via lignes_factures — pattern standard pour tous les Gold CA",
      "PRH_BTS absent missions → pattern releves JOIN (per_id+cnt_id) standardisé",
      "Factorisation CTEs B-02 reportée Phase 1 (DT-08)"
    ],

    "blockers": [
      {
        "id": "B-03",
        "description": "S3 Bronze toujours vide — aucun run Bronze LIVE exécuté",
        "bloque": "Tout le pipeline Silver + Gold",
        "resolution": "Premier run Bronze LIVE depuis laptop+VPN sur 5 tables pilotes + ref_metiers + ref_qualifications + lignes_factures"
      },
      {
        "id": "B-04",
        "description": "DT-09 : jointure releves (per_id+cnt_id) peut produire doublons si multiple relevés par contrat",
        "bloque": "Exactitude gold_staffing, gold_scorecard, gold_retention (agrégats potentiellement gonflés)",
        "resolution": "Ajouter pré-agrégation releves ou QUALIFY avant premier run Gold LIVE"
      },
      {
        "id": "B-05",
        "description": "lignes_factures (slv_facturation/lignes_factures) non encore dans Silver — scripts Silver existants couvrent factures mais pas lignes",
        "bloque": "CTE montants B-02 dans 4 scripts Gold (ca_mensuel, scorecard, retention, rentabilite)",
        "resolution": "Ajouter silver_factures_lignes dans silver_factures.py (Bronze WTLFAC → Silver lignes_factures)"
      }
    ],

    "next_actions": [
      {
        "ordre": 1,
        "action": "Vérifier que silver_factures.py produit bien slv_facturation/lignes_factures — sinon l'ajouter (Bronze WTLFAC → Silver)",
        "responsable": "Data Engineer",
        "effort": "1h"
      },
      {
        "ordre": 2,
        "action": "Probe WTPRH + PYCONTRAT (SELECT TOP 1 *) — lever DT-01 Session 1",
        "responsable": "Data Engineer",
        "effort": "30 min"
      },
      {
        "ordre": 3,
        "action": "Premier run Bronze LIVE (5 tables pilotes + WTLFAC + ref_metiers + ref_qualifications) — lever B-03",
        "responsable": "Data Engineer (laptop + VPN)",
        "effort": "1h"
      },
      {
        "ordre": 4,
        "action": "Fixer DT-09 : pré-agréger releves dans gold_staffing/scorecard/retention avant JOIN heures — lever B-04",
        "responsable": "Data Engineer",
        "effort": "1h"
      },
      {
        "ordre": 5,
        "action": "Run Silver LIVE après Bronze validé (probe d'abord)",
        "responsable": "Data Engineer",
        "effort": "2h"
      },
      {
        "ordre": 6,
        "action": "Run Gold LIVE : dim_calendrier → dim_agences → dim_clients → dim_interimaires → dim_metiers (dimensions d'abord)",
        "responsable": "Data Engineer",
        "effort": "1h"
      },
      {
        "ordre": 7,
        "action": "Run Gold LIVE : fact_ca_mensuel_client → fact_missions_detail → scorecard_agence → vue_360_client",
        "responsable": "Data Engineer",
        "effort": "2h"
      },
      {
        "ordre": 8,
        "action": "Réécrire dag_poc_pipeline.py avec TaskGroups + SILVER_DATE_PARTITION — lever DT-05",
        "responsable": "Data Engineer",
        "effort": "2h"
      },
      {
        "ordre": 9,
        "action": "Factoriser CTEs B-02 dans gold_helpers.py — lever DT-08 + DT-10",
        "responsable": "Data Engineer",
        "effort": "2h"
      },
      {
        "ordre": 10,
        "action": "Probe WTCLPT extra colonnes (SIREN source) + WTCMD extra (statut commande) — lever DT-03 + DT-11",
        "responsable": "Data Engineer",
        "effort": "30 min"
      }
    ],

    "contexte_minimal": {
      "stack": {
        "ingest": "pyodbc ODBC Driver 18 — TrustServerCertificate=yes, Encrypt=yes",
        "transform": "DuckDB 1.1+ — read_json_auto() Bronze, read_parquet() Silver",
        "warehouse": "PostgreSQL 16 OVHcloud Essential-4 GRA",
        "orchestration": "Managed Airflow OVHcloud — DAG gi_poc_pipeline (à réécrire)",
        "s3": "OVHcloud Object Storage GRA — s3.gra.perf.cloud.ovh.net"
      },
      "chemins_s3": {
        "bronze": "s3://gi-poc-bronze/raw_{table}/{YYYY/MM/DD}/*.json",
        "silver": "s3://gi-poc-silver/slv_{domaine}/{table}/**/*.parquet",
        "gold": "PostgreSQL — gld_commercial / gld_staffing / gld_performance / gld_clients / gld_operationnel / gld_shared"
      },
      "aliases_silver_canoniques": {
        "slv_facturation/factures": "efac_num, tie_id, ties_serv, rgpcnt_id, type_facture, date_facture, date_echeance, taux_tva, montant_ht(NULL)",
        "slv_facturation/lignes_factures": "fac_num, lfac_ord, libelle, lfac_base, lfac_taux, lfac_mnt",
        "slv_temps/releves_heures": "prh_bts, per_id, cnt_id, tie_id, date_debut, date_fin, valide, date_modif",
        "slv_temps/heures_detail": "rhd_ligne, prh_bts, fac_num, base_paye, taux_paye, base_fact, taux_fact, libelle",
        "slv_missions/missions": "per_id, cnt_id, tie_id, ties_serv, rgpcnt_id, date_debut, date_fin, motif_fin_code",
        "slv_missions/contrats": "per_id, cnt_id, ordre, met_id, date_debut, date_fin, taux_horaire_paye, taux_horaire_fact, duree_hebdo, poste",
        "slv_missions/commandes": "cmd_id, rgpcnt_id, cmd_date, statut(NULL), nb_salaries",
        "slv_clients/dim_clients": "tie_id, client_sk, raison_sociale, siren(NULL), siret(NULL), naf_code(NULL), naf_libelle(NULL), ville, code_postal, statut_client, effectif_tranche, adresse_complete, is_current, latitude, longitude",
        "slv_interimaires/dim_interimaires": "per_id, interimaire_sk, matricule, nom, prenom, ville, code_postal, date_entree, is_actif, is_candidat, is_permanent, agence_rattachement(NULL), is_current",
        "slv_interimaires/competences": "per_id, type_competence, code, is_active",
        "slv_agences/dim_agences": "rgpcnt_id, agence_sk, nom, marque(NULL), branche(NULL), ville, is_active"
      },
      "patterns_gold": {
        "B-02_montant_ht": "CTE montants AS (SELECT fac_num, SUM(lfac_mnt) FROM lignes GROUP BY fac_num) → JOIN sur efac_num",
        "PRH_BTS_absent_missions": "missions → releves (per_id+cnt_id) → heures (prh_bts) — chaîne 3 tables",
        "ref_metiers_bronze": "dim_metiers lit Bronze JSON directement (MET_LIBELLE, TQUA_ID, SPE_ID, NIVQ_ID)"
      },
      "nulls_connus": {
        "WTEFAC.montant_ht": "NULL — reconstitué via SUM(WTLFAC.lfac_mnt)",
        "WTTIESERV.siren_siret_naf": "NULL — investiguer WTCLPT extra (67 cols)",
        "WTPINT.agence_rattachement": "NULL — RGPCNT_ID absent DDL WTPINT",
        "WTPRH.rgpcnt_id": "NULL/WARN — absent DDL confirmé",
        "WTCMD.statut": "NULL — CMD_STATUT absent DDL visible"
      },
      "run_mode": "RUN_MODE=offline|probe|live — défaut live",
      "backfill": "SILVER_DATE_PARTITION=YYYY/MM/DD — override date_partition dans Config"
    },

    "fichiers_modifies": [
      {
        "fichier": "gold_ca_mensuel.py",
        "changement": "EFAC_DATE→date_facture, EFAC_TYPE→type_facture, EFAC_MONTANTHT→CTE montants B-02, RHD_BASEFACT→base_fact, PRH_BTS JOIN→fac_num JOIN"
      },
      {
        "fichier": "gold_staffing.py",
        "changement": "MISS_DATEDEBUT/FIN→date_debut/fin, RHD_BASEPAYE/FACT→base_paye/fact, CNT_TAUXPAYE/FACT→taux_horaire_paye/fact, CNT_DATEDEBUT/FIN→date_debut/fin, PRH_BTS→releves JOIN"
      },
      {
        "fichier": "gold_scorecard_agence.py",
        "changement": "Mêmes corrections que staffing + B-02 montants + CMD_STATUT→statut"
      },
      {
        "fichier": "gold_retention_client.py",
        "changement": "Mêmes corrections factures + B-02 + releves JOIN pour heures"
      },
      {
        "fichier": "gold_vue360_client.py",
        "changement": "EFAC_DATE→date_facture dans CTE derniere_fact, TIE_ID→tie_id lowercase, encours date_decision→_loaded_at"
      },
      {
        "fichier": "gold_competences.py",
        "changement": "MISS_DATEFIN→date_fin dans missions_actives CTE"
      },
      {
        "fichier": "gold_dimensions.py",
        "changement": "build_dim_metiers: MET_LIB→MET_LIBELLE, QUA_ID→TQUA_ID, QUA_LIBELLE→TQUA_LIBELLE, MET_SPE→SPE_ID, MET_NIVEAU→NIVQ_ID"
      }
    ],

    "fichiers_non_modifies_pass": [
      "gold_operationnel.py — aliases Silver lowercase déjà corrects",
      "gold_clients_detail.py — aliases Silver corrects, PG CA injection OK",
      "enrich_ban_geocode.py — lit adresse_complete/code_postal/latitude canonical",
      "rgpd_audit.py — scan regex patterns, pas de référence directe colonnes",
      "pipeline_utils.py — delta_col = noms DDL Bronze confirmés",
      "shared.py — non modifié (contrainte)"
    ]
  }
}

{
  "state_card": {
    "version": "3.0",
    "phase_sortante": "Revue code 20 points + Corrections pipeline + Probe Bronze (Session 3)",
    "phase_entrante": "LIVE Bronze + Silver Probe (Phase 1)",
    "date": "2026-03-12",
    "auteur": "Maha / CDO"
  },

  "revue_phase": {

    "ce_qui_fonctionne": [
      "Revue code 20 points livrée et intégrée : shared.py, pipeline_utils.py, 4×Bronze, Silver×4, Gold×2",
      "shared.py : generate_batch_id() UUID4, pseudonymize_nir SHA-256 256 bits (sans troncature), DuckDB CREATE SECRET (credentials hors logs), upload_to_s3 body_bytes calculé une seule fois, pg_bulk_insert + atomic_load_gold avec rollback explicite",
      "pipeline_utils.py : ParamSpec/TypeVar (with_retry préserve signature), _EPOCH_SENTINEL (mark_failed sans fausser next delta), _NON_RETRYABLE (erreurs config remontent immédiatement), wm.set() rows par table (élimine cumul bugué)",
      "bronze_missions.py : _ingest() typé int, chunking 100K, allow_null_delta PYCONTRAT, wm.set(n) corrigé",
      "bronze_agences.py + bronze_clients.py + bronze_interimaires.py : même niveau de corrections",
      "silver_clients_detail.py : TQUA→TQUA_ID (bug fix), date_partition, COUNT via read_parquet",
      "silver_interimaires_detail.py + silver_missions.py : date_partition, fetchone() safe",
      "silver_agences_light.py : c1/c2=0 avant try (UnboundLocalError éliminé), COUNT via read_parquet",
      "gold_clients_detail.py : cfg.pg_*→cfg.ovh_pg_* (AttributeError corrigé), execute_values batch 500, connexion PG unique",
      "filter_tables() : and tables — liste vide retourne [] silencieusement (TABLES_DELTA=[] dans bronze_clients ne bloque plus)",
      "Probe Bronze complet — 0 exception, 0 timeout sur 4 scripts :",
      "  bronze_missions : WTMISS=216 WTCNTI=432 WTEFAC=117 WTPRH=992 PYCONTRAT=950217 WTCMD=589155 WTPLAC=13238 WTLFAC=26515214 WTFACINFO=6562449 OK",
      "  bronze_interimaires : WTPEVAL=16 PYPERSONNE=519462 PYSALARIE=454040 WTPINT=516595 PYCOORDONNEE=1124674 WTPMET=1404129 WTPHAB=277440 WTPDIP=26503 WTEXP=417307 WTUGPINT=705145 OK",
      "  bronze_clients : CMTIERS=123952 WTTIESERV=211134 WTCLPT=71921 WTTIEINT=182368 WTENCOURSG=81911 WTUGCLI=108561 (WTCOEF+WTUGAG retirés) OK",
      "  bronze_agences : PYREGROUPECNT=296 PYENTREPRISE=108 PYETABLISSEMENT=400 WTUG=296 PYDOSPETA=295 OK",
      "README.md professionnel créé : architecture, stack, S3 paths, watermarks, RGPD, FinOps, tests, dette technique, décisions"
    ],

    "ce_qui_ne_fonctionne_pas": [
      "WTRHDON count=0 en delta (RHD_DATED = date métier, pas DATEMODIF) — reclassé full-load mais probe full non encore validé",
      "filter_tables() raise encore si TABLE_FILTER cible une table FULL et que TABLES_DELTA est non-vide — correction v3.1 appliquée (debug log non-bloquant)",
      "S3 Bronze vide — aucun run LIVE exécuté, Silver non testable",
      "RGPD_SALT non validé en prod (guard-rail actif mais .env.local non vérifié)",
      "DAG Airflow non mis à jour"
    ],

    "dette_technique_identifiee": [
      {
        "id": "DT-FILTER",
        "criticite": "LOW",
        "description": "filter_tables() appelée 2× par run() (DELTA/FULL) — validation TABLE_FILTER impossible de façon fiable dans la fonction. Correction v3.1 : debug log non-bloquant. Impact zéro en prod (TABLE_FILTER est un outil de debug).",
        "action": "Accepté — documenter dans TESTING.md que TABLE_FILTER cible une table à la fois (DELTA ou FULL)"
      },
      {
        "id": "DT-WTLFAC",
        "criticite": "MEDIUM",
        "description": "WTLFAC 26.5M lignes en full-load à chaque run = 265 chunks S3, ~800 MB/run. Coûteux en temps (~15 min VPN) et stockage S3.",
        "action": "Phase 1 : investiguer LFAC_DATEMODIF ou stratégie incrément par FAC_NUM pour passer en delta"
      },
      {
        "id": "DT-01",
        "criticite": "HIGH",
        "description": "WTPRH.PRH_DATEDEB/FIN et PYCONTRAT.CNT_DATEDEB/FIN — colonnes UNCERTAIN. silver_temps.py et silver_missions.py utilisent noms supposés.",
        "action": "SELECT TOP 1 * FROM WTPRH; SELECT TOP 1 * FROM PYCONTRAT; avant premier run LIVE Silver"
      },
      {
        "id": "DT-07",
        "criticite": "LOW",
        "description": "Coverage tests = 0%.",
        "action": "3 tests OFFLINE minimum par script Bronze"
      },
      {
        "id": "DT-08",
        "criticite": "HIGH",
        "description": "CTEs B-02 dupliquées dans 4 scripts Gold.",
        "action": "gold_helpers.py Phase 1"
      },
      {
        "id": "DT-09",
        "criticite": "HIGH",
        "description": "Jointure releves (per_id+cnt_id) peut produire doublons si plusieurs relevés par contrat.",
        "action": "QUALIFY avant JOIN dans gold_staffing/scorecard avant premier run Gold LIVE"
      }
    ],

    "decisions_prises": [
      "WTRHDON reclassé full-load (probe count=0 sur RHD_DATED = date métier — pas de DATEMODIF en DDL)",
      "WTCOEF + WTUGAG retirés bronze_clients (tables vides confirmées probe 2026-03-12)",
      "filter_tables() : ValueError supprimée → debug log non-bloquant (outil debug, pas guard-rail prod)",
      "Revue code 20 points intégrée : priorité bugs > performance > design",
      "gold_clients_detail.py : cfg.ovh_pg_* corrigé, execute_values batch 500, connexion PG unique"
    ]
  },

  "state_card_transition": {

    "decisions_prises": [
      "Probe Bronze 4 scripts validé : 0 exception, volumes cohérents avec base Evolia (~20 tables, ~34M lignes totales)",
      "WTRHDON → full-load définitif (reclassé — pas de colonne delta disponible)",
      "WTCOEF + WTUGAG exclus (tables vides — inutile d'alimenter S3 et watermarks)",
      "Ordre LIVE Bronze : agences → interimaires → clients → missions (WTLFAC en dernier — 265 chunks)",
      "Silver probe planifié immédiatement après Bronze LIVE table par table"
    ],

    "blockers": [
      {
        "id": "B-03",
        "description": "S3 Bronze vide — aucun run Bronze LIVE exécuté",
        "bloque": "Tout le pipeline Silver + Gold",
        "resolution": "Run Bronze LIVE depuis laptop + VPN GI Siège (ordre : agences → interimaires → clients → missions)"
      },
      {
        "id": "B-DT01",
        "description": "Colonnes WTPRH.PRH_DATEDEB/FIN et PYCONTRAT.CNT_DATEDEB/FIN UNCERTAIN — silver_temps.py et silver_missions.py peuvent échouer au probe Silver",
        "bloque": "silver_temps.py (probe), silver_missions.py (probe)",
        "resolution": "SELECT TOP 1 * FROM WTPRH; SELECT TOP 1 * FROM PYCONTRAT; avant Silver probe"
      }
    ],

    "next_actions": [
      {
        "ordre": 1,
        "action": "Probe WTRHDON full-load : $env:TABLE_FILTER='WTRHDON'; $env:RUN_MODE='probe'; uv run .\\scripts\\bronze_missions.py — confirmer count > 0",
        "responsable": "Data Engineer",
        "effort": "5 min"
      },
      {
        "ordre": 2,
        "action": "Vérifier RGPD_SALT dans .env.local (min 32 chars) + connectivité S3 + PostgreSQL OVH",
        "responsable": "Data Engineer",
        "effort": "15 min"
      },
      {
        "ordre": 3,
        "action": "Run Bronze LIVE — bronze_agences.py (296 agences, rapide)",
        "responsable": "Data Engineer (laptop + VPN)",
        "effort": "5 min"
      },
      {
        "ordre": 4,
        "action": "Run Bronze LIVE — bronze_interimaires.py (~4.5M lignes, ~5-10 min)",
        "responsable": "Data Engineer (laptop + VPN)",
        "effort": "15 min"
      },
      {
        "ordre": 5,
        "action": "Run Bronze LIVE — bronze_clients.py (~780K lignes, ~3 min)",
        "responsable": "Data Engineer (laptop + VPN)",
        "effort": "10 min"
      },
      {
        "ordre": 6,
        "action": "Run Bronze LIVE — bronze_missions.py (~34M lignes dont WTLFAC 26.5M, ~20-30 min estimé)",
        "responsable": "Data Engineer (laptop + VPN)",
        "effort": "30 min"
      },
      {
        "ordre": 7,
        "action": "Vérifier S3 Bronze : aws s3 ls s3://gi-poc-bronze/ --endpoint-url $OVH_S3_ENDPOINT — confirmer partitions créées",
        "responsable": "Data Engineer",
        "effort": "2 min"
      },
      {
        "ordre": 8,
        "action": "Probe SELECT TOP 1 * FROM WTPRH; SELECT TOP 1 * FROM PYCONTRAT; — lever DT-01 avant Silver probe",
        "responsable": "Data Engineer",
        "effort": "15 min"
      },
      {
        "ordre": 9,
        "action": "Silver probe table par table (coller les logs ici pour analyse) : agences → interimaires → clients → missions → temps → factures",
        "responsable": "Data Engineer",
        "effort": "1h"
      },
      {
        "ordre": 10,
        "action": "Silver LIVE après probe validé (toutes tables, 0 erreur)",
        "responsable": "Data Engineer",
        "effort": "1h"
      }
    ],

    "volumes_bronze_confirmes": {
      "bronze_agences": {
        "PYREGROUPECNT": 296,
        "PYENTREPRISE": 108,
        "PYETABLISSEMENT": 400,
        "WTUG": 296,
        "PYDOSPETA": 295
      },
      "bronze_clients": {
        "CMTIERS": 123952,
        "WTTIESERV": 211134,
        "WTCLPT": 71921,
        "WTTIEINT": 182368,
        "WTENCOURSG": 81911,
        "WTUGCLI": 108561,
        "WTCOEF": "RETIRÉ (vide)",
        "WTUGAG": "RETIRÉ (vide)"
      },
      "bronze_interimaires": {
        "WTPEVAL": 16,
        "PYPERSONNE": 519462,
        "PYSALARIE": 454040,
        "WTPINT": 516595,
        "PYCOORDONNEE": 1124674,
        "WTPMET": 1404129,
        "WTPHAB": 277440,
        "WTPDIP": 26503,
        "WTEXP": 417307,
        "WTUGPINT": 705145
      },
      "bronze_missions": {
        "WTMISS": "216 (delta depuis 2023-01-01)",
        "WTCNTI": "432 (delta depuis 2023-01-01)",
        "WTEFAC": "117 (delta depuis 2023-01-01)",
        "WTPRH": "992 (delta depuis 2023-01-01)",
        "PYCONTRAT": "950217 (delta + IS NULL — contrats actifs inclus)",
        "WTCMD": 589155,
        "WTPLAC": 13238,
        "WTLFAC": 26515214,
        "WTFACINFO": 6562449,
        "WTRHDON": "À confirmer (full-load — probe count=0 en delta RHD_DATED)"
      }
    },

    "avertissements_live": [
      "WTLFAC 26.5M lignes = 265 chunks × 100K — prévoir 20-30 min VPN. Lancer bronze_missions.py en dernier.",
      "PYCONTRAT 950K lignes delta : premier run = 3 ans historique (FALLBACK_SINCE 2023-01-01). Watermark se stabilise ensuite.",
      "PYPERSONNE 519K + PYCOORDONNEE 1.1M + WTPMET 1.4M : full-load à chaque run — surveiller temps d'exécution.",
      "RGPD_SALT obligatoire en LIVE — Config.__post_init__ refuse le démarrage si absent ou sentinel."
    ],

    "contexte_minimal": {
      "stack": {
        "ingest": "pyodbc ODBC Driver 18 — TrustServerCertificate=yes, Encrypt=yes",
        "transform": "DuckDB 1.1+ — CREATE SECRET S3, read_json_auto(), COPY TO Parquet ZSTD",
        "warehouse": "PostgreSQL 16 OVHcloud Essential-4 GRA",
        "orchestration": "Managed Airflow OVHcloud — DAG gi_poc_pipeline (à réécrire Phase 1)",
        "s3": "OVHcloud Object Storage GRA — s3.gra.perf.cloud.ovh.net",
        "packaging": "uv + hatchling — Python 3.12"
      },
      "chemins_s3": {
        "bronze": "s3://gi-poc-bronze/raw_{table}/{YYYY/MM/DD}/batch_{id}_{chunk:04d}.json",
        "silver": "s3://gi-poc-silver/slv_{domaine}/{table}/**/*.parquet",
        "gold": "PostgreSQL — gld_commercial / gld_staffing / gld_performance / gld_clients / gld_operationnel / gld_shared",
        "ops": "ops.pipeline_watermarks (PostgreSQL)"
      },
      "commandes_live": {
        "bronze_agences": "$env:RUN_MODE='live'; uv run .\\scripts\\bronze_agences.py",
        "bronze_interimaires": "$env:RUN_MODE='live'; uv run .\\scripts\\bronze_interimaires.py",
        "bronze_clients": "$env:RUN_MODE='live'; uv run .\\scripts\\bronze_clients.py",
        "bronze_missions": "$env:RUN_MODE='live'; uv run .\\scripts\\bronze_missions.py",
        "silver_probe": "$env:RUN_MODE='probe'; uv run .\\scripts\\silver_agences_light.py",
        "verifier_s3": "aws s3 ls s3://gi-poc-bronze/ --endpoint-url $env:OVH_S3_ENDPOINT",
        "verifier_watermarks": "psql -c 'SELECT * FROM ops.pipeline_watermarks ORDER BY updated_at DESC LIMIT 20'"
      }
    },

    "fichiers_modifies_v3": [
      {
        "fichier": "shared.py",
        "changement": "filter_tables() : ValueError→debug log non-bloquant (support TABLE_FILTER sur tables FULL depuis script avec TABLES_DELTA non-vide)"
      },
      {
        "fichier": "bronze_missions.py",
        "changement": "WTRHDON reclassé TABLES_FULL (RHD_DATED = date métier, pas DATEMODIF)"
      },
      {
        "fichier": "bronze_clients.py",
        "changement": "WTCOEF + WTUGAG retirés TABLES_FULL (tables vides confirmées probe 2026-03-12)"
      },
      {
        "fichier": "README.md",
        "changement": "Créé — documentation complète projet (architecture, stack, S3, watermarks, RGPD, FinOps, dette technique)"
      }
    ]
  }
}
