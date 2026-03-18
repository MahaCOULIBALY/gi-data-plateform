{
  "state_card": "gi-data-platform · Rupture CTT — Bronze → Silver → Gold",
  "date": "2026-03-15",
  "statut": "IMPLÉMENTÉ — en attente déploiement Silver + Gold live",

  "garde_absolue": "⚠️ NE PAS TOUCHER À WTEFAC. Elle est dans TABLES_DELTA ligne 49 de bronze_missions.py avec delta_col=EFAC_DTEEDI. La supprimer casserait : (1) WTLFAC entier (INNER JOIN WTEFAC dans _extract_wtlfac_filtered), (2) tout le CA net HT, (3) scorecard_agence, (4) les 3 dashboards Phase 3. La session ne doit modifier bronze_missions.py QUE sur WTMISS (_COLS) et TABLES_FULL (ajout WTFINMISS/PYMTFCNT).",

  "décisions_prises": [
    {
      "id": "D1",
      "quoi": "WTEFAC : aucune modification — elle reste dans TABLES_DELTA ligne 49, inchangée",
      "pourquoi": "Toutes les colonnes DSO sont déjà présentes dans _COLS['WTEFAC'] lignes 82-88. Hors périmètre de cette session."
    },
    {
      "id": "D2",
      "quoi": "MISS_ANNULE confirmé probe 2026-03-15 : NULL=2 341 013 / 0=559 375 → non-annulé ; 1=12 314 → annulé",
      "pourquoi": "TRY_CAST(MISS_ANNULE AS SMALLINT) = 1 dans le CASE Silver est correct. NULL et 0 traitent identiquement (non-annulé)."
    },
    {
      "id": "D3",
      "quoi": "Classification rupture via PYMTFCNT.MTFCNT_FINCNT + MTFCNT_LIBELLE (priorité sur LIKE FINMISS_LIBELLE)",
      "pourquoi": "MTFCNT_FINCNT=0 → changement admin (mission continue, EN_COURS). MTFCNT_FINCNT=1 + LIKE '%fin de mission%' → TERME_NORMAL (1 seul code sur 17). MTFCNT_FINCNT=1 autres → RUPTURE (16 codes : démission, licenciement, essai…). Fallback LIKE FINMISS_LIBELLE si MTFCNT_ID NULL."
    },
    {
      "id": "D4",
      "quoi": "WTFINMISS et PYMTFCNT ingérés en TABLES_FULL (full-load)",
      "pourquoi": "WTFINMISS=11 lignes, PYMTFCNT=17 lignes — référentiels quasi-statiques, full-load sans impact perf. Probes 2026-03-15 : WTFINMISS 1.47s / PYMTFCNT 0.1s."
    },
    {
      "id": "D5",
      "quoi": "Silver fin_mission créée dans silver_missions.py (table FIN_MISSION dans _TABLES)",
      "pourquoi": "Sources WTMISS + WTCNTI + WTFINMISS + PYMTFCNT toutes dans le périmètre missions. Pattern _Table avec sql_fn, cohérent avec l'organisation existante."
    },
    {
      "id": "D6",
      "quoi": "FINMISS_DTEAUTO non ingéré — int représentant un délai en jours, pas une date de mission",
      "pourquoi": "Métadonnée de configuration du code WTFINMISS, pas une valeur par contrat. Aucune valeur ajoutée pour le KPI rupture CTT."
    }
  ],

  "blockers": [],

  "next_actions": [
    {
      "ordre": 1,
      "action": "Déployer Silver FIN_MISSION en live",
      "fichier": "scripts/silver_missions.py",
      "detail": "TABLE_FILTER=FIN_MISSION RUN_MODE=live python scripts/silver_missions.py — vérifie que raw_wtfinmiss et raw_pymtfcnt sont bien dans cfg.date_partition avant de lancer"
    },
    {
      "ordre": 2,
      "action": "Déployer Gold fact_rupture_contrat en live",
      "fichier": "scripts/gold_qualite_missions.py",
      "detail": "RUN_MODE=live python scripts/gold_qualite_missions.py — dépend de l'étape 1"
    },
    {
      "ordre": 3,
      "action": "Valider les données Gold",
      "fichier": "—",
      "detail": "SELECT statut_fin_mission, COUNT(*) FROM gld_performance.fact_rupture_contrat GROUP BY 1 ORDER BY 1 — vérifier que EN_COURS est absent (filtré dans Gold), RUPTURE et TERME_NORMAL présents, ANNULEE cohérent (~12k missions)"
    },
    {
      "ordre": 4,
      "action": "Superset — Configurer les charts du dashboard 'Qualité & Performance Mission'",
      "fichier": "—",
      "detail": "Voir superset_indicateurs_complementaires_setup.md → Dashboard 1, Charts 1 (KPI Taux rupture) et Chart 3 (Bar taux rupture par agence). Source : gld_performance.fact_rupture_contrat"
    }
  ],

  "contexte_minimal": {
    "projet": "GI Data Lakehouse — Phase 4",
    "stack": "Evolia (SQL Server pymssql) → S3 Bronze JSON (OVH gi-poc-bronze) → S3 Silver Parquet (OVH gi-poc-silver) → PostgreSQL Gold (OVH gi_poc_ddi_gold) → Superset",
    "pipeline_bronze": "bronze_missions.py — TABLES_DELTA (watermark WatermarkStore) + TABLES_FULL (full-load). WTFINMISS (11 lignes) et PYMTFCNT (17 lignes) ajoutés en TABLES_FULL le 2026-03-15.",
    "pipeline_silver": "silver_missions.py — lecture S3 Bronze via DuckDB read_json_auto, écriture S3 Silver Parquet. FIN_MISSION ajoutée dans _TABLES avec _build_fin_mission_sql (JOIN WTMISS+WTCNTI+WTFINMISS+PYMTFCNT).",
    "pipeline_gold": "gold_qualite_missions.py (NOUVEAU) — lit slv_missions/fin_mission Parquet via DuckDB, écrit gld_performance.fact_rupture_contrat via pg_bulk_insert (TRUNCATE+COPY).",
    "mode_run": "RUN_MODE=probe|live, TABLE_FILTER=TABLE1,TABLE2 pour ciblage unitaire",
    "path_silver_cible": "s3://gi-poc-silver/slv_missions/fin_mission/**/*.parquet",
    "path_gold_cible": "gld_performance.fact_rupture_contrat (PostgreSQL Gold gi_poc_ddi_gold)",
    "case_statut_valide": "MTFCNT_FINCNT=0→EN_COURS | MTFCNT_FINCNT=1+LIKE '%fin de mission%'→TERME_NORMAL | MTFCNT_FINCNT=1→RUPTURE | MISS_ANNULE=1→ANNULEE | fallback LIKE sur FINMISS_LIBELLE si MTFCNT_ID NULL"
  },

  "fichiers_modifiés": [
    {
      "fichier": "scripts/bronze_missions.py",
      "changements": [
        "_COLS['WTMISS'] : MISS_ANNULE ajouté en fin (ligne 73)",
        "TABLES_FULL : TableConfig('WTFINMISS', '', ['FINMISS_CODE']) ajouté (ligne 65)",
        "TABLES_FULL : TableConfig('PYMTFCNT', '', ['MTFCNT_ID']) ajouté (ligne 67)",
        "_COLS['WTFINMISS'] = 'FINMISS_CODE,FINMISS_LIBELLE,FINMISS_IFM,FINMISS_CP,MTFCNT_ID' (ligne 109)",
        "_COLS['PYMTFCNT'] = 'MTFCNT_ID,MTFCNT_CODE,MTFCNT_LIBELLE,MTFCNT_FINCNT,MTFCNT_DADS' (ligne 112)"
      ]
    },
    {
      "fichier": "scripts/silver_missions.py",
      "changements": [
        "_build_fin_mission_sql() : nouvelle fonction — JOIN WTMISS+WTCNTI+WTFINMISS+PYMTFCNT",
        "_TABLES : _Table('FIN_MISSION', bronze='wtmiss', silver='slv_missions/fin_mission') ajoutée",
        "CASE statut_fin_mission validé probe 2026-03-15 (MTFCNT_FINCNT + fallback LIKE)"
      ]
    },
    {
      "fichier": "scripts/gold_qualite_missions.py",
      "statut": "NOUVEAU",
      "changements": [
        "build_rupture_contrat_query() : agrégation slv_missions/fin_mission → 10 colonnes",
        "run() : DuckDB → pg_bulk_insert → gld_performance.fact_rupture_contrat"
      ]
    },
    {
      "fichier": "scripts/purge_silver_gold.py",
      "statut": "NOUVEAU",
      "changements": [
        "purge_silver_s3() : DELETE objets S3 gi-poc-silver (filtre optionnel --pipeline)",
        "purge_gold_pg() : TRUNCATE CASCADE tables gld_* PostgreSQL (filtre optionnel --schema)",
        "CLI : --target s3|gold|all / --pipeline silver_missions / --schema gld_performance"
      ]
    }
  ],

  "specs_techniques_référence": {
    "analyse_kpi": "docs/ANALYSE_KPI_COMPLEMENTAIRES.md §3 (Taux de Rupture de Contrat)",
    "superset_setup": "docs/superset_indicateurs_complementaires_setup.md — Dashboard 1, Charts 1 et 3",
    "ddl_wtmiss": "docs/DDL_EVOLIA_FILTERED.sql ligne 28807 — MISS_ANNULE smallint NULL, FINMISS_CODE varchar(3)",
    "ddl_wtfinmiss": "docs/DDL_EVOLIA_FILTERED.sql ligne 27592 — FINMISS_CODE PK, FINMISS_LIBELLE varchar(35), MTFCNT_ID FK→PYMTFCNT",
    "ddl_pymtfcnt": "docs/DDL_EVOLIA_FILTERED.sql ligne 13353 — MTFCNT_FINCNT smallint (0=changement admin, 1=fin effective)"
  }
}
