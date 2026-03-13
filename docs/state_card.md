{
  "state_card": "gi-data-platform · Bronze → Silver",
  "date": "2026-03-12",

  "décisions_prises": [
    {
      "id": "D1",
      "quoi": "WTUG.UG_GPS : CAST(... AS NVARCHAR(MAX))",
      "pourquoi": "pyodbc ne supporte pas SQL Server type -151 (geography)"
    },
    {
      "id": "D2",
      "quoi": "conn.cancel() après chaque exception dans les boucles delta/full",
      "pourquoi": "évite l'erreur 'connexion occupée' en cascade (PYDOSPETA après WTUG)"
    },
    {
      "id": "D3",
      "quoi": "WTRHDON déplacé TABLES_FULL → TABLES_DELTA avec RHD_DATED",
      "pourquoi": "49.6M lignes total, ~759K depuis 2024-01-01 — évite OOM laptop"
    },
    {
      "id": "D4",
      "quoi": "FALLBACK_SINCE_OVERRIDE par table (WTRHDON = 2024-01-01)",
      "pourquoi": "horizon analytique 2 ans suffisant TT ; rétention légale reste dans Evolia"
    },
    {
      "id": "D5",
      "quoi": "RHD_DATED accepté comme proxy delta malgré sémantique date métier",
      "pourquoi": "pas de DATEMODIF en DDL source ; limitation documentée dans le code"
    }
  ],

  "blockers": [
    {
      "id": "B1",
      "table": "WTRHDON",
      "risque": "corrections rétroactives pré-2024 non capturées (limitation source)",
      "mitigation_prévue": "delta via WTPRH.PRH_MODIFDATE (JOIN PRH_BTS) — évolution prod"
    }
  ],

  "next_actions": [
    "Lancer probe silver (tous les pipelines silver existants)",
    "Identifier tables silver manquantes ou colonnes à corriger",
    "Valider WTUG après fix UG_GPS en live (batch_id distinct)"
  ],

  "contexte_minimal": {
    "projet": "GI Data Lakehouse — Phase 0 PoC",
    "stack": "Evolia (SQL Server ODBC) → S3 Bronze (OVH) → Postgres watermarks",
    "mode_run": "RUN_MODE=probe|live, TABLE_FILTER=<TABLE> pour ciblage",
    "chunk_size": "_CHUNK_SIZE lignes par fichier S3 (évite EntityTooLarge OVH)",
    "watermarks": "pipeline_utils.WatermarkStore — stocké Postgres, marque failed si erreur"
  },

  "fichiers_modifiés": [
    {
      "fichier": "scripts/bronze_agences.py",
      "changements": [
        "WTUG._COLS : UG_GPS wrappé CAST(... AS NVARCHAR(MAX))",
        "except blocks TABLES_DELTA + TABLES_FULL : conn.cancel() via getattr"
      ]
    },
    {
      "fichier": "scripts/bronze_missions.py",
      "changements": [
        "FALLBACK_SINCE_OVERRIDE dict ajouté (WTRHDON → 2024-01-01)",
        "WTRHDON déplacé TABLES_FULL → TABLES_DELTA (RHD_DATED)",
        "run() : since = wm.get() or FALLBACK_SINCE_OVERRIDE.get(tc.name, FALLBACK_SINCE)"
      ]
    }
  ],

  "bronze_s3_status": {
    "complet": true,
    "tables_chargées": [
      "PYREGROUPECNT","PYENTREPRISE","PYETABLISSEMENT","WTUG","PYDOSPETA",
      "PYPERSONNE","PYSALARIE","PYCOORDONNEE","PYCONTRAT",
      "WTMISS","WTCNTI","WTEFAC","WTPRH",
      "WTCMD","WTPLAC","WTLFAC","WTFACINFO",
      "WTRHDON (à lancer)",
      "CMTIERS","WTCLPT","WTENCOURSG","WTTIESERV","WTTIEINT","WTUGCLI",
      "WTEXP","WTPEVAL","WTPINT","WTPMET","WTPHAB","WTPDIP","WTUGPINT",
      "WTMISS","WTCNTI","WTPRH","WTEFAC",
      "WTCMD","WTPLAC","WTLFAC","WTFACINFO",
      "WTMISS","WTCNTI","WTPRH","WTEFAC","WTPRH","WTCNTI","WTMISS",
      "WTCMD","WTPLAC","WTLFAC","WTFACINFO","WTMISS"
    ]
  }
}
