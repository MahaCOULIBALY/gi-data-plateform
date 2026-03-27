"""gold_dimensions.py — 5 dimensions partagées (conformed) → gld_shared.
Phase 3 · GI Data Lakehouse · Manifeste v2.0
PRÉREQUIS : Silver dim_clients, dim_interimaires, dim_agences doivent exister.

=== CORRECTIONS SESSION 2 (audit Silver→Gold) ===
- build_dim_metiers: MET_LIB → MET_LIBELLE / QUA_ID → TQUA_ID / QUA_LIBELLE → TQUA_LIBELLE
- build_dim_metiers: MET_SPE → SPE_ID / MET_NIVEAU → NIVQ_ID
- Silver dim_clients/dim_interimaires/dim_agences: lowercase aliases OK

=== ENRICHISSEMENT RÉFÉRENTIELS (2026-03-12) ===
- build_dim_metiers: raw_ref_metiers → raw_wtmet / raw_ref_qualifications → raw_wtqua
- build_dim_metiers: +pcs_code (PCS_CODE_2003 INSEE), filtre MET_DELETE actif

=== CORRECTIONS SESSION 3 (audit Gold) ===
- G-DIM-B01 : TRUNCATE gld_shared.<dim> explicite avant pg_bulk_insert (idempotence PG)
- G-DIM-M01 : RunMode importé + guard log PROBE
- G-DIM-M02 : stats.rows_transformed incrémenté dans la boucle
- G-DIM-m01/m02 : imports sys / logging supprimés
- G-DIM-m03 : filter_tables importé et appliqué sur DIMENSIONS
- G-DIM-m04 : a.nom AS nom_agence dans build_dim_agences

# MIGRÉ : iceberg_scan(cfg.iceberg_path(*)) → read_parquet(s3://gi-poc-silver/slv_*) (D01)
# dim_metiers conservé (lit Bronze read_json_auto — pas de Silver Parquet)
"""
import json

from shared import (
    Config, RunMode, Stats,
    get_duckdb_connection, get_pg_connection, pg_bulk_insert,
    gold_filter_tables, filter_tables, logger,
)


PIPELINE = "gold_dimensions"
JOURS_FERIES_FIXES = [
    (1, 1), (5, 1), (5, 8), (7, 14), (8, 15), (11, 1), (11, 11), (12, 25),
]

NOMS_JOURS = {0: "lundi", 1: "mardi", 2: "mercredi",
              3: "jeudi", 4: "vendredi", 5: "samedi", 6: "dimanche"}
NOMS_MOIS = {1: "janvier", 2: "février", 3: "mars", 4: "avril", 5: "mai", 6: "juin",
             7: "juillet", 8: "août", 9: "septembre", 10: "octobre", 11: "novembre", 12: "décembre"}


def build_dim_calendrier(ddb) -> list[tuple]:
    """Génère dim_calendrier 2020-01-01 → 2035-12-31."""
    rows = ddb.execute("""
        SELECT d::DATE AS date_id,
               EXTRACT(dow FROM d)::INT AS dow,
               EXTRACT(week FROM d)::INT AS semaine_iso,
               EXTRACT(month FROM d)::INT AS mois,
               CEIL(EXTRACT(month FROM d) / 3.0)::INT AS trimestre,
               EXTRACT(year FROM d)::INT AS annee
        FROM generate_series('2020-01-01'::DATE, '2035-12-31'::DATE, INTERVAL '1 day') t(d)
    """).fetchall()
    feries = {(m, d) for m, d in JOURS_FERIES_FIXES}
    result = []
    for date_id, dow, sem, mois, trim, annee in rows:
        is_ferie = (mois, date_id.day) in feries
        is_ouvre = dow not in (5, 6) and not is_ferie
        result.append((
            str(date_id), dow, NOMS_JOURS.get(dow, ""), sem, mois,
            NOMS_MOIS.get(mois, ""), trim, annee, is_ouvre, is_ferie,
        ))
    return result


def build_dim_agences(ddb, cfg: Config) -> list[tuple]:
    return ddb.execute(f"""
        SELECT
            a.agence_sk, a.rgpcnt_id,
            a.nom AS nom_agence,                        -- G-DIM-m04 : alias explicite
            a.marque, a.branche,
            COALESCE(h.secteur, '')    AS secteur,
            COALESCE(h.perimetre, '')  AS perimetre,
            COALESCE(h.zone_geo, '')   AS zone_geo,
            COALESCE(a.ville, '')      AS ville,
            a.is_active
        FROM read_parquet('s3://{cfg.bucket_silver}/slv_agences/dim_agences/**/*.parquet') a
        LEFT JOIN read_parquet('s3://{cfg.bucket_silver}/slv_agences/hierarchie_territoriale/**/*.parquet') h
            ON h.rgpcnt_id = a.rgpcnt_id
    """).fetchall()


def build_dim_clients(ddb, cfg: Config) -> list[tuple]:
    # siret calculé depuis siren(9) + nic(5) — pas de colonne siret en Silver
    # naf_libelle : alimenté depuis Salesforce Code_NAF_Name via silver_clients (2026-03-26)
    return ddb.execute(f"""
        SELECT
            client_sk, tie_id, raison_sociale,
            NULLIF(TRIM(siren), '')                                 AS siren,
            nic,
            CASE WHEN LENGTH(NULLIF(TRIM(siren), '')) = 9
                      AND LENGTH(nic) = 5
                 THEN TRIM(siren) || nic ELSE NULL END              AS siret,
            naf_code,
            NULLIF(TRIM(COALESCE(naf_libelle::VARCHAR, '')), '')      AS naf_libelle,
            ville, code_postal, statut_client,
            effectif_tranche
        FROM read_parquet('s3://{cfg.bucket_silver}/slv_clients/dim_clients/**/*.parquet')
        WHERE is_current = true
        QUALIFY ROW_NUMBER() OVER (PARTITION BY tie_id ORDER BY valid_from DESC NULLS LAST) = 1
    """).fetchall()


def build_dim_interimaires(ddb, cfg: Config) -> list[tuple]:
    """RGPD : pas de NIR, pas de date_naissance, pas d'adresse en Gold."""
    return ddb.execute(f"""
        SELECT interimaire_sk, per_id, matricule, nom, prenom,
               ville, code_postal, date_entree, is_actif, is_candidat,
               is_permanent, agence_rattachement
        FROM read_parquet('s3://{cfg.bucket_silver}/slv_interimaires/dim_interimaires/**/*.parquet')
        WHERE is_current = true
        QUALIFY ROW_NUMBER() OVER (PARTITION BY per_id ORDER BY valid_from DESC NULLS LAST) = 1
    """).fetchall()


def build_dim_habilitations(ddb, cfg: Config) -> list[tuple]:
    """Référentiel habilitations (WTTHAB) — dédupliqué par code THAB_ID.
    Phase 5 · tâche #22.
    """
    return ddb.execute(f"""
        SELECT
            MD5(code)                AS habilitation_sk,
            code                     AS thab_id,
            MAX(libelle)             AS libelle,
            BOOL_OR(is_active)       AS is_active
        FROM read_parquet('s3://{cfg.bucket_silver}/slv_interimaires/competences/**/*.parquet')
        WHERE type_competence = 'HABILITATION' AND code != ''
        GROUP BY code
        ORDER BY code
    """).fetchall()


def build_dim_diplomes(ddb, cfg: Config) -> list[tuple]:
    """Référentiel diplômes (WTTDIP) — dédupliqué par code TDIP_ID.
    Phase 5 · tâche #23.
    """
    return ddb.execute(f"""
        SELECT
            MD5(code)                AS diplome_sk,
            code                     AS tdip_id,
            MAX(libelle)             AS libelle,
            MAX(niveau)              AS niveau,
            BOOL_OR(is_active)       AS is_active
        FROM read_parquet('s3://{cfg.bucket_silver}/slv_interimaires/competences/**/*.parquet')
        WHERE type_competence = 'DIPLOME' AND code != ''
        GROUP BY code
        ORDER BY code
    """).fetchall()


def build_dim_metiers(ddb, cfg: Config) -> list[tuple]:
    """Lit Bronze raw_wtmet + raw_wtqua (via bronze_interimaires depuis 2026-03-12).
    ROW_NUMBER déduplique par MET_ID/TQUA_ID — insensible au fait que Bronze
    n'a pas encore tourné aujourd'hui.
    """
    bronze = f"s3://{cfg.bucket_bronze}"
    return ddb.execute(f"""
        WITH met AS (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY MET_ID ORDER BY _loaded_at DESC) AS rn
            FROM read_json_auto('{bronze}/raw_wtmet/**/**/**/*.json',
                                union_by_name=true, hive_partitioning=false)
        ),
        qua AS (
            SELECT TQUA_ID, TQUA_LIBELLE,
                   ROW_NUMBER() OVER (PARTITION BY TQUA_ID ORDER BY _loaded_at DESC) AS rn
            FROM read_json_auto('{bronze}/raw_wtqua/**/**/**/*.json',
                                union_by_name=true, hive_partitioning=false)
        )
        SELECT
            MD5(MET_ID::VARCHAR)                                        AS metier_sk,
            MET_ID::INT                                                 AS met_id,
            TRIM(COALESCE(MET_CODE, ''))                                AS code_metier,
            TRIM(COALESCE(MET_LIBELLE, ''))                             AS libelle_metier,
            TRIM(COALESCE(q.TQUA_LIBELLE, ''))                         AS qualification,
            TRIM(COALESCE(CAST(m.SPE_ID AS VARCHAR), ''))               AS specialite,
            TRIM(COALESCE(CAST(m.NIVQ_ID AS VARCHAR), ''))              AS niveau,
            NULLIF(TRIM(COALESCE(m.PCS_CODE_2003::VARCHAR, '')), '')    AS pcs_code
        FROM met m
        LEFT JOIN qua q ON q.TQUA_ID = m.TQUA_ID AND q.rn = 1
        WHERE m.rn = 1
          AND m.MET_ID IS NOT NULL
          AND COALESCE(TRY_CAST(m.MET_DELETE AS INT), 0) = 0
    """).fetchall()


DIMENSIONS = {
    "dim_calendrier": {
        "cols": ["date_id", "jour_semaine", "nom_jour", "semaine_iso", "mois", "nom_mois",
                 "trimestre", "annee", "is_jour_ouvre", "is_jour_ferie"],
        "builder": "calendrier",
    },
    "dim_agences": {
        "cols": ["agence_sk", "rgpcnt_id", "nom_agence", "marque", "branche",
                 "secteur", "perimetre", "zone_geo", "ville", "is_active"],
        "builder": "agences",
    },
    "dim_clients": {
        "cols": ["client_sk", "tie_id", "raison_sociale", "siren", "nic", "siret",
                 "naf_code", "naf_libelle", "ville", "code_postal", "statut_client",
                 "effectif_tranche"],
        "builder": "clients",
    },
    "dim_interimaires": {
        "cols": ["interimaire_sk", "per_id", "matricule", "nom", "prenom",
                 "ville", "code_postal", "date_entree", "is_actif", "is_candidat",
                 "is_permanent", "agence_rattachement"],
        "builder": "interimaires",
    },
    "dim_metiers": {
        "cols": ["metier_sk", "met_id", "code_metier", "libelle_metier",
                 "qualification", "specialite", "niveau", "pcs_code"],
        "builder": "metiers",
    },
    "dim_habilitations": {
        "cols": ["habilitation_sk", "thab_id", "libelle", "is_active"],
        "builder": "habilitations",
    },
    "dim_diplomes": {
        "cols": ["diplome_sk", "tdip_id", "libelle", "niveau", "is_active"],
        "builder": "diplomes",
    },
}


def run(cfg: Config) -> dict:
    stats = Stats()
    logger.info(json.dumps(
        {"pipeline": "gold_dimensions", "mode": cfg.mode.name}))
    active = gold_filter_tables(list(DIMENSIONS), cfg)

    if cfg.mode == RunMode.OFFLINE:
        return stats.finish(cfg, PIPELINE)

    with get_duckdb_connection(cfg) as ddb:
        builders = {
            "calendrier":    lambda: build_dim_calendrier(ddb),
            "agences":       lambda: build_dim_agences(ddb, cfg),
            "clients":       lambda: build_dim_clients(ddb, cfg),
            "interimaires":  lambda: build_dim_interimaires(ddb, cfg),
            "metiers":       lambda: build_dim_metiers(ddb, cfg),
            "habilitations": lambda: build_dim_habilitations(ddb, cfg),
            "diplomes":      lambda: build_dim_diplomes(ddb, cfg),
        }
        with get_pg_connection(cfg) as pg:
            for dim_name, spec in DIMENSIONS.items():
                if dim_name not in active:
                    continue
                try:
                    rows = builders[spec["builder"]]()
                    pg_bulk_insert(cfg, pg, "gld_shared", dim_name,
                                   spec["cols"], rows, stats)
                    stats.extra[dim_name] = len(rows)
                    stats.rows_transformed += len(rows)
                    stats.tables_processed += 1
                    logger.info(json.dumps(
                        {"dim": dim_name, "rows": len(rows)}))
                except Exception as e:
                    logger.exception(json.dumps(
                        {"dim": dim_name, "error": str(e)}))
                    stats.errors.append(
                        {"dimension": dim_name, "error": str(e)})
    return stats.finish(cfg, PIPELINE)


if __name__ == "__main__":
    cfg = Config()
    run(cfg)
