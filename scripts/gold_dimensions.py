"""gold_dimensions.py — 5 dimensions partagées (conformed) → gld_shared.
Phase 3 · GI Data Lakehouse · Manifeste v2.0
PRÉREQUIS : Silver dim_clients, dim_interimaires, dim_agences doivent exister.

=== CORRECTIONS SESSION 2 (audit Silver→Gold) ===
- build_dim_metiers: MET_LIB → MET_LIBELLE (probe DDL confirmed)
- build_dim_metiers: QUA_ID → TQUA_ID (probe DDL: QUA_ID absent, TQUA_ID confirmed)
- build_dim_metiers: QUA_LIBELLE → TQUA_LIBELLE (probe DDL confirmed)
- build_dim_metiers: MET_SPE → SPE_ID (probe DDL: MET_SPECIALITE absent)
- build_dim_metiers: MET_NIVEAU → NIVQ_ID (probe DDL: MET_NIVEAU absent)
- Silver dim_clients/dim_interimaires/dim_agences: lowercase aliases OK
=== ENRICHISSEMENT RÉFÉRENTIELS (2026-03-12) ===
- build_dim_metiers: raw_ref_metiers → raw_wtmet (WTMET intégré dans bronze_interimaires)
- build_dim_metiers: raw_ref_qualifications → raw_wtqua (WTQUA ajouté bronze_interimaires 2026-03-12)
- build_dim_metiers: +pcs_code (PCS_CODE_2003 INSEE), filtre MET_DELETE actif
"""
import sys
import logging
from shared import Config, Stats, get_duckdb_connection, get_pg_connection, pg_bulk_insert, logger

JOURS_FERIES_FIXES = [
    (1, 1), (5, 1), (5, 8), (7, 14), (8, 15), (11, 1), (11, 11), (12, 25),
]

NOMS_JOURS = {0: "lundi", 1: "mardi", 2: "mercredi",
              3: "jeudi", 4: "vendredi", 5: "samedi", 6: "dimanche"}
NOMS_MOIS = {1: "janvier", 2: "février", 3: "mars", 4: "avril", 5: "mai", 6: "juin",
             7: "juillet", 8: "août", 9: "septembre", 10: "octobre", 11: "novembre", 12: "décembre"}


def build_dim_calendrier(ddb) -> list[tuple]:
    """Génère dim_calendrier 2020-01-01 → 2027-12-31."""
    rows = ddb.execute("""
        SELECT d::DATE AS date_id,
               EXTRACT(dow FROM d)::INT AS dow,
               EXTRACT(week FROM d)::INT AS semaine_iso,
               EXTRACT(month FROM d)::INT AS mois,
               CEIL(EXTRACT(month FROM d) / 3.0)::INT AS trimestre,
               EXTRACT(year FROM d)::INT AS annee
        FROM generate_series('2020-01-01'::DATE, '2027-12-31'::DATE, INTERVAL '1 day') t(d)
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
            a.agence_sk, a.rgpcnt_id, a.nom, a.marque, a.branche,
            COALESCE(h.secteur, '') AS secteur,
            COALESCE(h.perimetre, '') AS perimetre,
            COALESCE(h.zone_geo, '') AS zone_geo,
            COALESCE(a.ville, '') AS ville,
            a.is_active
        FROM iceberg_scan('{cfg.iceberg_path("agences", "dim_agences")}') a
        LEFT JOIN iceberg_scan('{cfg.iceberg_path("agences", "hierarchie_territoriale")}') h
            ON h.rgpcnt_id = a.rgpcnt_id
    """).fetchall()


def build_dim_clients(ddb, cfg: Config) -> list[tuple]:
    # siret calculé depuis siren(9) + nic(5) — pas de colonne siret en Silver
    # naf_libelle / effectif_tranche : absents Silver → NULL en attendant référentiel NAF Phase 2
    return ddb.execute(f"""
        SELECT
            client_sk, tie_id, raison_sociale,
            NULLIF(TRIM(siren), '')                                 AS siren,
            nic,
            CASE WHEN LENGTH(NULLIF(TRIM(siren), '')) = 9
                      AND LENGTH(nic) = 5
                 THEN TRIM(siren) || nic ELSE NULL END              AS siret,
            naf_code,
            NULL::VARCHAR                                           AS naf_libelle,
            ville, code_postal, statut_client,
            NULL::VARCHAR                                           AS effectif_tranche
        FROM iceberg_scan('{cfg.iceberg_path("clients", "dim_clients")}')
        WHERE is_current = true
    """).fetchall()


def build_dim_interimaires(ddb, cfg: Config) -> list[tuple]:
    """RGPD : pas de NIR, pas de date_naissance, pas d'adresse en Gold."""
    return ddb.execute(f"""
        SELECT interimaire_sk, per_id, matricule, nom, prenom,
               ville, code_postal, date_entree, is_actif, is_candidat,
               is_permanent, agence_rattachement
        FROM iceberg_scan('{cfg.iceberg_path("interimaires", "dim_interimaires")}')
        WHERE is_current = true
    """).fetchall()


def build_dim_metiers(ddb, cfg: Config) -> list[tuple]:
    """Lit Bronze raw_wtmet + raw_wtqua (via bronze_interimaires depuis 2026-03-12).
    Utilise cfg.date_partition pour éviter le full-scan S3 multi-batchs.
    WTQUA dédupliqué par ROW_NUMBER pour éviter doublons si plusieurs batchs journaliers.
    """
    bronze = f"s3://{cfg.bucket_bronze}"
    dp = cfg.date_partition
    return ddb.execute(f"""
        WITH met AS (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY MET_ID ORDER BY _loaded_at DESC) AS rn
            FROM read_json_auto('{bronze}/raw_wtmet/{dp}/*.json', union_by_name=true, hive_partitioning=false)
        ),
        qua AS (
            SELECT TQUA_ID, TQUA_LIBELLE,
                   ROW_NUMBER() OVER (PARTITION BY TQUA_ID ORDER BY _loaded_at DESC) AS rn
            FROM read_json_auto('{bronze}/raw_wtqua/{dp}/*.json', union_by_name=true, hive_partitioning=false)
        )
        SELECT
            MD5(MET_ID::VARCHAR) AS metier_sk,
            MET_ID::INT AS met_id,
            TRIM(COALESCE(MET_CODE, '')) AS code_metier,
            TRIM(COALESCE(MET_LIBELLE, '')) AS libelle_metier,
            TRIM(COALESCE(q.TQUA_LIBELLE, '')) AS qualification,
            TRIM(COALESCE(CAST(m.SPE_ID AS VARCHAR), '')) AS specialite,
            TRIM(COALESCE(CAST(m.NIVQ_ID AS VARCHAR), '')) AS niveau,
            NULLIF(TRIM(COALESCE(m.PCS_CODE_2003::VARCHAR, '')), '') AS pcs_code
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
                 "naf_code", "naf_libelle", "ville", "code_postal", "statut_client", "effectif_tranche"],
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
}


def run(cfg: Config) -> dict:
    stats = Stats()
    with get_duckdb_connection(cfg) as ddb:
        builders = {
            "calendrier": lambda: build_dim_calendrier(ddb),
            "agences": lambda: build_dim_agences(ddb, cfg),
            "clients": lambda: build_dim_clients(ddb, cfg),
            "interimaires": lambda: build_dim_interimaires(ddb, cfg),
            "metiers": lambda: build_dim_metiers(ddb, cfg),
        }
        with get_pg_connection(cfg) as pg:
            for dim_name, spec in DIMENSIONS.items():
                try:
                    rows = builders[spec["builder"]]()
                    pg_bulk_insert(cfg, pg, "gld_shared",
                                   dim_name, spec["cols"], rows, stats)
                    stats.extra[dim_name] = len(rows)
                    stats.tables_processed += 1
                    logger.info(f"{dim_name}: {len(rows)} rows")
                except Exception as e:
                    logger.exception(f"Error building {dim_name}: {e}")
                    stats.errors.append(
                        {"dimension": dim_name, "error": str(e)})
    return stats.finish()


if __name__ == "__main__":
    cfg = Config()
    run(cfg)
