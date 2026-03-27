"""seed_ref_naf.py — Alimente gld_shared.ref_naf_sections (référentiel INSEE NAF A-U).
Phase 2 · GI Data Lakehouse

But : fournir à Superset une table de jointure lisible pour les sections NAF.
      fact_ca_secteur_naf embarque déjà le mapping en SQL (pas de dépendance à cette table).
      Cette table est optionnelle — uniquement utile pour les dashboards Superset.

Exécution manuelle : python scripts/seed_ref_naf.py
"""
import json

from shared import Config, RunMode, Stats, get_pg_connection, logger

PIPELINE = "seed_ref_naf"
DOMAIN = "gld_shared"
TABLE = "ref_naf_sections"

# Nomenclature INSEE 2025 — 21 sections A-U avec plages de divisions
NAF_SECTIONS = [
    ("A", "Agriculture, sylviculture et pêche",         "01", "03"),
    ("B", "Industries extractives",                     "05", "09"),
    ("C", "Industrie manufacturière",                   "10", "33"),
    ("D", "Production et distribution énergie",         "35", "35"),
    ("E", "Eau, assainissement, déchets",               "36", "39"),
    ("F", "Construction",                               "41", "43"),
    ("G", "Commerce, réparation automobiles",           "45", "47"),
    ("H", "Transports et entreposage",                  "49", "53"),
    ("I", "Hébergement et restauration",                "55", "56"),
    ("J", "Information et communication",               "58", "63"),
    ("K", "Activités financières et assurance",         "64", "66"),
    ("L", "Activités immobilières",                     "68", "68"),
    ("M", "Activités spécialisées et techniques",       "69", "75"),
    ("N", "Services administratifs et de soutien",      "77", "82"),
    ("O", "Administration publique",                    "84", "84"),
    ("P", "Enseignement",                               "85", "85"),
    ("Q", "Santé humaine et action sociale",            "86", "88"),
    ("R", "Arts, spectacles et loisirs",                "90", "93"),
    ("S", "Autres activités de services",               "94", "96"),
    ("T", "Ménages employeurs",                         "97", "98"),
    ("U", "Activités extra-territoriales",              "99", "99"),
]

COLS = ["section_code", "libelle", "division_min", "division_max"]


def run(cfg: Config) -> dict:
    stats = Stats()

    if cfg.mode in (RunMode.OFFLINE, RunMode.PROBE):
        logger.info(json.dumps({"mode": cfg.mode.name, "action": "skipped"}))
        return stats.finish(cfg, PIPELINE)

    with get_pg_connection(cfg) as pg:
        try:
            with pg.cursor() as cur:
                cur.execute(f"TRUNCATE TABLE {DOMAIN}.{TABLE}")
                cur.executemany(
                    f"""
                    INSERT INTO {DOMAIN}.{TABLE}
                        (section_code, libelle, division_min, division_max)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (section_code) DO UPDATE SET
                        libelle      = EXCLUDED.libelle,
                        division_min = EXCLUDED.division_min,
                        division_max = EXCLUDED.division_max,
                        _loaded_at   = NOW()
                    """,
                    NAF_SECTIONS,
                )
            pg.commit()
            stats.tables_processed += 1
            stats.rows_transformed += len(NAF_SECTIONS)
            logger.info(json.dumps({
                "table": f"{DOMAIN}.{TABLE}",
                "rows": len(NAF_SECTIONS),
                "status": "ok",
            }))
        except Exception as e:
            logger.exception(json.dumps({"step": "pg_insert", "error": str(e)}))
            stats.errors.append({"step": "pg_insert", "error": str(e)})
            pg.rollback()

    return stats.finish(cfg, PIPELINE)


if __name__ == "__main__":
    run(Config())
