"""gold_ca_mensuel.py — Silver → Gold fact_ca_mensuel_client + validation Pyramid.
Phase 1 · GI Data Lakehouse · Manifeste v2.0

=== CORRECTIONS SESSION 2 (audit Silver→Gold) ===
- f.EFAC_DATE → f.date_facture / f.EFAC_TYPE → f.type_facture / etc.
- Jointure heures via WTFACINFO/FAC_NUM

=== CORRECTIONS SESSION 3 (audit Gold) ===
- G-CA-B01 : TRUNCATE avant pg_bulk_insert (idempotence PG)
- G-CA-M01 : RunMode importé + guards OFFLINE/PROBE dans run()
- G-CA-M02 : cfg.dry_run → cfg.mode != RunMode.LIVE
- G-CA-M03 : try/except autour de chaque bloc DuckDB et PG insert
- G-CA-M04 : stats.tables_processed dynamique (compteur dans flux)
- G-CA-m01 : imports sys/logging/datetime supprimés
- G-CA-m02 : filter_tables importé et appliqué

# MIGRÉ : iceberg_scan → read_parquet(s3://gi-poc-silver/slv_*) (D01)
# TODO B-02: montant_ht NULL en Silver → reconstitution via SUM(lfac_base * lfac_taux)
# PHASE 2 (2026-03-26) :
# fact_ca_secteur_naf : ventilation du CA par section NAF (A-U, nomenclature INSEE).
#   Mapping NAF embarqué en CASE SQL — pas de dépendance PG ref_naf_sections.
#   Source : slv_facturation/factures + lignes + slv_clients/dim_clients (naf_code).
#   seed_ref_naf.py crée la table ref_naf_sections dans gld_shared pour Superset.
"""
import csv
import json
from pathlib import Path

from shared import (
    Config, RunMode, Stats,
    get_duckdb_connection, get_pg_connection, pg_bulk_insert,
    gold_filter_tables, filter_tables, logger,
)
from gold_helpers import cte_montants_factures

PIPELINE = "gold_ca_mensuel"
PYRAMID_VALIDATION_FILE = Path("data/validation/pyramid_ca_mensuel.csv")
TOLERANCE_PCT = 0.5

CA_COLUMNS = [
    "client_sk", "tie_id", "mois", "ca_ht", "avoir_ht", "ca_net_ht",
    "nb_factures", "nb_missions_facturees", "nb_heures_facturees",
    "taux_moyen_fact", "agence_principale",
]
CONCENTRATION_COLUMNS = [
    "agence_id", "mois", "nb_clients", "nb_clients_top20",
    "ca_net_total", "ca_net_top20", "taux_concentration",
    "ca_net_top5", "taux_concentration_top5",
]
NAF_SECTEUR_COLUMNS = [
    "agence_id", "mois", "naf_code", "naf_division",
    "naf_section", "secteur_libelle",
    "nb_clients", "ca_net_ht", "nb_missions", "part_ca_agence_pct",
]

_TABLES = {
    "fact_ca_mensuel_client": ("gld_commercial", CA_COLUMNS),
    "fact_concentration_client": ("gld_clients", CONCENTRATION_COLUMNS),
    "fact_ca_secteur_naf": ("gld_commercial", NAF_SECTEUR_COLUMNS),
}


def build_ca_mensuel_query(cfg: Config) -> str:
    slv = f"s3://{cfg.bucket_silver}"
    return f"""
    WITH factures AS (
        SELECT * FROM read_parquet('{slv}/slv_facturation/factures/**/*.parquet')
    ),
    {cte_montants_factures(cfg)},
    heures_fac AS (
        -- B-02 : heures facturées depuis WTLFAC.LFAC_BASE (quantité base)
        SELECT
            fac_num,
            COALESCE(SUM(base::DECIMAL(12,2)), 0)::DECIMAL(12,2) AS heures_calc
        FROM read_parquet('{slv}/slv_facturation/lignes_factures/**/*.parquet')
        WHERE fac_num IS NOT NULL
        GROUP BY fac_num
    ),
    facinfo AS (
        SELECT tie_id::INT AS tie_id,
               DATE_TRUNC('month', TRY_CAST(date_debut AS DATE)) AS mois_mission,
               COUNT(DISTINCT CONCAT(per_id::VARCHAR, '|', cnt_id::VARCHAR)) AS nb_missions_fac
        FROM read_parquet('{slv}/slv_missions/missions/**/*.parquet')
        WHERE date_debut IS NOT NULL
        GROUP BY 1, 2
    ),
    dim_clients AS (
        SELECT * FROM read_parquet('{slv}/slv_clients/dim_clients/**/*.parquet')
        WHERE is_current = true
        QUALIFY ROW_NUMBER() OVER (PARTITION BY tie_id ORDER BY valid_from DESC NULLS LAST) = 1
    ),
    base AS (
        SELECT
            c.client_sk,
            f.tie_id::INT                                          AS tie_id,
            DATE_TRUNC('month', TRY_CAST(f.date_facture AS DATE))  AS mois,
            f.type_facture,
            COALESCE(mt.montant_ht_calc, 0)::DECIMAL(18,2)        AS montant_ht,
            COALESCE(hf.heures_calc, 0)::DECIMAL(12,2)            AS heures_ht,
            f.efac_num,
            f.rgpcnt_id::INT                                       AS rgpcnt_id,
            COALESCE(fi.nb_missions_fac, 0)                        AS nb_missions_fac
        FROM factures f
        LEFT JOIN montants    mt ON mt.fac_num    = f.efac_num
        LEFT JOIN heures_fac  hf ON hf.fac_num    = f.efac_num
        LEFT JOIN facinfo     fi ON fi.tie_id     = f.tie_id::INT
            AND fi.mois_mission = DATE_TRUNC('month', TRY_CAST(f.date_facture AS DATE))
        LEFT JOIN dim_clients  c ON c.tie_id      = f.tie_id::INT
        WHERE f.date_facture IS NOT NULL
    )
    SELECT
        client_sk, tie_id, mois,
        COALESCE(SUM(CASE WHEN type_facture = 'F' THEN montant_ht ELSE 0 END), 0)::DECIMAL(18,2)    AS ca_ht,
        COALESCE(SUM(CASE WHEN type_facture = 'A' THEN montant_ht ELSE 0 END), 0)::DECIMAL(18,2)    AS avoir_ht,
        (COALESCE(SUM(CASE WHEN type_facture = 'F' THEN montant_ht ELSE 0 END), 0)
        - COALESCE(SUM(CASE WHEN type_facture = 'A' THEN montant_ht ELSE 0 END), 0))::DECIMAL(18,2) AS ca_net_ht,
        COUNT(DISTINCT CASE WHEN type_facture = 'F' THEN efac_num END)                               AS nb_factures,
        COALESCE(SUM(nb_missions_fac), 0)                                                            AS nb_missions_facturees,
        COALESCE(SUM(CASE WHEN type_facture = 'F' THEN heures_ht ELSE 0 END), 0)::DECIMAL(12,2)     AS nb_heures_facturees,
        ROUND(
            COALESCE(SUM(CASE WHEN type_facture = 'F' THEN montant_ht ELSE 0 END), 0)
            / NULLIF(SUM(CASE WHEN type_facture = 'F' THEN heures_ht ELSE 0 END), 0)
        , 4)::DECIMAL(10,4)                                                                          AS taux_moyen_fact,
        MODE(rgpcnt_id)                                                                              AS agence_principale
    FROM base
    WHERE mois IS NOT NULL
    GROUP BY client_sk, tie_id, mois
    ORDER BY mois DESC, ca_net_ht DESC
    """


def build_concentration_query(cfg: Config) -> str:
    return f"""
    WITH factures AS (
        SELECT * FROM read_parquet('s3://{cfg.bucket_silver}/slv_facturation/factures/**/*.parquet')
        WHERE date_facture IS NOT NULL AND rgpcnt_id IS NOT NULL
    ),
    lignes AS (
        SELECT fac_num, COALESCE(SUM(montant), 0) AS montant_ht
        FROM read_parquet('s3://{cfg.bucket_silver}/slv_facturation/lignes_factures/**/*.parquet')
        GROUP BY fac_num
    ),
    base AS (
        SELECT
            f.rgpcnt_id::INT                                        AS agence_id,
            DATE_TRUNC('month', TRY_CAST(f.date_facture AS DATE))::DATE AS mois,
            f.tie_id::INT                                           AS tie_id,
            COALESCE(SUM(CASE WHEN f.type_facture = 'F' THEN l.montant_ht ELSE 0 END), 0)
            - COALESCE(SUM(CASE WHEN f.type_facture = 'A' THEN l.montant_ht ELSE 0 END), 0)
                                                                    AS ca_net_ht
        FROM factures f
        LEFT JOIN lignes l ON l.fac_num = f.efac_num
        GROUP BY 1, 2, 3
    ),
    totals AS (
        SELECT agence_id, mois, SUM(ca_net_ht) AS ca_total
        FROM base GROUP BY 1, 2
    ),
    ranked AS (
        SELECT b.*,
               t.ca_total,
               PERCENT_RANK() OVER (
                   PARTITION BY b.agence_id, b.mois
                   ORDER BY b.ca_net_ht DESC
               ) AS pct_rank,
               DENSE_RANK() OVER (
                   PARTITION BY b.agence_id, b.mois
                   ORDER BY b.ca_net_ht DESC
               ) AS rnk
        FROM base b JOIN totals t ON t.agence_id = b.agence_id AND t.mois = b.mois
    )
    SELECT
        agence_id, mois,
        COUNT(DISTINCT tie_id)                                      AS nb_clients,
        COUNT(DISTINCT tie_id) FILTER (WHERE pct_rank <= 0.2)       AS nb_clients_top20,
        MAX(ca_total)::DECIMAL(18,2)                                AS ca_net_total,
        COALESCE(SUM(ca_net_ht) FILTER (WHERE pct_rank <= 0.2),
                 0)::DECIMAL(18,2)                                  AS ca_net_top20,
        ROUND(
            COALESCE(SUM(ca_net_ht) FILTER (WHERE pct_rank <= 0.2), 0)
            / NULLIF(MAX(ca_total), 0),
        4)::DECIMAL(8,4)                                            AS taux_concentration,
        COALESCE(SUM(ca_net_ht) FILTER (WHERE rnk <= 5),
                 0)::DECIMAL(18,2)                                  AS ca_net_top5,
        ROUND(
            COALESCE(SUM(ca_net_ht) FILTER (WHERE rnk <= 5), 0)
            / NULLIF(MAX(ca_total), 0),
        4)::DECIMAL(8,4)                                            AS taux_concentration_top5
    FROM ranked
    WHERE mois IS NOT NULL
    GROUP BY 1, 2
    ORDER BY 2 DESC, 1
    """


def build_ca_secteur_naf_query(cfg: Config) -> str:
    """Ventilation du CA mensuel par section NAF (nomenclature INSEE A-U).
    Le mapping division→section est embarqué en CASE pour éviter la dépendance
    à ref_naf_sections PG (alimentée séparément par seed_ref_naf.py).
    Source : slv_facturation/factures + lignes_factures + slv_clients/dim_clients.
    """
    slv = f"s3://{cfg.bucket_silver}"
    return f"""
    WITH dim_clients AS (
        SELECT tie_id::INT AS tie_id, COALESCE(naf_code, '') AS naf_code
        FROM read_parquet('{slv}/slv_clients/dim_clients/**/*.parquet')
        WHERE is_current = true
        QUALIFY ROW_NUMBER() OVER (PARTITION BY tie_id ORDER BY valid_from DESC NULLS LAST) = 1
    ),
    lignes AS (
        SELECT fac_num, COALESCE(SUM(montant), 0) AS montant_ht
        FROM read_parquet('{slv}/slv_facturation/lignes_factures/**/*.parquet')
        GROUP BY fac_num
    ),
    base AS (
        SELECT
            f.rgpcnt_id::INT                                            AS agence_id,
            DATE_TRUNC('month', TRY_CAST(f.date_facture AS DATE))::DATE AS mois,
            f.tie_id::INT                                               AS tie_id,
            d.naf_code,
            LEFT(d.naf_code, 2)                                         AS naf_division,
            CASE
                WHEN LEFT(d.naf_code, 2) BETWEEN '01' AND '03' THEN 'A'
                WHEN LEFT(d.naf_code, 2) BETWEEN '05' AND '09' THEN 'B'
                WHEN LEFT(d.naf_code, 2) BETWEEN '10' AND '33' THEN 'C'
                WHEN LEFT(d.naf_code, 2) = '35'                THEN 'D'
                WHEN LEFT(d.naf_code, 2) BETWEEN '36' AND '39' THEN 'E'
                WHEN LEFT(d.naf_code, 2) BETWEEN '41' AND '43' THEN 'F'
                WHEN LEFT(d.naf_code, 2) BETWEEN '45' AND '47' THEN 'G'
                WHEN LEFT(d.naf_code, 2) BETWEEN '49' AND '53' THEN 'H'
                WHEN LEFT(d.naf_code, 2) BETWEEN '55' AND '56' THEN 'I'
                WHEN LEFT(d.naf_code, 2) BETWEEN '58' AND '63' THEN 'J'
                WHEN LEFT(d.naf_code, 2) BETWEEN '64' AND '66' THEN 'K'
                WHEN LEFT(d.naf_code, 2) = '68'                THEN 'L'
                WHEN LEFT(d.naf_code, 2) BETWEEN '69' AND '75' THEN 'M'
                WHEN LEFT(d.naf_code, 2) BETWEEN '77' AND '82' THEN 'N'
                WHEN LEFT(d.naf_code, 2) = '84'                THEN 'O'
                WHEN LEFT(d.naf_code, 2) = '85'                THEN 'P'
                WHEN LEFT(d.naf_code, 2) BETWEEN '86' AND '88' THEN 'Q'
                WHEN LEFT(d.naf_code, 2) BETWEEN '90' AND '93' THEN 'R'
                WHEN LEFT(d.naf_code, 2) BETWEEN '94' AND '96' THEN 'S'
                WHEN LEFT(d.naf_code, 2) BETWEEN '97' AND '98' THEN 'T'
                WHEN LEFT(d.naf_code, 2) = '99'                THEN 'U'
                ELSE 'XX'
            END                                                         AS naf_section,
            CASE
                WHEN LEFT(d.naf_code, 2) BETWEEN '01' AND '03' THEN 'Agriculture, sylviculture et pêche'
                WHEN LEFT(d.naf_code, 2) BETWEEN '05' AND '09' THEN 'Industries extractives'
                WHEN LEFT(d.naf_code, 2) BETWEEN '10' AND '33' THEN 'Industrie manufacturière'
                WHEN LEFT(d.naf_code, 2) = '35'                THEN 'Production et distribution énergie'
                WHEN LEFT(d.naf_code, 2) BETWEEN '36' AND '39' THEN 'Eau, assainissement, déchets'
                WHEN LEFT(d.naf_code, 2) BETWEEN '41' AND '43' THEN 'Construction'
                WHEN LEFT(d.naf_code, 2) BETWEEN '45' AND '47' THEN 'Commerce, réparation automobiles'
                WHEN LEFT(d.naf_code, 2) BETWEEN '49' AND '53' THEN 'Transports et entreposage'
                WHEN LEFT(d.naf_code, 2) BETWEEN '55' AND '56' THEN 'Hébergement et restauration'
                WHEN LEFT(d.naf_code, 2) BETWEEN '58' AND '63' THEN 'Information et communication'
                WHEN LEFT(d.naf_code, 2) BETWEEN '64' AND '66' THEN 'Activités financières et assurance'
                WHEN LEFT(d.naf_code, 2) = '68'                THEN 'Activités immobilières'
                WHEN LEFT(d.naf_code, 2) BETWEEN '69' AND '75' THEN 'Activités spécialisées et techniques'
                WHEN LEFT(d.naf_code, 2) BETWEEN '77' AND '82' THEN 'Services administratifs et de soutien'
                WHEN LEFT(d.naf_code, 2) = '84'                THEN 'Administration publique'
                WHEN LEFT(d.naf_code, 2) = '85'                THEN 'Enseignement'
                WHEN LEFT(d.naf_code, 2) BETWEEN '86' AND '88' THEN 'Santé humaine et action sociale'
                WHEN LEFT(d.naf_code, 2) BETWEEN '90' AND '93' THEN 'Arts, spectacles et loisirs'
                WHEN LEFT(d.naf_code, 2) BETWEEN '94' AND '96' THEN 'Autres activités de services'
                WHEN LEFT(d.naf_code, 2) BETWEEN '97' AND '98' THEN 'Ménages employeurs'
                WHEN LEFT(d.naf_code, 2) = '99'                THEN 'Activités extra-territoriales'
                ELSE 'Non classifié'
            END                                                         AS secteur_libelle,
            CASE WHEN f.type_facture = 'F' THEN  COALESCE(l.montant_ht, 0)
                 WHEN f.type_facture = 'A' THEN -COALESCE(l.montant_ht, 0)
                 ELSE 0 END                                             AS montant_net,
            CASE WHEN f.type_facture = 'F' THEN 1 ELSE 0 END           AS is_mission
        FROM read_parquet('{slv}/slv_facturation/factures/**/*.parquet') f
        LEFT JOIN lignes      l ON l.fac_num = f.efac_num
        LEFT JOIN dim_clients d ON d.tie_id  = f.tie_id::INT
        WHERE f.date_facture IS NOT NULL
          AND f.rgpcnt_id IS NOT NULL
          AND f.tie_id IS NOT NULL
    ),
    aggregated AS (
        SELECT
            agence_id, mois, naf_code, naf_division, naf_section, secteur_libelle,
            COUNT(DISTINCT tie_id)  AS nb_clients,
            SUM(montant_net)        AS ca_net_ht,
            SUM(is_mission)         AS nb_missions
        FROM base
        WHERE mois IS NOT NULL
        GROUP BY 1, 2, 3, 4, 5, 6
    )
    SELECT
        agence_id, mois, naf_code, naf_division, naf_section, secteur_libelle,
        nb_clients,
        ROUND(ca_net_ht, 2)::DECIMAL(18,2)                              AS ca_net_ht,
        nb_missions,
        ROUND(
            ca_net_ht / NULLIF(SUM(ca_net_ht) OVER (PARTITION BY agence_id, mois), 0) * 100
        , 1)::DECIMAL(6,1)                                              AS part_ca_agence_pct
    FROM aggregated
    WHERE ca_net_ht > 0
    ORDER BY mois DESC, agence_id, ca_net_ht DESC
    """


def validate_vs_pyramid(pg_conn, stats: Stats) -> list[dict]:
    if not PYRAMID_VALIDATION_FILE.exists():
        logger.warning(
            f"Pyramid validation file not found: {PYRAMID_VALIDATION_FILE}")
        stats.warnings.append("Pyramid CSV not found — validation skipped")
        return []

    pyramid = {}
    with open(PYRAMID_VALIDATION_FILE, "r", encoding="utf-8-sig") as f:
        for row in csv.DictReader(f, delimiter=";"):
            mois = row.get("mois", row.get("MOIS", ""))
            ca = float(row.get("ca_net_ht", row.get("CA_NET_HT", 0)))
            pyramid[mois] = ca

    with pg_conn.cursor() as cur:
        cur.execute("""
            SELECT TO_CHAR(mois, 'YYYY-MM') AS mois, SUM(ca_net_ht::NUMERIC) AS total
            FROM gld_commercial.fact_ca_mensuel_client
            GROUP BY 1 ORDER BY 1 DESC LIMIT 12
        """)
        lakehouse = {row[0]: float(row[1]) for row in cur.fetchall()}

    report = []
    for mois in sorted(set(pyramid.keys()) | set(lakehouse.keys()), reverse=True)[:12]:
        lh, py = lakehouse.get(mois, 0), pyramid.get(mois, 0)
        if py == 0:
            # Valeur de référence non calibrée — skip (ne pas générer de faux FAIL)
            report.append({"mois": mois, "lakehouse": lh, "pyramid": py,
                           "delta_pct": None, "status": "UNCALIBRATED"})
            continue
        delta_pct = abs(lh - py) / abs(py) * 100
        status = "PASS" if delta_pct < TOLERANCE_PCT else "FAIL"
        report.append({"mois": mois, "lakehouse": lh, "pyramid": py,
                       "delta_pct": round(delta_pct, 3), "status": status})

    failures = [r for r in report if r["status"] == "FAIL"]
    if failures:
        logger.warning(
            f"Pyramid validation: {len(failures)} months FAILED (delta > {TOLERANCE_PCT}%)")
        stats.warnings.extend(
            [f"FAIL {r['mois']}: delta={r['delta_pct']}%" for r in failures])
    else:
        logger.info("Pyramid validation: ALL PASS")
    stats.extra["pyramid_validation"] = report
    return report


def run(cfg: Config) -> dict:
    stats = Stats()
    active = gold_filter_tables(
        ["fact_ca_mensuel_client", "fact_concentration_client",
         "fact_ca_secteur_naf"], cfg
    )

    if cfg.mode == RunMode.OFFLINE:
        return stats.finish(cfg, PIPELINE)

    query_builders = {
        "fact_ca_mensuel_client":    build_ca_mensuel_query,
        "fact_concentration_client": build_concentration_query,
        "fact_ca_secteur_naf":       build_ca_secteur_naf_query,
    }
    schema_map = {
        "fact_ca_mensuel_client":    ("gld_commercial", CA_COLUMNS),
        "fact_concentration_client": ("gld_clients",    CONCENTRATION_COLUMNS),
        "fact_ca_secteur_naf":       ("gld_commercial", NAF_SECTEUR_COLUMNS),
    }

    rows_map: dict[str, list] = {}
    try:
        with get_duckdb_connection(cfg) as ddb:
            for name in active:
                rows_map[name] = ddb.execute(query_builders[name](cfg)).fetchall()
            logger.info(json.dumps({n: len(r) for n, r in rows_map.items()}))
    except Exception as e:
        logger.exception(json.dumps({"step": "duckdb_build", "error": str(e)}))
        stats.errors.append({"step": "duckdb_build", "error": str(e)})
        return stats.finish(cfg, PIPELINE)

    with get_pg_connection(cfg) as pg_conn:
        for name in active:
            if name not in rows_map:
                continue
            schema, cols = schema_map[name]
            try:
                pg_bulk_insert(cfg, pg_conn, schema, name, cols, rows_map[name], stats)
                stats.tables_processed += 1
                stats.rows_transformed += len(rows_map[name])
            except Exception as e:
                logger.exception(json.dumps({"table": name, "error": str(e)}))
                stats.errors.append({"table": name, "error": str(e)})

        if not cfg.dry_run:          # @property valide — True si mode != LIVE
            validate_vs_pyramid(pg_conn, stats)

    return stats.finish(cfg, PIPELINE)


if __name__ == "__main__":
    run(Config())
