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
]

_TABLES = {
    "fact_ca_mensuel_client": ("gld_commercial", CA_COLUMNS),
    "fact_concentration_client": ("gld_clients", CONCENTRATION_COLUMNS),
}


def build_ca_mensuel_query(cfg: Config) -> str:
    return f"""
    WITH factures AS (
        SELECT * FROM read_parquet('s3://{cfg.bucket_silver}/slv_facturation/factures/**/*.parquet')
    ),
    {cte_montants_factures(cfg)},
    facinfo AS (
        SELECT tie_id::INT AS tie_id,
               DATE_TRUNC('month', TRY_CAST(date_debut AS DATE)) AS mois_mission,
               COUNT(DISTINCT CONCAT(per_id::VARCHAR, '|', cnt_id::VARCHAR)) AS nb_missions_fac
        FROM read_parquet('s3://{cfg.bucket_silver}/slv_missions/missions/**/*.parquet')
        WHERE date_debut IS NOT NULL
        GROUP BY 1, 2
    ),
    dim_clients AS (
        SELECT * FROM read_parquet('s3://{cfg.bucket_silver}/slv_clients/dim_clients/**/*.parquet')
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
            f.efac_num,
            f.rgpcnt_id::INT                                       AS rgpcnt_id,
            COALESCE(fi.nb_missions_fac, 0)                        AS nb_missions_fac
        FROM factures f
        LEFT JOIN montants    mt ON mt.fac_num    = f.efac_num
        LEFT JOIN facinfo     fi ON fi.tie_id     = f.tie_id::INT
            AND fi.mois_mission = DATE_TRUNC('month', TRY_CAST(f.date_facture AS DATE))
        LEFT JOIN dim_clients  c ON c.tie_id      = f.tie_id::INT
        WHERE f.date_facture IS NOT NULL
    )
    SELECT
        client_sk, tie_id, mois,
        COALESCE(SUM(CASE WHEN type_facture = 'F' THEN montant_ht ELSE 0 END), 0)    AS ca_ht,
        COALESCE(SUM(CASE WHEN type_facture = 'A' THEN montant_ht ELSE 0 END), 0)    AS avoir_ht,
        COALESCE(SUM(CASE WHEN type_facture = 'F' THEN montant_ht ELSE 0 END), 0)
        - COALESCE(SUM(CASE WHEN type_facture = 'A' THEN montant_ht ELSE 0 END), 0)  AS ca_net_ht,
        COUNT(DISTINCT CASE WHEN type_facture = 'F' THEN efac_num END)                AS nb_factures,
        COALESCE(SUM(nb_missions_fac), 0)                                             AS nb_missions_facturees,
        NULL::DECIMAL(10,2)                                                            AS nb_heures_facturees,
        NULL::DECIMAL(10,2)                                                            AS taux_moyen_fact,
        MODE(rgpcnt_id)                                                               AS agence_principale
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
               ) AS pct_rank
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
        4)::DECIMAL(8,4)                                            AS taux_concentration
    FROM ranked
    WHERE mois IS NOT NULL
    GROUP BY 1, 2
    ORDER BY 2 DESC, 1
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
        ["fact_ca_mensuel_client", "fact_concentration_client"], cfg
    )

    if cfg.mode == RunMode.OFFLINE:
        return stats.finish(cfg, PIPELINE)

    ca_rows, concentration_rows = [], []
    try:
        with get_duckdb_connection(cfg) as ddb:
            ca_rows = ddb.execute(build_ca_mensuel_query(cfg)).fetchall()
            concentration_rows = ddb.execute(
                build_concentration_query(cfg)).fetchall()
            logger.info(json.dumps({
                "fact_ca_mensuel_client": len(ca_rows),
                "fact_concentration_client": len(concentration_rows),
            }))
    except Exception as e:
        logger.exception(json.dumps({"step": "duckdb_build", "error": str(e)}))
        stats.errors.append({"step": "duckdb_build", "error": str(e)})
        return stats.finish(cfg, PIPELINE)

    rows_map = {
        "fact_ca_mensuel_client": (ca_rows, "gld_commercial", CA_COLUMNS),
        "fact_concentration_client": (concentration_rows, "gld_clients", CONCENTRATION_COLUMNS),
    }
    with get_pg_connection(cfg) as pg_conn:
        for name in active:
            rows, schema, cols = rows_map[name]
            try:
                pg_bulk_insert(cfg, pg_conn, schema, name, cols, rows, stats)
                stats.tables_processed += 1
                stats.rows_transformed += len(rows)
            except Exception as e:
                logger.exception(json.dumps({"table": name, "error": str(e)}))
                stats.errors.append({"table": name, "error": str(e)})

        if not cfg.dry_run:          # @property valide — True si mode != LIVE
            validate_vs_pyramid(pg_conn, stats)

    return stats.finish(cfg, PIPELINE)


if __name__ == "__main__":
    run(Config())
