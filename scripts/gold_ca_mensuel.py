"""gold_ca_mensuel.py — Silver → Gold fact_ca_mensuel_client + validation Pyramid.
Phase 1 · GI Data Lakehouse · Manifeste v2.0

=== CORRECTIONS SESSION 2 (audit Silver→Gold) ===
- f.EFAC_DATE    → f.date_facture   (Silver alias canonical)
- f.EFAC_TYPE    → f.type_facture
- f.EFAC_MONTANTHT → f.montant_ht  (NULL — B-02: reconstituer via lignes_factures)
- f.EFAC_NUM     → f.efac_num       (case normalization)
- h.RHD_BASEFACT → h.base_fact
- f.PRH_BTS      → JOIN supprimé (PRH_BTS absent DDL WTEFAC)
- Jointure heures via WTFACINFO/FAC_NUM au lieu de PRH_BTS direct
- UPPERCASE → lowercase pour cohérence Silver aliases
- TODO B-02: montant_ht est NULL en Silver → reconstitution via SUM(lfac_base * lfac_taux)
"""
import sys
import csv
import json
import logging
from pathlib import Path
from datetime import datetime, timezone

from shared import Config, Stats, get_duckdb_connection, get_pg_connection, pg_bulk_insert, logger
from gold_helpers import cte_montants_factures

PYRAMID_VALIDATION_FILE = Path("data/validation/pyramid_ca_mensuel.csv")
TOLERANCE_PCT = 0.5


def build_ca_mensuel_query(cfg: Config) -> str:
    """Agrégation Silver → fact_ca_mensuel_client.
    NOTE B-02: montant_ht est NULL en Silver (EFAC_MONTANTHT absent DDL).
    Reconstitution via lignes_factures: SUM(lfac_base * lfac_taux) par facture.
    """
    silver = f"s3://{cfg.bucket_silver}"
    return f"""
    WITH factures AS (
        SELECT * FROM read_parquet('{silver}/slv_facturation/factures/**/*.parquet', hive_partitioning=true)
    ),
    {cte_montants_factures(silver)},
    -- nb_missions_facturees via WTFACINFO (fac_num ↔ per_id) — évite JOIN missions × factures
    facinfo AS (
        SELECT fac_num,
               COUNT(DISTINCT per_id::INT || '|' || cnt_id::INT) AS nb_missions_fac
        FROM read_parquet('{silver}/slv_missions/facinfo/**/*.parquet', hive_partitioning=true)
        GROUP BY fac_num
    ),
    dim_clients AS (
        SELECT * FROM read_parquet('{silver}/slv_clients/dim_clients/**/*.parquet', hive_partitioning=true)
        WHERE is_current = true
    ),
    base AS (
        SELECT
            c.client_sk,
            f.tie_id::INT                                         AS tie_id,
            DATE_TRUNC('month', TRY_CAST(f.date_facture AS DATE)) AS mois,
            f.type_facture                                        AS type_facture,
            -- B-02: utiliser montant reconstitué depuis lignes
            COALESCE(mt.montant_ht_calc, 0)::DECIMAL(18,2)     AS montant_ht,
            f.efac_num,
            f.rgpcnt_id::INT                                      AS rgpcnt_id,
            COALESCE(fi.nb_missions_fac, 0)                       AS nb_missions_fac
        FROM factures f
        LEFT JOIN montants mt ON mt.fac_num = f.efac_num
        LEFT JOIN facinfo fi ON fi.fac_num = f.efac_num
        LEFT JOIN dim_clients c ON c.tie_id = f.tie_id::INT
        WHERE f.date_facture IS NOT NULL
    )
    SELECT
        client_sk,
        tie_id,
        mois,
        COALESCE(SUM(CASE WHEN type_facture = 'F' THEN montant_ht ELSE 0 END), 0)     AS ca_ht,
        COALESCE(SUM(CASE WHEN type_facture = 'A' THEN montant_ht ELSE 0 END), 0)     AS avoir_ht,
        COALESCE(SUM(CASE WHEN type_facture = 'F' THEN montant_ht ELSE 0 END), 0)
        - COALESCE(SUM(CASE WHEN type_facture = 'A' THEN montant_ht ELSE 0 END), 0)   AS ca_net_ht,
        COUNT(DISTINCT CASE WHEN type_facture = 'F' THEN efac_num END)                 AS nb_factures,
        COALESCE(SUM(nb_missions_fac), 0)                                               AS nb_missions_facturees,
        NULL::DECIMAL(10,2)                                                             AS nb_heures_facturees,
        NULL::DECIMAL(10,2)                                                             AS taux_moyen_fact,
        MODE(rgpcnt_id)                                                                AS agence_principale
    FROM base
    WHERE mois IS NOT NULL
    GROUP BY client_sk, tie_id, mois
    ORDER BY mois DESC, ca_net_ht DESC
    """


def validate_vs_pyramid(pg_conn, stats: Stats) -> list[dict]:
    """Compare Gold avec export Pyramid BI. Retourne rapport de validation."""
    if not PYRAMID_VALIDATION_FILE.exists():
        logger.warning(
            f"Pyramid validation file not found: {PYRAMID_VALIDATION_FILE}")
        stats.warnings.append("Pyramid CSV not found — validation skipped")
        return []

    # Charger Pyramid
    pyramid = {}
    with open(PYRAMID_VALIDATION_FILE, "r", encoding="utf-8-sig") as f:
        for row in csv.DictReader(f, delimiter=";"):
            mois = row.get("mois", row.get("MOIS", ""))
            ca = float(row.get("ca_net_ht", row.get("CA_NET_HT", 0)))
            pyramid[mois] = ca

    # Charger Lakehouse
    with pg_conn.cursor() as cur:
        cur.execute("""
            SELECT TO_CHAR(mois, 'YYYY-MM') AS mois, SUM(ca_net_ht::NUMERIC) AS total
            FROM gld_commercial.fact_ca_mensuel_client
            GROUP BY 1 ORDER BY 1 DESC LIMIT 12
        """)
        lakehouse = {row[0]: float(row[1]) for row in cur.fetchall()}

    report = []
    for mois in sorted(set(pyramid.keys()) | set(lakehouse.keys()), reverse=True)[:12]:
        lh = lakehouse.get(mois, 0)
        py = pyramid.get(mois, 0)
        delta_pct = abs(lh - py) / max(abs(py), 1) * 100
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


def build_concentration_query(cfg: Config) -> str:
    """Concentration CA client par agence/mois (indice Pareto).
    nb_clients_top20 = clients représentant les 20% premiers par CA.
    taux_concentration = part du CA portée par ces clients.
    """
    silver = f"s3://{cfg.bucket_silver}"
    return f"""
    WITH factures AS (
        SELECT * FROM read_parquet('{silver}/slv_facturation/factures/**/*.parquet',
                                   hive_partitioning=true)
        WHERE date_facture IS NOT NULL AND rgpcnt_id IS NOT NULL
    ),
    lignes AS (
        SELECT fac_num, COALESCE(SUM(lfac_mnt), 0) AS montant_ht
        FROM read_parquet('{silver}/slv_facturation/lignes_factures/**/*.parquet',
                          hive_partitioning=true)
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
        agence_id,
        mois,
        COUNT(DISTINCT tie_id)                                     AS nb_clients,
        COUNT(DISTINCT tie_id) FILTER (WHERE pct_rank <= 0.2)      AS nb_clients_top20,
        MAX(ca_total)::DECIMAL(18,2)                               AS ca_net_total,
        COALESCE(SUM(ca_net_ht) FILTER (WHERE pct_rank <= 0.2),
                 0)::DECIMAL(18,2)                                 AS ca_net_top20,
        ROUND(
            COALESCE(SUM(ca_net_ht) FILTER (WHERE pct_rank <= 0.2), 0)
            / NULLIF(MAX(ca_total), 0),
        4)::DECIMAL(8,4)                                           AS taux_concentration
    FROM ranked
    WHERE mois IS NOT NULL
    GROUP BY 1, 2
    ORDER BY 2 DESC, 1
    """


def run(cfg: Config) -> dict:
    stats = Stats()
    ca_columns = [
        "client_sk", "tie_id", "mois", "ca_ht", "avoir_ht", "ca_net_ht",
        "nb_factures", "nb_missions_facturees", "nb_heures_facturees",
        "taux_moyen_fact", "agence_principale",
    ]
    concentration_columns = [
        "agence_id", "mois", "nb_clients", "nb_clients_top20",
        "ca_net_total", "ca_net_top20", "taux_concentration",
    ]

    with get_duckdb_connection(cfg) as ddb:
        ca_rows = ddb.execute(build_ca_mensuel_query(cfg)).fetchall()
        concentration_rows = ddb.execute(build_concentration_query(cfg)).fetchall()
        logger.info(f"fact_ca_mensuel_client: {len(ca_rows)} rows | "
                    f"fact_concentration_client: {len(concentration_rows)} rows")
        stats.rows_transformed = len(ca_rows) + len(concentration_rows)

    with get_pg_connection(cfg) as pg_conn:
        pg_bulk_insert(cfg, pg_conn, "gld_commercial",
                       "fact_ca_mensuel_client", ca_columns, ca_rows, stats)
        pg_bulk_insert(cfg, pg_conn, "gld_clients",
                       "fact_concentration_client", concentration_columns,
                       concentration_rows, stats)
        if not cfg.dry_run:
            validate_vs_pyramid(pg_conn, stats)

    stats.tables_processed = 2
    return stats.finish()


if __name__ == "__main__":
    cfg = Config()
    run(cfg)
