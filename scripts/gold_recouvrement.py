"""gold_recouvrement.py — Gold · Recouvrement & DSO : 2 tables → gld_operationnel.
Phase 3 · GI Data Lakehouse · Manifeste v2.0
Sources Silver :
  - slv_facturation/factures          (encours, date_echeance)
  - slv_facturation/lignes_factures   (montants HT via WTLFAC)
  - slv_clients/facturation_detail    (montant_regle — NULL en attendant probe WTEFAC)

Tables Gold produites :
  - gld_operationnel.fact_dso_client   : DSO par agence/client/mois
  - gld_operationnel.fact_balance_agee : balance âgée par agence/mois/tranche

Note DSO : montant_regle NULL dans facturation_detail (colonnes EFAC_MNTPAI/EFAC_TYPEPAI
  absentes du DDL Evolia probe 2026-03-26). encours_ht = SUM(factures) - 0 jusqu'à probe.
  Quand EFAC_MNTPAI confirmée et alimentée, le calcul sera automatiquement correct.

Snapshot : DATE_TRUNC('month', CURRENT_DATE) — premier jour du mois courant.
"""
import json

from shared import (
    Config, RunMode, Stats,
    get_duckdb_connection, get_pg_connection, pg_bulk_insert,
    filter_tables, logger,
)

PIPELINE = "gold_recouvrement"
DOMAIN = "gld_operationnel"

COLS = {
    "fact_dso_client": [
        "agence_id", "tie_id", "mois",
        "encours_ht", "dso_jours", "nb_factures_ouvertes", "montant_echu",
    ],
    "fact_balance_agee": [
        "agence_id", "mois", "tranche", "montant_echu", "nb_factures",
    ],
}


def build_dso_query(cfg: Config) -> str:
    """DSO par agence/client/mois.

    encours_ht   = SUM(factures HT net avoirs) - SUM(montant_regle)
    dso_jours    = encours_ht / NULLIF(ca_mensuel / nb_jours_mois, 0)
    montant_echu = encours sur factures dont date_echeance < snapshot_date
    snapshot     = DATE_TRUNC('month', CURRENT_DATE)
    """
    s = f"s3://{cfg.bucket_silver}"
    return f"""
WITH
factures_mensuel AS (
    SELECT
        f.rgpcnt_id                                                 AS agence_id,
        f.tie_id::INT                                               AS tie_id,
        DATE_TRUNC('month', f.date_facture)::DATE                   AS mois,
        SUM(CASE WHEN f.type_facture = 'F' THEN COALESCE(l.montant, 0)
                 WHEN f.type_facture = 'A' THEN -COALESCE(l.montant, 0)
                 ELSE 0 END)                                        AS ca_net_ht,
        COUNT(DISTINCT CASE WHEN f.type_facture = 'F'
                            THEN f.efac_num END)                    AS nb_factures
    FROM read_parquet('{s}/slv_facturation/factures/**/*.parquet') f
    JOIN read_parquet('{s}/slv_facturation/lignes_factures/**/*.parquet') l
        ON l.fac_num = f.efac_num
    WHERE f.date_facture IS NOT NULL
      AND f.rgpcnt_id IS NOT NULL
      AND f.tie_id IS NOT NULL
    GROUP BY 1, 2, 3
),
reglements AS (
    SELECT
        rgpcnt_id                                                   AS agence_id,
        tie_id,
        DATE_TRUNC('month', date_paiement)::DATE                    AS mois,
        SUM(COALESCE(montant_regle, 0))                             AS montant_regle_total
    FROM read_parquet('{s}/slv_clients/facturation_detail/**/*.parquet')
    WHERE date_paiement IS NOT NULL
      AND rgpcnt_id IS NOT NULL
      AND tie_id IS NOT NULL
    GROUP BY 1, 2, 3
),
echus AS (
    -- Montant échu : factures dont date_echeance < snapshot courant
    SELECT
        f.rgpcnt_id                                                 AS agence_id,
        f.tie_id::INT                                               AS tie_id,
        DATE_TRUNC('month', f.date_facture)::DATE                   AS mois,
        SUM(CASE WHEN f.type_facture = 'F' THEN COALESCE(l.montant, 0)
                 WHEN f.type_facture = 'A' THEN -COALESCE(l.montant, 0)
                 ELSE 0 END)                                        AS montant_echu
    FROM read_parquet('{s}/slv_facturation/factures/**/*.parquet') f
    JOIN read_parquet('{s}/slv_facturation/lignes_factures/**/*.parquet') l
        ON l.fac_num = f.efac_num
    WHERE f.date_echeance < DATE_TRUNC('month', CURRENT_DATE)::DATE
      AND f.date_facture IS NOT NULL
      AND f.rgpcnt_id IS NOT NULL
      AND f.tie_id IS NOT NULL
    GROUP BY 1, 2, 3
)
SELECT
    fm.agence_id,
    fm.tie_id,
    fm.mois,
    ROUND(
        fm.ca_net_ht - COALESCE(r.montant_regle_total, 0)
    , 2)::DECIMAL(18,2)                                             AS encours_ht,
    ROUND(
        (fm.ca_net_ht - COALESCE(r.montant_regle_total, 0))
        / NULLIF(
            fm.ca_net_ht
            / NULLIF(
                DATEDIFF('day', fm.mois,
                         (fm.mois + INTERVAL '1 month')::DATE)::DECIMAL
              , 0)
          , 0)
    , 1)::DECIMAL(8,1)                                              AS dso_jours,
    fm.nb_factures                                                  AS nb_factures_ouvertes,
    ROUND(COALESCE(e.montant_echu, 0), 2)::DECIMAL(18,2)           AS montant_echu
FROM factures_mensuel fm
LEFT JOIN reglements r
    ON  r.agence_id = fm.agence_id
    AND r.tie_id    = fm.tie_id
    AND r.mois      = fm.mois
LEFT JOIN echus e
    ON  e.agence_id = fm.agence_id
    AND e.tie_id    = fm.tie_id
    AND e.mois      = fm.mois
WHERE fm.agence_id IS NOT NULL
  AND fm.tie_id IS NOT NULL
ORDER BY fm.mois DESC, fm.agence_id, fm.tie_id
"""


def build_balance_agee_query(cfg: Config) -> str:
    """Balance âgée par agence/mois/tranche.

    Retard calculé au snapshot (DATE_TRUNC('month', CURRENT_DATE)).
    Tranches : '0-30j' / '31-60j' / '61-90j' / '>90j'.
    Seules les factures échues (date_echeance < snapshot) sont incluses.
    """
    s = f"s3://{cfg.bucket_silver}"
    return f"""
WITH
snapshot AS (
    SELECT DATE_TRUNC('month', CURRENT_DATE)::DATE AS snapshot_date
),
factures_echus AS (
    SELECT
        f.rgpcnt_id                                                 AS agence_id,
        DATE_TRUNC('month', f.date_facture)::DATE                   AS mois,
        f.efac_num,
        DATEDIFF('day', f.date_echeance, s.snapshot_date)          AS retard_jours,
        SUM(CASE WHEN f.type_facture = 'F' THEN COALESCE(l.montant, 0)
                 WHEN f.type_facture = 'A' THEN -COALESCE(l.montant, 0)
                 ELSE 0 END)                                        AS montant_echu
    FROM read_parquet('{s}/slv_facturation/factures/**/*.parquet') f
    JOIN read_parquet('{s}/slv_facturation/lignes_factures/**/*.parquet') l
        ON l.fac_num = f.efac_num
    CROSS JOIN snapshot s
    WHERE f.date_echeance < s.snapshot_date
      AND f.date_facture IS NOT NULL
      AND f.rgpcnt_id IS NOT NULL
    GROUP BY 1, 2, 3, 4
)
SELECT
    agence_id,
    mois,
    CASE
        WHEN retard_jours BETWEEN 0  AND 30 THEN '0-30j'
        WHEN retard_jours BETWEEN 31 AND 60 THEN '31-60j'
        WHEN retard_jours BETWEEN 61 AND 90 THEN '61-90j'
        ELSE '>90j'
    END                                                             AS tranche,
    ROUND(SUM(montant_echu), 2)::DECIMAL(18,2)                     AS montant_echu,
    COUNT(DISTINCT efac_num)::INT                                   AS nb_factures
FROM factures_echus
WHERE agence_id IS NOT NULL
GROUP BY 1, 2, 3
ORDER BY mois DESC, agence_id, tranche
"""


def run(cfg: Config) -> dict:
    stats = Stats()

    all_tables = list(COLS)
    active = filter_tables(all_tables, cfg)

    if cfg.mode in (RunMode.OFFLINE, RunMode.PROBE):
        for name in active:
            logger.info(json.dumps(
                {"mode": cfg.mode.name, "table": name, "action": "skipped"}))
            stats.tables_processed += 1
        return stats.finish(cfg, PIPELINE)

    queries = {
        "fact_dso_client":   build_dso_query(cfg),
        "fact_balance_agee": build_balance_agee_query(cfg),
    }

    with get_duckdb_connection(cfg) as ddb:
        with get_pg_connection(cfg) as pg:
            for name in active:
                try:
                    rows = ddb.execute(queries[name]).fetchall()
                    with pg.cursor() as cur:
                        cur.execute(f"TRUNCATE TABLE {DOMAIN}.{name}")
                    pg.commit()
                    pg_bulk_insert(cfg, pg, DOMAIN, name, COLS[name], rows, stats)
                    stats.tables_processed += 1
                    stats.rows_transformed += len(rows)
                    logger.info(json.dumps(
                        {"table": name, "rows": len(rows), "status": "ok"}))
                except Exception as e:
                    logger.exception(json.dumps(
                        {"table": name, "error": str(e)}))
                    stats.errors.append({"table": name, "error": str(e)})

    return stats.finish(cfg, PIPELINE)


if __name__ == "__main__":
    run(Config())
