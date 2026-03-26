"""gold_operationnel.py — Gold · Opérationnel : 4 tables → gld_operationnel.
Phase 0 · GI Data Lakehouse · Manifeste v2.0
Sources Silver : slv_temps/releves_heures, heures_detail, slv_missions/commandes + missions
Schemas Gold   : gld_operationnel

=== CORRECTIONS SESSION 3 (audit Gold) ===
- G-OP-B01 : TRUNCATE avant chaque pg_bulk_insert (idempotence PG — 4 tables)
- G-OP-M01 : stats.tables_processed + rows_transformed incrémentés dans les boucles
- G-OP-M02 : guard RunMode.OFFLINE/PROBE ajouté en début de run()
- G-OP-m01 : filter_tables importé et appliqué
- NOTE G-OP-M03 : rh.tie_id à confirmer DESCRIBE silver_rh en PROBE
                  (fallback JOIN missions prévu si absent)

# MIGRÉ : iceberg_scan → read_parquet(s3://gi-poc-silver/slv_*) (D01)
"""
import json

from shared import (
    Config, RunMode, Stats,
    get_duckdb_connection, get_pg_connection, pg_bulk_insert,
    filter_tables, logger,
)

PIPELINE = "gold_operationnel"
DOMAIN = "gld_operationnel"

SQL = {
    "fact_heures_hebdo": """
        SELECT
            rh.rgpcnt_id                                        AS agence_id,
            rh.tie_id                                           AS tie_id,
            -- G-OP-M03 : tie_id à confirmer par DESCRIBE silver_rh en PROBE
            -- Si absent du Silver releves_heures, remplacer par :
            -- m.tie_id avec JOIN silver_missions m ON m.per_id=rh.per_id AND m.cnt_id=rh.cnt_id
            DATE_TRUNC('week', rh.date_modif)::DATE             AS semaine_debut,
            SUM(rd.base_paye)                                   AS heures_paye,
            SUM(rd.base_fact)                                   AS heures_fact,
            COUNT(DISTINCT rh.prh_bts)                          AS nb_releves
        FROM silver_rh rh
        JOIN silver_rd rd ON rd.prh_bts = rh.prh_bts
        WHERE rh.valide = true
          AND rh.rgpcnt_id IS NOT NULL
          AND rh.tie_id IS NOT NULL
        GROUP BY 1, 2, 3
    """,
    "fact_commandes_pipeline": """
        SELECT
            rgpcnt_id                                           AS agence_id,
            DATE_TRUNC('week', cmd_date)::DATE                  AS semaine_debut,
            COUNT(*)                                            AS nb_commandes,
            COUNT(*) FILTER (WHERE stat_code = 'P')             AS nb_pourvues,
            COUNT(*) FILTER (WHERE stat_code = 'O')             AS nb_ouvertes,
            COALESCE(
                COUNT(*) FILTER (WHERE stat_code = 'P')::DECIMAL
                / NULLIF(COUNT(*), 0), 0
            )                                                   AS taux_satisfaction
        FROM silver_cmd
        WHERE rgpcnt_id IS NOT NULL AND cmd_date IS NOT NULL
        GROUP BY 1, 2
    """,
}

COLS = {
    "fact_heures_hebdo": ["agence_id", "tie_id", "semaine_debut",
                          "heures_paye", "heures_fact", "nb_releves"],
    "fact_commandes_pipeline": ["agence_id", "semaine_debut", "nb_commandes",
                                "nb_pourvues", "nb_ouvertes", "taux_satisfaction"],
}


def _register_views(ddb, cfg: Config) -> None:
    ddb.execute(
        f"CREATE OR REPLACE VIEW silver_rh  AS SELECT * FROM read_parquet("
        f"'s3://{cfg.bucket_silver}/slv_temps/releves_heures/**/*.parquet')"
    )
    ddb.execute(
        f"CREATE OR REPLACE VIEW silver_rd  AS SELECT * FROM read_parquet("
        f"'s3://{cfg.bucket_silver}/slv_temps/heures_detail/**/*.parquet')"
    )
    ddb.execute(
        f"CREATE OR REPLACE VIEW silver_cmd AS SELECT * FROM read_parquet("
        f"'s3://{cfg.bucket_silver}/slv_missions/commandes/**/*.parquet')"
    )


def build_delai_placement_query(cfg: Config) -> str:
    """Délai entre commande et début mission (categorie_delai, delai_placement_heures
    ajoutés Silver missions 2026-03-13 — à confirmer DESCRIBE en PROBE).
    """
    return f"""
    SELECT
        m.rgpcnt_id                                                AS agence_id,
        DATE_TRUNC('week', m.date_fin)::DATE                       AS semaine_debut,
        m.categorie_delai,
        COUNT(*)                                                   AS nb_missions,
        ROUND(AVG(m.delai_placement_heures), 2)::DECIMAL(10,2)    AS delai_moyen_heures,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY m.delai_placement_heures)
                                                                   ::DECIMAL(10,2) AS delai_median_heures
    FROM read_parquet('s3://{cfg.bucket_silver}/slv_missions/missions/**/*.parquet') m
    WHERE m.rgpcnt_id IS NOT NULL
      AND m.date_fin IS NOT NULL
      AND m.delai_placement_heures IS NOT NULL
    GROUP BY 1, 2, 3
    ORDER BY 2 DESC, 1
    """


def build_echus_hebdo_query(cfg: Config) -> str:
    """Montants hebdomadaires des factures par client selon date d'échéance.
    Jointure factures × lignes_factures (montants via WTLFAC — EFAC_MONTANTHT absent DDL).
    type_facture='F' = facture, 'A' = avoir (soustrait du montant net).
    """
    return f"""
    SELECT
        f.tie_id::INT                                                       AS tie_id,
        f.rgpcnt_id                                                         AS agence_id,
        DATE_TRUNC('week', f.date_echeance)::DATE                           AS semaine_debut,
        SUM(CASE WHEN f.type_facture = 'F' THEN COALESCE(l.montant, 0) ELSE 0 END)
                                                                            AS montant_factures,
        SUM(CASE WHEN f.type_facture = 'A' THEN COALESCE(l.montant, 0) ELSE 0 END)
                                                                            AS montant_avoirs,
        SUM(CASE WHEN f.type_facture = 'F' THEN  COALESCE(l.montant, 0)
                 WHEN f.type_facture = 'A' THEN -COALESCE(l.montant, 0)
                 ELSE 0 END)                                                AS montant_echu_ht,
        COUNT(DISTINCT CASE WHEN f.type_facture = 'F' THEN f.efac_num END) AS nb_factures
    FROM read_parquet('s3://{cfg.bucket_silver}/slv_facturation/factures/**/*.parquet') f
    JOIN read_parquet('s3://{cfg.bucket_silver}/slv_facturation/lignes_factures/**/*.parquet') l
        ON l.fac_num = f.efac_num
    WHERE f.date_echeance IS NOT NULL
      AND f.tie_id IS NOT NULL
      AND f.rgpcnt_id IS NOT NULL
    GROUP BY 1, 2, 3
    ORDER BY 3 DESC, 1
    """


def build_conformite_dpae_query(cfg: Config) -> str:
    """Taux DPAE par agence/mois (statut_dpae = MISS_FLAGDPAE, ecart_heures — à confirmer PROBE)."""
    return f"""
    SELECT
        m.rgpcnt_id                                                AS agence_id,
        DATE_TRUNC('month', m.date_fin)::DATE                      AS mois,
        COUNT(*)                                                   AS nb_missions,
        COUNT(*) FILTER (WHERE m.statut_dpae IS NOT NULL)          AS nb_dpae_transmises,
        COUNT(*) FILTER (WHERE m.statut_dpae IS NULL)              AS nb_dpae_manquantes,
        ROUND(
            COUNT(*) FILTER (WHERE m.statut_dpae IS NOT NULL)::DECIMAL
            / NULLIF(COUNT(*), 0),
        4)                                                         AS taux_conformite_dpae,
        ROUND(AVG(m.ecart_heures) FILTER (WHERE m.ecart_heures IS NOT NULL),
              2)::DECIMAL(10,2)                                    AS ecart_moyen_heures
    FROM read_parquet('s3://{cfg.bucket_silver}/slv_missions/missions/**/*.parquet') m
    WHERE m.rgpcnt_id IS NOT NULL AND m.date_fin IS NOT NULL
    GROUP BY 1, 2
    ORDER BY 2 DESC, 1
    """


def run(cfg: Config) -> dict:
    stats = Stats()

    all_tables = list(COLS) + ["fact_delai_placement", "fact_conformite_dpae", "fact_echus_hebdo"]
    active = filter_tables(all_tables, cfg)  # G-OP-m01

    if cfg.mode in (RunMode.OFFLINE, RunMode.PROBE):  # G-OP-M02
        for name in active:
            logger.info(json.dumps(
                {"mode": cfg.mode.name, "table": name, "action": "skipped"}))
            stats.tables_processed += 1
        return stats.finish(cfg, PIPELINE)

    with get_duckdb_connection(cfg) as ddb:
        try:
            _register_views(ddb, cfg)
        except Exception as e:
            logger.exception(json.dumps(
                {"step": "register_views", "error": str(e)}))
            stats.errors.append({"step": "register_views", "error": str(e)})
            return stats.finish(cfg, PIPELINE)

        _new_tables = {
            "fact_delai_placement": (
                build_delai_placement_query(cfg),
                ["agence_id", "semaine_debut", "categorie_delai",
                 "nb_missions", "delai_moyen_heures", "delai_median_heures"],
            ),
            "fact_conformite_dpae": (
                build_conformite_dpae_query(cfg),
                ["agence_id", "mois", "nb_missions", "nb_dpae_transmises",
                 "nb_dpae_manquantes", "taux_conformite_dpae", "ecart_moyen_heures"],
            ),
            "fact_echus_hebdo": (
                build_echus_hebdo_query(cfg),
                ["tie_id", "agence_id", "semaine_debut", "montant_factures",
                 "montant_avoirs", "montant_echu_ht", "nb_factures"],
            ),
        }

        with get_pg_connection(cfg) as pg:
            for name in ("fact_heures_hebdo", "fact_commandes_pipeline"):
                if name not in active:
                    continue
                try:
                    rows = ddb.execute(SQL[name]).fetchall()
                    # G-OP-B01 : idempotence PG
                    with pg.cursor() as cur:
                        cur.execute(f"TRUNCATE TABLE {DOMAIN}.{name}")
                    pg.commit()
                    pg_bulk_insert(cfg, pg, DOMAIN, name,
                                   COLS[name], rows, stats)
                    stats.tables_processed += 1          # G-OP-M01
                    stats.rows_transformed += len(rows)  # G-OP-M01
                    logger.info(json.dumps(
                        {"table": name, "rows": len(rows), "status": "ok"}))
                except Exception as e:
                    logger.exception(json.dumps(
                        {"table": name, "error": str(e)}))
                    stats.errors.append({"table": name, "error": str(e)})

            for name, (query, cols) in _new_tables.items():
                if name not in active:
                    continue
                try:
                    rows = ddb.execute(query).fetchall()
                    # G-OP-B01 : idempotence PG
                    with pg.cursor() as cur:
                        cur.execute(f"TRUNCATE TABLE {DOMAIN}.{name}")
                    pg.commit()
                    pg_bulk_insert(cfg, pg, DOMAIN, name, cols, rows, stats)
                    stats.tables_processed += 1          # G-OP-M01
                    stats.rows_transformed += len(rows)  # G-OP-M01
                    logger.info(json.dumps(
                        {"table": name, "rows": len(rows), "status": "ok"}))
                except Exception as e:
                    logger.exception(json.dumps(
                        {"table": name, "error": str(e)}))
                    stats.errors.append({"table": name, "error": str(e)})

    return stats.finish(cfg, PIPELINE)


if __name__ == "__main__":
    run(Config())
