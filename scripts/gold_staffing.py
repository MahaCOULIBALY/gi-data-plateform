"""gold_staffing.py — Silver → Gold fact_activite_int + fact_missions_detail.
Phase 2 · GI Data Lakehouse · Manifeste v2.0

=== CORRECTIONS SESSION 2 (audit Silver→Gold) ===
- m.MISS_DATEDEBUT → m.date_debut  (Silver alias canonical)
- m.MISS_DATEFIN   → m.date_fin
- h.RHD_BASEPAYE   → h.base_paye
- h.RHD_BASEFACT   → h.base_fact
- h.RHD_TAUXFACT   → h.taux_fact
- c.CNT_TAUXPAYE   → c.taux_horaire_paye
- c.CNT_TAUXFACT   → c.taux_horaire_fact
- c.CNT_DATEDEBUT  → c.date_debut  (contrats alias)
- c.CNT_DATEFIN    → c.date_fin    (contrats alias)
- c.MET_ID         → c.met_id      (case normalization)
- m.PRH_BTS        → supprimé (PRH_BTS absent Silver missions)
- Jointure heures via fac_num au lieu de PRH_BTS
"""
import sys
import logging
from shared import Config, Stats, get_duckdb_connection, get_pg_connection, pg_bulk_insert, logger


def build_activite_query(cfg: Config) -> str:
    silver = f"s3://{cfg.bucket_silver}"
    return f"""
    WITH missions AS (
        SELECT * FROM read_parquet('{silver}/slv_missions/missions/**/*.parquet', hive_partitioning=true)
    ),
    heures AS (
        SELECT * FROM read_parquet('{silver}/slv_temps/heures_detail/**/*.parquet', hive_partitioning=true)
    ),
    releves AS (
        SELECT * FROM read_parquet('{silver}/slv_temps/releves_heures/**/*.parquet', hive_partitioning=true)
    ),
    dim_int AS (
        SELECT * FROM read_parquet('{silver}/slv_interimaires/dim_interimaires/**/*.parquet', hive_partitioning=true)
        WHERE is_current = true
    ),
    cal AS (
        SELECT DISTINCT DATE_TRUNC('month', d::DATE) AS mois, 22 * 7.0 AS heures_dispo_mois
        FROM generate_series('2020-01-01'::DATE, '2027-12-31'::DATE, INTERVAL '1 month') t(d)
    ),
    base AS (
        SELECT
            di.interimaire_sk,
            m.per_id::INT AS per_id,
            DATE_TRUNC('month', TRY_CAST(m.date_debut AS DATE)) AS mois,
            m.cnt_id::INT AS cnt_id,
            m.tie_id::INT AS tie_id,
            m.rgpcnt_id::INT AS rgpcnt_id,
            h.base_paye::DECIMAL(10,2)                           AS base_paye,
            h.base_fact::DECIMAL(10,2)                           AS base_fact,
            h.taux_fact::DECIMAL(10,4)                           AS taux_fact
        FROM missions m
        LEFT JOIN releves r ON r.per_id = m.per_id AND r.cnt_id = m.cnt_id
        LEFT JOIN heures h ON h.prh_bts = r.prh_bts
        LEFT JOIN dim_int di ON di.per_id = m.per_id::INT
        WHERE m.per_id IS NOT NULL AND m.date_debut IS NOT NULL
    )
    SELECT
        interimaire_sk,
        per_id,
        mois,
        COUNT(DISTINCT cnt_id) AS nb_missions,
        COUNT(DISTINCT rgpcnt_id) AS nb_agences,
        COUNT(DISTINCT tie_id) AS nb_clients,
        COALESCE(SUM(base_paye), 0) AS heures_travaillees,
        COALESCE(c.heures_dispo_mois, 154.0) AS heures_disponibles,
        CASE WHEN c.heures_dispo_mois > 0
             THEN ROUND(COALESCE(SUM(base_paye), 0) / c.heures_dispo_mois, 4)
             ELSE 0 END AS taux_occupation,
        COALESCE(SUM(base_fact * taux_fact), 0) AS ca_genere
    FROM base b
    LEFT JOIN cal c ON c.mois = b.mois
    WHERE mois IS NOT NULL
    GROUP BY interimaire_sk, per_id, mois, c.heures_dispo_mois
    """


def build_missions_detail_query(cfg: Config) -> str:
    silver = f"s3://{cfg.bucket_silver}"
    return f"""
    WITH missions AS (
        SELECT * FROM read_parquet('{silver}/slv_missions/missions/**/*.parquet', hive_partitioning=true)
    ),
    contrats AS (
        SELECT * FROM read_parquet('{silver}/slv_missions/contrats/**/*.parquet', hive_partitioning=true)
    ),
    releves AS (
        SELECT * FROM read_parquet('{silver}/slv_temps/releves_heures/**/*.parquet', hive_partitioning=true)
    ),
    heures AS (
        SELECT prh_bts, SUM(base_paye::DECIMAL(10,2)) AS total_heures
        FROM read_parquet('{silver}/slv_temps/heures_detail/**/*.parquet', hive_partitioning=true)
        GROUP BY prh_bts
    )
    SELECT
        MD5(CONCAT(m.per_id::VARCHAR, '|', m.cnt_id::VARCHAR, '|', m.tie_id::VARCHAR)) AS mission_sk,
        m.per_id::INT AS per_id,
        m.cnt_id::INT AS cnt_id,
        m.tie_id::INT AS tie_id,
        m.rgpcnt_id::INT AS agence_id,
        TRY_CAST(c.met_id AS INT) AS metier_id,
        TRY_CAST(c.date_debut AS DATE) AS date_debut,
        TRY_CAST(c.date_fin AS DATE) AS date_fin,
        DATEDIFF('day', TRY_CAST(c.date_debut AS DATE), TRY_CAST(c.date_fin AS DATE)) AS duree_jours,
        c.taux_horaire_paye::DECIMAL(10,4) AS taux_horaire_paye,
        c.taux_horaire_fact::DECIMAL(10,4) AS taux_horaire_fact,
        c.taux_horaire_fact::DECIMAL(10,4) - c.taux_horaire_paye::DECIMAL(10,4) AS marge_horaire,
        COALESCE(h.total_heures, 0) AS heures_totales,
        COALESCE(h.total_heures, 0) * c.taux_horaire_fact::DECIMAL(10,4) AS ca_mission,
        COALESCE(h.total_heures, 0) * c.taux_horaire_paye::DECIMAL(10,4) AS cout_mission,
        COALESCE(h.total_heures, 0) * (c.taux_horaire_fact::DECIMAL(10,4) - c.taux_horaire_paye::DECIMAL(10,4)) AS marge_mission,
        CASE WHEN COALESCE(h.total_heures, 0) * c.taux_horaire_fact::DECIMAL(10,4) > 0
             THEN ROUND((COALESCE(h.total_heures, 0) * (c.taux_horaire_fact::DECIMAL(10,4) - c.taux_horaire_paye::DECIMAL(10,4)))
                  / (COALESCE(h.total_heures, 0) * c.taux_horaire_fact::DECIMAL(10,4)), 4)
             ELSE 0 END AS taux_marge
    FROM missions m
    LEFT JOIN contrats c ON c.per_id = m.per_id AND c.cnt_id = m.cnt_id
    -- PRH_BTS absent Silver missions → jointure via releves (per_id+cnt_id)
    LEFT JOIN releves r ON r.per_id = m.per_id AND r.cnt_id = m.cnt_id
    LEFT JOIN heures h ON h.prh_bts = r.prh_bts
    WHERE m.per_id IS NOT NULL AND m.cnt_id IS NOT NULL
    """


def run(cfg: Config) -> dict:
    stats = Stats()
    activite_cols = ["interimaire_sk", "per_id", "mois", "nb_missions", "nb_agences",
                     "nb_clients", "heures_travaillees", "heures_disponibles", "taux_occupation", "ca_genere"]
    missions_cols = ["mission_sk", "per_id", "cnt_id", "tie_id", "agence_id", "metier_id",
                     "date_debut", "date_fin", "duree_jours", "taux_horaire_paye", "taux_horaire_fact",
                     "marge_horaire", "heures_totales", "ca_mission", "cout_mission", "marge_mission", "taux_marge"]

    with get_duckdb_connection(cfg) as ddb:
        activite_rows = ddb.execute(build_activite_query(cfg)).fetchall()
        missions_rows = ddb.execute(
            build_missions_detail_query(cfg)).fetchall()
        logger.info(
            f"fact_activite_int: {len(activite_rows)} rows | fact_missions_detail: {len(missions_rows)} rows")

    with get_pg_connection(cfg) as pg:
        pg_bulk_insert(cfg, pg, "gld_staffing", "fact_activite_int",
                       activite_cols, activite_rows, stats)
        pg_bulk_insert(cfg, pg, "gld_staffing", "fact_missions_detail",
                       missions_cols, missions_rows, stats)

    stats.tables_processed = 2
    stats.rows_transformed = len(activite_rows) + len(missions_rows)
    stats.extra["activite_rows"] = len(activite_rows)
    stats.extra["missions_rows"] = len(missions_rows)
    return stats.finish()


if __name__ == "__main__":
    cfg = Config()
    run(cfg)
