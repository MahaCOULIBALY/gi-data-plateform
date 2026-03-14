"""gold_helpers.py — CTEs partagées entre les scripts Gold (DT-08).
Évite la duplication de la logique B-02 (reconstitution montant HT) dans 4 scripts.
"""


def cte_montants_factures(silver: str) -> str:
    """CTE B-02 : reconstitution montant HT depuis lignes_factures.
    EFAC_MONTANTHT est NULL en Silver (absent DDL Evolia) — montant_ht_calc = SUM(lfac_mnt).
    Usage : inclure dans un WITH et joindre sur fac_num = efac_num.
    """
    return f"""
    lignes_b02 AS (
        SELECT * FROM read_parquet('{silver}/slv_facturation/lignes_factures/**/*.parquet',
                                   hive_partitioning=true)
    ),
    montants AS (
        SELECT fac_num, COALESCE(SUM(lfac_mnt), 0)::DECIMAL(18,2) AS montant_ht_calc
        FROM lignes_b02
        GROUP BY fac_num
    )"""


def cte_heures_par_contrat(silver: str) -> str:
    """CTE DT-09 : heures pré-agrégées par (per_id, cnt_id).
    Résout le problème de doublons dû à N relevés par contrat (semaines distinctes).
    Usage : joindre sur hc.per_id = m.per_id AND hc.cnt_id = m.cnt_id.
    """
    return f"""
    heures_par_contrat AS (
        SELECT r.per_id, r.cnt_id,
               SUM(h.base_paye::DECIMAL(10,2)) AS h_paye,
               SUM(h.base_fact::DECIMAL(10,2)) AS h_fact
        FROM read_parquet('{silver}/slv_temps/releves_heures/**/*.parquet',
                          hive_partitioning=true) r
        LEFT JOIN read_parquet('{silver}/slv_temps/heures_detail/**/*.parquet',
                               hive_partitioning=true) h
            ON h.prh_bts = r.prh_bts
        WHERE r.per_id IS NOT NULL AND r.cnt_id IS NOT NULL
        GROUP BY r.per_id, r.cnt_id
    )"""


def cte_missions_distinct(silver: str) -> str:
    """CTE missions dédupliquées (per_id, cnt_id, tie_id, rgpcnt_id).
    À utiliser pour les JOINs depuis factures où seule la clé (tie_id, rgpcnt_id)
    est disponible — évite le produit cartésien factures × missions.
    """
    return f"""
    missions_distinct AS (
        SELECT DISTINCT per_id, cnt_id, tie_id, rgpcnt_id
        FROM read_parquet('{silver}/slv_missions/missions/**/*.parquet',
                          hive_partitioning=true)
        WHERE per_id IS NOT NULL AND cnt_id IS NOT NULL
    )"""
