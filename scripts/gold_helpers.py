"""gold_helpers.py — CTEs partagées entre les scripts Gold (DT-08).
Évite la duplication de la logique B-02 (reconstitution montant HT) dans 4 scripts.

=== CORRECTIONS SESSION 3 (audit Gold) ===
- G-HLP-m01 : cte_montants_factures — CTE intermédiaire lignes_b02 fusionnée
              dans montants (projection minimale, SELECT * supprimé).
- G-HLP-m02 : cte_missions_distinct — casts ::INT ajoutés pour cohérence callers.

=== PROBE 2026-03-25 — colonnes confirmées ===
- slv_facturation/lignes_factures : fac_num, lfac_ord, libelle, base, taux,
  montant (← LFAC_MNT), rubrique, _batch_id, _loaded_at
- slv_facturation/factures        : efac_num, montant_ht = NULL::DECIMAL (B-02 confirmé)
- Bucket Silver réel              : gi-data-prod-silver (OVH_S3_BUCKET_SILVER)

# MIGRÉ : iceberg_scan → read_parquet(s3://{cfg.bucket_silver}/slv_*) (D01)
"""
from shared import Config


def cte_montants_factures(cfg: Config) -> str:
    """CTE B-02 : reconstitution montant HT depuis lignes_factures.

    WTEFAC.montant_ht = NULL::DECIMAL (absent DDL Evolia EFAC_MONTANTHT) —
    montant_ht_calc est reconstitué par SUM(montant) depuis WTLFAC (lignes).
    Colonne confirmée PROBE 2026-03-25 : LFAC_MNT → montant DECIMAL(18,2).

    Produit une CTE : montants (fac_num, montant_ht_calc)

    Usage dans l'appelant :
        WITH factures AS (...),
        {cte_montants_factures(cfg)},          ← virgule obligatoire si CTE suit
        ...
        LEFT JOIN montants mt ON mt.fac_num = f.efac_num
        ... COALESCE(mt.montant_ht_calc, 0)::DECIMAL(18,2) ...
    """
    return f"""
    montants AS (
        SELECT
            fac_num,
            COALESCE(SUM(montant::DECIMAL(18,2)), 0)::DECIMAL(18,2) AS montant_ht_calc
        FROM read_parquet('s3://{cfg.bucket_silver}/slv_facturation/lignes_factures/**/*.parquet')
        WHERE fac_num IS NOT NULL
        GROUP BY fac_num
    )"""


def cte_heures_par_contrat(cfg: Config) -> str:
    """CTE DT-09 : heures pré-agrégées par (per_id, cnt_id).

    Résout le fan-out dû à N relevés par contrat (une ligne par semaine dans
    slv_temps/releves_heures — une ligne par détail dans slv_temps/heures_detail).
    Sans pré-agrégation, un JOIN direct missions × relevés multiplierait les lignes.

    Produit une CTE : heures_par_contrat (per_id, cnt_id, h_paye, h_fact)

    Usage dans l'appelant :
        {cte_heures_par_contrat(cfg)},         ← virgule obligatoire si CTE suit
        ...
        LEFT JOIN heures_par_contrat hc
            ON hc.per_id = m.per_id AND hc.cnt_id = m.cnt_id
        ... hc.h_paye * c.taux_paye            (coût masse salariale)
        ... hc.h_fact * c.taux_fact            (CA missions)
    """
    return f"""
    heures_par_contrat AS (
        SELECT
            r.per_id,
            r.cnt_id,
            SUM(h.base_paye::DECIMAL(10,2)) AS h_paye,
            SUM(h.base_fact::DECIMAL(10,2)) AS h_fact
        FROM read_parquet('s3://{cfg.bucket_silver}/slv_temps/releves_heures/**/*.parquet') r
        LEFT JOIN read_parquet('s3://{cfg.bucket_silver}/slv_temps/heures_detail/**/*.parquet') h
            ON h.prh_bts = r.prh_bts
        WHERE r.per_id IS NOT NULL AND r.cnt_id IS NOT NULL
        GROUP BY r.per_id, r.cnt_id
    )"""


def cte_missions_distinct(cfg: Config) -> str:
    """CTE missions dédupliquées sur (per_id, cnt_id, tie_id, rgpcnt_id).

    À utiliser pour les JOINs depuis factures où seule la clé (tie_id, rgpcnt_id)
    est disponible — évite le produit cartésien factures × missions.

    G-HLP-m02 : casts ::INT pour cohérence avec les callers
                (gold_retention_client, gold_rentabilite_client).

    Produit une CTE : missions_distinct (per_id, cnt_id, tie_id, rgpcnt_id)

    Usage dans l'appelant :
        {cte_missions_distinct(cfg)},          ← virgule obligatoire si CTE suit
        ...
        LEFT JOIN missions_distinct m
            ON m.tie_id = f.tie_id::INT AND m.rgpcnt_id = f.rgpcnt_id::INT
    """
    return f"""
    missions_distinct AS (
        SELECT DISTINCT
            per_id::INT    AS per_id,
            cnt_id::INT    AS cnt_id,
            tie_id::INT    AS tie_id,
            rgpcnt_id::INT AS rgpcnt_id
        FROM read_parquet('s3://{cfg.bucket_silver}/slv_missions/missions/**/*.parquet')
        WHERE per_id IS NOT NULL AND cnt_id IS NOT NULL
    )"""
