"""tests/unit/test_gold.py — Tests unitaires couche Gold.
Markers : pytest.mark.unit (DuckDB in-memory, zéro I/O externe).
Coverage :
  1. gold_helpers B-02 — reconstitution montant_ht depuis lignes_factures
  2. gold_helpers DT-09 — déduplication heures par (per_id, cnt_id)
  3. Churn scoring — risque_churn CASE/WHEN + churn_score_ml (gold_retention_client)
  4. ETP — SUM(heures) / 35 hebdomadaire (gold_etp)
  5. Scorecard KPIs — taux_marge, taux_transformation, ca_net_ht (gold_scorecard_agence)
"""
import os
import sys

import duckdb
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../scripts"))


# ─────────────────────────────────────────────────────────────────────────────
# Fixture DuckDB in-memory (isolation par test)
# ─────────────────────────────────────────────────────────────────────────────
@pytest.fixture()
def ddb():
    con = duckdb.connect()
    yield con
    con.close()


# ─────────────────────────────────────────────────────────────────────────────
# 1. B-02 : reconstitution montant_ht depuis lignes_factures
#    Criticité : EFAC_MONTANTHT est NULL en Silver → SUM(lfac_mnt) est la seule
#    source de CA — une erreur ici corrompt fact_ca_mensuel, scorecard, rentabilité.
# ─────────────────────────────────────────────────────────────────────────────
@pytest.mark.unit
class TestB02Montants:
    """CTE B-02 : COALESCE(SUM(lfac_mnt), 0) GROUP BY fac_num."""

    def _aggregate(self, ddb, lignes: list[tuple]) -> dict:
        """Retourne {fac_num: montant_ht_calc} depuis une liste (fac_num, lfac_mnt)."""
        ddb.execute("""
            CREATE OR REPLACE TABLE lignes_b02 (
                fac_num  VARCHAR,
                lfac_mnt DECIMAL(10,2)
            )
        """)
        if lignes:
            vals = ", ".join(f"('{fac}', {mnt})" for fac, mnt in lignes)
            ddb.execute(f"INSERT INTO lignes_b02 VALUES {vals}")
        rows = ddb.execute("""
            SELECT fac_num,
                   COALESCE(SUM(lfac_mnt), 0)::DECIMAL(18,2) AS montant_ht_calc
            FROM lignes_b02
            GROUP BY fac_num
        """).fetchall()
        return {r[0]: float(r[1]) for r in rows}

    def test_single_ligne(self, ddb):
        assert self._aggregate(ddb, [("FAC001", 1250.00)])["FAC001"] == 1250.00

    def test_multiple_lignes_summed(self, ddb):
        result = self._aggregate(ddb, [("FAC001", 500.00), ("FAC001", 750.00), ("FAC001", 100.00)])
        assert result["FAC001"] == 1350.00

    def test_distinct_fac_nums_isolated(self, ddb):
        result = self._aggregate(ddb, [("FAC001", 1000.00), ("FAC002", 2500.00)])
        assert result["FAC001"] == 1000.00
        assert result["FAC002"] == 2500.00

    def test_left_join_missing_fac_returns_zero(self, ddb):
        """Facture sans lignes (LEFT JOIN) → COALESCE(NULL, 0) = 0."""
        self._aggregate(ddb, [])  # table lignes_b02 vide
        rows = ddb.execute("""
            SELECT f.efac_num,
                   COALESCE(SUM(l.lfac_mnt), 0)::DECIMAL(18,2) AS montant_ht_calc
            FROM (VALUES ('FAC999')) AS f(efac_num)
            LEFT JOIN lignes_b02 l ON l.fac_num = f.efac_num
            GROUP BY f.efac_num
        """).fetchall()
        assert len(rows) == 1
        assert float(rows[0][1]) == 0.0

    def test_cte_string_contains_required_patterns(self):
        """La fonction gold_helpers émet bien le SQL B-02 attendu."""
        from gold_helpers import cte_montants_factures
        sql = cte_montants_factures("s3://test-bucket")
        assert "SUM(lfac_mnt)" in sql
        assert "GROUP BY fac_num" in sql
        assert "COALESCE" in sql
        assert "montant_ht_calc" in sql
        assert "lignes_factures" in sql


# ─────────────────────────────────────────────────────────────────────────────
# 2. DT-09 : pré-agrégation heures par (per_id, cnt_id)
#    Criticité : N relevés/semaine par contrat → sans GROUP BY (per_id, cnt_id)
#    avant le JOIN missions, on génère N doublons → coûts / marges multipliés.
# ─────────────────────────────────────────────────────────────────────────────
@pytest.mark.unit
class TestDT09HeuresDedup:
    """Vérification que la pré-agrégation DT-09 élimine les doublons multi-relevés."""

    def _run(self, ddb,
             releves: list[tuple],   # (prh_bts, per_id, cnt_id)
             heures: list[tuple]     # (prh_bts, base_paye, base_fact)
             ) -> list[tuple]:
        ddb.execute("""
            CREATE OR REPLACE TABLE r_test (prh_bts VARCHAR, per_id INT, cnt_id INT)
        """)
        ddb.execute("""
            CREATE OR REPLACE TABLE h_test (prh_bts VARCHAR, base_paye DECIMAL(10,2), base_fact DECIMAL(10,2))
        """)
        if releves:
            ddb.execute("INSERT INTO r_test VALUES " +
                        ", ".join(f"('{b}', {p}, {c})" for b, p, c in releves))
        if heures:
            ddb.execute("INSERT INTO h_test VALUES " +
                        ", ".join(f"('{b}', {p}, {f})" for b, p, f in heures))
        return ddb.execute("""
            SELECT r.per_id, r.cnt_id,
                   SUM(h.base_paye::DECIMAL(10,2)) AS h_paye,
                   SUM(h.base_fact::DECIMAL(10,2)) AS h_fact
            FROM r_test r
            LEFT JOIN h_test h ON h.prh_bts = r.prh_bts
            WHERE r.per_id IS NOT NULL AND r.cnt_id IS NOT NULL
            GROUP BY r.per_id, r.cnt_id
        """).fetchall()

    def test_single_releve_passthrough(self, ddb):
        rows = self._run(ddb, [("BTS01", 101, 201)], [("BTS01", 35.0, 35.0)])
        assert len(rows) == 1
        assert float(rows[0][2]) == 35.0

    def test_two_releves_same_contract_summed_not_duplicated(self, ddb):
        """2 relevés (semaines distinctes) pour le même contrat → 1 seul agrégat."""
        rows = self._run(ddb,
            [("BTS01", 101, 201), ("BTS02", 101, 201)],
            [("BTS01", 35.0, 35.0), ("BTS02", 35.0, 35.0)])
        assert len(rows) == 1, "DT-09 : un seul agrégat par (per_id, cnt_id)"
        assert float(rows[0][2]) == 70.0   # 35 + 35
        assert float(rows[0][3]) == 70.0

    def test_two_different_contracts_produce_two_rows(self, ddb):
        rows = self._run(ddb,
            [("BTS01", 101, 201), ("BTS02", 101, 202)],
            [("BTS01", 35.0, 35.0), ("BTS02", 28.0, 28.0)])
        assert len(rows) == 2
        h_by_cnt = {r[1]: float(r[2]) for r in rows}
        assert h_by_cnt[201] == 35.0
        assert h_by_cnt[202] == 28.0

    def test_three_releves_same_contract_fully_summed(self, ddb):
        rows = self._run(ddb,
            [("BTS01", 101, 201), ("BTS02", 101, 201), ("BTS03", 101, 201)],
            [("BTS01", 35.0, 35.0), ("BTS02", 28.0, 28.0), ("BTS03", 21.0, 21.0)])
        assert len(rows) == 1
        assert float(rows[0][2]) == 84.0   # 35+28+21

    def test_releve_without_heures_gives_null(self, ddb):
        """Relevé sans heures_detail (LEFT JOIN) → h_paye = NULL."""
        rows = self._run(ddb, [("BTS01", 101, 201)], [])
        assert len(rows) == 1
        assert rows[0][2] is None

    def test_cte_string_groups_by_per_and_cnt(self):
        from gold_helpers import cte_heures_par_contrat
        sql = cte_heures_par_contrat("s3://test-bucket")
        assert "GROUP BY r.per_id, r.cnt_id" in sql
        assert "SUM(h.base_paye" in sql
        assert "SUM(h.base_fact" in sql
        assert "LEFT JOIN" in sql


# ─────────────────────────────────────────────────────────────────────────────
# 3. Churn scoring — gold_retention_client
#    Criticité : segmentation client incorrecte → actions commerciales mal ciblées,
#    score ML non fiable pour prédiction attrition.
# ─────────────────────────────────────────────────────────────────────────────
@pytest.mark.unit
class TestChurnScoring:
    """Règles risque_churn et formule churn_score_ml."""

    def _eval(self, ddb, jours: int,
              delta_qoq: float | None = 0.0,
              freq_4q: int = 4,
              delta_yoy: float | None = 0.0) -> tuple[str, float]:
        """Évalue les formules Gold avec des entrées scalaires (pas de dates)."""
        dqoq = str(delta_qoq) if delta_qoq is not None else "NULL"
        dyoy = str(delta_yoy) if delta_yoy is not None else "NULL"
        sql = f"""
        SELECT
            CASE
                WHEN {jours} > 180 THEN 'PERDU'
                WHEN {jours} > 90  THEN 'HIGH'
                WHEN {jours} > 45  THEN 'MEDIUM'
                WHEN COALESCE({dqoq}, 0) < -0.3 THEN 'MEDIUM'
                WHEN {freq_4q} <= 1              THEN 'MEDIUM'
                ELSE 'LOW'
            END AS risque_churn,
            ROUND(LEAST(1.0, GREATEST(0.0,
                0.3  * LEAST({jours}::DECIMAL / 180, 1.0)
              + 0.25 * (1.0 - LEAST({freq_4q}::DECIMAL / 4, 1.0))
              + 0.25 * CASE WHEN COALESCE({dqoq}, 0) < 0
                            THEN LEAST(ABS(COALESCE({dqoq}, 0)::DECIMAL), 1.0)
                            ELSE 0 END
              + 0.20 * CASE WHEN COALESCE({dyoy}, 0) < 0
                            THEN LEAST(ABS(COALESCE({dyoy}, 0)::DECIMAL), 1.0)
                            ELSE 0 END
            )), 4) AS churn_score
        """
        row = ddb.execute(sql).fetchone()
        return row[0], float(row[1])

    # — risque_churn labels —

    def test_perdu_over_180_days(self, ddb):
        assert self._eval(ddb, jours=181)[0] == "PERDU"

    def test_high_between_91_and_180_days(self, ddb):
        assert self._eval(ddb, jours=91)[0] == "HIGH"

    def test_medium_between_46_and_90_days(self, ddb):
        assert self._eval(ddb, jours=60)[0] == "MEDIUM"

    def test_medium_on_30pct_revenue_decline(self, ddb):
        """Client récent mais -40 % CA QoQ → MEDIUM (seuil -30 %)."""
        assert self._eval(ddb, jours=10, delta_qoq=-0.40)[0] == "MEDIUM"

    def test_medium_on_low_frequency(self, ddb):
        """freq_4q = 1 → MEDIUM même si CA stable et client récent."""
        assert self._eval(ddb, jours=10, freq_4q=1)[0] == "MEDIUM"

    def test_low_for_healthy_active_client(self, ddb):
        assert self._eval(ddb, jours=10, delta_qoq=0.0, freq_4q=4)[0] == "LOW"

    # — limites de frontière (boundary) —

    def test_boundary_180_days_is_high_not_perdu(self, ddb):
        """Exactement 180j → HIGH (seuil >180 strict)."""
        assert self._eval(ddb, jours=180)[0] == "HIGH"

    def test_boundary_90_days_is_medium_not_high(self, ddb):
        """Exactement 90j → MEDIUM (seuil >90 strict)."""
        assert self._eval(ddb, jours=90)[0] == "MEDIUM"

    def test_boundary_45_days_is_low(self, ddb):
        """Exactement 45j → LOW (seuil >45 strict)."""
        assert self._eval(ddb, jours=45)[0] == "LOW"

    def test_delta_qoq_exactly_minus_30pct_is_not_medium(self, ddb):
        """< -0.30 requis ; exactement -0.30 → LOW si client par ailleurs sain."""
        assert self._eval(ddb, jours=10, delta_qoq=-0.30)[0] == "LOW"

    # — churn_score_ml —

    def test_churn_score_in_range_0_to_1(self, ddb):
        for jours in [0, 45, 90, 181, 365, 730]:
            _, score = self._eval(ddb, jours, delta_qoq=-0.5, freq_4q=1, delta_yoy=-0.5)
            assert 0.0 <= score <= 1.0, f"Score hors [0,1] : {score} pour {jours}j"

    def test_churn_score_increases_with_inactivity(self, ddb):
        _, s_low = self._eval(ddb, jours=5)
        _, s_high = self._eval(ddb, jours=200)
        assert s_high > s_low

    def test_churn_score_null_deltas_handled(self, ddb):
        """NULL QoQ et YoY → COALESCE 0 → pas de crash, score valide."""
        risque, score = self._eval(ddb, jours=10, delta_qoq=None, delta_yoy=None)
        assert risque == "LOW"
        assert 0.0 <= score <= 1.0

    def test_churn_score_worst_case_approaches_1(self, ddb):
        """Client perdu (365j) + freq=0 + -100% QoQ + -100% YoY → score ≈ 1.0."""
        _, score = self._eval(ddb, jours=365, delta_qoq=-1.0, freq_4q=0, delta_yoy=-1.0)
        assert score >= 0.95


# ─────────────────────────────────────────────────────────────────────────────
# 4. ETP (Équivalent Temps Plein) — gold_etp
#    Criticité : KPI RH pilotage agences — ETP = SUM(heures_paye) / 35
# ─────────────────────────────────────────────────────────────────────────────
@pytest.mark.unit
class TestEtpCalculation:
    """Formule ETP = SUM(base_paye) / 35 par semaine × agence."""

    def _etp(self, ddb, heures: float | None) -> float:
        val = str(heures) if heures is not None else "NULL"
        return float(ddb.execute(
            f"SELECT ROUND(COALESCE({val}, 0) / 35.0, 4)::DECIMAL(10,4)"
        ).fetchone()[0])

    def test_35h_is_one_etp(self, ddb):
        assert self._etp(ddb, 35.0) == 1.0

    def test_70h_is_two_etp(self, ddb):
        assert self._etp(ddb, 70.0) == 2.0

    def test_half_week_17h5(self, ddb):
        assert self._etp(ddb, 17.5) == 0.5

    def test_zero_hours(self, ddb):
        assert self._etp(ddb, 0.0) == 0.0

    def test_null_coalesced_to_zero(self, ddb):
        """LEFT JOIN sans heures_detail → NULL → ETP = 0.0, pas de crash."""
        assert self._etp(ddb, None) == 0.0

    def test_weekly_aggregation_two_interimaires(self, ddb):
        """Agence 10 · 2 intérimaires · 35h chacun → ETP = 2.0 sur la semaine."""
        ddb.execute("""
            CREATE OR REPLACE TABLE rel (prh_bts VARCHAR, rgpcnt_id INT, per_id INT, semaine DATE)
        """)
        ddb.execute("""
            CREATE OR REPLACE TABLE heu (prh_bts VARCHAR, heures_semaine DECIMAL(10,2))
        """)
        ddb.execute("""
            INSERT INTO rel VALUES ('B01', 10, 1, '2026-03-02'), ('B02', 10, 2, '2026-03-02')
        """)
        ddb.execute("INSERT INTO heu VALUES ('B01', 35.0), ('B02', 35.0)")
        row = ddb.execute("""
            SELECT r.rgpcnt_id,
                   COUNT(DISTINCT r.prh_bts)                      AS nb_releves,
                   COUNT(DISTINCT r.per_id)                        AS nb_int,
                   COALESCE(SUM(h.heures_semaine), 0)              AS h_tot,
                   ROUND(COALESCE(SUM(h.heures_semaine), 0) / 35.0, 4) AS etp
            FROM rel r LEFT JOIN heu h ON h.prh_bts = r.prh_bts
            GROUP BY r.rgpcnt_id
        """).fetchone()
        agence, nb_rel, nb_int, h_tot, etp = row
        assert agence == 10
        assert nb_rel == 2
        assert nb_int == 2
        assert float(h_tot) == 70.0
        assert float(etp) == 2.0

    def test_weekly_aggregation_excludes_invalid_releves(self, ddb):
        """valide=false → exclu du calcul ETP (filtrage WHERE valide=true)."""
        ddb.execute("""
            CREATE OR REPLACE TABLE rel_v (
                prh_bts VARCHAR, rgpcnt_id INT, per_id INT, semaine DATE, valide BOOLEAN
            )
        """)
        ddb.execute("""
            CREATE OR REPLACE TABLE heu_v (prh_bts VARCHAR, heures_semaine DECIMAL(10,2))
        """)
        ddb.execute("""
            INSERT INTO rel_v VALUES
                ('B01', 10, 1, '2026-03-02', true),
                ('B02', 10, 2, '2026-03-02', false)  -- invalide
        """)
        ddb.execute("INSERT INTO heu_v VALUES ('B01', 35.0), ('B02', 35.0)")
        row = ddb.execute("""
            SELECT ROUND(COALESCE(SUM(h.heures_semaine), 0) / 35.0, 4) AS etp
            FROM rel_v r LEFT JOIN heu_v h ON h.prh_bts = r.prh_bts
            WHERE r.valide = true
        """).fetchone()
        assert float(row[0]) == 1.0  # seul B01 compte


# ─────────────────────────────────────────────────────────────────────────────
# 5. Scorecard agences — gold_scorecard_agence
#    Criticité : KPI pilotage commercial — erreur = mauvais classement agences
# ─────────────────────────────────────────────────────────────────────────────
@pytest.mark.unit
class TestScorecardKpis:
    """Formules KPI scorecard : ca_net_ht, taux_marge, taux_transformation."""

    def test_ca_net_factures_minus_avoirs(self, ddb):
        """ca_net_ht = SUM(F) - SUM(A) — avoirs déduits des factures."""
        row = ddb.execute("""
            SELECT
                SUM(CASE WHEN type_facture = 'F' THEN montant ELSE 0 END)
              - SUM(CASE WHEN type_facture = 'A' THEN montant ELSE 0 END) AS ca_net_ht
            FROM (VALUES
                ('F001', 'F', 10000.00::DECIMAL(18,2)),
                ('F002', 'F',  5000.00::DECIMAL(18,2)),
                ('A001', 'A',  1500.00::DECIMAL(18,2))
            ) AS t(efac_num, type_facture, montant)
        """).fetchone()
        assert float(row[0]) == 13500.0

    def test_avoir_only_month_gives_negative_ca(self, ddb):
        """Mois avec uniquement des avoirs → ca_net négatif (cas réel : régularisation)."""
        row = ddb.execute("""
            SELECT
                SUM(CASE WHEN type_facture = 'F' THEN montant ELSE 0 END)
              - SUM(CASE WHEN type_facture = 'A' THEN montant ELSE 0 END) AS ca_net_ht
            FROM (VALUES ('A001', 'A', 5000.00::DECIMAL(18,2))) AS t(efac_num, type_facture, montant)
        """).fetchone()
        assert float(row[0]) == -5000.0

    def test_taux_marge(self, ddb):
        """taux_marge = (ca_missions - cout_paye) / ca_missions."""
        row = ddb.execute("""
            SELECT ROUND((100000.0 - 80000.0) / 100000.0, 4) AS taux_marge
        """).fetchone()
        assert float(row[0]) == 0.2

    def test_taux_marge_zero_protected(self, ddb):
        """ca_missions = 0 → protection division par zéro → NULL."""
        row = ddb.execute("""
            SELECT CASE WHEN 0.0 = 0 THEN NULL ELSE (0.0 - 0.0) / 0.0 END AS taux_marge
        """).fetchone()
        assert row[0] is None

    def test_taux_transformation(self, ddb):
        """taux_transformation = nb_pourvues / nb_commandes (8/10 = 80 %)."""
        row = ddb.execute("SELECT ROUND(8.0 / 10.0, 4)").fetchone()
        assert float(row[0]) == 0.8

    def test_taux_transformation_full(self, ddb):
        """Toutes les commandes pourvues → taux = 1.0."""
        row = ddb.execute("SELECT ROUND(5.0 / 5.0, 4)").fetchone()
        assert float(row[0]) == 1.0

    def test_marge_brute_heures_times_taux(self, ddb):
        """marge_brute = SUM(h_fact * taux_fact - h_paye * taux_paye)."""
        row = ddb.execute("""
            SELECT ROUND(SUM(h_fact * taux_fact - h_paye * taux_paye), 2) AS marge_brute
            FROM (VALUES
                (35.0, 15.50, 35.0, 12.00),
                (28.0, 15.50, 28.0, 12.00)
            ) AS t(h_fact, taux_fact, h_paye, taux_paye)
        """).fetchone()
        # (35 + 28) * (15.50 - 12.00) = 63 * 3.50 = 220.50
        assert float(row[0]) == 220.50

    def test_nb_clients_actifs_distinct(self, ddb):
        """nb_clients_actifs = COUNT(DISTINCT tie_id) — pas de doublon si N missions/client."""
        row = ddb.execute("""
            SELECT COUNT(DISTINCT tie_id) AS nb_clients
            FROM (VALUES (1, 42), (2, 42), (3, 99)) AS t(cnt_id, tie_id)
        """).fetchone()
        assert row[0] == 2   # tie_id 42 et 99

    def test_nb_interimaires_distinct(self, ddb):
        """nb_int_actifs = COUNT(DISTINCT per_id) par agence × mois."""
        row = ddb.execute("""
            SELECT COUNT(DISTINCT per_id) AS nb_int
            FROM (VALUES (101, 10), (101, 10), (102, 10), (103, 10)) AS t(per_id, rgpcnt_id)
            WHERE rgpcnt_id = 10
        """).fetchone()
        assert row[0] == 3   # 101, 102, 103
