"""tests/unit/test_critical.py — Tests unitaires pour les 3 fonctions critiques.
Markers : pytest.mark.unit (pas de réseau, pas de DB, pas de S3).
Coverage cible : pseudonymize_nir, WatermarkStore, _apply_scd2 / _hash.
"""
import hashlib
import sqlite3
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch, call

import pytest

# ── Helpers ───────────────────────────────────────────────────────────────────
# Les scripts sont dans scripts/ — ajout au path géré par conftest.py ou pyproject.toml
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../scripts"))


# ─────────────────────────────────────────────────────────────────────────────
# 1. pseudonymize_nir  (shared.py)
#    Criticité : RGPD — un hash incorrect = NIR en clair en Gold
# ─────────────────────────────────────────────────────────────────────────────
@pytest.mark.unit
class TestPseudonymizeNir:
    def _import(self):
        from shared import pseudonymize_nir
        return pseudonymize_nir

    def test_known_vector(self):
        """Hash SHA-256 déterministe — vecteur fixe pour non-régression."""
        pseudonymize_nir = self._import()
        nir = "1 85 05 75 056 123 45"
        salt = "test_salt_32_chars_minimum______"
        expected = hashlib.sha256(f"{nir}{salt}".encode()).hexdigest()
        assert pseudonymize_nir(nir, salt) == expected

    def test_none_returns_none(self):
        pseudonymize_nir = self._import()
        assert pseudonymize_nir(None, "any_salt") is None

    def test_empty_string_returns_none(self):
        pseudonymize_nir = self._import()
        assert pseudonymize_nir("", "any_salt") is None

    def test_different_salts_produce_different_hashes(self):
        pseudonymize_nir = self._import()
        nir = "2 75 12 75 123 456 78"
        h1 = pseudonymize_nir(nir, "salt_one_32_chars_minimum_______")
        h2 = pseudonymize_nir(nir, "salt_two_32_chars_minimum_______")
        assert h1 != h2

    def test_output_is_64_hex_chars(self):
        """SHA-256 = 256 bits = 64 hex chars."""
        pseudonymize_nir = self._import()
        result = pseudonymize_nir("1850575056123", "test_salt_32_chars_minimum______")
        assert result is not None
        assert len(result) == 64
        assert all(c in "0123456789abcdef" for c in result)


# ─────────────────────────────────────────────────────────────────────────────
# 2. WatermarkStore  (pipeline_utils.py)
#    Criticité : watermark incorrect = perte de delta ou re-ingestion complète
# ─────────────────────────────────────────────────────────────────────────────
@pytest.mark.unit
class TestWatermarkStore:
    """Utilise un mock psycopg2 connection — pas de DB réelle."""

    def _make_store(self):
        from pipeline_utils import WatermarkStore
        conn = MagicMock()
        conn.cursor.return_value.__enter__ = lambda s: s
        conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        conn.cursor.return_value.fetchone = MagicMock(return_value=None)
        return WatermarkStore(conn, "test_pipeline"), conn

    def test_get_returns_none_when_no_row(self):
        store, conn = self._make_store()
        conn.cursor.return_value.fetchone.return_value = None
        result = store.get("MY_TABLE")
        assert result is None

    def test_get_returns_none_for_epoch_sentinel(self):
        from pipeline_utils import _EPOCH_SENTINEL
        store, conn = self._make_store()
        conn.cursor.return_value.fetchone.return_value = (_EPOCH_SENTINEL,)
        result = store.get("MY_TABLE")
        assert result is None

    def test_get_returns_aware_datetime(self):
        store, conn = self._make_store()
        ts = datetime(2026, 1, 15, 10, 0, 0)  # naive
        conn.cursor.return_value.fetchone.return_value = (ts,)
        result = store.get("MY_TABLE")
        assert result is not None
        assert result.tzinfo is not None

    def test_set_calls_commit(self):
        store, conn = self._make_store()
        ts = datetime(2026, 3, 13, tzinfo=timezone.utc)
        store.set("MY_TABLE", ts, rows=1000)
        conn.commit.assert_called()

    def test_mark_failed_does_not_change_last_success(self):
        """mark_failed doit laisser last_success inchangé (sentinel epoch pour premier échec)."""
        store, conn = self._make_store()
        store.mark_failed("MY_TABLE", "Connection refused")
        # Vérifie que l'INSERT utilise l'epoch sentinel, pas NOW()
        executed_sql = conn.cursor.return_value.execute.call_args_list
        upsert_call = [c for c in executed_sql if "INSERT INTO" in str(c)]
        assert len(upsert_call) >= 1
        # Le 3ème paramètre du tuple VALUES doit être l'epoch sentinel
        insert_args = upsert_call[-1][0][1]  # (pipeline, table, last_success, error)
        from pipeline_utils import _EPOCH_SENTINEL
        assert insert_args[2] == _EPOCH_SENTINEL


# ─────────────────────────────────────────────────────────────────────────────
# 3. _apply_scd2 / _hash  (silver_clients.py)
#    Criticité : hash incorrect = versions SCD2 incorrectes (fermeture intempestive ou oubli)
# ─────────────────────────────────────────────────────────────────────────────
@pytest.mark.unit
class TestSilverClientsScd2:
    NOW = datetime(2026, 3, 13, 12, 0, 0, tzinfo=timezone.utc)

    def _make_row(self, tie_id: int, raison_sociale: str = "Acme", ville: str = "Paris") -> dict:
        return {
            "tie_id": tie_id, "raison_sociale": raison_sociale, "siren": "123456789",
            "nic": "00012", "naf_code": "7022Z", "adresse_complete": "1 rue de la Paix 75001 Paris",
            "ville": ville, "code_postal": "75001", "statut_client": "CLIENT",
            "ca_potentiel": 50000.0, "date_creation_fiche": None, "_source_raw_id": "batch_001",
        }

    def _make_existing(self, tie_id: int, change_hash: str) -> dict:
        return {
            "tie_id": tie_id, "client_sk": "abc123", "change_hash": change_hash,
            "raison_sociale": "Acme", "siren": "123456789", "nic": "00012",
            "naf_code": "7022Z", "adresse_complete": "1 rue de la Paix 75001 Paris",
            "ville": "Paris", "code_postal": "75001", "statut_client": "CLIENT",
            "ca_potentiel": 50000.0, "date_creation_fiche": None,
            "is_current": True, "valid_from": "2026-01-01", "valid_to": None,
            "_source_raw_id": "batch_000", "_loaded_at": "2026-01-01",
        }

    def test_new_client_creates_record(self):
        from silver_clients import _apply_scd2
        staging = [self._make_row(1)]
        new, closed, unchanged = _apply_scd2(staging, [], self.NOW)
        assert len(new) == 1
        assert len(closed) == 0
        assert new[0]["is_current"] is True
        assert new[0]["tie_id"] == 1

    def test_unchanged_client_not_duplicated(self):
        from silver_clients import _hash, _apply_scd2
        row = self._make_row(1)
        existing = [self._make_existing(1, _hash(row))]
        new, closed, unchanged = _apply_scd2([row], existing, self.NOW)
        assert len(new) == 0
        assert len(closed) == 0
        assert len(unchanged) == 1

    def test_changed_client_closes_old_and_creates_new(self):
        from silver_clients import _hash, _apply_scd2
        old_row = self._make_row(1, ville="Lyon")
        new_row = self._make_row(1, ville="Paris")
        existing = [self._make_existing(1, _hash(old_row))]
        new, closed, unchanged = _apply_scd2([new_row], existing, self.NOW)
        assert len(new) == 1
        assert len(closed) == 1
        assert closed[0]["is_current"] is False
        assert closed[0]["valid_to"] == self.NOW.isoformat()
        assert new[0]["ville"] == "Paris"

    def test_hash_differs_on_tracked_column_change(self):
        from silver_clients import _hash
        r1 = self._make_row(1, ville="Lyon")
        r2 = self._make_row(1, ville="Paris")
        assert _hash(r1) != _hash(r2)

    def test_hash_stable_on_non_tracked_column_change(self):
        """_source_raw_id n'est pas dans SCD2_TRACKED_COLS — le hash ne doit pas changer."""
        from silver_clients import _hash, SCD2_TRACKED_COLS
        assert "_source_raw_id" not in SCD2_TRACKED_COLS
        r1 = {**self._make_row(1), "_source_raw_id": "batch_001"}
        r2 = {**self._make_row(1), "_source_raw_id": "batch_999"}
        assert _hash(r1) == _hash(r2)
