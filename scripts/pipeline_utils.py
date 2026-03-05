"""pipeline_utils.py — Watermark et retry pour GI Data Lakehouse (Manifeste v2.0).
Deux responsabilités distinctes :
  - WatermarkStore : dernière extraction réussie par table (PostgreSQL)
  - with_retry : décorateur exponentiel pour connexions réseau (ODBC, S3, PG)
"""
import functools
import json
import logging
import time
from datetime import datetime, timezone
from typing import Callable, TypeVar, Any
import psycopg2
import psycopg2.sql

logger = logging.getLogger("gi-data")
F = TypeVar("F", bound=Callable[..., Any])

# ── Retry ────────────────────────────────────────────────────────────────────

RETRYABLE = (
    ConnectionError, TimeoutError, OSError,
    psycopg2.OperationalError,
)
try:
    import pyodbc
    RETRYABLE = (*RETRYABLE, pyodbc.OperationalError, pyodbc.Error)
except ImportError:
    pass
try:
    from botocore.exceptions import BotoCoreError, ClientError
    RETRYABLE = (*RETRYABLE, BotoCoreError, ClientError)
except ImportError:
    pass


def with_retry(max_attempts: int = 3, base_delay: float = 2.0, backoff: float = 2.0):
    """Décorateur retry exponentiel pour toute fonction I/O réseau.
    Lève la dernière exception si toutes les tentatives échouent.

    Usage :
        @with_retry(max_attempts=3)
        def upload(cfg, data): ...
    """
    def decorator(fn: F) -> F:
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            delay = base_delay
            last_exc: Exception | None = None
            for attempt in range(1, max_attempts + 1):
                try:
                    return fn(*args, **kwargs)
                except RETRYABLE as exc:
                    last_exc = exc
                    if attempt == max_attempts:
                        logger.error(
                            f"{fn.__name__} failed after {max_attempts} attempts: {exc}")
                        raise
                    logger.warning(
                        f"{fn.__name__} attempt {attempt}/{max_attempts} failed: {exc} — retry in {delay:.1f}s")
                    time.sleep(delay)
                    delay *= backoff
            raise last_exc  # unreachable but satisfies type checker
        return wrapper  # type: ignore[return-value]
    return decorator


# ── WatermarkStore ───────────────────────────────────────────────────────────

DDL_WATERMARK = """
CREATE TABLE IF NOT EXISTS pipeline.watermarks (
    pipeline_name  TEXT        NOT NULL,
    table_name     TEXT        NOT NULL,
    last_extracted TIMESTAMPTZ NOT NULL,
    last_run_ok    BOOLEAN     NOT NULL DEFAULT true,
    run_stats      JSONB,
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (pipeline_name, table_name)
)
"""


class WatermarkStore:
    """Gestion des watermarks dans PostgreSQL.
    Chaque table Bronze a son propre watermark par pipeline.

    Usage :
        wm = WatermarkStore(pg_conn, "bronze_clients")
        since = wm.get("WTTIESERV")        # datetime | None
        wm.set("WTTIESERV", datetime.now(timezone.utc), stats)
    """

    DEFAULT_SINCE = datetime(2023, 1, 1, tzinfo=timezone.utc)

    def __init__(self, conn, pipeline_name: str) -> None:
        self.conn = conn
        self.pipeline = pipeline_name
        self._ensure_schema()

    def _ensure_schema(self) -> None:
        with self.conn.cursor() as cur:
            cur.execute("CREATE SCHEMA IF NOT EXISTS pipeline")
            cur.execute(DDL_WATERMARK)
        self.conn.commit()

    def get(self, table_name: str) -> datetime:
        """Retourne le dernier watermark réussi, ou DEFAULT_SINCE si absent."""
        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT last_extracted FROM pipeline.watermarks "
                "WHERE pipeline_name = %s AND table_name = %s AND last_run_ok = true",
                (self.pipeline, table_name),
            )
            row = cur.fetchone()
        if row:
            dt = row[0]
            return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt
        logger.info(
            f"No watermark for {self.pipeline}.{table_name} — using DEFAULT_SINCE {self.DEFAULT_SINCE.date()}")
        return self.DEFAULT_SINCE

    def set(self, table_name: str, extracted_at: datetime, stats: dict | None = None, ok: bool = True) -> None:
        """Upsert du watermark — appelé après chaque extraction réussie."""
        with self.conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO pipeline.watermarks (pipeline_name, table_name, last_extracted, last_run_ok, run_stats)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (pipeline_name, table_name) DO UPDATE
                    SET last_extracted = EXCLUDED.last_extracted,
                        last_run_ok    = EXCLUDED.last_run_ok,
                        run_stats      = EXCLUDED.run_stats,
                        updated_at     = NOW()
                """,
                (self.pipeline, table_name, extracted_at, ok,
                 json.dumps(stats, default=str) if stats else None),
            )
        self.conn.commit()
        logger.info(
            f"Watermark updated: {self.pipeline}.{table_name} → {extracted_at.isoformat()} (ok={ok})")

    def mark_failed(self, table_name: str, error: str) -> None:
        """Marque le run comme échoué sans modifier last_extracted."""
        with self.conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO pipeline.watermarks (pipeline_name, table_name, last_extracted, last_run_ok, run_stats)
                VALUES (%s, %s, NOW(), false, %s)
                ON CONFLICT (pipeline_name, table_name) DO UPDATE
                    SET last_run_ok = false,
                        run_stats   = EXCLUDED.run_stats,
                        updated_at  = NOW()
                """,
                (self.pipeline, table_name, json.dumps({"error": error})),
            )
        self.conn.commit()
