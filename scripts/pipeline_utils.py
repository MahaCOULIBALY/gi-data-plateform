"""pipeline_utils.py — Utilitaires pipeline : WatermarkStore + with_retry + RunRecorder.
GI Data Lakehouse · Manifeste v2.0

Tables cibles (schéma ops, séparé des schémas gld_*) :
  - ops.pipeline_watermarks : bornes delta Bronze (upsert, état courant)
  - ops.pipeline_runs       : historique d'exécutions toutes couches (append-only)
"""
import json
import logging
import time
from datetime import datetime, timezone
from functools import wraps
from typing import Callable, Optional, ParamSpec, TypeVar

_P = ParamSpec("_P")
_R = TypeVar("_R")

import psycopg2

logger = logging.getLogger(__name__)

# ── Schéma ops — séparé de public et des schémas gld_* ───────────────────────
_SCHEMA = "ops"
_TABLE = f"{_SCHEMA}.pipeline_watermarks"

# Sentinel utilisé comme last_success lors du premier enregistrement d'un échec
# (évite last_success = NOW() qui fausserait le prochain calcul de delta)
_EPOCH_SENTINEL = datetime(1970, 1, 1, tzinfo=timezone.utc)

_DDL_ENSURE_SCHEMA = f"CREATE SCHEMA IF NOT EXISTS {_SCHEMA};"
_DDL_ENSURE_TABLE = f"""
CREATE TABLE IF NOT EXISTS {_TABLE} (
    pipeline        VARCHAR(100)  NOT NULL,
    table_name      VARCHAR(100)  NOT NULL,
    last_success    TIMESTAMPTZ   NOT NULL,
    last_status     VARCHAR(20)   NOT NULL DEFAULT 'success',
    last_error      TEXT,
    run_count       INTEGER       NOT NULL DEFAULT 1,
    rows_ingested   BIGINT        NOT NULL DEFAULT 0,
    created_at      TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_pipeline_watermarks PRIMARY KEY (pipeline, table_name)
);
"""
# Migrations idempotentes pour les tables créées avant l'ajout de ces colonnes
_DDL_MIGRATIONS = [
    f"ALTER TABLE {_TABLE} ADD COLUMN IF NOT EXISTS last_status    VARCHAR(20)  NOT NULL DEFAULT 'success';",
    f"ALTER TABLE {_TABLE} ADD COLUMN IF NOT EXISTS last_error     TEXT;",
    f"ALTER TABLE {_TABLE} ADD COLUMN IF NOT EXISTS run_count      INTEGER      NOT NULL DEFAULT 1;",
    f"ALTER TABLE {_TABLE} ADD COLUMN IF NOT EXISTS rows_ingested  BIGINT       NOT NULL DEFAULT 0;",
    f"ALTER TABLE {_TABLE} ADD COLUMN IF NOT EXISTS created_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW();",
    f"ALTER TABLE {_TABLE} ADD COLUMN IF NOT EXISTS updated_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW();",
]

# Exceptions non-transientes — pas de retry (erreurs de programmation ou config)
_NON_RETRYABLE = (
    KeyError, ValueError, TypeError, AttributeError,
    NotImplementedError, ImportError,
)


class WatermarkStore:
    """Gestion des bornes delta par pipeline et table dans ops.pipeline_watermarks."""

    def __init__(self, conn: psycopg2.extensions.connection, pipeline: str) -> None:
        self._conn = conn
        self._pipeline = pipeline
        self._ensure_table()

    # ── Setup ─────────────────────────────────────────────────────────────────
    def _ensure_table(self) -> None:
        """Crée le schéma ops + la table si absents, applique les migrations (idempotent)."""
        with self._conn.cursor() as cur:
            cur.execute(_DDL_ENSURE_SCHEMA)
            cur.execute(_DDL_ENSURE_TABLE)
            for migration in _DDL_MIGRATIONS:
                cur.execute(migration)
        self._conn.commit()

    # ── Read ──────────────────────────────────────────────────────────────────
    def get(self, table_name: str) -> Optional[datetime]:
        """Retourne le dernier timestamp de succès, ou None si absent."""
        with self._conn.cursor() as cur:
            cur.execute(
                f"SELECT last_success FROM {_TABLE} "
                f"WHERE pipeline = %s AND table_name = %s",
                (self._pipeline, table_name),
            )
            row = cur.fetchone()
        if row is None:
            return None
        ts = row[0]
        # Garantir timezone-aware
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        # Sentinel epoch → traiter comme absent (premier run après échec sans run réussi)
        if ts == _EPOCH_SENTINEL:
            return None
        return ts

    # ── Write ─────────────────────────────────────────────────────────────────
    def set(self, table_name: str, ts: datetime, rows: int = 0) -> None:
        """Upsert watermark succès — incrémente run_count et rows_ingested.
        rows : nombre de lignes ingérées pour CETTE table dans ce run (pas le cumul pipeline).
        """
        with self._conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {_TABLE}
                    (pipeline, table_name, last_success, last_status,
                     last_error, run_count, rows_ingested, updated_at)
                VALUES (%s, %s, %s, 'success', NULL, 1, %s, NOW())
                ON CONFLICT (pipeline, table_name) DO UPDATE SET
                    last_success  = EXCLUDED.last_success,
                    last_status   = 'success',
                    last_error    = NULL,
                    run_count     = {_TABLE}.run_count + 1,
                    rows_ingested = {_TABLE}.rows_ingested + EXCLUDED.rows_ingested,
                    updated_at    = NOW()
                """,
                (self._pipeline, table_name, ts, rows),
            )
        self._conn.commit()
        logger.info(json.dumps({
            "watermark_set": True,
            "pipeline": self._pipeline,
            "table": table_name,
            "ts": ts.isoformat(),
            "rows_ingested": rows,
        }))

    def mark_failed(self, table_name: str, error_msg: str) -> None:
        """Enregistre un échec sans modifier last_success (borne delta conservée).
        Si première insertion (pas de watermark existant), last_success = epoch sentinel
        pour que le prochain run reparte du FALLBACK_SINCE, pas de NOW().
        """
        with self._conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {_TABLE}
                    (pipeline, table_name, last_success, last_status,
                     last_error, updated_at)
                VALUES (%s, %s, %s, 'failed', %s, NOW())
                ON CONFLICT (pipeline, table_name) DO UPDATE SET
                    last_status = 'failed',
                    last_error  = EXCLUDED.last_error,
                    updated_at  = NOW()
                """,
                (self._pipeline, table_name, _EPOCH_SENTINEL, error_msg[:1000]),
            )
        self._conn.commit()
        logger.warning(json.dumps({
            "watermark_failed": True,
            "pipeline": self._pipeline,
            "table": table_name,
            "error": error_msg[:200],
        }))


# ── ops.pipeline_runs — historique append-only ────────────────────────────────
_TABLE_RUNS = "ops.pipeline_runs"

_DDL_PIPELINE_RUNS = f"""
CREATE TABLE IF NOT EXISTS {_TABLE_RUNS} (
    id               BIGSERIAL      PRIMARY KEY,
    pipeline         VARCHAR(100)   NOT NULL,
    layer            VARCHAR(20)    NOT NULL,
    mode             VARCHAR(20)    NOT NULL DEFAULT 'LIVE',
    status           VARCHAR(20)    NOT NULL,
    started_at       TIMESTAMPTZ    NOT NULL,
    ended_at         TIMESTAMPTZ    NOT NULL,
    duration_s       NUMERIC(10,3)  NOT NULL,
    tables_processed INTEGER        NOT NULL DEFAULT 0,
    rows_ingested    BIGINT         NOT NULL DEFAULT 0,
    rows_transformed BIGINT         NOT NULL DEFAULT 0,
    rows_rejected    BIGINT         NOT NULL DEFAULT 0,
    error_count      INTEGER        NOT NULL DEFAULT 0,
    warning_count    INTEGER        NOT NULL DEFAULT 0,
    errors           JSONB,
    warnings         JSONB,
    extra            JSONB,
    created_at       TIMESTAMPTZ    NOT NULL DEFAULT NOW()
);
"""

_DDL_RUNS_INDEXES = [
    f"CREATE INDEX IF NOT EXISTS idx_pr_pipeline_started ON {_TABLE_RUNS} (pipeline, started_at DESC);",
    f"CREATE INDEX IF NOT EXISTS idx_pr_layer_started    ON {_TABLE_RUNS} (layer,    started_at DESC);",
    f"CREATE INDEX IF NOT EXISTS idx_pr_status_started   ON {_TABLE_RUNS} (status,   started_at DESC);",
]

_DDL_RUNS_VIEW = """
CREATE OR REPLACE VIEW ops.v_pipeline_last_run AS
SELECT DISTINCT ON (pipeline)
    pipeline, layer, mode, status,
    started_at, ended_at, duration_s,
    tables_processed, rows_ingested, rows_transformed,
    error_count, warning_count, errors
FROM ops.pipeline_runs
ORDER BY pipeline, started_at DESC;
"""

# Flag process-level : la DDL n'est vérifiée qu'une seule fois par process
_runs_table_ready: bool = False


def _ensure_pipeline_runs(conn: psycopg2.extensions.connection) -> None:
    """Crée ops.pipeline_runs + vue + index si absents (idempotent, IF NOT EXISTS)."""
    global _runs_table_ready
    if _runs_table_ready:
        return
    with conn.cursor() as cur:
        cur.execute(_DDL_ENSURE_SCHEMA)
        cur.execute(_DDL_PIPELINE_RUNS)
        for stmt in _DDL_RUNS_INDEXES:
            cur.execute(stmt)
        cur.execute(_DDL_RUNS_VIEW)
    conn.commit()
    _runs_table_ready = True


def record_pipeline_run(
    conn: psycopg2.extensions.connection,
    pipeline: str,
    mode: str,
    stats: dict,
) -> None:
    """Insère une ligne dans ops.pipeline_runs (append-only, jamais UPDATE).

    pipeline : nom du script sans .py  (ex: gold_staffing, silver_missions)
    mode     : cfg.mode.name           (LIVE | PROBE | OFFLINE)
    stats    : dict retourné par Stats.finish()
    """
    _ensure_pipeline_runs(conn)

    layer = pipeline.split("_")[0] if "_" in pipeline else "unknown"

    errors   = stats.get("errors", []) or []
    warnings = stats.get("warnings", []) or []
    tables   = stats.get("tables_processed", 0)

    if not errors:
        status = "success"
    elif tables > 0:
        status = "partial"
    else:
        status = "failed"

    try:
        started = datetime.fromisoformat(stats["started_at"])
        ended   = datetime.fromisoformat(stats["ended_at"])
        duration_s = round((ended - started).total_seconds(), 3)
    except Exception:
        duration_s = 0.0

    with conn.cursor() as cur:
        cur.execute(
            f"""
            INSERT INTO {_TABLE_RUNS} (
                pipeline, layer, mode, status,
                started_at, ended_at, duration_s,
                tables_processed, rows_ingested, rows_transformed, rows_rejected,
                error_count, warning_count,
                errors, warnings, extra
            ) VALUES (
                %s, %s, %s, %s,
                %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s,
                %s::jsonb, %s::jsonb, %s::jsonb
            )
            """,
            (
                pipeline, layer, mode, status,
                stats.get("started_at"), stats.get("ended_at"), duration_s,
                tables,
                stats.get("rows_ingested", 0),
                stats.get("rows_transformed", 0),
                stats.get("rows_rejected", 0),
                len(errors),
                len(warnings),
                json.dumps(errors)   if errors   else None,
                json.dumps(warnings) if warnings else None,
                json.dumps(stats.get("extra", {})) or None,
            ),
        )
    conn.commit()
    logger.info(json.dumps({
        "pipeline_run_recorded": True,
        "pipeline": pipeline,
        "status": status,
        "duration_s": duration_s,
    }))


# ── Retry decorator ───────────────────────────────────────────────────────────
def with_retry(
    max_attempts: int = 3,
    base_delay: float = 2.0,
    backoff: float = 2.0,
) -> Callable[[Callable[_P, _R]], Callable[_P, _R]]:
    """Décorateur retry exponentiel pour les appels réseau / DB.
    Typage générique (ParamSpec + TypeVar) — préserve la signature et le type de retour
    de la fonction décorée (Pylance/mypy compatibles).
    Les exceptions non-transientes (_NON_RETRYABLE) remontent immédiatement sans retry.
    """
    def decorator(func: Callable[_P, _R]) -> Callable[_P, _R]:
        @wraps(func)
        def wrapper(*args: _P.args, **kwargs: _P.kwargs) -> _R:
            last_exc: Optional[Exception] = None
            delay = base_delay
            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except _NON_RETRYABLE:
                    # Erreur de programmation ou config — pas de retry, remonte immédiatement
                    raise
                except Exception as exc:
                    last_exc = exc
                    logger.warning(json.dumps({
                        "retry": attempt,
                        "max": max_attempts,
                        "func": func.__name__,
                        "error": str(exc),
                        "wait_s": delay,
                    }))
                    if attempt < max_attempts:
                        time.sleep(delay)
                        delay *= backoff
            raise last_exc  # type: ignore[misc]  # contexte original préservé (dernier except)
        return wrapper  # type: ignore[return-value]
    return decorator
