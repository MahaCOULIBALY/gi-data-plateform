"""purge_gold.py — Purge PostgreSQL Gold (TRUNCATE toutes les tables gld_*).
GI Data Lakehouse · Manifeste v2.1

Usage :
    uv run purge_gold.py                                # probe : tables + nb lignes
    RUN_MODE=live uv run purge_gold.py                  # TRUNCATE CASCADE atomique toutes les tables gld_*
    RUN_MODE=live uv run purge_gold.py --schema gld_performance  # un schéma seulement
    uv run purge_gold.py --schema gld_performance       # probe sur un schéma

Schémas Gold (--schema) :
    gld_staffing | gld_commercial | gld_performance |
    gld_shared   | gld_operationnel | gld_clients

Mode par défaut : PROBE (aucune suppression).
"""
import argparse
import json
import logging
import os
import sys
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Optional

import psycopg2
from dotenv import load_dotenv

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(module)s.%(funcName)s:%(lineno)d | %(message)s",
)
logger = logging.getLogger("gi-purge-gold")

# ── .env.local ────────────────────────────────────────────────────────────────


def _find_env_file() -> Path | None:
    current = Path(__file__).resolve().parent
    for _ in range(4):
        candidate = current / ".env.local"
        if candidate.exists():
            return candidate
        current = current.parent
    return None


_ENV_FILE = _find_env_file()
if _ENV_FILE:
    load_dotenv(_ENV_FILE)
    logger.info(json.dumps({"env": str(_ENV_FILE), "loaded": True}))
else:
    logger.warning(json.dumps({"env": ".env.local", "loaded": False,
                               "note": "Variables lues depuis l'environnement système"}))

# ── Enums ─────────────────────────────────────────────────────────────────────


class RunMode(str, Enum):
    PROBE = "probe"
    LIVE = "live"


_ALL_GOLD_SCHEMAS = [
    "gld_staffing",
    "gld_commercial",
    "gld_performance",
    "gld_shared",
    "gld_operationnel",
    "gld_clients",
]

# ── Config ────────────────────────────────────────────────────────────────────


@dataclass
class Config:
    mode: RunMode = field(default_factory=lambda: RunMode(
        os.environ.get("RUN_MODE", "probe").lower()))
    schema_filter: Optional[str] = None

    pg_host: str = field(default_factory=lambda: os.environ["OVH_PG_HOST"])
    pg_port: str = field(
        default_factory=lambda: os.environ.get("OVH_PG_PORT", "20184"))
    pg_database: str = field(
        default_factory=lambda: os.environ.get("OVH_PG_DATABASE", "gi_data"))
    pg_user: str = field(default_factory=lambda: os.environ["OVH_PG_USER"])
    pg_password: str = field(
        default_factory=lambda: os.environ["OVH_PG_PASSWORD"])

# ── Stats ─────────────────────────────────────────────────────────────────────


@dataclass
class Stats:
    tables_found: int = 0
    tables_truncated: int = 0
    rows_before: int = 0
    errors: list = field(default_factory=list)

    def report(self) -> dict:
        return {
            "gold_pg": {
                "tables_found": self.tables_found,
                "tables_truncated": self.tables_truncated,
                "rows_purged": self.rows_before,
            },
            "errors": self.errors,
        }

# ── Helpers PostgreSQL ────────────────────────────────────────────────────────


def _pg_connect(cfg: Config):
    return psycopg2.connect(
        host=cfg.pg_host, port=cfg.pg_port, dbname=cfg.pg_database,
        user=cfg.pg_user, password=cfg.pg_password, sslmode="require",
    )


def _list_tables(cur, schemas: list[str]) -> list[tuple[str, str]]:
    """Retourne [(schema, table)] pour tous les schémas Gold ciblés."""
    placeholders = ",".join(["%s"] * len(schemas))
    cur.execute(
        f"SELECT table_schema, table_name "
        f"FROM information_schema.tables "
        f"WHERE table_schema IN ({placeholders}) AND table_type = 'BASE TABLE' "
        f"ORDER BY table_schema, table_name",
        schemas,
    )
    return cur.fetchall()


def _count_rows(cur, schema: str, table: str) -> int:
    """Compte les lignes d'une table (utilisé en PROBE uniquement)."""
    cur.execute(f'SELECT COUNT(*) FROM "{schema}"."{table}"')
    return cur.fetchone()[0]


def _list_cascade_dependencies(cur, schemas: list[str]) -> list[dict]:
    """
    Retourne les tables HORS schémas Gold qui seraient affectées par le CASCADE.
    Utile en PROBE pour anticiper les effets de bord sur d'autres schémas.
    """
    placeholders = ",".join(["%s"] * len(schemas))
    cur.execute(
        f"""
        SELECT DISTINCT
            ref_ns.nspname  AS dependent_schema,
            ref_cl.relname  AS dependent_table,
            src_ns.nspname  AS referenced_schema,
            src_cl.relname  AS referenced_table
        FROM pg_constraint c
        JOIN pg_class ref_cl ON ref_cl.oid = c.conrelid
        JOIN pg_namespace ref_ns ON ref_ns.oid = ref_cl.relnamespace
        JOIN pg_class src_cl ON src_cl.oid = c.confrelid
        JOIN pg_namespace src_ns ON src_ns.oid = src_cl.relnamespace
        WHERE c.contype = 'f'
          AND src_ns.nspname IN ({placeholders})
          AND ref_ns.nspname NOT IN ({placeholders})
        ORDER BY dependent_schema, dependent_table
        """,
        schemas * 2,
    )
    return [
        {"dependent": f"{r[0]}.{r[1]}", "references": f"{r[2]}.{r[3]}"}
        for r in cur.fetchall()
    ]

# ── Run ───────────────────────────────────────────────────────────────────────


def run(cfg: Config) -> Stats:
    stats = Stats()
    schemas = [cfg.schema_filter] if cfg.schema_filter else _ALL_GOLD_SCHEMAS

    if cfg.schema_filter and cfg.schema_filter not in _ALL_GOLD_SCHEMAS:
        logger.error(json.dumps({"error": f"schéma inconnu : '{cfg.schema_filter}'",
                                 "known_schemas": _ALL_GOLD_SCHEMAS}))
        stats.errors.append(
            {"step": "schema_filter", "error": f"inconnu : {cfg.schema_filter}"})
        return stats

    logger.info(json.dumps({"start": True, "mode": cfg.mode.value,
                            "schemas": schemas, "schema_filter": cfg.schema_filter or "(all)"}))

    try:
        conn = _pg_connect(cfg)
    except Exception as e:
        stats.errors.append({"step": "pg_connect", "error": str(e)})
        logger.error(json.dumps({"step": "pg_connect", "error": str(e)}))
        return stats

    try:
        with conn:
            with conn.cursor() as cur:
                tables = _list_tables(cur, schemas)
                stats.tables_found = len(tables)

                if not tables:
                    logger.info(json.dumps(
                        {"gold_pg": "no tables found", "schemas": schemas}))
                    return stats

                # ── Comptage des lignes (PROBE + LIVE pour reporting) ──────
                row_counts = {}
                for schema, table in tables:
                    count = _count_rows(cur, schema, table)
                    row_counts[f"{schema}.{table}"] = count
                stats.rows_before = sum(row_counts.values())

                logger.info(json.dumps({
                    "mode": cfg.mode.value,
                    "tables_found": stats.tables_found,
                    "total_rows": stats.rows_before,
                    "detail": row_counts,
                }))

                # ── Simulation CASCADE (PROBE uniquement) ─────────────────
                if cfg.mode == RunMode.PROBE:
                    cascade_deps = _list_cascade_dependencies(cur, schemas)
                    if cascade_deps:
                        logger.warning(json.dumps({
                            "probe_cascade_warning": (
                                "Ces tables HORS schémas Gold seraient vidées par CASCADE"
                            ),
                            "affected_external_tables": cascade_deps,
                        }))
                    else:
                        logger.info(json.dumps(
                            {"probe_cascade": "aucune dépendance externe détectée"}))

                    logger.info(json.dumps({
                        "probe": "NO truncation performed",
                        "would_truncate_tables": stats.tables_found,
                        "would_purge_rows": stats.rows_before,
                        "hint": "Relancer avec RUN_MODE=live pour exécuter",
                    }))
                    logger.info(json.dumps({"summary": stats.report()}))
                    return stats

                # ── LIVE : un seul TRUNCATE atomique ─────────────────────
                # Toutes les tables dans une seule instruction = tout ou rien.
                # Impossible d'avoir un état partiellement purgé.
                table_list = ", ".join(f'"{s}"."{t}"' for s, t in tables)
                cur.execute(f"TRUNCATE TABLE {table_list} CASCADE")
                stats.tables_truncated = len(tables)

                logger.info(json.dumps({
                    "gold_purge": "DONE",
                    "tables_truncated": stats.tables_truncated,
                    "rows_purged": stats.rows_before,
                }))

        # with conn: → commit automatique ici (pas besoin de conn.commit() explicite)

    except Exception as e:
        # Erreur inattendue (ex: réseau coupé pendant le TRUNCATE)
        # with conn: déclenche un ROLLBACK automatique → état cohérent garanti
        stats.errors.append({"step": "truncate_gold", "error": str(e)})
        logger.error(json.dumps({"step": "truncate_gold", "error": str(e),
                                 "note": "ROLLBACK automatique — aucune table modifiée"}))
    finally:
        conn.close()

    logger.info(json.dumps({"summary": stats.report()}))
    if stats.errors:
        logger.error(json.dumps({"errors": stats.errors}))
    return stats


# ── CLI ───────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    p = argparse.ArgumentParser(
        description="Purge Gold PostgreSQL — GI Data Lakehouse")
    p.add_argument("--schema", default=None, metavar="SCHEMA",
                   help="Restreindre à un schéma Gold (ex: gld_performance). Défaut : tous les gld_*.")
    args = p.parse_args()
    cfg = Config(schema_filter=args.schema)
    result = run(cfg)
    sys.exit(1 if result.errors else 0)
