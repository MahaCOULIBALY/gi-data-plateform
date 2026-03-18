"""purge_silver_gold.py — Utilitaire de purge S3 Silver + tables PostgreSQL Gold.
GI Data Lakehouse · Manifeste v2.0

Usage :
    uv run purge_silver_gold.py --target s3            # purge bucket gi-poc-silver uniquement
    uv run purge_silver_gold.py --target gold          # TRUNCATE toutes les tables gld_* PG
    uv run purge_silver_gold.py --target all           # S3 Silver + Gold PG

    # Filtres optionnels (combinables) :
    uv run purge_silver_gold.py --target s3   --pipeline silver_missions
    uv run purge_silver_gold.py --target gold --schema  gld_performance
    uv run purge_silver_gold.py --target all  --pipeline silver_missions --schema gld_performance

Pipelines Silver reconnus (--pipeline) :
    silver_missions | silver_interimaires | silver_agences |
    silver_clients  | silver_temps        | silver_facturation

Schémas Gold reconnus (--schema) :
    gld_staffing | gld_commercial | gld_performance |
    gld_shared   | gld_operationnel | gld_clients

Mode par défaut : PROBE (aucune suppression, affiche ce qui serait supprimé).
Passer RUN_MODE=live pour exécuter réellement.
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

import boto3
import psycopg2
from botocore.exceptions import ClientError
from dotenv import load_dotenv

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(module)s.%(funcName)s:%(lineno)d | %(message)s",
)
logger = logging.getLogger("gi-purge")


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
    logger.warning(json.dumps({
        "env": ".env.local", "loaded": False,
        "note": "Variables lues depuis l'environnement système",
    }))


# ── Enums ─────────────────────────────────────────────────────────────────────
class RunMode(str, Enum):
    PROBE = "probe"
    LIVE  = "live"


class PurgeTarget(str, Enum):
    S3   = "s3"
    GOLD = "gold"
    ALL  = "all"


# ── Mapping pipeline → préfixe S3 Silver ─────────────────────────────────────
_PIPELINE_PREFIX: dict[str, str] = {
    "silver_missions":     "slv_missions/",
    "silver_interimaires": "slv_interimaires/",
    "silver_agences":      "slv_agences/",
    "silver_clients":      "slv_clients/",
    "silver_temps":        "slv_temps/",
    "silver_facturation":  "slv_facturation/",
}

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
    mode:            RunMode     = field(default_factory=lambda: RunMode(
                                      os.environ.get("RUN_MODE", "probe").lower()))
    target:          PurgeTarget = PurgeTarget.S3
    pipeline_filter: Optional[str] = None  # Silver : restreint à 1 pipeline
    schema_filter:   Optional[str] = None  # Gold   : restreint à 1 schéma

    # S3
    s3_endpoint:   str = field(default_factory=lambda: os.environ["OVH_S3_ENDPOINT"])
    s3_access_key: str = field(default_factory=lambda: os.environ["OVH_S3_ACCESS_KEY"])
    s3_secret_key: str = field(default_factory=lambda: os.environ["OVH_S3_SECRET_KEY"])
    bucket_silver: str = field(default_factory=lambda:
                               os.environ.get("SILVER_BUCKET", "gi-poc-silver"))

    # PostgreSQL Gold
    pg_host:     str = field(default_factory=lambda: os.environ["OVH_PG_HOST"])
    pg_port:     str = field(default_factory=lambda:
                             os.environ.get("OVH_PG_PORT", "20184"))
    pg_database: str = field(default_factory=lambda:
                             os.environ.get("OVH_PG_DATABASE", "gi_poc_ddi_gold"))
    pg_user:     str = field(default_factory=lambda: os.environ["OVH_PG_USER"])
    pg_password: str = field(default_factory=lambda: os.environ["OVH_PG_PASSWORD"])


# ── Stats ─────────────────────────────────────────────────────────────────────
@dataclass
class Stats:
    s3_objects_found:   int  = 0
    s3_objects_deleted: int  = 0
    s3_bytes_freed:     int  = 0
    gold_tables_found:  int  = 0
    gold_tables_truncated: int = 0
    errors: list = field(default_factory=list)

    def report(self) -> dict:
        return {
            "silver_s3": {
                "objects_found":   self.s3_objects_found,
                "objects_deleted": self.s3_objects_deleted,
                "freed_mb":        round(self.s3_bytes_freed / 1_048_576, 2),
            },
            "gold_pg": {
                "tables_found":    self.gold_tables_found,
                "tables_truncated": self.gold_tables_truncated,
            },
            "errors": self.errors,
        }


# ── S3 helpers ────────────────────────────────────────────────────────────────
def _s3_client(cfg: Config):
    return boto3.client(
        "s3",
        endpoint_url=cfg.s3_endpoint,
        aws_access_key_id=cfg.s3_access_key,
        aws_secret_access_key=cfg.s3_secret_key,
    )


def _list_objects(s3, bucket: str, prefix: str = "") -> list[dict]:
    paginator = s3.get_paginator("list_objects_v2")
    params = {"Bucket": bucket}
    if prefix:
        params["Prefix"] = prefix
    objects = []
    for page in paginator.paginate(**params):
        objects.extend(page.get("Contents", []))
    return objects


def purge_silver_s3(cfg: Config, stats: Stats) -> None:
    """Purge le bucket Silver S3 (Parquet). Filtre optionnel par pipeline."""
    s3 = _s3_client(cfg)

    if cfg.pipeline_filter:
        prefix = _PIPELINE_PREFIX.get(cfg.pipeline_filter.lower())
        if prefix is None:
            known = list(_PIPELINE_PREFIX.keys())
            logger.error(json.dumps({
                "error": f"pipeline inconnu : '{cfg.pipeline_filter}'",
                "known_pipelines": known,
            }))
            stats.errors.append({"step": "pipeline_filter",
                                  "error": f"inconnu : {cfg.pipeline_filter}"})
            return
    else:
        prefix = ""

    logger.info(json.dumps({
        "action": "list_s3_silver",
        "bucket": cfg.bucket_silver,
        "prefix": prefix or "(all)",
    }))

    try:
        objects = _list_objects(s3, cfg.bucket_silver, prefix)
    except ClientError as e:
        stats.errors.append({"step": "list_s3_silver", "error": str(e)})
        logger.error(json.dumps({"step": "list_s3_silver", "error": str(e)}))
        return

    stats.s3_objects_found = len(objects)
    stats.s3_bytes_freed   = sum(o.get("Size", 0) for o in objects)

    if not objects:
        logger.info(json.dumps({"s3_silver": "empty", "bucket": cfg.bucket_silver}))
        return

    prefixes = sorted({"/".join(o["Key"].split("/")[:2]) for o in objects})
    logger.info(json.dumps({
        "mode":            cfg.mode.value,
        "bucket":          cfg.bucket_silver,
        "objects_found":   stats.s3_objects_found,
        "size_mb":         round(stats.s3_bytes_freed / 1_048_576, 2),
        "top_prefixes":    prefixes,
    }))

    if cfg.mode == RunMode.PROBE:
        logger.info(json.dumps({
            "probe":           "NO deletion performed",
            "would_delete":    stats.s3_objects_found,
            "would_free_mb":   round(stats.s3_bytes_freed / 1_048_576, 2),
        }))
        return

    BATCH = 1_000
    for i in range(0, len(objects), BATCH):
        batch = objects[i:i + BATCH]
        try:
            s3.delete_objects(
                Bucket=cfg.bucket_silver,
                Delete={"Objects": [{"Key": o["Key"]} for o in batch]},
            )
            stats.s3_objects_deleted += len(batch)
            logger.info(json.dumps({
                "s3_silver_deleted_batch": len(batch),
                "total_deleted":          stats.s3_objects_deleted,
            }))
        except ClientError as e:
            stats.errors.append({"step": "delete_s3_silver_batch", "error": str(e)})
            logger.error(json.dumps({"step": "delete_s3_silver_batch", "error": str(e)}))

    logger.info(json.dumps({
        "s3_silver_purge": "DONE",
        "objects_deleted": stats.s3_objects_deleted,
        "freed_mb":        round(stats.s3_bytes_freed / 1_048_576, 2),
    }))


# ── Gold PostgreSQL helpers ───────────────────────────────────────────────────
def _pg_connect(cfg: Config):
    return psycopg2.connect(
        host=cfg.pg_host, port=cfg.pg_port, dbname=cfg.pg_database,
        user=cfg.pg_user, password=cfg.pg_password, sslmode="require",
    )


def purge_gold_pg(cfg: Config, stats: Stats) -> None:
    """TRUNCATE toutes les tables des schémas gld_*. Filtre optionnel par schéma."""
    schemas = [cfg.schema_filter] if cfg.schema_filter else _ALL_GOLD_SCHEMAS

    # Validation schéma
    if cfg.schema_filter and cfg.schema_filter not in _ALL_GOLD_SCHEMAS:
        logger.error(json.dumps({
            "error":         f"schéma inconnu : '{cfg.schema_filter}'",
            "known_schemas": _ALL_GOLD_SCHEMAS,
        }))
        stats.errors.append({"step": "schema_filter",
                              "error": f"inconnu : {cfg.schema_filter}"})
        return

    try:
        conn = _pg_connect(cfg)
    except Exception as e:
        stats.errors.append({"step": "pg_connect", "error": str(e)})
        logger.error(json.dumps({"step": "pg_connect", "error": str(e)}))
        return

    with conn:
        with conn.cursor() as cur:
            # Découverte des tables existantes
            placeholders = ",".join(["%s"] * len(schemas))
            cur.execute(
                f"SELECT table_schema, table_name "
                f"FROM information_schema.tables "
                f"WHERE table_schema IN ({placeholders}) "
                f"  AND table_type = 'BASE TABLE' "
                f"ORDER BY table_schema, table_name",
                schemas,
            )
            tables = cur.fetchall()
            stats.gold_tables_found = len(tables)

            logger.info(json.dumps({
                "mode":          cfg.mode.value,
                "schemas":       schemas,
                "tables_found":  stats.gold_tables_found,
                "detail":        [f"{s}.{t}" for s, t in tables],
            }))

            if cfg.mode == RunMode.PROBE:
                logger.info(json.dumps({
                    "probe":        "NO truncation performed",
                    "would_truncate": stats.gold_tables_found,
                }))
                conn.close()
                return

            # LIVE — TRUNCATE CASCADE par table
            for schema, table in tables:
                try:
                    cur.execute(
                        f'TRUNCATE TABLE "{schema}"."{table}" CASCADE'
                    )
                    stats.gold_tables_truncated += 1
                    logger.info(json.dumps({
                        "truncated": f"{schema}.{table}",
                    }))
                except Exception as e:
                    stats.errors.append({
                        "step":  f"truncate_{schema}.{table}",
                        "error": str(e),
                    })
                    logger.error(json.dumps({
                        "step":  f"truncate_{schema}.{table}",
                        "error": str(e),
                    }))

        conn.commit()

    conn.close()
    logger.info(json.dumps({
        "gold_purge":      "DONE",
        "tables_truncated": stats.gold_tables_truncated,
    }))


# ── Orchestrateur ─────────────────────────────────────────────────────────────
def run(cfg: Config) -> Stats:
    stats = Stats()

    logger.info(json.dumps({
        "start":           True,
        "mode":            cfg.mode.value,
        "target":          cfg.target.value,
        "pipeline_filter": cfg.pipeline_filter or "(all)",
        "schema_filter":   cfg.schema_filter   or "(all)",
    }))

    if cfg.target in (PurgeTarget.S3, PurgeTarget.ALL):
        purge_silver_s3(cfg, stats)

    if cfg.target in (PurgeTarget.GOLD, PurgeTarget.ALL):
        purge_gold_pg(cfg, stats)

    report = stats.report()
    logger.info(json.dumps({"summary": report}))

    if stats.errors:
        logger.error(json.dumps({"errors": stats.errors}))

    if cfg.mode == RunMode.PROBE:
        logger.info(json.dumps({
            "probe_complete": True,
            "hint": "Relancer avec RUN_MODE=live pour exécuter la purge",
        }))

    return stats


# ── CLI ───────────────────────────────────────────────────────────────────────
def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Purge S3 Silver et/ou tables Gold PostgreSQL — GI Data Lakehouse"
    )
    p.add_argument(
        "--target",
        choices=["s3", "gold", "all"],
        default="s3",
        help="Cible : s3 (Silver Parquet) | gold (TRUNCATE gld_*) | all (défaut: s3)",
    )
    p.add_argument(
        "--pipeline",
        default=None,
        metavar="PIPELINE",
        help=(
            "Restreindre S3 à un pipeline Silver "
            "(ex: silver_missions). Défaut : tous."
        ),
    )
    p.add_argument(
        "--schema",
        default=None,
        metavar="SCHEMA",
        help=(
            "Restreindre Gold à un schéma PostgreSQL "
            "(ex: gld_performance). Défaut : tous les gld_*."
        ),
    )
    return p.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    cfg = Config(
        target=PurgeTarget(args.target),
        pipeline_filter=args.pipeline,
        schema_filter=args.schema,
    )
    result = run(cfg)
    sys.exit(1 if result.errors else 0)
