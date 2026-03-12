"""purge_bronze.py — Utilitaire de purge S3 Bronze + watermarks PostgreSQL.
GI Data Lakehouse · Manifeste v2.0

Usage :
    uv run purge_bronze.py --target s3          # purge bucket gi-poc-bronze uniquement
    uv run purge_bronze.py --target watermarks  # purge watermarks PG uniquement
    uv run purge_bronze.py --target all         # purge S3 + watermarks
    uv run purge_bronze.py --target s3 --pipeline bronze_missions  # purge 1 pipeline S3

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

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(module)s.%(funcName)s:%(lineno)d | %(message)s",
)
logger = logging.getLogger("gi-purge")

# ── .env.local : recherche depuis le répertoire du script vers la racine projet ──


def _find_env_file() -> Path | None:
    """Remonte l'arborescence jusqu'à trouver .env.local (script → projet root)."""
    current = Path(__file__).resolve().parent
    for _ in range(4):  # max 4 niveaux
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
        "env": ".env.local",
        "loaded": False,
        "searched_from": str(Path(__file__).resolve().parent),
        "note": "Variables lues depuis l'environnement système",
    }))


# ── Enums ─────────────────────────────────────────────────────────────────────
class RunMode(str, Enum):
    PROBE = "probe"
    LIVE = "live"


class PurgeTarget(str, Enum):
    S3 = "s3"
    WATERMARKS = "watermarks"
    ALL = "all"


# ── Config ────────────────────────────────────────────────────────────────────
@dataclass
class Config:
    mode: RunMode = field(default_factory=lambda: RunMode(
        os.environ.get("RUN_MODE", "probe").lower()))
    target: PurgeTarget = PurgeTarget.S3
    pipeline_filter: Optional[str] = None   # si défini : purge 1 seul pipeline

    # S3
    s3_endpoint: str = field(
        default_factory=lambda: os.environ["OVH_S3_ENDPOINT"])
    s3_access_key: str = field(
        default_factory=lambda: os.environ["OVH_S3_ACCESS_KEY"])
    s3_secret_key: str = field(
        default_factory=lambda: os.environ["OVH_S3_SECRET_KEY"])
    bucket_bronze: str = field(default_factory=lambda:
                               os.environ.get("BRONZE_BUCKET", "gi-poc-bronze"))

    # PostgreSQL
    pg_host: str = field(default_factory=lambda: os.environ["OVH_PG_HOST"])
    pg_port: str = field(default_factory=lambda:
                         os.environ.get("OVH_PG_PORT", "5432"))
    pg_database: str = field(default_factory=lambda:
                             os.environ.get("OVH_PG_DATABASE", "gi_poc_ddi_gold"))
    pg_user: str = field(default_factory=lambda: os.environ["OVH_PG_USER"])
    pg_password: str = field(
        default_factory=lambda: os.environ["OVH_PG_PASSWORD"])


# ── Stats ─────────────────────────────────────────────────────────────────────
@dataclass
class Stats:
    s3_objects_found: int = 0
    s3_objects_deleted: int = 0
    s3_bytes_freed: int = 0
    wm_rows_found: int = 0
    wm_rows_deleted: int = 0
    errors: list = field(default_factory=list)

    def report(self) -> dict:
        return {
            "s3": {
                "objects_found": self.s3_objects_found,
                "objects_deleted": self.s3_objects_deleted,
                "bytes_freed_mb": round(self.s3_bytes_freed / 1_048_576, 2),
            },
            "watermarks": {
                "rows_found": self.wm_rows_found,
                "rows_deleted": self.wm_rows_deleted,
            },
            "errors": self.errors,
        }


# ── S3 helper ─────────────────────────────────────────────────────────────────
def _s3_client(cfg: Config):
    return boto3.client(
        "s3",
        endpoint_url=cfg.s3_endpoint,
        aws_access_key_id=cfg.s3_access_key,
        aws_secret_access_key=cfg.s3_secret_key,
    )


def _list_objects(s3, bucket: str, prefix: str = "") -> list[dict]:
    """Liste tous les objets (pagination complète)."""
    paginator = s3.get_paginator("list_objects_v2")
    params = {"Bucket": bucket}
    if prefix:
        params["Prefix"] = prefix
    objects = []
    for page in paginator.paginate(**params):
        objects.extend(page.get("Contents", []))
    return objects


def purge_s3(cfg: Config, stats: Stats) -> None:
    s3 = _s3_client(cfg)
    prefix = f"raw_{cfg.pipeline_filter.lower()}/" if cfg.pipeline_filter else ""

    logger.info(json.dumps({
        "action": "list_s3",
        "bucket": cfg.bucket_bronze,
        "prefix": prefix or "(all)",
    }))

    try:
        objects = _list_objects(s3, cfg.bucket_bronze, prefix)
    except ClientError as e:
        stats.errors.append({"step": "list_s3", "error": str(e)})
        logger.error(json.dumps({"step": "list_s3", "error": str(e)}))
        return

    stats.s3_objects_found = len(objects)
    stats.s3_bytes_freed = sum(o.get("Size", 0) for o in objects)

    if not objects:
        logger.info(json.dumps({"s3": "empty", "bucket": cfg.bucket_bronze}))
        return

    # Aperçu des préfixes (top-level)
    prefixes = sorted({o["Key"].split("/")[0] for o in objects})
    logger.info(json.dumps({
        "mode": cfg.mode.value,
        "bucket": cfg.bucket_bronze,
        "objects_found": stats.s3_objects_found,
        "size_mb": round(stats.s3_bytes_freed / 1_048_576, 2),
        "prefixes": prefixes,
    }))

    if cfg.mode == RunMode.PROBE:
        logger.info(json.dumps({
            "probe": "NO deletion performed",
            "would_delete": stats.s3_objects_found,
            "would_free_mb": round(stats.s3_bytes_freed / 1_048_576, 2),
        }))
        return

    # ── LIVE : suppression par batch de 1 000 (limite AWS/OVH) ──
    BATCH = 1_000
    for i in range(0, len(objects), BATCH):
        batch = objects[i:i + BATCH]
        try:
            s3.delete_objects(
                Bucket=cfg.bucket_bronze,
                Delete={"Objects": [{"Key": o["Key"]} for o in batch]},
            )
            stats.s3_objects_deleted += len(batch)
            logger.info(json.dumps({
                "s3_deleted_batch": len(batch),
                "total_deleted": stats.s3_objects_deleted,
            }))
        except ClientError as e:
            stats.errors.append({"step": "delete_s3_batch", "error": str(e)})
            logger.error(json.dumps(
                {"step": "delete_s3_batch", "error": str(e)}))

    logger.info(json.dumps({
        "s3_purge": "DONE",
        "objects_deleted": stats.s3_objects_deleted,
        "freed_mb": round(stats.s3_bytes_freed / 1_048_576, 2),
    }))


# ── Watermarks helper ─────────────────────────────────────────────────────────
_ALL_PIPELINES = [
    "bronze_missions",
    "bronze_agences",
    "bronze_clients",
    "bronze_interimaires",
]


def purge_watermarks(cfg: Config, stats: Stats) -> None:
    pipelines = [
        cfg.pipeline_filter] if cfg.pipeline_filter else _ALL_PIPELINES

    try:
        conn = psycopg2.connect(
            host=cfg.pg_host, port=cfg.pg_port, dbname=cfg.pg_database,
            user=cfg.pg_user, password=cfg.pg_password, sslmode="require",
        )
    except Exception as e:
        stats.errors.append({"step": "pg_connect", "error": str(e)})
        logger.error(json.dumps({"step": "pg_connect", "error": str(e)}))
        return

    with conn:
        with conn.cursor() as cur:
            placeholders = ",".join(["%s"] * len(pipelines))

            # Comptage
            cur.execute(
                f"SELECT pipeline, table_name, last_success "
                f"FROM ops.pipeline_watermarks "
                f"WHERE pipeline IN ({placeholders})",
                pipelines,
            )
            rows = cur.fetchall()
            stats.wm_rows_found = len(rows)

            logger.info(json.dumps({
                "mode": cfg.mode.value,
                "watermarks_found": stats.wm_rows_found,
                "pipelines": pipelines,
                "detail": [{"pipeline": r[0], "table": r[1],
                            "last_success": str(r[2])} for r in rows],
            }))

            if cfg.mode == RunMode.PROBE:
                logger.info(json.dumps({
                    "probe": "NO deletion performed",
                    "would_delete": stats.wm_rows_found,
                }))
                conn.close()
                return

            # ── LIVE ──
            cur.execute(
                f"DELETE FROM ops.pipeline_watermarks "
                f"WHERE pipeline IN ({placeholders})",
                pipelines,
            )
            stats.wm_rows_deleted = cur.rowcount
            logger.info(json.dumps({
                "watermarks_purge": "DONE",
                "rows_deleted": stats.wm_rows_deleted,
            }))

    conn.close()


# ── Orchestrateur ─────────────────────────────────────────────────────────────
def run(cfg: Config) -> Stats:
    stats = Stats()

    logger.info(json.dumps({
        "start": True,
        "mode": cfg.mode.value,
        "target": cfg.target.value,
        "bucket": cfg.bucket_bronze,
        "pipeline_filter": cfg.pipeline_filter or "(all)",
    }))

    if cfg.target in (PurgeTarget.S3, PurgeTarget.ALL):
        purge_s3(cfg, stats)

    if cfg.target in (PurgeTarget.WATERMARKS, PurgeTarget.ALL):
        purge_watermarks(cfg, stats)

    report = stats.report()
    logger.info(json.dumps({"summary": report}))

    if stats.errors:
        logger.error(json.dumps({"errors": stats.errors}))
        return stats

    if cfg.mode == RunMode.PROBE:
        logger.info(json.dumps({
            "probe_complete": True,
            "hint": "Relancer avec RUN_MODE=live pour exécuter la purge",
        }))

    return stats


# ── CLI ───────────────────────────────────────────────────────────────────────
def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Purge S3 Bronze et/ou watermarks PostgreSQL — GI Data Lakehouse"
    )
    p.add_argument(
        "--target",
        choices=["s3", "watermarks", "all"],
        default="s3",
        help="Cible de purge : s3 | watermarks | all (défaut: s3)",
    )
    p.add_argument(
        "--pipeline",
        default=None,
        metavar="PIPELINE",
        help=(
            "Restreindre à un seul pipeline "
            "(ex: bronze_missions). Défaut : tous les pipelines."
        ),
    )
    return p.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    cfg = Config(
        target=PurgeTarget(args.target),
        pipeline_filter=args.pipeline,
    )
    result = run(cfg)
    sys.exit(1 if result.errors else 0)
