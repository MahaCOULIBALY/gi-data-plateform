"""purge_silver.py — Purge S3 Silver (Parquet ZSTD) + suppression complète des versions.
GI Data Lakehouse · Manifeste v2.1

Usage :
    uv run purge_silver.py                                            # probe : affiche ce qui serait supprimé
    RUN_MODE=live uv run purge_silver.py                              # supprime tout le bucket Silver
    RUN_MODE=live uv run purge_silver.py --pipeline silver_missions   # un pipeline seulement
    RUN_MODE=live uv run purge_silver.py --bucket_name gi-poc-silver  # bucket custom

Pipelines reconnus (--pipeline) :
    silver_missions | silver_interimaires | silver_agences |
    silver_clients  | silver_temps        | silver_facturation

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

import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(module)s.%(funcName)s:%(lineno)d | %(message)s",
)
logger = logging.getLogger("gi-purge-silver")

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


# ── Mapping pipeline → préfixe S3 Silver ─────────────────────────────────────
_PIPELINE_PREFIX: dict[str, str] = {
    "silver_missions": "slv_missions/",
    "silver_interimaires": "slv_interimaires/",
    "silver_agences": "slv_agences/",
    "silver_clients": "slv_clients/",
    "silver_temps": "slv_temps/",
    "silver_facturation": "slv_facturation/",
}

# ── Config ────────────────────────────────────────────────────────────────────


@dataclass
class Config:
    mode: RunMode = field(default_factory=lambda: RunMode(
        os.environ.get("RUN_MODE", "probe").lower()))
    pipeline_filter: Optional[str] = None
    bucket_override: Optional[str] = None

    s3_endpoint: str = field(
        default_factory=lambda: os.environ["OVH_S3_ENDPOINT"])
    s3_access_key: str = field(
        default_factory=lambda: os.environ["OVH_S3_ACCESS_KEY"])
    s3_secret_key: str = field(
        default_factory=lambda: os.environ["OVH_S3_SECRET_KEY"])
    s3_region: str = field(default_factory=lambda: os.environ.get(
        "OVH_S3_REGION", "eu-west-par"))
    _bucket_default: str = field(default_factory=lambda: os.environ.get(
        "OVH_S3_BUCKET_SILVER", "gi-data-prod-silver"))

    @property
    def bucket_silver(self) -> str:
        return self.bucket_override or self._bucket_default

# ── Stats ─────────────────────────────────────────────────────────────────────


@dataclass
class Stats:
    versions_found: int = 0
    versions_deleted: int = 0
    markers_found: int = 0
    markers_deleted: int = 0
    bytes_freed: int = 0
    errors: list = field(default_factory=list)

    def report(self) -> dict:
        return {
            "silver_s3": {
                "versions_found": self.versions_found,
                "versions_deleted": self.versions_deleted,
                "markers_found": self.markers_found,
                "markers_deleted": self.markers_deleted,
                "freed_mb": round(self.bytes_freed / 1_048_576, 2),
            },
            "errors": self.errors,
        }

# ── S3 ────────────────────────────────────────────────────────────────────────


def _s3_client(cfg: Config):
    return boto3.client(
        "s3",
        endpoint_url=cfg.s3_endpoint,
        aws_access_key_id=cfg.s3_access_key,
        aws_secret_access_key=cfg.s3_secret_key,
        region_name=cfg.s3_region,
    )


def _list_all_versions(s3, bucket: str, prefix: str = "") -> tuple[list[dict], list[dict]]:
    """
    Retourne (versions, delete_markers) via list_object_versions (pagination complète).
    Contrairement à list_objects_v2, cette API expose TOUT l'historique versionné.
    """
    paginator = s3.get_paginator("list_object_versions")
    params = {"Bucket": bucket}
    if prefix:
        params["Prefix"] = prefix

    versions: list[dict] = []
    markers: list[dict] = []

    for page in paginator.paginate(**params):
        versions.extend(page.get("Versions", []))
        markers.extend(page.get("DeleteMarkers", []))

    return versions, markers


def _delete_batch(s3, bucket: str, items: list[dict], stats: Stats, step_label: str) -> None:
    """Supprime une liste d'objets {Key, VersionId} par batch de 1 000."""
    BATCH = 1_000
    for i in range(0, len(items), BATCH):
        batch = items[i:i + BATCH]
        try:
            resp = s3.delete_objects(
                Bucket=bucket,
                Delete={"Objects": [
                    {"Key": o["Key"], "VersionId": o["VersionId"]} for o in batch]},
            )
            failed = resp.get("Errors", [])
            deleted = len(batch) - len(failed)

            if step_label == "versions":
                stats.versions_deleted += deleted
            else:
                stats.markers_deleted += deleted

            if failed:
                for err in failed:
                    stats.errors.append({
                        "step": f"delete_{step_label}",
                        "key": err.get("Key"),
                        "version_id": err.get("VersionId"),
                        "error": err.get("Message"),
                    })
                logger.error(json.dumps(
                    {f"{step_label}_partial_failure": len(failed)}))
            else:
                logger.info(json.dumps({
                    f"{step_label}_deleted_batch": deleted,
                    f"total_{step_label}_deleted": stats.versions_deleted if step_label == "versions" else stats.markers_deleted,
                }))
        except ClientError as e:
            stats.errors.append(
                {"step": f"delete_{step_label}_batch", "error": str(e)})
            logger.error(json.dumps(
                {"step": f"delete_{step_label}_batch", "error": str(e)}))

# ── Run ───────────────────────────────────────────────────────────────────────


def run(cfg: Config) -> Stats:
    stats = Stats()
    logger.info(json.dumps({
        "start": True,
        "mode": cfg.mode.value,
        "bucket": cfg.bucket_silver,
        "pipeline_filter": cfg.pipeline_filter or "(all)",
    }))

    s3 = _s3_client(cfg)

    if cfg.pipeline_filter:
        prefix = _PIPELINE_PREFIX.get(cfg.pipeline_filter.lower())
        if prefix is None:
            logger.error(json.dumps({
                "error": f"pipeline inconnu : '{cfg.pipeline_filter}'",
                "known_pipelines": list(_PIPELINE_PREFIX.keys()),
            }))
            stats.errors.append(
                {"step": "pipeline_filter", "error": f"inconnu : {cfg.pipeline_filter}"})
            return stats
    else:
        prefix = ""

    try:
        versions, markers = _list_all_versions(s3, cfg.bucket_silver, prefix)
    except ClientError as e:
        stats.errors.append({"step": "list_object_versions", "error": str(e)})
        logger.error(json.dumps(
            {"step": "list_object_versions", "error": str(e)}))
        return stats

    stats.versions_found = len(versions)
    stats.markers_found = len(markers)
    stats.bytes_freed = sum(v.get("Size", 0) for v in versions)

    logger.info(json.dumps({
        "mode": cfg.mode.value,
        "bucket": cfg.bucket_silver,
        "versions_found": stats.versions_found,
        "delete_markers_found": stats.markers_found,
        "size_mb": round(stats.bytes_freed / 1_048_576, 2),
    }))

    if not versions and not markers:
        logger.info(json.dumps(
            {"s3_silver": "already empty (no versions, no markers)"}))
        logger.info(json.dumps({"summary": stats.report()}))
        return stats

    if cfg.mode == RunMode.PROBE:
        logger.info(json.dumps({
            "probe": "NO deletion performed",
            "would_delete_versions": stats.versions_found,
            "would_delete_markers": stats.markers_found,
            "would_free_mb": round(stats.bytes_freed / 1_048_576, 2),
            "hint": "Relancer avec RUN_MODE=live pour exécuter",
        }))
        logger.info(json.dumps({"summary": stats.report()}))
        return stats

    # ── LIVE ──────────────────────────────────────────────────────────────────
    if versions:
        logger.info(json.dumps(
            {"action": "delete_versions", "count": len(versions)}))
        _delete_batch(s3, cfg.bucket_silver, versions, stats, "versions")

    if markers:
        logger.info(json.dumps(
            {"action": "delete_markers", "count": len(markers)}))
        _delete_batch(s3, cfg.bucket_silver, markers, stats, "markers")

    logger.info(json.dumps({
        "s3_silver_purge": "DONE",
        "versions_deleted": stats.versions_deleted,
        "markers_deleted": stats.markers_deleted,
        "freed_mb": round(stats.bytes_freed / 1_048_576, 2),
    }))
    logger.info(json.dumps({"summary": stats.report()}))
    if stats.errors:
        logger.error(json.dumps({"errors": stats.errors}))
    return stats


# ── CLI ───────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    p = argparse.ArgumentParser(
        description="Purge S3 Silver — GI Data Lakehouse")
    p.add_argument("--pipeline", default=None, metavar="PIPELINE",
                   help="Restreindre à un pipeline Silver (ex: silver_missions). Défaut : tous.")
    p.add_argument("--bucket_name", default=None, metavar="BUCKET",
                   help="Overrider le bucket cible (défaut: OVH_S3_BUCKET_SILVER ou gi-data-prod-silver).")
    args = p.parse_args()
    cfg = Config(pipeline_filter=args.pipeline,
                 bucket_override=args.bucket_name)
    result = run(cfg)
    sys.exit(1 if result.errors else 0)
