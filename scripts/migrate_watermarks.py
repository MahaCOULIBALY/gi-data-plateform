"""migrate_watermarks.py — Création table pipeline_watermarks dans PostgreSQL Gold.
GI Data Lakehouse · Manifeste v2.0 · One-shot migration

Usage :
    uv run scripts/migrate_watermarks.py          # crée la table si absente
    uv run scripts/migrate_watermarks.py --drop   # DROP + recréation (reset total)
"""
import argparse
import json
import logging
import os
import sys
from pathlib import Path

import psycopg2
from dotenv import load_dotenv

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(module)s.%(funcName)s:%(lineno)d | %(message)s",
)
logger = logging.getLogger("gi-migrate")

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

# ── DDL ───────────────────────────────────────────────────────────────────────
_DDL_SCHEMA = "CREATE SCHEMA IF NOT EXISTS ops;"

_DDL_TABLE = """
CREATE TABLE IF NOT EXISTS ops.pipeline_watermarks (
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
)
"""

_DDL_DROP = "DROP TABLE IF EXISTS ops.pipeline_watermarks CASCADE;"

_DDL_INDEX_1 = "CREATE INDEX IF NOT EXISTS idx_wm_pipeline ON ops.pipeline_watermarks (pipeline)"
_DDL_INDEX_2 = "CREATE INDEX IF NOT EXISTS idx_wm_updated  ON ops.pipeline_watermarks (updated_at DESC)"


def migrate(drop_first: bool = False) -> None:
    try:
        conn = psycopg2.connect(
            host=os.environ["OVH_PG_HOST"],
            port=os.environ.get("OVH_PG_PORT", "5432"),
            dbname=os.environ.get("OVH_PG_DATABASE", "gi_poc_ddi_gold"),
            user=os.environ["OVH_PG_USER"],
            password=os.environ["OVH_PG_PASSWORD"],
            sslmode="require",
        )
    except Exception as e:
        logger.error(json.dumps({"step": "pg_connect", "error": str(e)}))
        sys.exit(1)

    with conn:
        with conn.cursor() as cur:
            if drop_first:
                logger.warning(json.dumps(
                    {"action": "DROP TABLE ops.pipeline_watermarks"}))
                cur.execute(_DDL_DROP)
                logger.info(json.dumps({"drop": "OK"}))

            cur.execute(_DDL_SCHEMA)
            cur.execute(_DDL_TABLE)
            cur.execute(_DDL_INDEX_1)
            cur.execute(_DDL_INDEX_2)
            logger.info(json.dumps(
                {"create": "OK", "table": "ops.pipeline_watermarks"}))

            # Vérification post-création
            cur.execute("""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_schema = 'ops' AND table_name = 'pipeline_watermarks'
                ORDER BY ordinal_position
            """)
            cols = [{"col": r[0], "type": r[1]} for r in cur.fetchall()]
            logger.info(json.dumps({"columns": cols}))

    conn.close()
    logger.info(json.dumps({"migration": "DONE", "table": "ops.pipeline_watermarks", "hint":
                            "ops.pipeline_watermarks prête — relancer purge_bronze.py --target watermarks"}))


if __name__ == "__main__":
    p = argparse.ArgumentParser(
        description="Crée la table pipeline_watermarks dans PostgreSQL Gold")
    p.add_argument("--drop", action="store_true",
                   help="DROP + recréation (reset total des watermarks)")
    args = p.parse_args()
    migrate(drop_first=args.drop)
