"""enrich_ban_geocode.py — Géocodage dim_clients via API BAN (Base Adresse Nationale).
Phase 1 · GI Data Lakehouse · Manifeste v2.0
"""
import sys
import json
import time
import logging
import urllib.request
import urllib.parse
from datetime import datetime, timezone

from shared import Config, Stats, get_duckdb_connection, logger

BAN_ENDPOINT = "https://api-adresse.data.gouv.fr/search/"
BATCH_SIZE = 40
BATCH_PAUSE_S = 1.0
MIN_SCORE = 0.5


def geocode_address(adresse: str, code_postal: str) -> dict | None:
    params = urllib.parse.urlencode({"q": adresse, "limit": 1, "postcode": code_postal})
    url = f"{BAN_ENDPOINT}?{params}"
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode())
            features = data.get("features", [])
            if not features:
                return None
            feat = features[0]
            score = feat["properties"].get("score", 0)
            coords = feat["geometry"]["coordinates"]  # [lon, lat]
            return {"longitude": coords[0], "latitude": coords[1], "score": score}
    except Exception:
        return None


def run(cfg: Config) -> dict:
    stats = Stats()
    stats.extra = {"geocoded_ok": 0, "geocoded_low_score": 0, "failed": 0, "skipped_no_address": 0}
    silver_path = f"s3://{cfg.bucket_silver}/slv_clients/dim_clients"

    with get_duckdb_connection(cfg) as ddb:
        try:
            clients = ddb.execute(f"""
                SELECT tie_id, adresse_complete, code_postal
                FROM read_parquet('{silver_path}/**/*.parquet', hive_partitioning=true)
                WHERE is_current = true AND latitude IS NULL
                  AND adresse_complete IS NOT NULL AND TRIM(adresse_complete) != ''
            """).fetchall()
        except Exception as e:
            logger.error(f"Cannot read Silver dim_clients: {e}")
            stats.errors.append({"error": str(e)})
            return stats.finish()

        logger.info(f"Clients to geocode: {len(clients)}")
        updates = []

        for i, (tie_id, adresse, cp) in enumerate(clients):
            if not adresse or not adresse.strip():
                stats.extra["skipped_no_address"] += 1
                continue

            result = geocode_address(adresse.strip(), str(cp or "").strip())
            if result is None:
                stats.extra["failed"] += 1
                logger.warning(f"BAN failed for tie_id={tie_id}")
            elif result["score"] < MIN_SCORE:
                stats.extra["geocoded_low_score"] += 1
                logger.warning(f"BAN low score ({result['score']:.2f}) for tie_id={tie_id}")
            else:
                updates.append((tie_id, result["latitude"], result["longitude"]))
                stats.extra["geocoded_ok"] += 1

            if (i + 1) % BATCH_SIZE == 0:
                time.sleep(BATCH_PAUSE_S)

        logger.info(f"Geocoding complete: {len(updates)} OK, {stats.extra['failed']} failed")

        # Apply updates to Silver Parquet (read → update → rewrite)
        if updates and not cfg.dry_run:
            ddb.execute("CREATE TEMP TABLE geo_updates (tie_id INT, latitude DOUBLE, longitude DOUBLE)")
            ddb.executemany("INSERT INTO geo_updates VALUES (?, ?, ?)", updates)
            ddb.execute(f"""
                CREATE TABLE dim_clients_updated AS
                SELECT
                    d.* EXCLUDE (latitude, longitude),
                    COALESCE(g.latitude, d.latitude) AS latitude,
                    COALESCE(g.longitude, d.longitude) AS longitude
                FROM read_parquet('{silver_path}/**/*.parquet', hive_partitioning=true) d
                LEFT JOIN geo_updates g ON g.tie_id = d.tie_id AND d.is_current = true
            """)
            ddb.execute(f"""
                COPY dim_clients_updated TO '{silver_path}'
                (FORMAT PARQUET, PARTITION_BY (is_current), OVERWRITE_OR_IGNORE true)
            """)
            ddb.execute("DROP TABLE dim_clients_updated")
            ddb.execute("DROP TABLE geo_updates")
        elif cfg.dry_run:
            logger.info(f"[DRY-RUN] Would update {len(updates)} clients with geocoding")

        stats.rows_transformed = len(updates)

    return stats.finish()


if __name__ == "__main__":
    cfg = Config()
    if "--dry-run" in sys.argv:
        cfg.dry_run = True
    run(cfg)
