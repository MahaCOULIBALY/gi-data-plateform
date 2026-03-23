"""probe_pg.py — Valide la connectivité PostgreSQL 18 Gold (OVH EU-WEST-PAR).
Exécution : RUN_MODE=probe python probe_pg.py
Exit 0 si succès, Exit 1 si erreur avec message explicite.
"""
import json
import sys

from shared import Config, RunMode, get_pg_connection, logger


def run(cfg: Config) -> None:
    logger.info(json.dumps({"probe": "pg_start", "host": cfg.ovh_pg_host, "port": cfg.ovh_pg_port}))

    try:
        conn = get_pg_connection(cfg)
    except Exception as e:
        logger.error(json.dumps({"probe": "pg_connect_failed", "error": str(e)}))
        sys.exit(1)

    try:
        with conn.cursor() as cur:
            cur.execute("SELECT version();")
            row = cur.fetchone()
            version = row[0] if row else "unknown"

            cur.execute("SELECT current_database();")
            row = cur.fetchone()
            database = row[0] if row else "unknown"

            cur.execute("SELECT pg_size_pretty(pg_database_size(current_database()));")
            row = cur.fetchone()
            db_size = row[0] if row else "unknown"

            cur.execute(
                "SELECT schema_name FROM information_schema.schemata "
                "WHERE schema_name NOT LIKE 'pg_%' AND schema_name != 'information_schema' "
                "ORDER BY schema_name;"
            )
            schemas = [r[0] for r in cur.fetchall()]

        logger.info(json.dumps({
            "probe": "pg_ok",
            "version": version,
            "database": database,
            "db_size": db_size,
            "schemas": schemas,
        }))

        if cfg.mode == RunMode.PROBE:
            logger.info(json.dumps({"probe": "pg_probe_only", "note": "no write — probe mode"}))

    except Exception as e:
        logger.error(json.dumps({"probe": "pg_query_failed", "error": str(e)}))
        sys.exit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    run(Config())
