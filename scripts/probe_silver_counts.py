"""probe_silver_counts.py — Valide la lisibilité de toutes les tables Silver Parquet
nécessaires aux scripts Gold, sans aucune connexion PostgreSQL.

Exécution :
    python scripts/probe_silver_counts.py
    RUN_MODE=probe python scripts/probe_silver_counts.py

Sortie : JSON par table → {namespace, table, path, rows, status, error?}
Exit 0 si toutes les tables sont lisibles (rows >= 0).
Exit 1 si au moins une table est en erreur.

Utilité : permet de valider la totalité du chemin S3→DuckDB avant que
l'accès PostgreSQL soit ouvert par l'équipe infra.
"""
import json
import sys
from dataclasses import dataclass, field
from typing import Optional

from shared import Config, get_duckdb_connection, logger

# ── Tables Silver requises par les scripts Gold ───────────────────────────────
# Format : (namespace, table) — mapped vers slv_{namespace}/{table}/**/*.parquet
SILVER_TABLES = [
    # Agences
    ("agences",      "dim_agences"),
    ("agences",      "hierarchie_territoriale"),
    # Clients
    ("clients",      "dim_clients"),
    ("clients",      "sites_mission"),
    ("clients",      "encours_credit"),
    # Missions
    ("missions",     "missions"),
    ("missions",     "contrats"),
    ("missions",     "commandes"),
    ("missions",     "fin_mission"),
    # Facturation
    ("facturation",  "factures"),
    ("facturation",  "lignes_factures"),
    # Temps
    ("temps",        "releves_heures"),
    ("temps",        "heures_detail"),
    # Intérimaires
    ("interimaires", "dim_interimaires"),
    ("interimaires", "competences"),
    ("interimaires", "fidelisation"),
    ("interimaires", "portefeuille_agences"),
]


@dataclass
class TableResult:
    namespace: str
    table: str
    path: str
    rows: Optional[int] = None
    columns: Optional[int] = None
    status: str = "PENDING"
    error: Optional[str] = None


def probe_table(ddb, cfg: Config, namespace: str, table: str) -> TableResult:
    path = f"s3://{cfg.bucket_silver}/slv_{namespace}/{table}/**/*.parquet"
    result = TableResult(namespace=namespace, table=table, path=path)
    try:
        row = ddb.execute(
            f"SELECT COUNT(*) FROM read_parquet('{path}')"
        ).fetchone()
        result.rows = row[0] if row else 0

        desc = ddb.execute(
            f"DESCRIBE SELECT * FROM read_parquet('{path}') LIMIT 0"
        ).fetchall()
        result.columns = len(desc)
        result.status = "OK"
    except Exception as e:
        result.status = "ERROR"
        result.error = str(e)
    return result


def run(cfg: Config) -> int:
    logger.info(json.dumps({
        "probe": "silver_counts_start",
        "bucket": cfg.bucket_silver,
        "tables": len(SILVER_TABLES),
    }))

    results: list[TableResult] = []

    with get_duckdb_connection(cfg) as ddb:
        for namespace, table in SILVER_TABLES:
            result = probe_table(ddb, cfg, namespace, table)
            results.append(result)
            log = {
                "namespace": result.namespace,
                "table": result.table,
                "rows": result.rows,
                "columns": result.columns,
                "status": result.status,
            }
            if result.error:
                log["error"] = result.error
                logger.error(json.dumps(log))
            else:
                logger.info(json.dumps(log))

    # ── Résumé ──────────────────────────────────────────────────────────────
    ok = [r for r in results if r.status == "OK"]
    errors = [r for r in results if r.status == "ERROR"]

    total_rows = sum(r.rows or 0 for r in ok)

    summary = {
        "probe": "silver_counts_summary",
        "tables_ok": len(ok),
        "tables_error": len(errors),
        "total_rows": total_rows,
        "status": "ALL_OK" if not errors else "PARTIAL_ERRORS",
    }
    if errors:
        summary["errors"] = [
            {"namespace": r.namespace, "table": r.table, "error": r.error}
            for r in errors
        ]

    logger.info(json.dumps(summary))

    if errors:
        logger.error(f"{len(errors)} table(s) inaccessible(s) — vérifier S3 credentials / prefixes Silver")
        return 1

    logger.info(f"✓ {len(ok)}/{len(SILVER_TABLES)} tables Silver lisibles — "
                f"{total_rows:,} lignes totales — prêt pour Gold probe dès PG accessible")
    return 0


if __name__ == "__main__":
    cfg = Config()
    sys.exit(run(cfg))
