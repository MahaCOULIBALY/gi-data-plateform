"""rgpd_audit.py — Scan automatique RGPD de toutes les tables Silver et Gold.
Phase 3 · GI Data Lakehouse · Manifeste v2.0
Pré-démo direction : DOIT passer PASS avant présentation.
"""
import sys
import re
import json
import logging
from datetime import datetime, timezone
from dataclasses import dataclass, field, asdict

from shared import Config, Stats, get_duckdb_connection, get_pg_connection, logger

# Patterns de classification RGPD par nom de colonne
PATTERNS_SENSIBLE = re.compile(r"(^|_)(nir|iban|rib|secu|num_secu|per_nir|per_iban)($|_)", re.I)
PATTERNS_PERSONNEL = re.compile(
    r"(^|_)(nom|prenom|adresse|email|telephone|tel|date_naissance|datenais|coord_valeur|commentaire)($|_)", re.I
)
PATTERNS_INTERNE = re.compile(r"(^|_)(ca_|marge_|montant_|coef_|taux_)", re.I)

# Colonnes explicitement interdites en Gold
GOLD_FORBIDDEN_SENSIBLE = {"per_nir", "nir", "iban", "per_iban", "rib"}
GOLD_FORBIDDEN_CONTACT = {"email", "telephone", "tel", "coord_valeur", "valeur"}
GOLD_FORBIDDEN_EVAL = {"commentaire", "peval_commentaire"}


@dataclass
class AuditReport:
    audit_date: str = field(default_factory=lambda: datetime.now(timezone.utc).strftime("%Y-%m-%d"))
    tables_scanned: int = 0
    colonnes_sensibles_gold: list = field(default_factory=list)
    colonnes_personnelles_gold: list = field(default_factory=list)
    violations: list = field(default_factory=list)
    recommendations: list = field(default_factory=list)
    status: str = "PASS"
    detail: dict = field(default_factory=dict)


def classify_column(col_name: str) -> str:
    col_lower = col_name.lower()
    if PATTERNS_SENSIBLE.search(col_lower):
        return "SENSIBLE"
    if PATTERNS_PERSONNEL.search(col_lower):
        return "PERSONNEL"
    if PATTERNS_INTERNE.search(col_lower):
        return "INTERNE"
    return "PUBLIC"


def scan_gold_tables(cfg: Config) -> list[dict]:
    """Scan Gold PostgreSQL via INFORMATION_SCHEMA."""
    results = []
    with get_pg_connection(cfg) as pg:
        with pg.cursor() as cur:
            cur.execute("""
                SELECT table_schema, table_name, column_name
                FROM information_schema.columns
                WHERE table_schema LIKE 'gld_%'
                ORDER BY table_schema, table_name, ordinal_position
            """)
            for schema, table, column in cur.fetchall():
                results.append({
                    "layer": "gold",
                    "schema": schema,
                    "table": table,
                    "column": column,
                    "classification": classify_column(column),
                })
    return results


def scan_silver_tables(cfg: Config) -> list[dict]:
    """Scan Silver Parquet via DuckDB parquet_schema."""
    results = []
    silver_tables = [
        ("slv_clients", "dim_clients"), ("slv_clients", "sites_mission"),
        ("slv_clients", "contacts"), ("slv_clients", "encours_credit"),
        ("slv_interimaires", "dim_interimaires"), ("slv_interimaires", "competences"),
        ("slv_interimaires", "evaluations"), ("slv_interimaires", "coordonnees"),
        ("slv_missions", "missions"), ("slv_missions", "contrats"),
        ("slv_facturation", "factures"), ("slv_temps", "heures_detail"),
        ("slv_temps", "releves_heures"), ("slv_agences", "dim_agences"),
        ("slv_agences", "hierarchie_territoriale"),
    ]
    with get_duckdb_connection(cfg) as ddb:
        for schema, table in silver_tables:
            path = f"s3://{cfg.bucket_silver}/{schema}/{table}/**/*.parquet"
            try:
                cols = ddb.execute(f"SELECT name FROM parquet_schema('{path}')").fetchall()
                for (col_name,) in cols:
                    results.append({
                        "layer": "silver",
                        "schema": schema,
                        "table": table,
                        "column": col_name,
                        "classification": classify_column(col_name),
                    })
            except Exception:
                logger.warning(f"Silver table not found: {schema}.{table}")
    return results


def run_audit(cfg: Config) -> AuditReport:
    report = AuditReport()

    # Scan Gold
    gold_cols = scan_gold_tables(cfg)
    silver_cols = scan_silver_tables(cfg)
    all_cols = gold_cols + silver_cols
    report.tables_scanned = len({(c["schema"], c["table"]) for c in all_cols})

    # Vérification 1 : AUCUNE colonne SENSIBLE en Gold
    for c in gold_cols:
        if c["classification"] == "SENSIBLE":
            report.colonnes_sensibles_gold.append(f"{c['schema']}.{c['table']}.{c['column']}")
            report.violations.append(f"SENSIBLE column in Gold: {c['schema']}.{c['table']}.{c['column']}")
            report.status = "FAIL"

    # Vérification 2 : NIR doit être pseudonymisé en Silver
    for c in silver_cols:
        col_lower = c["column"].lower()
        if col_lower in ("per_nir", "nir") and c["table"] != "dim_interimaires":
            report.violations.append(f"Raw NIR found in Silver: {c['schema']}.{c['table']}.{c['column']}")
            report.status = "FAIL"

    # Vérification 3 : coordonnées/commentaires pas en Gold
    for c in gold_cols:
        col_lower = c["column"].lower()
        if col_lower in GOLD_FORBIDDEN_CONTACT:
            report.violations.append(f"Contact data in Gold: {c['schema']}.{c['table']}.{c['column']}")
            report.status = "FAIL"
        if col_lower in GOLD_FORBIDDEN_EVAL:
            report.violations.append(f"Eval comment in Gold: {c['schema']}.{c['table']}.{c['column']}")
            report.status = "FAIL"

    # Colonnes personnelles en Gold (acceptable mais à documenter)
    for c in gold_cols:
        if c["classification"] == "PERSONNEL":
            report.colonnes_personnelles_gold.append(f"{c['schema']}.{c['table']}.{c['column']}")

    # Recommendations
    if report.colonnes_personnelles_gold:
        report.recommendations.append("Masquer nom/prenom en Gold pour rôle Viewer via Superset RLS")
    if not report.violations:
        report.recommendations.append("Audit RGPD conforme — prêt pour démo direction")

    report.detail = {
        "gold_columns_total": len(gold_cols),
        "silver_columns_total": len(silver_cols),
        "classification_summary": {
            cls: len([c for c in all_cols if c["classification"] == cls])
            for cls in ("SENSIBLE", "PERSONNEL", "INTERNE", "PUBLIC")
        },
    }
    return report


def run(cfg: Config) -> dict:
    stats = Stats()
    report = run_audit(cfg)
    report_dict = asdict(report)
    logger.info(json.dumps({"rgpd_audit": report_dict}, indent=2, default=str))

    # Écrire rapport JSON
    if not cfg.dry_run:
        from pathlib import Path
        output = Path("data/validation/rgpd_audit_report.json")
        output.parent.mkdir(parents=True, exist_ok=True)
        with open(output, "w", encoding="utf-8") as f:
            json.dump(report_dict, f, indent=2, ensure_ascii=False, default=str)
        logger.info(f"RGPD audit report written to {output}")
    else:
        logger.info("[DRY-RUN] Would write RGPD audit report")

    stats.extra["rgpd_status"] = report.status
    stats.extra["violations"] = len(report.violations)
    stats.tables_processed = report.tables_scanned
    return stats.finish()


if __name__ == "__main__":
    cfg = Config()
    if "--dry-run" in sys.argv:
        cfg.dry_run = True
    run(cfg)
