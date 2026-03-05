"""probe_ddl.py — Validation DDL Evolia : INFORMATION_SCHEMA vs colonnes attendues ETL.
Phase 0 · GI Data Lakehouse · Manifeste v2.0
Usage  : RUN_MODE=probe python probe_ddl.py > ddl_probe_result.json
Sortie : JSON { summary, tables: { expected, actual, missing, extra, uncertain_missing, status } }
Schéma : probe_ddl_schema.py (données pures — modifier sans toucher ce fichier)
"""
import json
import sys
from dataclasses import dataclass, asdict
from datetime import datetime, timezone

import pyodbc
import os

from shared import Config, RunMode, get_evolia_connection, logger
from probe_ddl_schema import EXPECTED, UNCERTAIN_COLUMNS


@dataclass
class TableResult:
    table: str
    domain: str
    expected_count: int
    actual_count: int
    missing: list[str]           # attendues mais absentes du DDL réel
    uncertain_missing: list[str]  # subset de missing marquées ? dans schema
    extra: list[str]             # présentes DDL mais non utilisées (info)
    status: str                  # PASS | WARN | FAIL | NOT_FOUND
    error: str = ""


# Table → domaine (pour grouper le rapport)
_DOMAIN_MAP: dict[str, str] = {
    "PYPERSONNE": "interimaires", "PYSALARIE": "interimaires",
    "WTPINT": "interimaires", "PYCOORDONNEE": "interimaires",
    "WTPMET": "interimaires", "WTPHAB": "interimaires",
    "WTPDIP": "interimaires", "WTEXP": "interimaires",
    "WTPEVAL": "interimaires", "RHPERSONNE": "interimaires",
    "WTTIESERV": "clients", "WTCLPT": "clients", "WTTIEINT": "clients",
    "WTCOEF": "clients", "WTENCOURSG": "clients", "WTUGCLI": "clients",
    "WTUG": "agences", "PYETABLISSEMENT": "agences",
    "WTMISS": "missions", "WTCNTI": "missions",
    "WTCMD": "missions", "WTPLAC": "missions",
    "WTEFAC": "facturation", "WTFAC": "facturation",
    "WTLFAC": "facturation", "CMFACTURES": "facturation", "WTFACINFO": "facturation",
    "WTPRH": "temps", "WTRHDON": "temps",
    "WTPSTG": "interimaires",
    "PYCONTRAT": "contrats_paie",
    "WTRFAN": "facturation", "WTCALF": "facturation",
    "WTUGAG": "agences",
    "WTMET": "referentiels", "WTQUA": "referentiels",
    "WTNIVQ": "referentiels", "WTSPE": "referentiels",
    "WTTHAB": "referentiels", "WTORG": "referentiels",
    "WTTDIP": "referentiels", "WTFINMISS": "referentiels",
    "WTNAF2008": "referentiels", "WTCOMMUNE": "referentiels",
    "CMDEVISES": "referentiels",
}


def _get_actual_columns(conn: pyodbc.Connection, table: str) -> list[str]:
    """Retourne les colonnes réelles depuis INFORMATION_SCHEMA, ordonnées."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
            "WHERE TABLE_NAME = ? ORDER BY ORDINAL_POSITION",
            table,
        )
        return [row[0] for row in cur.fetchall()]


def probe_table(conn: pyodbc.Connection, table: str, expected: list[str]) -> TableResult:
    domain = _DOMAIN_MAP.get(table, "unknown")
    uncertain = UNCERTAIN_COLUMNS.get(table, [])

    try:
        actual = _get_actual_columns(conn, table)
    except Exception as e:
        return TableResult(
            table=table, domain=domain,
            expected_count=len(expected), actual_count=0,
            missing=expected, uncertain_missing=uncertain,
            extra=[], status="FAIL", error=str(e),
        )

    if not actual:
        return TableResult(
            table=table, domain=domain,
            expected_count=len(expected), actual_count=0,
            missing=expected, uncertain_missing=uncertain,
            extra=[], status="NOT_FOUND",
            error="Table absente de INFORMATION_SCHEMA",
        )

    actual_set = set(actual)
    expected_set = set(expected)
    missing = sorted(expected_set - actual_set)
    uncertain_missing = [c for c in missing if c in uncertain]
    confirmed_missing = [c for c in missing if c not in uncertain]
    extra = sorted(actual_set - expected_set)

    # PASS = 0 missing | WARN = uniquement ? manquants (normal) | FAIL = confirmed missing
    if not missing:
        status = "PASS"
    elif not confirmed_missing:
        status = "WARN"
    else:
        status = "FAIL"

    return TableResult(
        table=table, domain=domain,
        expected_count=len(expected), actual_count=len(actual),
        missing=missing, uncertain_missing=uncertain_missing,
        extra=extra[:20],
        status=status,
    )


def run(cfg: Config) -> dict:
    results: dict[str, dict] = {}
    status_counts: dict[str, int] = {
        "PASS": 0, "WARN": 0, "FAIL": 0, "NOT_FOUND": 0}

    with get_evolia_connection(cfg) as conn:
        for table, expected in EXPECTED.items():
            r = probe_table(conn, table, expected)
            results[table] = asdict(r)
            status_counts[r.status] = status_counts.get(r.status, 0) + 1

            log_fn = logger.info if r.status in (
                "PASS", "WARN") else logger.warning
            log_fn(json.dumps({
                "table": table, "domain": r.domain, "status": r.status,
                "missing_confirmed": [c for c in r.missing if c not in r.uncertain_missing],
                "missing_uncertain": r.uncertain_missing,
            }))

    by_domain: dict[str, list[str]] = {}
    for table, r in results.items():
        d = r["domain"]
        by_domain.setdefault(d, [])
        if r["status"] != "PASS":
            by_domain[d].append(table)

    summary = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "total_tables": len(results),
        **status_counts,
        "domains_with_issues": {k: v for k, v in by_domain.items() if v},
        "tables": results,
    }
    return summary


def _ensure_probe_env() -> None:
    """Injecte des valeurs factices pour les secrets non-Evolia absents.
    probe_ddl n'utilise que la connexion Evolia — S3 et PG ne sont pas contactés.
    """
    _defaults = {
        "OVH_S3_ENDPOINT": "https://s3.probe.local",
        "OVH_S3_ACCESS_KEY": "probe",
        "OVH_S3_SECRET_KEY": "probe",
        "PG_USER": "probe",
        "PG_PASSWORD": "probe",
    }
    for key, val in _defaults.items():
        os.environ.setdefault(key, val)


if __name__ == "__main__":
    _ensure_probe_env()
    cfg = Config()
    if cfg.mode == RunMode.OFFLINE:
        logger.error(
            "probe_ddl requiert RUN_MODE=probe (connexion Evolia nécessaire)")
        sys.exit(1)
    output = run(cfg)
    print(json.dumps(output, indent=2, ensure_ascii=False))
    sys.exit(0 if output["FAIL"] == 0 and output["NOT_FOUND"] == 0 else 1)
