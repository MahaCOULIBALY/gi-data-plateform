"""shared.py — Module partagé GI Data Lakehouse (Manifeste v2.0).
RunMode 3 niveaux · Config · Stats · connexions · helpers RGPD.
Note : extract_table_delta vit dans les scripts Bronze (bronze-specific).
"""
import io
import os
import json
import hashlib
import logging
import sys
import uuid
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from enum import Enum
from functools import lru_cache
from pathlib import Path
from typing import Any
import boto3
import duckdb
import psycopg2
import psycopg2.extras
import psycopg2.sql
import pymssql
import pyodbc
from dotenv import load_dotenv


# Charge .env.local (dev) ou .env (CI/prod) — AVANT les dataclasses
_project_root = Path(__file__).resolve().parent.parent
for _env_file in (".env", ".env.local"):
    _candidate = _project_root / _env_file
    if _candidate.is_file():
        load_dotenv(_candidate, override=False)
        break

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(module)s.%(funcName)s:%(lineno)d | %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(
            "/opt/groupe-interaction/etc/gi-data-platform/gi-data-platform.log"),
    ],
)
logger = logging.getLogger("gi-data")

# Si /opt/gi-data-platform/gi-data-platform.log n'existe pas, le créer
if not Path("/opt/groupe-interaction/etc/gi-data-platform/gi-data-platform.log").exists():
    with open("/opt/groupe-interaction/etc/gi-data-platform/gi-data-platform.log", "w") as f:
        f.write("Starting gi-data-platform pipeline\n")

# Taille max d'un objet S3 OVH avant EntityTooLarge — partagée par tous les scripts Bronze
_CHUNK_SIZE = 100_000

_RGPD_SALT_SENTINEL = "CHANGE_ME_32CHARS_MINIMUM!!!!!!!!"


class RunMode(Enum):
    OFFLINE = "offline"  # Zéro connexion externe — CI/CD, mocks, tests unitaires
    PROBE = "probe"    # Connexions actives, comptages/scans, zéro écriture
    LIVE = "live"     # Pipeline complet — production


def filter_tables(tables: list, cfg: "Config") -> list:
    """Filtre _TABLES selon TABLE_FILTER env var.
    TABLE_FILTER=WTMISS,WTCNTI → traite uniquement ces tables.
    Vide ou absent → toutes les tables.

    Appelée séparément pour TABLES_DELTA et TABLES_FULL dans chaque run() :
    retourne [] silencieusement si aucune table de la liste courante n'est dans
    TABLE_FILTER (la table peut être dans l'autre liste). La validation "table
    inconnue" se fait via le log WARNING ci-dessous uniquement sur la dernière
    liste (TABLES_FULL) — couvre les fautes de frappe sans faux positifs.
    """
    raw = os.environ.get("TABLE_FILTER", "").strip()
    if not raw:
        return tables
    allowed = {t.strip().upper() for t in raw.split(",")}
    filtered = [t for t in tables if t.name.upper() in allowed]
    if not filtered and tables:
        # Avertissement non-bloquant — la table peut figurer dans l'autre liste (DELTA/FULL)
        logger.debug(json.dumps({"table_filter": raw,
                                 "searched_in": [t.name for t in tables],
                                 "found": 0}))
    return filtered


def get_run_mode() -> RunMode:
    """Charge RUN_MODE depuis env. Lève ValueError sur valeur invalide — pas de fallback silencieux."""
    val = os.environ.get("RUN_MODE", "live").lower()
    try:
        return RunMode(val)
    except ValueError:
        raise ValueError(
            f"RUN_MODE='{val}' invalide. Valeurs : offline | probe | live")


@dataclass
class Config:
    """Configuration centralisée — secrets chargés une seule fois au démarrage."""
    evolia_server: str = field(
        default_factory=lambda: os.environ["EVOLIA_SERVER"])
    evolia_db: str = field(default_factory=lambda: os.environ["EVOLIA_DB"])
    evolia_user: str = field(default_factory=lambda: os.environ["EVOLIA_USER"])
    evolia_password: str = field(
        default_factory=lambda: os.environ["EVOLIA_PASSWORD"])
    evolia_odbc_driver: str = field(
        default_factory=lambda: os.environ["EVOLIA_ODBC_DRIVER"])
    evolia_port: int = field(
        default_factory=lambda: int(os.environ.get("EVOLIA_PORT", "1433")))
    s3_endpoint: str = field(
        default_factory=lambda: os.environ["OVH_S3_ENDPOINT"])
    s3_access_key: str = field(
        default_factory=lambda: os.environ["OVH_S3_ACCESS_KEY"])
    s3_secret_key: str = field(
        default_factory=lambda: os.environ["OVH_S3_SECRET_KEY"])
    bucket_bronze: str = "gi-poc-bronze"
    bucket_silver: str = "gi-poc-silver"
    bucket_gold: str = "gi-poc-gold"
    ovh_pg_host: str = field(default_factory=lambda: os.environ["OVH_PG_HOST"])
    ovh_pg_port: int = field(default_factory=lambda: int(
        os.environ.get("OVH_PG_PORT", "20184")))
    ovh_pg_database: str = field(
        default_factory=lambda: os.environ.get("OVH_PG_DATABASE", "gi_poc_ddi_gold"))
    ovh_pg_user: str = field(
        default_factory=lambda: os.environ.get("OVH_PG_USER", ""))
    ovh_pg_password: str = field(
        default_factory=lambda: os.environ.get("OVH_PG_PASSWORD", ""))
    rgpd_salt: str = field(default_factory=lambda: os.environ.get(
        "RGPD_SALT", _RGPD_SALT_SENTINEL))
    alert_email: str = field(default_factory=lambda: os.environ.get(
        "ALERT_EMAIL", "data-team@groupe-interaction.fr"))
    mode: RunMode = field(default_factory=get_run_mode)
    # FinOps — partition Silver : lit uniquement la partition Bronze du jour (pas full-scan S3)
    # Override via env SILVER_DATE_PARTITION=2026/03/05 pour rejeu / backfill
    date_partition: str = field(
        default_factory=lambda: os.environ.get(
            "SILVER_DATE_PARTITION",
            datetime.now(timezone.utc).strftime("%Y/%m/%d"),
        )
    )

    def __post_init__(self) -> None:
        # Refus explicite du salt par défaut en LIVE et PROBE (PROBE se connecte à Evolia en production)
        if self.mode in (RunMode.LIVE, RunMode.PROBE) and self.rgpd_salt == _RGPD_SALT_SENTINEL:
            raise ValueError(
                "RGPD_SALT doit être défini en mode LIVE et PROBE. "
                "Configurez la variable d'environnement RGPD_SALT (min 32 chars)."
            )

    @property
    def dry_run(self) -> bool:
        """Rétrocompatibilité — True si mode != LIVE."""
        return self.mode != RunMode.LIVE


@dataclass
class Stats:
    tables_processed: int = 0
    rows_ingested: int = 0
    rows_transformed: int = 0
    rows_rejected: int = 0
    bytes_written: int = 0
    errors: list[dict[str, str]] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    started_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat())
    ended_at: str = ""
    extra: dict = field(default_factory=dict)

    def finish(self) -> dict:
        self.ended_at = datetime.now(timezone.utc).isoformat()
        result = asdict(self)
        logger.info(json.dumps({"stats": result}, default=str))
        return result


@dataclass
class TableConfig:
    name: str
    delta_col: str
    pk_cols: list[str]
    rgpd_flag: str = ""  # "" | "SENSIBLE" | "PERSONNEL"
    # True → WHERE delta_col >= ? OR delta_col IS NULL (ex: contrats actifs sans date fin)
    allow_null_delta: bool = False


# ── Helpers ─────────────────────────────────────────────────────────────────────

def generate_batch_id() -> str:
    """UUID4 — pas de collision même en exécutions parallèles (K8s CronJobs)."""
    return uuid.uuid4().hex[:8]


def today_s3_prefix() -> str:
    return datetime.now(timezone.utc).strftime("%Y/%m/%d")


def s3_bronze(cfg: "Config", table: str) -> str:
    """Chemin S3 Bronze partitionné par date — FinOps : évite le full-scan S3.
    Retourne : s3://{bucket}/raw_{table}/{YYYY/MM/DD}/*.json
    Override : SILVER_DATE_PARTITION=2026/03/01 pour backfill ou rejeu.
    """
    return f"s3://{cfg.bucket_bronze}/raw_{table.lower()}/{cfg.date_partition}/*.json"


def s3_bronze_range(cfg: "Config", table: str, days_back: int = 0) -> str:
    """Wildcard multi-jours pour joins inter-domaines (ex: Silver missions lit 7j de Bronze).
    days_back=0 → partition du jour uniquement (défaut FinOps)
    days_back>0 → full-scan sur la plage (à utiliser avec parcimonie)
    """
    if days_back == 0:
        return s3_bronze(cfg, table)
    return f"s3://{cfg.bucket_bronze}/raw_{table.lower()}/**/*.json"


def hash_sk(*parts: Any) -> str:
    return hashlib.md5("|".join(str(p) for p in parts if p is not None).encode()).hexdigest()


def pseudonymize_nir(nir: str | None, salt: str) -> str | None:
    """SHA-256 + salt — hash complet (256 bits) pour résistance collision RGPD."""
    if not nir:
        return None
    return hashlib.sha256(f"{nir}{salt}".encode()).hexdigest()


# ── Connexions ───────────────────────────────────────────────────────────────────

def get_evolia_connection(cfg: Config):
    """Connexion Evolia via pymssql (FreeTDS).
    Remplace pyodbc/ODBC Driver 17-18 dont le handshake TLS pré-login est
    incompatible avec Windows Schannel de SQL Server 2016 SP2 → erreur 0x2746.
    pymssql (FreeTDS) utilise un mécanisme TLS différent qui fonctionne avec
    cette version du serveur.
    """
    host, _, port_str = cfg.evolia_server.partition(",")
    port = int(port_str) if port_str else cfg.evolia_port
    conn = pymssql.connect(
        server=host,
        port=port,
        database=cfg.evolia_db,
        user=cfg.evolia_user,
        password=cfg.evolia_password,
        login_timeout=30,
        tds_version="7.4",   # TDS 7.4 = SQL Server 2012-2019
        autocommit=True,
    )
    return conn


@lru_cache(maxsize=1)
def _s3_singleton(endpoint: str, access_key: str, secret_key: str):
    """Un seul client boto3 par processus."""
    return boto3.client("s3", endpoint_url=endpoint,
                        aws_access_key_id=access_key, aws_secret_access_key=secret_key)


def get_s3_client(cfg: Config):
    return _s3_singleton(cfg.s3_endpoint, cfg.s3_access_key, cfg.s3_secret_key)


def get_pg_connection(cfg: Config):
    return psycopg2.connect(host=cfg.ovh_pg_host, port=cfg.ovh_pg_port, dbname=cfg.ovh_pg_database,
                            user=cfg.ovh_pg_user, password=cfg.ovh_pg_password, sslmode="require")


def get_duckdb_connection(cfg: Config) -> duckdb.DuckDBPyConnection:
    """INSTALL + LOAD httpfs — idempotent (pas de re-download si déjà installé).
    Nécessaire à chaque upgrade DuckDB (cache extensions versionnée par ~/.duckdb/extensions/<version>/).
    En prod K8s OVH : httpfs pré-installé dans l'image → INSTALL est no-op.
    Credentials via CREATE SECRET (DuckDB ≥ 0.10) — évite les credentials en clair
    dans les logs SET et gère les apostrophes défensivement.
    """
    conn = duckdb.connect()
    conn.execute("INSTALL httpfs; LOAD httpfs;")
    # Strip scheme + trailing slash — DuckDB duplique le bucket si trailing slash présent
    endpoint = cfg.s3_endpoint.replace(
        "https://", "").replace("http://", "").rstrip("/")
    # Dérive la région depuis le pattern OVH : s3.{region}.io.cloud.ovh.net
    # DuckDB v1.4+ exige REGION même pour un endpoint custom (sinon HTTP 400)
    region = endpoint.split(".")[1] if endpoint.startswith(
        "s3.") else "us-east-1"

    # Échappement défensif — les clés S3 sont alphanumériques mais on ne présume pas
    def _q(s: str) -> str:
        return s.replace("'", "''")

    try:
        conn.execute(f"""
            CREATE OR REPLACE SECRET s3_gi (
                TYPE S3,
                KEY_ID '{_q(cfg.s3_access_key)}',
                SECRET '{_q(cfg.s3_secret_key)}',
                ENDPOINT '{_q(endpoint)}',
                REGION '{_q(region)}',
                URL_STYLE 'path',
                USE_SSL true
            )
        """)
    except Exception:
        raise RuntimeError(
            "Failed to configure S3 secret — check OVH_S3_ACCESS_KEY / OVH_S3_SECRET_KEY") from None
    return conn


# ── I/O ─────────────────────────────────────────────────────────────────────────

def upload_to_s3(cfg: Config, data: list[dict], bucket: str, key: str, stats: Stats) -> None:
    if cfg.mode == RunMode.OFFLINE:
        logger.info(
            f"[OFFLINE] Skip upload {len(data)} rows → s3://{bucket}/{key}")
        return
    if cfg.mode == RunMode.PROBE:
        logger.info(
            f"[PROBE] Would upload {len(data)} rows → s3://{bucket}/{key}")
        return
    body_bytes = "\n".join(json.dumps(r, default=str)
                           for r in data).encode("utf-8")
    get_s3_client(cfg).put_object(Bucket=bucket, Key=key, Body=body_bytes)
    stats.bytes_written += len(body_bytes)


# Convention de nommage → type PG (évite le tout-TEXT, améliore perfs Superset)
_PG_TYPES: list[tuple[str, str]] = [
    ("_sk", "TEXT"), ("_id", "BIGINT"),
    ("mois", "DATE"), ("trimestre", "DATE"), ("_date", "DATE"), ("annee", "INT"),
    ("ca_", "NUMERIC(18,2)"), ("_ht", "NUMERIC(18,2)"), ("marge", "NUMERIC(18,2)"),
    ("taux_", "NUMERIC(8,4)"), ("_taux", "NUMERIC(8,4)"), ("score", "NUMERIC(8,4)"),
    ("nb_", "INT"), ("rang_", "INT"), ("heures", "NUMERIC(10,2)"), ("jours", "INT"),
    ("is_", "BOOLEAN"),
]


def _col_pg_type(col: str) -> str:
    c = col.lower()
    for pat, pg_type in _PG_TYPES:
        if c.startswith(pat) or c.endswith(pat.strip("_")):
            return pg_type
    return "TEXT"


def pg_bulk_insert(
    cfg: Config, conn, schema: str, table: str,
    columns: list[str], rows: list[tuple], stats: Stats,
) -> None:
    """TRUNCATE + COPY via StringIO. DDL auto-typé par convention de nommage.
    Transaction explicite avec rollback — la table n'est jamais laissée vide sur erreur COPY.
    Usage : tables volumineuses Gold, rechargement complet acceptable.
    """
    if cfg.mode in (RunMode.OFFLINE, RunMode.PROBE):
        logger.info(
            f"[{cfg.mode.value.upper()}] Would insert {len(rows)} rows → {schema}.{table}")
        return
    ddl_cols = ", ".join(f'"{c}" {_col_pg_type(c)}' for c in columns)
    try:
        with conn.cursor() as cur:
            cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
            cur.execute(
                f'CREATE TABLE IF NOT EXISTS "{schema}"."{table}" ({ddl_cols})')
            cur.execute(f'TRUNCATE TABLE "{schema}"."{table}"')
            buf = io.StringIO()
            for row in rows:
                buf.write("\t".join("\\N" if v is None else str(v)
                          for v in row) + "\n")
            buf.seek(0)
            cur.copy_from(buf, f"{schema}.{table}",
                          columns=columns, null="\\N")
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    stats.rows_ingested += len(rows)


def atomic_load_gold(
    cfg: Config, conn, schema: str, table: str,
    columns: list[str], rows: list[tuple], stats: Stats,
) -> None:
    """Insert atomique via psycopg2.sql (injection-safe) + execute_batch.
    Usage : tables critiques (fact_ca, scorecard) — rollback auto sur erreur.
    """
    if cfg.mode in (RunMode.OFFLINE, RunMode.PROBE):
        logger.info(
            f"[{cfg.mode.value.upper()}] Would atomic-load {len(rows)} rows → {schema}.{table}")
        return
    fqn = psycopg2.sql.SQL("{}.{}").format(
        psycopg2.sql.Identifier(schema), psycopg2.sql.Identifier(table))
    insert_sql = psycopg2.sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
        fqn,
        psycopg2.sql.SQL(", ").join(map(psycopg2.sql.Identifier, columns)),
        psycopg2.sql.SQL(", ").join(
            [psycopg2.sql.Placeholder()] * len(columns)),
    )
    try:
        with conn.cursor() as cur:
            cur.execute(psycopg2.sql.SQL("TRUNCATE TABLE {}").format(fqn))
            psycopg2.extras.execute_batch(cur, insert_sql, rows, page_size=500)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    stats.rows_ingested += len(rows)
