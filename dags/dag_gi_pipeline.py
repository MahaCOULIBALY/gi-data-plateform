"""dag_gi_pipeline.py — Pipeline unifié GI Data Lakehouse (prod-ready).
Phase 0-3 · Bronze → Silver → Gold → Cleanup.
Schedule : 05:00 UTC quotidien.

Architecture :
  bronze (5 tâches parallèles)
    └── silver (10 tâches, clients SCD2 séquentiel)
          └── gold_dimensions
                └── gold_facts (7 tâches parallèles)
                      └── gold_views (2 tâches parallèles — lisent Gold PG)
                            └── rgpd_audit

Prérequis serveur FRDC1PIPELINE01 :
  - /opt/groupe-interaction/etl/gi-data-plateform/ déployé (scripts/ + .env)
  - RUN_MODE non requis (default "live" dans shared.py)
  - Variables Airflow ou .env : OVH_S3_*, OVH_PG_*, EVOLIA_*, RGPD_SALT
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup

SCRIPTS = "/opt/groupe-interaction/etl/gi-data-plateform/scripts"

_DEFAULT_ARGS = {
    "owner": "data-team",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=1),
    "email": ["data-team@groupe-interaction.fr"],
    "email_on_failure": True,
    "email_on_retry": False,
}


def _bash(task_id: str, script: str, sla_minutes: int = 20) -> BashOperator:
    """Crée un BashOperator standardisé vers /opt/gi-data-platform/scripts/."""
    return BashOperator(
        task_id=task_id,
        bash_command=f"cd {SCRIPTS} && python {script}",
        sla=timedelta(minutes=sla_minutes),
    )


with DAG(
    dag_id="gi_pipeline",
    description="GI Data Lakehouse — Bronze S3 → Silver Parquet → Gold PostgreSQL",
    start_date=datetime(2026, 3, 13),
    schedule_interval="0 5 * * *",
    catchup=False,
    default_args=_DEFAULT_ARGS,
    max_active_runs=1,
    tags=["bronze", "silver", "gold", "prod"],
    doc_md=__doc__,
) as dag:

    # ── BRONZE (5 sources parallèles) ─────────────────────────────────────
    with TaskGroup("bronze", tooltip="Extraction Evolia → S3 Bronze") as bronze_group:
        bro_agences     = _bash("bronze_agences",          "bronze_agences.py",          sla_minutes=15)
        bro_missions    = _bash("bronze_missions",         "bronze_missions.py",          sla_minutes=30)
        bro_interims    = _bash("bronze_interimaires",     "bronze_interimaires.py",      sla_minutes=30)
        bro_clients     = _bash("bronze_clients",          "bronze_clients.py",           sla_minutes=15)
        bro_clients_ext = _bash("bronze_clients_external", "bronze_clients_external.py",  sla_minutes=20)

    # ── SILVER ────────────────────────────────────────────────────────────
    with TaskGroup("silver", tooltip="Transformation Bronze → Silver Parquet") as silver_group:
        # Parallèle — indépendants
        slv_agences    = _bash("silver_agences_light",       "silver_agences_light.py",       sla_minutes=10)
        slv_missions   = _bash("silver_missions",            "silver_missions.py",            sla_minutes=20)
        slv_temps      = _bash("silver_temps",               "silver_temps.py",               sla_minutes=30)
        slv_interims   = _bash("silver_interimaires",        "silver_interimaires.py",        sla_minutes=15)
        slv_int_detail = _bash("silver_interimaires_detail", "silver_interimaires_detail.py", sla_minutes=15)
        slv_comp       = _bash("silver_competences",         "silver_competences.py",         sla_minutes=10)
        slv_factures   = _bash("silver_factures",            "silver_factures.py",            sla_minutes=20)

        # Chaîne clients : SCD2 → enrichissement BAN/géocode → détail
        slv_clients_scd2   = _bash("silver_clients",       "silver_clients.py",       sla_minutes=15)
        slv_clients_enrich = _bash("enrich_ban_geocode",   "enrich_ban_geocode.py",   sla_minutes=20)
        slv_clients_detail = _bash("silver_clients_detail", "silver_clients_detail.py", sla_minutes=10)

        slv_clients_scd2 >> slv_clients_enrich >> slv_clients_detail

    # ── GOLD DIMENSIONS (après tout le Silver) ────────────────────────────
    gold_dims = _bash("gold_dimensions", "gold_dimensions.py", sla_minutes=15)

    # ── GOLD FACTS (parallèle — lisent Silver + dim tables) ──────────────
    with TaskGroup("gold_facts", tooltip="Tables Gold factuelles") as gold_facts_group:
        gld_ca        = _bash("gold_ca_mensuel",        "gold_ca_mensuel.py",        sla_minutes=20)
        gld_staffing  = _bash("gold_staffing",          "gold_staffing.py",          sla_minutes=20)
        gld_ops       = _bash("gold_operationnel",      "gold_operationnel.py",      sla_minutes=15)
        gld_etp       = _bash("gold_etp",               "gold_etp.py",               sla_minutes=10)
        gld_scorecard = _bash("gold_scorecard_agence",  "gold_scorecard_agence.py",  sla_minutes=15)
        gld_comp      = _bash("gold_competences",       "gold_competences.py",       sla_minutes=15)
        gld_retention = _bash("gold_retention_client",  "gold_retention_client.py",  sla_minutes=15)

    # ── GOLD VIEWS (lisent Gold PG — dépendent des facts) ─────────────────
    with TaskGroup("gold_views", tooltip="Vues Gold enrichies (lit PostgreSQL)") as gold_views_group:
        # gold_vue360_client lit gld_commercial + gld_staffing
        # gold_clients_detail lit gld_commercial.fact_ca_mensuel_client
        gld_vue360     = _bash("gold_vue360_client",  "gold_vue360_client.py",  sla_minutes=15)
        gld_cli_detail = _bash("gold_clients_detail", "gold_clients_detail.py", sla_minutes=15)

    # ── CLEANUP ───────────────────────────────────────────────────────────
    rgpd = _bash("rgpd_audit", "rgpd_audit.py", sla_minutes=10)

    # ── GRAPHE DE DÉPENDANCES ─────────────────────────────────────────────
    bronze_group >> silver_group >> gold_dims >> gold_facts_group >> gold_views_group >> rgpd
