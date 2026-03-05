"""
DAG Phase 0 — Pipeline Bronze → Silver → Gold.
Quotidien 05:00 UTC. Bronze → Silver → Gold CA → Gold Opérationnel + Gold Clients (parallèle).
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

DEFAULT_ARGS = {
    "owner": "data-team",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=45),
}


def _bronze():
    from scripts.bronze_missions import run, Config
    stats = run(Config())
    if stats["errors"]:
        raise RuntimeError(f"Bronze: {len(stats['errors'])} erreurs")


def _silver():
    from scripts.silver_missions import run, Config
    from scripts.silver_factures import run as run_fac
    from scripts.silver_temps import run as run_temps
    from scripts.silver_clients_detail import run as run_cli
    cfg = Config()
    for fn in (run, run_fac, run_temps, run_cli):
        s = fn(cfg)
        if s["errors"]:
            raise RuntimeError(f"Silver {fn.__module__}: {s['errors']}")


def _gold_ca():
    from scripts.gold_ca_mensuel import run, Config
    stats = run(Config())
    if stats["errors"]:
        raise RuntimeError(f"Gold CA: {stats['errors']}")


def _gold_operationnel():
    from scripts.gold_operationnel import run, Config
    stats = run(Config())
    if stats["errors"]:
        raise RuntimeError(f"Gold Opérationnel: {stats['errors']}")


def _gold_clients():
    from scripts.gold_clients_detail import run, Config
    stats = run(Config())
    if stats["errors"]:
        raise RuntimeError(f"Gold Clients: {stats['errors']}")


with DAG(
    dag_id="gi_phase0_pipeline",
    description="Phase 0 : ERP Evolia → Bronze S3 → Silver Parquet → Gold PostgreSQL",
    start_date=datetime(2026, 2, 1),
    schedule_interval="0 5 * * *",
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["phase0", "bronze", "silver", "gold"],
) as dag:

    bronze = PythonOperator(task_id="bronze_missions", python_callable=_bronze)
    silver = PythonOperator(task_id="silver_all", python_callable=_silver)
    gold_ca = PythonOperator(task_id="gold_ca_mensuel",
                             python_callable=_gold_ca)
    gold_ops = PythonOperator(
        task_id="gold_operationnel", python_callable=_gold_operationnel)
    gold_cli = PythonOperator(
        task_id="gold_clients_detail", python_callable=_gold_clients)

    # gold_clients_detail dépend de gold_ca (lit fact_ca_mensuel_client via PostgreSQL)
    bronze >> silver >> gold_ca >> [gold_ops, gold_cli]
