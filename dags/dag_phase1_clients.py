"""dag_phase1_clients.py — Orchestration Phase 1 : Clients + CA mensuel.
GI Data Lakehouse · OVHcloud Managed Airflow
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor

SCRIPTS = "/opt/gi-data-platform/scripts"

default_args = {
    "owner": "data-team",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email": ["data-team@groupe-interaction.fr"],
    "email_on_failure": True,
    "execution_timeout": timedelta(minutes=30),
}

with DAG(
    "gi_phase1_clients",
    start_date=datetime(2026, 3, 1),
    schedule_interval="0 5 * * *",
    catchup=False,
    default_args=default_args,
    tags=["phase1", "clients", "facturation"],
    max_active_runs=1,
    sla_miss_callback=None,
) as dag:

    wait_phase0 = ExternalTaskSensor(
        task_id="wait_phase0_silver",
        external_dag_id="gi_poc_pipeline",
        external_task_id="silver_dbt",
        timeout=3600,
        poke_interval=60,
        mode="reschedule",
    )

    bronze_clients = BashOperator(
        task_id="bronze_clients",
        bash_command=f"cd {SCRIPTS} && python bronze_clients.py",
        sla=timedelta(minutes=15),
    )

    bronze_external = BashOperator(
        task_id="bronze_clients_external",
        bash_command=f"cd {SCRIPTS} && python bronze_clients_external.py",
        sla=timedelta(minutes=20),
    )

    silver_dim_clients = BashOperator(
        task_id="silver_dim_clients",
        bash_command=f"cd {SCRIPTS} && python silver_clients.py",
        sla=timedelta(minutes=15),
    )

    enrich_ban = BashOperator(
        task_id="enrich_ban_geocode",
        bash_command=f"cd {SCRIPTS} && python enrich_ban_geocode.py",
        sla=timedelta(minutes=20),
    )

    silver_detail = BashOperator(
        task_id="silver_clients_detail",
        bash_command=f"cd {SCRIPTS} && python silver_clients_detail.py",
        sla=timedelta(minutes=10),
    )

    gold_ca = BashOperator(
        task_id="gold_ca_mensuel",
        bash_command=f"cd {SCRIPTS} && python gold_ca_mensuel.py",
        sla=timedelta(minutes=15),
    )

    (wait_phase0
     >> bronze_clients >> bronze_external
     >> silver_dim_clients >> enrich_ban >> silver_detail
     >> gold_ca)
