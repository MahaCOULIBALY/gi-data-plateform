"""dag_phase2_interimaires.py — Orchestration Phase 2 : Intérimaires + Staffing.
GI Data Lakehouse · OVHcloud Managed Airflow
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup

SCRIPTS = "/opt/gi-data-platform/scripts"

default_args = {
    "owner": "data-team",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email": ["data-team@groupe-interaction.fr"],
    "email_on_failure": True,
    "execution_timeout": timedelta(minutes=40),
}

with DAG(
    "gi_phase2_interimaires",
    start_date=datetime(2026, 3, 1),
    schedule_interval="0 5 30 * *",  # 05:30 UTC
    catchup=False,
    default_args=default_args,
    tags=["phase2", "interimaires", "staffing"],
    max_active_runs=1,
) as dag:

    wait_phase1 = ExternalTaskSensor(
        task_id="wait_phase1_complete",
        external_dag_id="gi_phase1_clients",
        external_task_id="gold_ca_mensuel",
        timeout=3600,
        poke_interval=60,
        mode="reschedule",
    )

    bronze = BashOperator(
        task_id="bronze_interimaires",
        bash_command=f"cd {SCRIPTS} && python bronze_interimaires.py",
        sla=timedelta(minutes=20),
    )

    with TaskGroup("silver_parallel") as silver_group:
        silver_dim = BashOperator(
            task_id="silver_dim_interimaires",
            bash_command=f"cd {SCRIPTS} && python silver_interimaires.py",
        )
        silver_comp = BashOperator(
            task_id="silver_competences",
            bash_command=f"cd {SCRIPTS} && python silver_competences.py",
        )
        silver_detail = BashOperator(
            task_id="silver_interimaires_detail",
            bash_command=f"cd {SCRIPTS} && python silver_interimaires_detail.py",
        )

    gold = BashOperator(
        task_id="gold_staffing",
        bash_command=f"cd {SCRIPTS} && python gold_staffing.py",
        sla=timedelta(minutes=20),
    )

    wait_phase1 >> bronze >> silver_group >> gold
