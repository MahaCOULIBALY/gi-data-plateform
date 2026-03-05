"""dag_phase3_performance.py — Orchestration Phase 3 : Dimensions + 360° + RGPD.
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
    "execution_timeout": timedelta(minutes=30),
}

with DAG(
    "gi_phase3_performance",
    start_date=datetime(2026, 3, 1),
    schedule_interval="30 7 * * *",
    catchup=False,
    default_args=default_args,
    tags=["phase3", "performance", "360", "rgpd"],
    max_active_runs=1,
) as dag:

    wait_phase2 = ExternalTaskSensor(
        task_id="wait_phase2_gold",
        external_dag_id="gi_phase2_interimaires",
        external_task_id="gold_staffing",
        timeout=3600, poke_interval=60, mode="reschedule",
    )

    silver_agences = BashOperator(
        task_id="silver_agences_light",
        bash_command=f"cd {SCRIPTS} && python silver_agences_light.py",
    )

    gold_dims = BashOperator(
        task_id="gold_dimensions",
        bash_command=f"cd {SCRIPTS} && python gold_dimensions.py",
        sla=timedelta(minutes=15),
    )

    with TaskGroup("gold_vues_360") as vues_group:
        vue_client = BashOperator(
            task_id="gold_vue360_client",
            bash_command=f"cd {SCRIPTS} && python gold_vue360_client.py",
        )
        comp_dispo = BashOperator(
            task_id="gold_competences",
            bash_command=f"cd {SCRIPTS} && python gold_competences.py",
        )

    rgpd = BashOperator(
        task_id="rgpd_audit",
        bash_command=f"cd {SCRIPTS} && python rgpd_audit.py",
        sla=timedelta(minutes=10),
    )

    wait_phase2 >> silver_agences >> gold_dims >> vues_group >> rgpd
