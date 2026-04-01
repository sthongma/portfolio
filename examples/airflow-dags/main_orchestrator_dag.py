"""
Main Orchestrator DAG.

Coordinates the entire data pipeline by triggering child DAGs sequentially.
Each DAG must complete successfully before the next one starts.

Schedule: "0 1 * * *" (01:00 UTC = 08:00 local time)
Flow: Bronze -> Silver -> Gold -> Source Freshness
Strategy: Synchronous (wait_for_completion=True), fail-fast (retries=0)
"""

from datetime import timedelta

from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from config.pipeline_config import COMMON_DAG_ARGS, ORCHESTRATOR_CONFIG
from notifications import pipeline_summary_callback

DAG_ID = "main_orchestrator_dag"
SCHEDULE = ORCHESTRATOR_CONFIG["schedule"]

DEFAULT_ARGS = {
    "owner": "data_engineer",
    "depends_on_past": False,
    "email_on_failure": False,  # Using custom pipeline_summary_callback instead
    "email_on_retry": False,
    "retries": 0,  # Fail fast — no retries
    "execution_timeout": timedelta(hours=ORCHESTRATOR_CONFIG["execution_timeout_hours"]),
}


with DAG(
    dag_id=DAG_ID,
    default_args=DEFAULT_ARGS,
    description="Orchestrator for bronze -> silver -> gold pipeline",
    schedule_interval=SCHEDULE,
    tags=["orchestrator", "production"],
    on_success_callback=pipeline_summary_callback,
    on_failure_callback=pipeline_summary_callback,
    **COMMON_DAG_ARGS,
) as dag:

    # Task 1: Bronze Ingestion (import raw files -> database)
    trigger_bronze = TriggerDagRunOperator(
        task_id="trigger_bronze_ingestion",
        trigger_dag_id="bronze_ingestion_dag",
        wait_for_completion=True,
        poke_interval=ORCHESTRATOR_CONFIG["poke_interval"],
        execution_timeout=ORCHESTRATOR_CONFIG["child_dag_timeouts"]["bronze"],
        do_xcom_push=True,
        conf={"parent_run_id": "{{ run_id }}"},
    )

    # Task 2: Silver Transformation (staging + dedup via dbt)
    trigger_silver = TriggerDagRunOperator(
        task_id="trigger_silver_transformation",
        trigger_dag_id="silver_transformation_dag",
        wait_for_completion=True,
        poke_interval=ORCHESTRATOR_CONFIG["poke_interval"],
        execution_timeout=ORCHESTRATOR_CONFIG["child_dag_timeouts"]["silver"],
        do_xcom_push=True,
        conf={"parent_run_id": "{{ run_id }}"},
    )

    # Task 3: Gold Analytics (dim + intermediate + fact + report via dbt)
    trigger_gold = TriggerDagRunOperator(
        task_id="trigger_gold_analytics",
        trigger_dag_id="gold_analytics_dag",
        wait_for_completion=True,
        poke_interval=ORCHESTRATOR_CONFIG["poke_interval"],
        execution_timeout=ORCHESTRATOR_CONFIG["child_dag_timeouts"]["gold"],
        do_xcom_push=True,
        conf={"parent_run_id": "{{ run_id }}"},
    )

    # Task 4: Source Freshness Check (verify data recency)
    trigger_source_freshness = TriggerDagRunOperator(
        task_id="trigger_source_freshness",
        trigger_dag_id="source_freshness_dag",
        wait_for_completion=True,
        poke_interval=ORCHESTRATOR_CONFIG["poke_interval"],
        execution_timeout=ORCHESTRATOR_CONFIG["child_dag_timeouts"]["source_freshness"],
        do_xcom_push=True,
        conf={"parent_run_id": "{{ run_id }}"},
    )

    # Sequential dependencies: each must complete before the next starts
    trigger_bronze >> trigger_silver >> trigger_gold >> trigger_source_freshness
