"""
Gold Analytics DAG.

Builds the gold and intermediate analytics layers using dbt.
Triggered by main_orchestrator_dag after silver layer completes.

Flow: dim + intermediate -> fact -> report -> snapshot (SCD Type 2)
Schedule: None (event-driven)
"""

from airflow import DAG

from config.pipeline_config import (
    COMMON_DAG_ARGS,
    DBT_CONFIG,
    GOLD_DEFAULT_ARGS,
    POOL_CONFIG,
    TAGS,
)
from operators.dbt_operator import DbtRunOperator


with DAG(
    dag_id="gold_analytics_dag",
    default_args=GOLD_DEFAULT_ARGS,
    description="Build gold and intermediate analytics layers using dbt",
    schedule_interval=None,  # Event-driven (triggered by orchestrator)
    tags=TAGS["gold"],
    **COMMON_DAG_ARGS,
) as dag:

    # Task 1: dim + intermediate models (70 models)
    # dbt automatically handles internal dependencies
    dbt_run_dim_intermediate = DbtRunOperator(
        task_id="dbt_run_dim_intermediate",
        dbt_command="run",
        dbt_project_path=str(DBT_CONFIG["project_dir"]),
        select="gold_dag.dim gold_dag.intermediate",
        parse_results=True,
        pool=POOL_CONFIG["dbt_pool_name"],
        pool_slots=POOL_CONFIG["dbt_pool_slots"],
    )

    # Task 2: fact models (5 models, depend on intermediate)
    dbt_run_fact = DbtRunOperator(
        task_id="dbt_run_fact",
        dbt_command="run",
        dbt_project_path=str(DBT_CONFIG["project_dir"]),
        select="gold_dag.fact",
        parse_results=True,
        pool=POOL_CONFIG["dbt_pool_name"],
        pool_slots=POOL_CONFIG["dbt_pool_slots"],
    )

    # Task 3: report models (18 models, depend on fact tables)
    dbt_run_report = DbtRunOperator(
        task_id="dbt_run_report",
        dbt_command="run",
        dbt_project_path=str(DBT_CONFIG["project_dir"]),
        select="gold_dag.report",
        parse_results=True,
        pool=POOL_CONFIG["dbt_pool_name"],
        pool_slots=POOL_CONFIG["dbt_pool_slots"],
    )

    # Task 4: gold-level snapshots (SCD Type 2)
    dbt_run_gold_snapshot = DbtRunOperator(
        task_id="dbt_run_gold_snapshot",
        dbt_command="snapshot",
        dbt_project_path=str(DBT_CONFIG["project_dir"]),
        select="tag:snapshot_gold",
        parse_results=False,
        pool=POOL_CONFIG["dbt_pool_name"],
        pool_slots=POOL_CONFIG["dbt_pool_slots"],
    )

    # Pipeline: dim+intermediate -> fact -> report -> snapshot
    dbt_run_dim_intermediate >> dbt_run_fact >> dbt_run_report >> dbt_run_gold_snapshot
