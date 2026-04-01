# Airflow DAGs — Code Examples

Sanitized examples from a production Airflow 2.11 orchestration layer with 6 DAGs, 3 custom operators, and sequential pipeline coordination.

## Files

| File | What it demonstrates |
|------|---------------------|
| `main_orchestrator_dag.py` | Sequential DAG triggering with TriggerDagRunOperator, synchronous wait, fail-fast strategy |
| `gold_analytics_dag.py` | dbt-based analytics DAG: dim → intermediate → fact → report → snapshot pipeline |
| `base_pipeline_operator.py` | Custom operator base class: subprocess execution, path resolution (Docker/local), JSON result parsing, XCom stats |
