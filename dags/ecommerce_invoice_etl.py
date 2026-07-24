from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="ecommerce_invoice_etl",
    description="Local invoice CSV to PostgreSQL star schema",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args={"owner": "data-engineering", "retries": 2, "retry_delay": timedelta(minutes=2)},
    tags=["ecommerce", "data-quality", "postgresql"],
) as dag:
    generate_demo_data = BashOperator(
        task_id="generate_demo_data",
        bash_command="python /opt/project/src/generate_data.py",
    )
    transform_and_load = BashOperator(
        task_id="transform_and_load",
        bash_command="python /opt/project/src/pipeline.py",
    )
    verify_run = BashOperator(
        task_id="verify_run",
        bash_command=(
            "python -c \"import json; "
            "d=json.load(open('/opt/project/artifacts/run_summary.json')); "
            "assert d['status']=='success' and d['warehouse_rows']>0; print(d)\""
        ),
    )
    generate_demo_data >> transform_and_load >> verify_run

