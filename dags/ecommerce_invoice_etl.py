from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="ecommerce_invoice_etl",
    description="Complete UCI Online Retail workbook to an incremental PostgreSQL warehouse",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    params={
        "start_at": "",
        "end_at": "",
        "full_refresh": False,
        "force_download": False,
    },
    default_args={"owner": "data-engineering", "retries": 2, "retry_delay": timedelta(minutes=2)},
    tags=["ecommerce", "incremental", "data-quality", "postgresql"],
) as dag:
    download_source = BashOperator(
        task_id="download_public_source",
        bash_command=(
            "python /opt/project/src/download_data.py "
            "{% if params.force_download %}--force{% endif %}"
        ),
    )
    transform_and_load = BashOperator(
        task_id="transform_and_load",
        bash_command=(
            "python /opt/project/src/pipeline.py "
            "{% if params.full_refresh %}--full-refresh{% endif %} "
            "{% if params.start_at %}--start-at '{{ params.start_at }}'{% endif %} "
            "{% if params.end_at %}--end-at '{{ params.end_at }}'{% endif %}"
        ),
    )
    data_quality_gate = BashOperator(
        task_id="data_quality_gate",
        bash_command=(
            "python -c \"import json; "
            "d=json.load(open('/opt/project/artifacts/run_summary.json')); "
            "assert d['status']=='success' and d['warehouse_rows']>0; "
            "assert d['rejection_rate']<0.05; "
            "assert d['distinct_lines']==d['accepted_rows']; print(d)\""
        ),
    )
    publish_observability = BashOperator(
        task_id="publish_observability_artifacts",
        bash_command=(
            "test -s /opt/project/artifacts/metrics.prom && "
            "test -s /opt/project/artifacts/dashboard.html"
        ),
    )
    download_source >> transform_and_load >> data_quality_gate >> publish_observability
