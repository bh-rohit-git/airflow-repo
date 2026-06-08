"""Orchestrate the raw-ingestion Scala JAR on Databricks."""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator

DATABRICKS_CONN_ID = "databricks_default"
CLUSTER_ID = "{{ var.value.databricks_cluster_id }}"
RAW_INGESTION_JAR = "{{ var.value.raw_ingestion_jar_uri }}"

default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

raw_ingestion_payload = {
    "existing_cluster_id": CLUSTER_ID,
    "spark_jar_task": {
        "main_class_name": "com.acme.RawIngestionJob",
        "jar_uri": RAW_INGESTION_JAR,
    },
}

with DAG(
    dag_id="raw_ingestion",
    default_args=default_args,
    description="Load raw sales data via Scala JAR",
    schedule="0 6 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["databricks", "raw-ingestion"],
) as dag:
    run_raw_ingestion = DatabricksSubmitRunOperator(
        task_id="run_raw_ingestion",
        databricks_conn_id=DATABRICKS_CONN_ID,
        json=raw_ingestion_payload,
    )
