"""
Airflow DAG: submit Job 2 (bronze_orders → silver_orders) as a serverless Databricks JAR task.

Reads new bronze Delta commits, joins customers, MERGE upserts into silver.
Stops after idle timeout with no new bronze rows (AvailableNow loop in the JAR).

Idle timeout:
  - Trigger config: {"idle_timeout": "5 minutes"}
  - Or DAG param (default "2 minutes")
"""

from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator

try:
    from airflow.sdk import Param
except ImportError:  # Airflow < 3
    from airflow.models.param import Param

DATABRICKS_CONN_ID = "databricks_default"
MAIN_CLASS = "com.example.streaming.OrderEnrichmentMain"
SERVERLESS_ENV_VERSION = "4"

BRONZE_TABLE = "`databricks-migrate-activity`.schema1.bronze_orders"
CUSTOMER_TABLE = "`databricks-migrate-activity`.schema1.customers"
SILVER_TABLE = "`databricks-migrate-activity`.schema1.silver_orders"
CHECKPOINT_LOCATION = (
    "gs://bh-migrate-poc-bucket/pubsub-setup/checkpoints/silver_orders"
)
UC_JAR_PATH = (
    "/Volumes/databricks-migrate-activity/schema1/jars/"
    "databricks-structured-streaming-assembly-0.1.0.jar"
)
TRIGGER_INTERVAL = "1 minute"
DEFAULT_IDLE_TIMEOUT = "2 minutes"
SAFETY_TIMEOUT_SECONDS = 7200


def build_serverless_jar_payload() -> dict:
    parameters = [
        "--source-table",
        BRONZE_TABLE,
        "--customer-table",
        CUSTOMER_TABLE,
        "--target-table",
        SILVER_TABLE,
        "--checkpoint-location",
        CHECKPOINT_LOCATION,
        "--trigger-interval",
        TRIGGER_INTERVAL,
        "--idle-timeout",
        "{{ dag_run.conf.get('idle_timeout') or params.idle_timeout }}",
    ]

    return {
        "run_name": "order-enrichment-bronze-to-silver-serverless",
        "timeout_seconds": SAFETY_TIMEOUT_SECONDS,
        "performance_target": "PERFORMANCE_OPTIMIZED",
        "tasks": [
            {
                "task_key": "order_enrichment",
                "spark_jar_task": {
                    "main_class_name": MAIN_CLASS,
                    "parameters": parameters,
                },
                "environment_key": "jar_env",
            }
        ],
        "environments": [
            {
                "environment_key": "jar_env",
                "spec": {
                    "environment_version": SERVERLESS_ENV_VERSION,
                    "java_dependencies": [UC_JAR_PATH],
                },
            }
        ],
    }


with DAG(
    dag_id="run_order_enrichment_bronze_to_silver",
    description="Serverless JAR: bronze_orders → silver_orders (stops after idle timeout)",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["databricks", "delta", "streaming", "serverless", "silver"],
    params={
        "idle_timeout": Param(
            DEFAULT_IDLE_TIMEOUT,
            type="string",
            description=(
                "Stop Spark after this long with no new bronze rows "
                "(e.g. '2 minutes', '5 minutes'). "
                'Trigger conf: {"idle_timeout": "5 minutes"}'
            ),
        ),
    },
) as dag:
    DatabricksSubmitRunOperator(
        task_id="submit_order_enrichment_serverless_jar",
        databricks_conn_id=DATABRICKS_CONN_ID,
        json=build_serverless_jar_payload(),
        wait_for_termination=True,
    )
