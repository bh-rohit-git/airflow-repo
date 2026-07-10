"""
Airflow DAG: submit Job 1 (Pub/Sub → bronze_orders) as a serverless Databricks JAR task.

Serverless Pub/Sub auth — set Airflow Variable (required):
  pubsub_service_credential = <Unity Catalog service credential name>
  Example: gcp-pubsub

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
MAIN_CLASS = "com.example.streaming.RawOrderIngestionMain"
SERVERLESS_ENV_VERSION = "4"

GCP_PROJECT = "nprd-bh-use1-dev"
PUBSUB_TOPIC = "order-events"
PUBSUB_SUBSCRIPTION = "order-events-databricks"
BRONZE_TABLE = "`databricks-migrate-activity`.schema1.bronze_orders"
CHECKPOINT_LOCATION = (
    "gs://bh-migrate-poc-bucket/pubsub-setup/checkpoints/bronze_orders_pubsub"
)
UC_JAR_PATH = (
    "/Volumes/databricks-migrate-activity/schema1/jars/"
    "databricks-structured-streaming-assembly-0.1.0.jar"
)
TRIGGER_INTERVAL = "30 seconds"
DEFAULT_IDLE_TIMEOUT = "2 minutes"
SAFETY_TIMEOUT_SECONDS = 7200


def build_serverless_jar_payload() -> dict:
    """Payload for jobs/runs/submit.

    pubsub_service_credential and idle_timeout are resolved at task runtime
    from Airflow Variables / DAG params (Jinja templates).
    """
    parameters = [
        "--gcp-project",
        GCP_PROJECT,
        "--topic-id",
        PUBSUB_TOPIC,
        "--subscription-id",
        PUBSUB_SUBSCRIPTION,
        "--target-table",
        BRONZE_TABLE,
        "--checkpoint-location",
        CHECKPOINT_LOCATION,
        "--trigger-interval",
        TRIGGER_INTERVAL,
        "--idle-timeout",
        "{{ dag_run.conf.get('idle_timeout') or params.idle_timeout }}",
        # UC service credential name from Airflow Variable (Admin → Variables)
        "--service-credential",
        "{{ var.value.pubsub_service_credential }}",
    ]

    return {
        "run_name": "raw-order-ingestion-pubsub-serverless",
        "timeout_seconds": SAFETY_TIMEOUT_SECONDS,
        "performance_target": "PERFORMANCE_OPTIMIZED",
        "tasks": [
            {
                "task_key": "raw_order_ingestion",
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
    dag_id="run_raw_order_ingestion_pubsub",
    description="Serverless JAR: Pub/Sub → bronze_orders (stops after idle timeout)",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["databricks", "pubsub", "streaming", "serverless"],
    params={
        "idle_timeout": Param(
            DEFAULT_IDLE_TIMEOUT,
            type="string",
            description=(
                "Stop Spark after this long with no Pub/Sub input "
                "(e.g. '2 minutes', '5 minutes'). "
                'Trigger conf: {"idle_timeout": "5 minutes"}'
            ),
        ),
    },
) as dag:
    DatabricksSubmitRunOperator(
        task_id="submit_raw_order_ingestion_serverless_jar",
        databricks_conn_id=DATABRICKS_CONN_ID,
        json=build_serverless_jar_payload(),
        wait_for_termination=True,
    )
