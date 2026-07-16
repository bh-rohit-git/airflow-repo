"""
Airflow DAG: submit Job 1 (Pub/Sub → bronze_orders) as a serverless Databricks JAR task.

Trigger with JSON conf (see run_raw_order_ingestion_trigger_conf.example.json) or use DAG param defaults.
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
PUBSUB_SERVICE_CREDENTIAL = "gcp-pubsub"
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
RUN_NAME = "raw-order-ingestion-pubsub-serverless"
PERFORMANCE_TARGET = "PERFORMANCE_OPTIMIZED"


def _conf_or_param(key: str) -> str:
    """Resolve trigger conf override with DAG param fallback."""
    return f"{{{{ dag_run.conf.get('{key}', params.{key}) }}}}"


def build_serverless_jar_payload() -> dict:
    """Payload for jobs/runs/submit."""
    parameters = [
        "--gcp-project",
        _conf_or_param("gcp_project"),
        "--topic-id",
        _conf_or_param("pubsub_topic"),
        "--subscription-id",
        _conf_or_param("pubsub_subscription"),
        "--target-table",
        _conf_or_param("bronze_table"),
        "--checkpoint-location",
        _conf_or_param("checkpoint_location"),
        "--trigger-interval",
        _conf_or_param("trigger_interval"),
        "--idle-timeout",
        _conf_or_param("idle_timeout"),
        "--service-credential",
        _conf_or_param("pubsub_service_credential"),
    ]

    return {
        "run_name": _conf_or_param("run_name"),
        "timeout_seconds": _conf_or_param("safety_timeout_seconds"),
        "performance_target": _conf_or_param("performance_target"),
        "tasks": [
            {
                "task_key": "raw_order_ingestion",
                "spark_jar_task": {
                    "main_class_name": _conf_or_param("main_class"),
                    "parameters": parameters,
                },
                "environment_key": "jar_env",
            }
        ],
        "environments": [
            {
                "environment_key": "jar_env",
                "spec": {
                    "environment_version": _conf_or_param("serverless_env_version"),
                    "java_dependencies": [_conf_or_param("uc_jar_path")],
                },
            }
        ],
    }


with DAG(
    dag_id="dualrun_child_unmigrated_RawOrderIngestionMain_9a2b82f1e94c",
    description="Serverless JAR: Pub/Sub → bronze_orders (stops after idle timeout)",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["databricks", "pubsub", "streaming", "serverless"],
    params={
        "gcp_project": Param(
            GCP_PROJECT,
            type="string",
            description="GCP project id for Pub/Sub (JAR --gcp-project)",
        ),
        "pubsub_topic": Param(
            PUBSUB_TOPIC,
            type="string",
            description="Pub/Sub topic id (JAR --topic-id)",
        ),
        "pubsub_subscription": Param(
            PUBSUB_SUBSCRIPTION,
            type="string",
            description="Pub/Sub subscription id (JAR --subscription-id)",
        ),
        "bronze_table": Param(
            BRONZE_TABLE,
            type="string",
            description="Bronze Delta target table (JAR --target-table)",
        ),
        "checkpoint_location": Param(
            CHECKPOINT_LOCATION,
            type="string",
            description="GCS checkpoint path for structured streaming",
        ),
        "trigger_interval": Param(
            TRIGGER_INTERVAL,
            type="string",
            description="Spark micro-batch trigger (e.g. '30 seconds', '1 minute')",
        ),
        "idle_timeout": Param(
            DEFAULT_IDLE_TIMEOUT,
            type="string",
            description=(
                "Stop Spark after this long with no Pub/Sub input "
                "(e.g. '2 minutes', '5 minutes')"
            ),
        ),
        "pubsub_service_credential": Param(
            PUBSUB_SERVICE_CREDENTIAL,
            type="string",
            description="Unity Catalog service credential name for Pub/Sub auth (JAR --service-credential)",
        ),
        "main_class": Param(
            MAIN_CLASS,
            type="string",
            description="Databricks JAR entrypoint main class",
        ),
        "uc_jar_path": Param(
            UC_JAR_PATH,
            type="string",
            description="Unity Catalog volume path to the assembly JAR",
        ),
        "serverless_env_version": Param(
            SERVERLESS_ENV_VERSION,
            type="string",
            description="Databricks serverless environment version",
        ),
        "safety_timeout_seconds": Param(
            SAFETY_TIMEOUT_SECONDS,
            type="integer",
            description="Databricks job run safety timeout in seconds",
        ),
        "run_name": Param(
            RUN_NAME,
            type="string",
            description="Databricks jobs/runs/submit run_name",
        ),
        "performance_target": Param(
            PERFORMANCE_TARGET,
            type="string",
            description="Serverless performance target (e.g. PERFORMANCE_OPTIMIZED)",
        ),
    },
) as dag:
    DatabricksSubmitRunOperator(
        task_id="submit_raw_order_ingestion_serverless_jar",
        databricks_conn_id=DATABRICKS_CONN_ID,
        json=build_serverless_jar_payload(),
        wait_for_termination=True,
    )
