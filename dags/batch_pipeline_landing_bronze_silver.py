"""
Pipeline 1 — simple batch: landing → bronze, then bronze → silver.

Serverless Databricks JAR tasks. JAR path + main classes are hardcoded;
all other inputs are DAG params (overridable via dag_run.conf).
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
UC_JAR_PATH = (
    "/Volumes/databricks-migrate-activity/schema1/jars/"
    "databricks-batch-jobs-assembly-0.1.0.jar"
)
LANDING_TO_BRONZE_MAIN = "com.example.batch.LandingToBronzeMain"
BRONZE_TO_SILVER_MAIN = "com.example.batch.BronzeToSilverMain"
SERVERLESS_ENV_VERSION = "4"
PERFORMANCE_TARGET = "PERFORMANCE_OPTIMIZED"
SAFETY_TIMEOUT_SECONDS = 3600

# Defaults (parameterized)
LANDING_PATH = "gs://bh-migrate-poc-bucket/batch2/landing"
PROCESSED_PATH = "gs://bh-migrate-poc-bucket/batch2/landing/processed"
BRONZE_TABLE = "`databricks-migrate-activity`.batch2.b2_bronze_orders"
SILVER_TABLE = "`databricks-migrate-activity`.batch2.b2_silver_orders"


def _conf_or_param(key: str) -> str:
    return f"{{{{ dag_run.conf.get('{key}', params.{key}) }}}}"


def _serverless_jar_task(
    task_key: str,
    main_class: str,
    parameters: list[str],
    run_name: str,
) -> dict:
    return {
        "run_name": run_name,
        "timeout_seconds": SAFETY_TIMEOUT_SECONDS,
        "performance_target": PERFORMANCE_TARGET,
        "tasks": [
            {
                "task_key": task_key,
                "spark_jar_task": {
                    "main_class_name": main_class,
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
    dag_id="batch_pipeline_landing_bronze_silver",
    description="Simple batch: landing→bronze then bronze→silver (serverless JAR)",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["databricks", "batch", "serverless", "medallion"],
    params={
        "landing_path": Param(LANDING_PATH, type="string"),
        "processed_path": Param(PROCESSED_PATH, type="string"),
        "bronze_table": Param(BRONZE_TABLE, type="string"),
        "silver_table": Param(SILVER_TABLE, type="string"),
    },
) as dag:
    landing_to_bronze = DatabricksSubmitRunOperator(
        task_id="landing_to_bronze",
        databricks_conn_id=DATABRICKS_CONN_ID,
        json=_serverless_jar_task(
            task_key="landing_to_bronze",
            main_class=LANDING_TO_BRONZE_MAIN,
            parameters=[
                "--landing-path",
                _conf_or_param("landing_path"),
                "--processed-path",
                _conf_or_param("processed_path"),
                "--bronze-table",
                _conf_or_param("bronze_table"),
            ],
            run_name="batch2-landing-to-bronze",
        ),
        wait_for_termination=True,
    )

    bronze_to_silver = DatabricksSubmitRunOperator(
        task_id="bronze_to_silver",
        databricks_conn_id=DATABRICKS_CONN_ID,
        json=_serverless_jar_task(
            task_key="bronze_to_silver",
            main_class=BRONZE_TO_SILVER_MAIN,
            parameters=[
                "--bronze-table",
                _conf_or_param("bronze_table"),
                "--silver-table",
                _conf_or_param("silver_table"),
            ],
            run_name="batch2-bronze-to-silver",
        ),
        wait_for_termination=True,
    )

    landing_to_bronze >> bronze_to_silver
