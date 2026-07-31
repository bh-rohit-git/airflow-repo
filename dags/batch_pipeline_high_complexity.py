"""
Pipeline 3 — high complexity batch: enriched → gold customer 360 + daily metrics.

Serverless Databricks JAR task. JAR path + main class are hardcoded;
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
MAIN_CLASS = "com.example.batch.Customer360GoldMain"
SERVERLESS_ENV_VERSION = "4"
PERFORMANCE_TARGET = "PERFORMANCE_OPTIMIZED"
SAFETY_TIMEOUT_SECONDS = 7200

ENRICHED_TABLE = "`databricks-migrate-activity`.batch2.b2_enriched_orders"
RETURNS_TABLE = "`databricks-migrate-activity`.batch2.b2_returns"
PROMOTIONS_TABLE = "`databricks-migrate-activity`.batch2.b2_promotions"
GOLD_CUSTOMER_TABLE = "`databricks-migrate-activity`.batch2.b2_gold_customer_360"
GOLD_DAILY_TABLE = "`databricks-migrate-activity`.batch2.b2_gold_daily_metrics"


def _conf_or_param(key: str) -> str:
    return f"{{{{ dag_run.conf.get('{key}', params.{key}) }}}}"


def build_payload() -> dict:
    return {
        "run_name": "batch2-customer-360-gold",
        "timeout_seconds": SAFETY_TIMEOUT_SECONDS,
        "performance_target": PERFORMANCE_TARGET,
        "tasks": [
            {
                "task_key": "customer_360_gold",
                "spark_jar_task": {
                    "main_class_name": MAIN_CLASS,
                    "parameters": [
                        "--enriched-table",
                        _conf_or_param("enriched_table"),
                        "--returns-table",
                        _conf_or_param("returns_table"),
                        "--promotions-table",
                        _conf_or_param("promotions_table"),
                        "--gold-customer-table",
                        _conf_or_param("gold_customer_table"),
                        "--gold-daily-table",
                        _conf_or_param("gold_daily_table"),
                    ],
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
    dag_id="batch_pipeline_high_complexity",
    description="High complexity batch: enriched→gold (windows, joins, MERGE)",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["databricks", "batch", "serverless", "high"],
    params={
        "enriched_table": Param(ENRICHED_TABLE, type="string"),
        "returns_table": Param(RETURNS_TABLE, type="string"),
        "promotions_table": Param(PROMOTIONS_TABLE, type="string"),
        "gold_customer_table": Param(GOLD_CUSTOMER_TABLE, type="string"),
        "gold_daily_table": Param(GOLD_DAILY_TABLE, type="string"),
    },
) as dag:
    DatabricksSubmitRunOperator(
        task_id="customer_360_gold",
        databricks_conn_id=DATABRICKS_CONN_ID,
        json=build_payload(),
        wait_for_termination=True,
    )
