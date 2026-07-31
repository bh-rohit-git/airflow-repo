"""
Pipeline 2 — mid complexity batch: silver → enriched_orders (join + risk + MERGE).

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
MAIN_CLASS = "com.example.batch.OrderEnrichmentBatchMain"
SERVERLESS_ENV_VERSION = "4"
PERFORMANCE_TARGET = "PERFORMANCE_OPTIMIZED"
SAFETY_TIMEOUT_SECONDS = 3600

SILVER_TABLE = "`databricks-migrate-activity`.batch2.b2_silver_orders"
CUSTOMER_TABLE = "`databricks-migrate-activity`.batch2.b2_customers"
PRODUCT_TABLE = "`databricks-migrate-activity`.batch2.b2_products"
ENRICHED_TABLE = "`databricks-migrate-activity`.batch2.b2_enriched_orders"


def _conf_or_param(key: str) -> str:
    return f"{{{{ dag_run.conf.get('{key}', params.{key}) }}}}"


def build_payload() -> dict:
    return {
        "run_name": "batch2-order-enrichment",
        "timeout_seconds": SAFETY_TIMEOUT_SECONDS,
        "performance_target": PERFORMANCE_TARGET,
        "tasks": [
            {
                "task_key": "order_enrichment",
                "spark_jar_task": {
                    "main_class_name": MAIN_CLASS,
                    "parameters": [
                        "--silver-table",
                        _conf_or_param("silver_table"),
                        "--customer-table",
                        _conf_or_param("customer_table"),
                        "--product-table",
                        _conf_or_param("product_table"),
                        "--enriched-table",
                        _conf_or_param("enriched_table"),
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
    dag_id="batch_pipeline_mid_complexity",
    description="Mid complexity batch: silver→enriched (joins, risk, MERGE)",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["databricks", "batch", "serverless", "mid"],
    params={
        "silver_table": Param(SILVER_TABLE, type="string"),
        "customer_table": Param(CUSTOMER_TABLE, type="string"),
        "product_table": Param(PRODUCT_TABLE, type="string"),
        "enriched_table": Param(ENRICHED_TABLE, type="string"),
    },
) as dag:
    DatabricksSubmitRunOperator(
        task_id="order_enrichment_batch",
        databricks_conn_id=DATABRICKS_CONN_ID,
        json=build_payload(),
        wait_for_termination=True,
    )
