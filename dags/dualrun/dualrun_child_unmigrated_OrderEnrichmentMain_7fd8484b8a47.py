"""
Airflow DAG: submit Job 2 (bronze_orders → silver_orders) as a serverless Databricks JAR task.

Reads new bronze Delta commits, joins customers, MERGE upserts into silver.
Stops after idle timeout with no new bronze rows (AvailableNow loop in the JAR).

Trigger with JSON conf (see run_order_enrichment_trigger_conf.example.json) or use DAG param defaults.
"""
from __future__ import annotations
from datetime import datetime
from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
try:
    from airflow.sdk import Param
except ImportError:
    from airflow.models.param import Param
DATABRICKS_CONN_ID = 'databricks_default'
MAIN_CLASS = 'com.example.streaming.OrderEnrichmentMain'
SERVERLESS_ENV_VERSION = '4'
BRONZE_TABLE = '`databricks-migrate-activity`.schema1.bronze_orders'
CUSTOMER_TABLE = '`databricks-migrate-activity`.schema1.customers'
SILVER_TABLE = '`databricks-migrate-activity`.schema1.silver_orders'
CHECKPOINT_LOCATION = 'gs://bh-migrate-poc-bucket/pubsub-setup/checkpoints/silver_orders'
UC_JAR_PATH = '/Volumes/databricks-migrate-activity/schema1/jars/databricks-structured-streaming-assembly-0.1.0.jar'
TRIGGER_INTERVAL = '1 minute'
DEFAULT_IDLE_TIMEOUT = '2 minutes'
SAFETY_TIMEOUT_SECONDS = 7200
RUN_NAME = 'order-enrichment-bronze-to-silver-serverless'
PERFORMANCE_TARGET = 'PERFORMANCE_OPTIMIZED'

def _conf_or_param(key: str) -> str:
    """Resolve trigger conf override with DAG param fallback."""
    return f"{{{{ dag_run.conf.get('{key}', params.{key}) }}}}"

def build_serverless_jar_payload() -> dict:
    parameters = ['--source-table', _conf_or_param('bronze_table'), '--customer-table', _conf_or_param('customer_table'), '--target-table', _conf_or_param('silver_table'), '--checkpoint-location', _conf_or_param('checkpoint_location'), '--trigger-interval', _conf_or_param('trigger_interval'), '--idle-timeout', _conf_or_param('idle_timeout')]
    return {'run_name': _conf_or_param('run_name'), 'timeout_seconds': _conf_or_param('safety_timeout_seconds'), 'performance_target': _conf_or_param('performance_target'), 'tasks': [{'task_key': 'order_enrichment', 'spark_jar_task': {'main_class_name': _conf_or_param('main_class'), 'parameters': parameters}, 'environment_key': 'jar_env'}], 'environments': [{'environment_key': 'jar_env', 'spec': {'environment_version': _conf_or_param('serverless_env_version'), 'java_dependencies': [_conf_or_param('uc_jar_path')]}}]}
with DAG(dag_id='dualrun_child_unmigrated_OrderEnrichmentMain_7fd8484b8a47', description='Serverless JAR: bronze_orders → silver_orders (stops after idle timeout)', start_date=datetime(2025, 1, 1), schedule=None, catchup=False, tags=['databricks', 'delta', 'streaming', 'serverless', 'silver'], params={'bronze_table': Param(BRONZE_TABLE, type='string', description='Bronze Delta source table (JAR --source-table)'), 'customer_table': Param(CUSTOMER_TABLE, type='string', description='Customer dimension table (JAR --customer-table)'), 'silver_table': Param(SILVER_TABLE, type='string', description='Silver MERGE target table (JAR --target-table)'), 'checkpoint_location': Param(CHECKPOINT_LOCATION, type='string', description='GCS checkpoint path for structured streaming'), 'trigger_interval': Param(TRIGGER_INTERVAL, type='string', description="Spark micro-batch trigger (e.g. '1 minute', '30 seconds')"), 'idle_timeout': Param(DEFAULT_IDLE_TIMEOUT, type='string', description="Stop Spark after this long with no new bronze rows (e.g. '2 minutes', '5 minutes')"), 'main_class': Param(MAIN_CLASS, type='string', description='Databricks JAR entrypoint main class'), 'uc_jar_path': Param(UC_JAR_PATH, type='string', description='Unity Catalog volume path to the assembly JAR'), 'serverless_env_version': Param(SERVERLESS_ENV_VERSION, type='string', description='Databricks serverless environment version'), 'safety_timeout_seconds': Param(SAFETY_TIMEOUT_SECONDS, type='integer', description='Databricks job run safety timeout in seconds'), 'run_name': Param(RUN_NAME, type='string', description='Databricks jobs/runs/submit run_name'), 'performance_target': Param(PERFORMANCE_TARGET, type='string', description='Serverless performance target (e.g. PERFORMANCE_OPTIMIZED)')}) as dag:
    DatabricksSubmitRunOperator(task_id='submit_order_enrichment_serverless_jar', databricks_conn_id=DATABRICKS_CONN_ID, json=build_serverless_jar_payload(), wait_for_termination=True)
