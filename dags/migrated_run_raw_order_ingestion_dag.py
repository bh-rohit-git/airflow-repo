"""
Airflow DAG: submit Job 1 (Pub/Sub → bronze_orders) as a serverless Databricks JAR task.

Trigger with JSON conf (see run_raw_order_ingestion_trigger_conf.example.json) or use DAG param defaults.
"""
from __future__ import annotations
from datetime import datetime
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateClusterOperator, DataprocDeleteClusterOperator
import copy

def _coerce_dataproc_int(value):
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.isdigit():
        return int(value)
    return value

def _duration_seconds_value(value, default=1800):
    if isinstance(value, int):
        return value
    if isinstance(value, dict) and 'seconds' in value:
        return int(value['seconds'])
    if isinstance(value, str):
        stripped = value.strip()
        if stripped.endswith('s'):
            stripped = stripped[:-1].strip()
        if stripped.isdigit():
            return int(stripped)
    return default

def _coerce_duration_proto(value, default=1800):
    return {'seconds': _duration_seconds_value(value, default)}

def normalize_dataproc_cluster_config_types(config):
    normalized = copy.deepcopy(config)
    for role in ('master_config', 'worker_config'):
        section = normalized.get(role)
        if not isinstance(section, dict):
            continue
        if 'num_instances' in section:
            section['num_instances'] = _coerce_dataproc_int(section['num_instances'])
        disk = section.get('disk_config')
        if isinstance(disk, dict) and 'boot_disk_size_gb' in disk:
            disk['boot_disk_size_gb'] = _coerce_dataproc_int(disk['boot_disk_size_gb'])
    lifecycle = normalized.get('lifecycle_config')
    if isinstance(lifecycle, dict):
        if 'idle_delete_ttl' in lifecycle:
            lifecycle['idle_delete_ttl'] = _coerce_duration_proto(lifecycle['idle_delete_ttl'])
        if 'auto_delete_ttl' in lifecycle:
            lifecycle['auto_delete_ttl'] = _coerce_duration_proto(lifecycle['auto_delete_ttl'])
    return normalized

class TypedDataprocCreateClusterOperator(DataprocCreateClusterOperator):

    def execute(self, context):
        if isinstance(self.cluster_config, dict):
            self.cluster_config = normalize_dataproc_cluster_config_types(self.cluster_config)
        return super().execute(context)
from pathlib import Path
import yaml
PUBSUB_LANDING_URI = 'gs://BUCKET/pubsub-landing/orders'
_ENV_CONFIG_PATH = Path(__file__).resolve().parent.parent / 'plugins' / 'config_dev.yml'
_DAG_SIZING_PATH = Path(__file__).resolve().parent.parent / 'plugins' / 'migrated_run_raw_order_ingestion_pubsub_cluster_config.yml'
if not _ENV_CONFIG_PATH.is_file():
    raise FileNotFoundError('config_dev.yml not found under plugins/. Provision it in the target environment (e.g. gs://<composer_bucket>/plugins/config_dev.yml).')
if not _DAG_SIZING_PATH.is_file():
    raise FileNotFoundError('migrated_run_raw_order_ingestion_pubsub_cluster_config.yml not found under plugins/. Upload it to gs://<composer_bucket>/plugins/migrated_run_raw_order_ingestion_pubsub_cluster_config.yml.')
with _ENV_CONFIG_PATH.open(encoding='utf-8') as _env_config_file:
    _ENV_CONFIG = yaml.safe_load(_env_config_file)
with _DAG_SIZING_PATH.open(encoding='utf-8') as _dag_sizing_file:
    _DAG_SIZING = yaml.safe_load(_dag_sizing_file)
GCP_PROJECT_ID = _ENV_CONFIG['gcp_project_id']
GCP_REGION = _ENV_CONFIG['gcp_region']
DATABRICKS_HOST = _ENV_CONFIG.get('databricks_host', '')
DATABRICKS_TOKEN = _ENV_CONFIG.get('databricks_token', '')
DATAPROC_CLUSTER_NAME = _DAG_SIZING['dataproc_cluster_name']
_BASE_CLUSTER_CONFIG = normalize_dataproc_cluster_config_types(_ENV_CONFIG['dataproc_cluster_config'])
_SIZING = normalize_dataproc_cluster_config_types({'master_config': _DAG_SIZING['master_config'], 'worker_config': _DAG_SIZING['worker_config']})
DATAPROC_CLUSTER_CONFIG = {**_BASE_CLUSTER_CONFIG, 'master_config': _SIZING['master_config'], 'worker_config': _SIZING['worker_config']}
try:
    from airflow.sdk import Param
except ImportError:
    from airflow.models.param import Param
DATABRICKS_CONN_ID = 'databricks_default'
MAIN_CLASS = 'com.example.streaming.RawOrderIngestionMain'
SERVERLESS_ENV_VERSION = '4'
GCP_PROJECT = 'nprd-bh-use1-dev'
PUBSUB_TOPIC = 'order-events'
PUBSUB_SUBSCRIPTION = 'order-events-databricks'
PUBSUB_SERVICE_CREDENTIAL = 'gcp-pubsub'
BRONZE_TABLE = '`databricks-migrate-activity`.schema1.bronze_orders'
CHECKPOINT_LOCATION = 'gs://bh-migrate-poc-bucket/pubsub-setup/checkpoints/bronze_orders_pubsub'
UC_JAR_PATH = '/Volumes/databricks-migrate-activity/schema1/jars/databricks-structured-streaming-assembly-0.1.0.jar'
TRIGGER_INTERVAL = '30 seconds'
DEFAULT_IDLE_TIMEOUT = '2 minutes'
SAFETY_TIMEOUT_SECONDS = 7200
RUN_NAME = 'raw-order-ingestion-pubsub-serverless'
PERFORMANCE_TARGET = 'PERFORMANCE_OPTIMIZED'

def _conf_or_param(key: str) -> str:
    """Resolve trigger conf override with DAG param fallback."""
    return f"{{{{ dag_run.conf.get('{key}', params.{key}) }}}}"

def build_serverless_jar_payload() -> dict:
    """Payload for jobs/runs/submit."""
    parameters = ['--landing-path', _conf_or_param('landing_path'), '--target-table', _conf_or_param('bronze_table'), '--checkpoint-location', _conf_or_param('checkpoint_location'), '--trigger-interval', _conf_or_param('trigger_interval'), '--idle-timeout', _conf_or_param('idle_timeout')]
    return {'run_name': _conf_or_param('run_name'), 'timeout_seconds': _conf_or_param('safety_timeout_seconds'), 'performance_target': _conf_or_param('performance_target'), 'tasks': [{'task_key': 'raw_order_ingestion', 'spark_jar_task': {'main_class_name': _conf_or_param('main_class'), 'parameters': parameters}, 'environment_key': 'jar_env'}], 'environments': [{'environment_key': 'jar_env', 'spec': {'environment_version': _conf_or_param('serverless_env_version'), 'java_dependencies': [_conf_or_param('uc_jar_path')]}}]}
with DAG(dag_id='migrated_run_raw_order_ingestion_pubsub', description='Serverless JAR: Pub/Sub → bronze_orders (stops after idle timeout)', start_date=datetime(2025, 1, 1), schedule=None, catchup=False, tags=['databricks', 'pubsub', 'streaming', 'serverless'], params={'bronze_table': Param(BRONZE_TABLE, type='string', description='Bronze Delta target table (JAR --target-table)'), 'checkpoint_location': Param(CHECKPOINT_LOCATION, type='string', description='GCS checkpoint path for structured streaming'), 'trigger_interval': Param(TRIGGER_INTERVAL, type='string', description="Spark micro-batch trigger (e.g. '30 seconds', '1 minute')"), 'idle_timeout': Param(DEFAULT_IDLE_TIMEOUT, type='string', description="Stop Spark after this long with no Pub/Sub input (e.g. '2 minutes', '5 minutes')"), 'main_class': Param(MAIN_CLASS, type='string', description='Databricks JAR entrypoint main class'), 'uc_jar_path': Param(UC_JAR_PATH, type='string', description='Unity Catalog volume path to the assembly JAR'), 'serverless_env_version': Param(SERVERLESS_ENV_VERSION, type='string', description='Databricks serverless environment version'), 'safety_timeout_seconds': Param(SAFETY_TIMEOUT_SECONDS, type='integer', description='Databricks job run safety timeout in seconds'), 'run_name': Param(RUN_NAME, type='string', description='Databricks jobs/runs/submit run_name'), 'performance_target': Param(PERFORMANCE_TARGET, type='string', description='Serverless performance target (e.g. PERFORMANCE_OPTIMIZED)'), 'landing_path': Param(PUBSUB_LANDING_URI, type='string', description='GCS Pub/Sub landing prefix (JAR --landing-path)')}) as dag:
    DatabricksSubmitRunOperator(task_id='submit_raw_order_ingestion_serverless_jar', databricks_conn_id=DATABRICKS_CONN_ID, json=build_serverless_jar_payload(), wait_for_termination=True)
