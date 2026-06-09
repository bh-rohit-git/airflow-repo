"""Orchestrate the raw-ingestion Scala JAR on Databricks."""
from __future__ import annotations
from datetime import datetime, timedelta
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
_CLUSTER_CONFIG_PATH = Path(__file__).resolve().parent.parent / 'plugins' / 'raw_ingestion_cluster_config.yml'
if not _CLUSTER_CONFIG_PATH.is_file():
    raise FileNotFoundError('raw_ingestion_cluster_config.yml not found under plugins/. Upload it to gs://<composer_bucket>/plugins/raw_ingestion_cluster_config.yml.')
with _CLUSTER_CONFIG_PATH.open(encoding='utf-8') as _cluster_config_file:
    _PLATFORM = yaml.safe_load(_cluster_config_file)
GCP_PROJECT_ID = _PLATFORM['gcp_project_id']
GCP_REGION = _PLATFORM['gcp_region']
DATAPROC_CLUSTER_NAME = _PLATFORM['dataproc_cluster_name']
DATAPROC_CLUSTER_CONFIG = normalize_dataproc_cluster_config_types(_PLATFORM['dataproc_cluster_config'])
default_args = {'owner': 'data-platform', 'depends_on_past': False, 'retries': 1, 'retry_delay': timedelta(minutes=5)}
with DAG(dag_id='raw_ingestion', default_args=default_args, description='Load raw sales data via Scala JAR', schedule='0 6 * * *', start_date=datetime(2024, 1, 1), catchup=False, tags=['databricks', 'raw-ingestion']) as dag:
    run_raw_ingestion = DataprocSubmitJobOperator(task_id='run_raw_ingestion', project_id=GCP_PROJECT_ID, region=GCP_REGION, job={'placement': {'cluster_name': DATAPROC_CLUSTER_NAME}, 'spark_job': {'main_class': 'com.acme.RawIngestionJob', 'jar_file_uris': [_PLATFORM['job_jar_uri']]}})
create_cluster = TypedDataprocCreateClusterOperator(task_id='create_cluster', project_id=GCP_PROJECT_ID, region=GCP_REGION, cluster_name=DATAPROC_CLUSTER_NAME, cluster_config=DATAPROC_CLUSTER_CONFIG)
delete_cluster = DataprocDeleteClusterOperator(task_id='delete_cluster', project_id=GCP_PROJECT_ID, region=GCP_REGION, cluster_name=DATAPROC_CLUSTER_NAME, trigger_rule='all_done')
create_cluster >> run_raw_ingestion
run_raw_ingestion >> delete_cluster
