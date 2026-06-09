"""Orchestrate the raw-ingestion Scala JAR on Databricks."""
from __future__ import annotations
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateClusterOperator, DataprocDeleteClusterOperator
GCP_PROJECT_ID = '{{ var.value.gcp_project_id }}'
GCP_REGION = '{{ var.value.gcp_region }}'
DATAPROC_CLUSTER_NAME = '{{ var.value.dataproc_cluster_name }}'
DATAPROC_CLUSTER_CONFIG = {'gce_cluster_config': {'zone_uri': '{{ var.value.dataproc_zone }}', 'subnetwork_uri': '{{ var.value.dataproc_subnetwork }}', 'service_account': '{{ var.value.dataproc_service_account }}'}, 'master_config': {'num_instances': '{{ var.value.dataproc_master_num_instances }}', 'machine_type_uri': '{{ var.value.dataproc_master_machine_type }}', 'disk_config': {'boot_disk_size_gb': '{{ var.value.dataproc_master_boot_disk_size_gb }}', 'boot_disk_type': '{{ var.value.dataproc_master_boot_disk_type }}'}}, 'worker_config': {'num_instances': '{{ var.value.dataproc_worker_num_instances }}', 'machine_type_uri': '{{ var.value.dataproc_worker_machine_type }}', 'disk_config': {'boot_disk_size_gb': '{{ var.value.dataproc_worker_boot_disk_size_gb }}', 'boot_disk_type': '{{ var.value.dataproc_worker_boot_disk_type }}'}}, 'software_config': {'image_version': '{{ var.value.dataproc_image_version }}', 'properties': {'spark:spark.sql.extensions': 'io.delta.sql.DeltaSparkSessionExtension', 'spark:spark.sql.catalog.spark_catalog': 'org.apache.spark.sql.delta.catalog.DeltaCatalog'}}, 'lifecycle_config': {'idle_delete_ttl': '{{ var.value.dataproc_idle_delete_ttl }}'}}
RAW_INGESTION_JAR = '{{ var.value.raw_ingestion_jar_uri }}'
default_args = {'owner': 'data-platform', 'depends_on_past': False, 'retries': 1, 'retry_delay': timedelta(minutes=5)}
with DAG(dag_id='raw_ingestion', default_args=default_args, description='Load raw sales data via Scala JAR', schedule='0 6 * * *', start_date=datetime(2024, 1, 1), catchup=False, tags=['databricks', 'raw-ingestion']) as dag:
    run_raw_ingestion = DataprocSubmitJobOperator(task_id='run_raw_ingestion', project_id=GCP_PROJECT_ID, region=GCP_REGION, job={'placement': {'cluster_name': DATAPROC_CLUSTER_NAME}, 'spark_job': {'main_class': 'com.acme.RawIngestionJob', 'jar_file_uris': [RAW_INGESTION_JAR]}})
create_cluster = DataprocCreateClusterOperator(task_id='create_cluster', project_id=GCP_PROJECT_ID, region=GCP_REGION, cluster_name=DATAPROC_CLUSTER_NAME, cluster_config=DATAPROC_CLUSTER_CONFIG)
delete_cluster = DataprocDeleteClusterOperator(task_id='delete_cluster', project_id=GCP_PROJECT_ID, region=GCP_REGION, cluster_name=DATAPROC_CLUSTER_NAME, trigger_rule='all_done')
create_cluster >> run_raw_ingestion
run_raw_ingestion >> delete_cluster
