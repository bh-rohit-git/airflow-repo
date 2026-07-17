from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from airflow.sdk import Param

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="test_spark_merge_serverless_dag",
    default_args=default_args,
    description="Trigger Spark merge job on Databricks serverless (JAR task)",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["databricks", "spark", "serverless"],
    params={
        "input_path": Param(
            default="gs://databricks-8259550742932689-unitycatalog/8259550742932689/incoming/customer_events_merge.csv",
            type="string",
            description="GCS path to the input CSV file or folder",
        ),
        "target_table": Param(
            default="`databricks-migrate-activity`.schema1.customer_events",
            type="string",
            description="Target table",
        ),
    },
) as dag:

    run_spark_merge = DatabricksSubmitRunOperator(
        task_id="run_spark_merge",
        databricks_conn_id="databricks_default",
        json={
            "run_name": "spark_merge_job_serverless",
            "tasks": [
                {
                    "task_key": "spark_merge_job",
                    "environment_key": "default",
                    "spark_jar_task": {
                        "main_class_name": "com.example.merge.Main",
                        "parameters": [
                            "--input-path",
                            "{{ dag_run.conf.get('input_path', params.input_path) }}",
                            "--target-table",
                            "{{ dag_run.conf.get('target_table', params.target_table) }}",
                            "--merge-key",
                            "id",
                        ],
                    },
                }
            ],
            "environments": [
                {
                    "environment_key": "default",
                    "spec": {
                        "environment_version": "4",
                        "java_dependencies": [
                            "/Volumes/databricks-migrate-activity/schema1/jars/spark-merge-job-assembly.jar"
                        ],
                    },
                }
            ],
        },
    )
