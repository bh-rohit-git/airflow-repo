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
    dag_id="run_spark_merge_dag",
    default_args=default_args,
    description="Trigger Spark merge job on Databricks",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["databricks", "spark"],
    params={
        "input_path": Param(
            default="gs://databricks-migrate-activity/incoming/sample/customer_events_merge.csv",
            type="string",
            description="GCS path to the input CSV file or folder",
        ),
    },
) as dag:

    run_spark_merge = DatabricksSubmitRunOperator(
        task_id="run_spark_merge",
        databricks_conn_id="databricks_default",
        json={
            "run_name": "spark_merge_job",
            "tasks": [
                {
                    "task_key": "spark_merge_job",
                    "notebook_task": {
                        "notebook_path": "/Workspace/Users/abhishek@bighammer.ai/run_spark_merge",
                        "base_parameters": {
                            "input_path": "{{ dag_run.conf.get('input_path', params.input_path) }}",
                            "target_table": "`databricks-migrate-activity`.schema1.customer_events",
                            "merge_key": "id",
                        },
                    },
                    "libraries": [
                        {
                            "jar": "/Volumes/databricks-migrate-activity/schema1/jars/spark-merge-job-assembly.jar",
                        }
                    ],
                    "new_cluster": {
                        "spark_version": "15.4.x-scala2.12",
                        "node_type_id": "n2-standard-4",
                        "num_workers": 1,
                        "data_security_mode": "SINGLE_USER",
                        "single_user_name": "abhishek@bighammer.ai",
                        "gcp_attributes": {
                            "google_service_account": "databricks-compute@nprd-bh-use1-dev.iam.gserviceaccount.com",
                            "use_preemptible_executors": False,
                        },
                        "spark_conf": {
                            "spark.driver.memory": "4g",
                            "spark.databricks.delta.tempPath": "gs://databricks-migrate-activity/tmp/delta-staging",
                            "spark.sql.warehouse.dir": "gs://databricks-migrate-activity/tmp/warehouse",
                        },
                    },
                }
            ],
        },
    )
