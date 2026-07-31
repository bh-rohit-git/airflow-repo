"""
Orchestrator — every 5 minutes, while landing seed data remains:

  1) trigger batch_pipeline_landing_bronze_silver
  2) trigger batch_pipeline_mid_complexity
  3) trigger batch_pipeline_high_complexity

All jar paths / tables / landing paths are hardcoded here for easy execution.
Stops (short-circuit) when no unprocessed landing JSON files remain.
"""

from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.python import ShortCircuitOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook

# --- hardcoded for easy runs ---
LANDING_PATH = "gs://bh-migrate-poc-bucket/batch2/landing"
LANDING_BUCKET = "bh-migrate-poc-bucket"
LANDING_PREFIX = "batch2/landing/"

PIPELINE_1 = "batch_pipeline_landing_bronze_silver"
PIPELINE_2 = "batch_pipeline_mid_complexity"
PIPELINE_3 = "batch_pipeline_high_complexity"

CONF_PIPELINE_1 = {
    "landing_path": LANDING_PATH,
    "processed_path": f"{LANDING_PATH}/processed",
    "bronze_table": "`databricks-migrate-activity`.batch2.b2_bronze_orders",
    "silver_table": "`databricks-migrate-activity`.batch2.b2_silver_orders",
}

CONF_PIPELINE_2 = {
    "silver_table": "`databricks-migrate-activity`.batch2.b2_silver_orders",
    "customer_table": "`databricks-migrate-activity`.batch2.b2_customers",
    "product_table": "`databricks-migrate-activity`.batch2.b2_products",
    "enriched_table": "`databricks-migrate-activity`.batch2.b2_enriched_orders",
}

CONF_PIPELINE_3 = {
    "enriched_table": "`databricks-migrate-activity`.batch2.b2_enriched_orders",
    "returns_table": "`databricks-migrate-activity`.batch2.b2_returns",
    "promotions_table": "`databricks-migrate-activity`.batch2.b2_promotions",
    "gold_customer_table": "`databricks-migrate-activity`.batch2.b2_gold_customer_360",
    "gold_daily_table": "`databricks-migrate-activity`.batch2.b2_gold_daily_metrics",
}


def landing_seed_remaining(**_context) -> bool:
    """Return True if unprocessed landing JSON still exists under LANDING_PREFIX."""
    hook = GCSHook()
    blobs = hook.list(LANDING_BUCKET, prefix=LANDING_PREFIX) or []
    remaining = [
        b
        for b in blobs
        if b.endswith(".json")
        and "/processed/" not in b
        and b.startswith(LANDING_PREFIX)
        and b.count("/") == LANDING_PREFIX.count("/")  # only top-level landing files
    ]
    print(f"[batch_orchestrator] remaining landing files: {len(remaining)}")
    for name in remaining[:5]:
        print(f"  - {name}")
    return len(remaining) > 0


with DAG(
    dag_id="batch_orchestrator",
    description="Every 5m: pipeline1 → pipeline2 → pipeline3 until landing seed consumed",
    start_date=datetime(2026, 1, 1),
    schedule="*/5 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["databricks", "batch", "orchestrator"],
) as dag:
    check_seed = ShortCircuitOperator(
        task_id="check_landing_seed_remaining",
        python_callable=landing_seed_remaining,
    )

    trigger_p1 = TriggerDagRunOperator(
        task_id="trigger_pipeline_1",
        trigger_dag_id=PIPELINE_1,
        conf=CONF_PIPELINE_1,
        wait_for_completion=True,
        poke_interval=30,
    )

    trigger_p2 = TriggerDagRunOperator(
        task_id="trigger_pipeline_2",
        trigger_dag_id=PIPELINE_2,
        conf=CONF_PIPELINE_2,
        wait_for_completion=True,
        poke_interval=30,
    )

    trigger_p3 = TriggerDagRunOperator(
        task_id="trigger_pipeline_3",
        trigger_dag_id=PIPELINE_3,
        conf=CONF_PIPELINE_3,
        wait_for_completion=True,
        poke_interval=30,
    )

    check_seed >> trigger_p1 >> trigger_p2 >> trigger_p3
