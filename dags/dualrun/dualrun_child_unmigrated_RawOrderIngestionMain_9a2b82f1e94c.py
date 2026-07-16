"""Dual-run unmigrated child for run_raw_order_ingestion_pubsub

Auto-generated dual-run placeholder — replace by re-emitting after the source DAG
is available under the migrate output / orchestrator checkout.
"""

from __future__ import annotations

from datetime import datetime

from airflow import DAG

with DAG(
    dag_id="dualrun_child_unmigrated_RawOrderIngestionMain_9a2b82f1e94c",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["dual-run", "bh-migrate", "placeholder"],
) as dag:
    pass
