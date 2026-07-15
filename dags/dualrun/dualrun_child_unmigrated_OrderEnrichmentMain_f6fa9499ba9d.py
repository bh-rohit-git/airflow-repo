"""Dual-run unmigrated child for run_order_enrichment_bronze_to_silver

Auto-generated dual-run placeholder — replace by re-emitting after the source DAG
is available under the migrate output / orchestrator checkout.
"""

from __future__ import annotations

from datetime import datetime

from airflow import DAG

with DAG(
    dag_id="dualrun_child_unmigrated_OrderEnrichmentMain_f6fa9499ba9d",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["dual-run", "bh-migrate", "placeholder"],
) as dag:
    pass
