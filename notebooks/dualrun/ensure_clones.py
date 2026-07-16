# Databricks notebook source

# COMMAND ----------
"""Shared dual-run clone setup (bh-migrate).

Expects notebook widgets / base_parameters:
  - clone_plan_json: ClonePlan dict with ``clones`` (+ optional ``preamble_cells``)
"""

from __future__ import annotations

import json
import os

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()


def _widget(name: str, default: str = "") -> str:
    try:
        from pyspark.dbutils import DBUtils
        raw = DBUtils().widgets.get(name)
        if raw is not None and str(raw).strip():
            return str(raw).strip()
    except Exception:
        pass
    return str(os.environ.get(name, default) or default).strip()


def _load_plan_json() -> dict:
    raw = _widget("clone_plan_json", "{}")
    try:
        parsed = json.loads(raw or "{}")
        return parsed if isinstance(parsed, dict) else {}
    except json.JSONDecodeError as exc:
        raise ValueError(f"invalid clone_plan_json: {exc}") from exc


def _exists(fqn: str) -> bool:
    try:
        return spark.catalog.tableExists(fqn)
    except Exception:
        return False


plan_data = _load_plan_json()
clones = plan_data.get("clones") or []
if not clones:
    raise ValueError("clone_plan_json has no clones — parent DAG must pass a clone plan")

# Preamble cells define names such as resolved_clone_locations; share one namespace
# across exec() and clone execution (isolated exec() locals are not visible here).
_runtime_ns = {"spark": spark, "json": json}

for cell in plan_data.get("preamble_cells") or []:
    exec(cell, _runtime_ns)

for clone_item in clones:
    target = clone_item.get("target") or {}
    fqn = target.get("fqn", "")
    stmt_sql = clone_item.get("sql", "")
    skip = clone_item.get("skip_if_exists", True)
    if skip and fqn and _exists(fqn):
        print(f"skip existing clone: {fqn}")
        continue
    if "{resolved_clone_locations[" in stmt_sql:
        index = int(target.get("index") or 1)
        source = target.get("source") or {}
        catalog = (source.get("catalog") or "").strip("`")
        schema = (source.get("source_schema") or "").strip("`")
        table_name = source.get("table_name") or ""
        source_ref = f"`{catalog}`.{schema}.{table_name}" if catalog else source.get("ref", "")
        target_ref = (
            f"`{catalog}`.{schema}.{fqn.rsplit('.', 1)[-1]}"
            if catalog and "." in fqn
            else fqn
        )
        location = _runtime_ns["resolved_clone_locations"][index]
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS {target_ref}\n"
            f"DEEP CLONE {source_ref}\n"
            f"LOCATION '{location}'"
        )
    else:
        spark.sql(stmt_sql)
    print(f"clone ready: {fqn}")

print(json.dumps({"status": "ok", "clone_count": len(clones)}))
