# Databricks notebook source
# MAGIC %md
# MAGIC # Set Pipeline Libraries (multi-library variant)
# MAGIC
# MAGIC Companion to `update_pipeline_notebook.py`. That one swaps a single
# MAGIC library at a time (find-and-replace by path); this one accepts a
# MAGIC full JSON array of paths and sets the pipeline's `libraries[]` to
# MAGIC exactly that list.
# MAGIC
# MAGIC Used by parent jobs that have multi-library phases — e.g. the
# MAGIC SF=10 SDP_FMH_Dev pipeline whose incremental phase loads two
# MAGIC notebooks (`dlt_incremental_liquid_fmhdev` for the rest of the
# MAGIC dimensional model + `dlt_fmh_per_batch_liquid` for the FMH
# MAGIC foreachBatch sink).
# MAGIC
# MAGIC **Widgets**
# MAGIC - `pipeline_id` (STRING, required)
# MAGIC - `libraries_json` (STRING, required) — JSON array of notebook
# MAGIC   paths. Sets `libraries[]` to exactly these (no merge).

# COMMAND ----------

import json

dbutils.widgets.text("pipeline_id", "", "pipeline_id")
dbutils.widgets.text("libraries_json", "[]", "libraries_json")

pipeline_id = dbutils.widgets.get("pipeline_id").strip()
libs_json = dbutils.widgets.get("libraries_json").strip()

if not pipeline_id:
    raise ValueError("pipeline_id widget is required")

paths = json.loads(libs_json)
if not isinstance(paths, list) or not paths:
    raise ValueError("libraries_json must be a non-empty JSON array of notebook paths")

print(f"pipeline_id={pipeline_id}")
print(f"setting libraries[]:")
for p in paths:
    print(f"  - {p}")

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
pipeline = w.pipelines.get(pipeline_id=pipeline_id)
spec = pipeline.spec.as_dict() if pipeline.spec else None
if not spec:
    raise RuntimeError(f"Pipeline {pipeline_id} has no spec to update")

spec["libraries"] = [{"notebook": {"path": p}} for p in paths]
w.api_client.do("PUT", f"/api/2.0/pipelines/{pipeline_id}", body=spec)
print("OK — pipeline libraries set")
dbutils.notebook.exit("updated")
