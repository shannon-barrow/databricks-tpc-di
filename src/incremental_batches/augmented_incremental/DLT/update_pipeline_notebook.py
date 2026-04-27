# Databricks notebook source
# MAGIC %md
# MAGIC # Update Pipeline Notebook (historical <-> incremental swap)
# MAGIC
# MAGIC Flips the pipeline's DLT notebook between `dlt_incremental` and `dlt_historical`.
# MAGIC
# MAGIC **Widgets**
# MAGIC - `pipeline_id` (STRING): Target pipeline ID
# MAGIC - `historical_flag` (BOOLEAN as "true"/"false"): TRUE -> switch to historical; FALSE -> switch to incremental

# COMMAND ----------

dbutils.widgets.text("pipeline_id", "", "pipeline_id")
dbutils.widgets.dropdown("historical_flag", "false", ["true", "false"], "historical_flag")

pipeline_id = dbutils.widgets.get("pipeline_id").strip()
historical_flag = dbutils.widgets.get("historical_flag").strip().lower() == "true"

if not pipeline_id:
    raise ValueError("pipeline_id widget is required")

print(f"pipeline_id={pipeline_id}  historical_flag={historical_flag}")

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

pipeline = w.pipelines.get(pipeline_id=pipeline_id)
spec_dict = pipeline.spec.as_dict() if pipeline.spec else None
if not spec_dict or not spec_dict.get("libraries"):
    raise RuntimeError(f"Pipeline {pipeline_id} has no spec/libraries to update")

incremental_suffix = "/src/incremental_batches/augmented_incremental/DLT/dlt_incremental"
historical_suffix  = "/src/incremental_batches/augmented_incremental/DLT/dlt_historical"

desired_suffix = historical_suffix if historical_flag else incremental_suffix
current_suffix = incremental_suffix if historical_flag else historical_suffix

# COMMAND ----------

libraries = spec_dict["libraries"]
match_idx = None

for i, lib in enumerate(libraries):
    path = (lib.get("notebook") or {}).get("path")
    if not path:
        continue
    if path.endswith(desired_suffix):
        print(f"Pipeline already uses desired notebook ({path}). No update needed.")
        dbutils.notebook.exit("no_change")
    if path.endswith(current_suffix):
        match_idx = i

if match_idx is None:
    raise RuntimeError(
        f"Could not find a library notebook ending with '{current_suffix}' in pipeline {pipeline_id}. "
        f"Libraries: {[(l.get('notebook') or {}).get('path') for l in libraries]}"
    )

old_path = libraries[match_idx]["notebook"]["path"]
new_path = old_path[: -len(current_suffix)] + desired_suffix
print(f"Rewriting library[{match_idx}]: {old_path}  ->  {new_path}")
libraries[match_idx]["notebook"]["path"] = new_path

# COMMAND ----------

w.api_client.do("PUT", f"/api/2.0/pipelines/{pipeline_id}", body=spec_dict)
print(f"Updated pipeline {pipeline_id}. New library path: {new_path}")

dbutils.notebook.exit("updated")