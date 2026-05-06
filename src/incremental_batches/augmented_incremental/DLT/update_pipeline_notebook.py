# Databricks notebook source
# MAGIC %md
# MAGIC # Update Pipeline Notebook
# MAGIC
# MAGIC Swaps one of the pipeline's library notebooks. Two modes:
# MAGIC
# MAGIC 1. **Explicit (preferred)** — set both `from_notebook` and `to_notebook` to full
# MAGIC    workspace paths. The script finds the library matching `from_notebook` exactly
# MAGIC    and rewrites it to `to_notebook`. Use this for ad-hoc swaps to test variants.
# MAGIC 2. **Legacy historical/incremental** — leave `from_notebook`/`to_notebook` empty
# MAGIC    and set `historical_flag` to flip between the hardcoded `dlt_historical` and
# MAGIC    `dlt_incremental` suffixes.
# MAGIC
# MAGIC **Widgets**
# MAGIC - `pipeline_id` (STRING, required)
# MAGIC - `from_notebook` (STRING, optional) — full workspace path to swap from
# MAGIC - `to_notebook` (STRING, optional) — full workspace path to swap to
# MAGIC - `historical_flag` (true/false) — only used when from/to are empty

# COMMAND ----------

dbutils.widgets.text("pipeline_id", "", "pipeline_id")
dbutils.widgets.text("from_notebook", "", "from_notebook (optional, full path)")
dbutils.widgets.text("to_notebook", "", "to_notebook (optional, full path)")
dbutils.widgets.dropdown("historical_flag", "false", ["true", "false"], "historical_flag")

pipeline_id = dbutils.widgets.get("pipeline_id").strip()
from_nb = dbutils.widgets.get("from_notebook").strip()
to_nb = dbutils.widgets.get("to_notebook").strip()
historical_flag = dbutils.widgets.get("historical_flag").strip().lower() == "true"

if not pipeline_id:
    raise ValueError("pipeline_id widget is required")
if bool(from_nb) != bool(to_nb):
    raise ValueError("from_notebook and to_notebook must both be set or both empty")

explicit_mode = bool(from_nb)
print(f"pipeline_id={pipeline_id}  mode={'explicit' if explicit_mode else 'legacy'}  "
      f"historical_flag={historical_flag}  from={from_nb or '(unset)'}  to={to_nb or '(unset)'}")

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

pipeline = w.pipelines.get(pipeline_id=pipeline_id)
spec_dict = pipeline.spec.as_dict() if pipeline.spec else None
if not spec_dict or not spec_dict.get("libraries"):
    raise RuntimeError(f"Pipeline {pipeline_id} has no spec/libraries to update")

if explicit_mode:
    desired_path = to_nb
    match_pattern = from_nb
    match_kind = "exact"
else:
    incremental_suffix = "/src/incremental_batches/augmented_incremental/DLT/dlt_incremental"
    historical_suffix  = "/src/incremental_batches/augmented_incremental/DLT/dlt_historical"
    desired_path = historical_suffix if historical_flag else incremental_suffix
    match_pattern = incremental_suffix if historical_flag else historical_suffix
    match_kind = "endswith"

# COMMAND ----------

libraries = spec_dict["libraries"]
match_idx = None

def _matches(path, pattern, kind):
    return path == pattern if kind == "exact" else path.endswith(pattern)

def _already_desired(path, desired, kind):
    return path == desired if kind == "exact" else path.endswith(desired)

for i, lib in enumerate(libraries):
    path = (lib.get("notebook") or {}).get("path")
    if not path:
        continue
    if _already_desired(path, desired_path, match_kind):
        print(f"Pipeline already uses desired notebook ({path}). No update needed.")
        dbutils.notebook.exit("no_change")
    if _matches(path, match_pattern, match_kind):
        match_idx = i

if match_idx is None:
    raise RuntimeError(
        f"Could not find a library notebook matching '{match_pattern}' "
        f"({'exact' if match_kind == 'exact' else 'endswith'}) in pipeline {pipeline_id}. "
        f"Libraries: {[(l.get('notebook') or {}).get('path') for l in libraries]}"
    )

old_path = libraries[match_idx]["notebook"]["path"]
if match_kind == "exact":
    new_path = desired_path
else:
    new_path = old_path[: -len(match_pattern)] + desired_path
print(f"Rewriting library[{match_idx}]: {old_path}  ->  {new_path}")
libraries[match_idx]["notebook"]["path"] = new_path

# COMMAND ----------

w.api_client.do("PUT", f"/api/2.0/pipelines/{pipeline_id}", body=spec_dict)
print(f"Updated pipeline {pipeline_id}. New library path: {new_path}")

dbutils.notebook.exit("updated")