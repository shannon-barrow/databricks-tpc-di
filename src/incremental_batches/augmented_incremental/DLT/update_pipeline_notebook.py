# Databricks notebook source
# MAGIC %md
# MAGIC # Update Pipeline Notebook
# MAGIC
# MAGIC Swaps the pipeline's library notebooks between historical and incremental
# MAGIC sets. Two modes:
# MAGIC
# MAGIC 1. **Legacy historical/incremental (default)** — set `historical_flag`
# MAGIC    to `true` to ensure the pipeline's library list is the *historical*
# MAGIC    set, or `false` for the *incremental* set. The script identifies
# MAGIC    existing managed libraries by their notebook basename (one of
# MAGIC    `dlt_historical`, `dlt_incremental`, `dlt_incremental_fmh`),
# MAGIC    removes them all, and adds the target set back using the same path
# MAGIC    prefix. Non-managed libraries (e.g. `dlt_ingest_bronze`) are preserved.
# MAGIC
# MAGIC      - Historical set:  `dlt_historical`
# MAGIC      - Incremental set: `dlt_incremental`, `dlt_incremental_fmh`
# MAGIC
# MAGIC 2. **Explicit single-notebook swap** — set both `from_notebook` and
# MAGIC    `to_notebook` to full workspace paths. The script finds the library
# MAGIC    matching `from_notebook` exactly and rewrites it to `to_notebook`.
# MAGIC    Use this for ad-hoc swaps between variants. Other libraries are
# MAGIC    left alone.
# MAGIC
# MAGIC **Widgets**
# MAGIC - `pipeline_id` (STRING, required)
# MAGIC - `historical_flag` (true/false) — used in legacy mode
# MAGIC - `from_notebook` (STRING, optional) — explicit-mode swap source
# MAGIC - `to_notebook` (STRING, optional) — explicit-mode swap target

# COMMAND ----------

dbutils.widgets.text("pipeline_id", "", "pipeline_id")
dbutils.widgets.text("from_notebook", "", "from_notebook (optional, full path)")
dbutils.widgets.text("to_notebook", "", "to_notebook (optional, full path)")
dbutils.widgets.dropdown("historical_flag", "false", ["true", "false"], "historical_flag")

pipeline_id     = dbutils.widgets.get("pipeline_id").strip()
from_nb         = dbutils.widgets.get("from_notebook").strip()
to_nb           = dbutils.widgets.get("to_notebook").strip()
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

libraries = spec_dict["libraries"]

# COMMAND ----------

HISTORICAL_BASENAMES  = ["dlt_historical"]
INCREMENTAL_BASENAMES = ["dlt_incremental", "dlt_incremental_fmh"]
MANAGED_BASENAMES     = set(HISTORICAL_BASENAMES + INCREMENTAL_BASENAMES)

def _basename(path: str) -> str:
    return path.rsplit("/", 1)[-1] if path else ""

def _dirname(path: str) -> str:
    return path.rsplit("/", 1)[0] if "/" in path else ""

# COMMAND ----------

if explicit_mode:
    # Single-notebook swap: find one entry matching from_nb exactly, rewrite it.
    match_idx = None
    for i, lib in enumerate(libraries):
        path = (lib.get("notebook") or {}).get("path")
        if not path:
            continue
        if path == to_nb:
            print(f"Pipeline already uses desired notebook ({path}). No update needed.")
            dbutils.notebook.exit("no_change")
        if path == from_nb:
            match_idx = i

    if match_idx is None:
        raise RuntimeError(
            f"Could not find a library notebook matching '{from_nb}' (exact) in "
            f"pipeline {pipeline_id}. Libraries: "
            f"{[(l.get('notebook') or {}).get('path') for l in libraries]}"
        )
    print(f"Rewriting library[{match_idx}]: {libraries[match_idx]['notebook']['path']}  ->  {to_nb}")
    libraries[match_idx]["notebook"]["path"] = to_nb

else:
    # Legacy mode: replace the managed (historical/incremental) library set
    # wholesale. Non-managed libraries (e.g. dlt_ingest_bronze) are kept.
    target_basenames = HISTORICAL_BASENAMES if historical_flag else INCREMENTAL_BASENAMES

    managed = [
        (i, (lib.get("notebook") or {}).get("path"))
        for i, lib in enumerate(libraries)
        if _basename((lib.get("notebook") or {}).get("path") or "") in MANAGED_BASENAMES
    ]
    if not managed:
        raise RuntimeError(
            f"Pipeline {pipeline_id} has no managed library notebooks "
            f"({sorted(MANAGED_BASENAMES)}) to swap. Libraries: "
            f"{[(l.get('notebook') or {}).get('path') for l in libraries]}"
        )

    existing_managed_basenames = {_basename(p) for _, p in managed}
    if existing_managed_basenames == set(target_basenames):
        print(f"Pipeline managed libraries already match target set "
              f"{sorted(target_basenames)}. No update needed.")
        dbutils.notebook.exit("no_change")

    # Use the first managed library's directory as the prefix for new entries.
    prefix = _dirname(managed[0][1])
    print(f"Managed library prefix: {prefix}")
    print(f"Current managed: {sorted(existing_managed_basenames)}")
    print(f"Target managed:  {sorted(target_basenames)}")

    # Remove every managed library; keep non-managed.
    kept = [
        lib for lib in libraries
        if _basename((lib.get("notebook") or {}).get("path") or "") not in MANAGED_BASENAMES
    ]
    # Append the target set in order.
    for bn in target_basenames:
        kept.append({"notebook": {"path": f"{prefix}/{bn}"}})
    spec_dict["libraries"] = kept
    libraries = kept
    print(f"Final library list ({len(libraries)}): "
          f"{[(l.get('notebook') or {}).get('path') for l in libraries]}")

# COMMAND ----------

w.api_client.do("PUT", f"/api/2.0/pipelines/{pipeline_id}", body=spec_dict)
print(f"Updated pipeline {pipeline_id}.")

dbutils.notebook.exit("updated")
