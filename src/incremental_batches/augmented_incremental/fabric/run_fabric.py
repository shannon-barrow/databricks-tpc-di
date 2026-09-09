# Databricks notebook source
# Per-batch engine task — the CHILD job's `run_fabric` task (the swap-in for the
# other competitors' `dbt_run`). Runs ON Databricks, triggers the Fabric
# `batch_runner` driver notebook (notebooks/batch_runner.py) via the Job Scheduler REST API, and
# blocks until it completes. The driver runs the 17-node structured-streaming
# DAG via runMultiple in one Fabric session.
#
# Ordered after `simulate_filedrops` in the child (which has already dropped this
# batch's file where Fabric reads it). Because the streams are checkpointed +
# Trigger.AvailableNow, this trigger processes only the newly dropped batch.

dbutils.widgets.text("wh_db",                 "",  "wh_db prefix; working schema = {wh_db}_{scale_factor}")
dbutils.widgets.dropdown("scale_factor",      "10", ["10","100","1000","5000","10000","20000"])
dbutils.widgets.text("batch_date",            "",  "This batch's date (from the parent for_each loop)")
dbutils.widgets.text("fabric_workspace_id",   "4f119fd7-d1f7-48bc-be77-6c41c782b541", "pmt_fabric_ws")
dbutils.widgets.text("fabric_runner_notebook","batch_runner", "Fabric driver notebook display name (notebooks/batch_runner.py)")
dbutils.widgets.text("secret_scope",          "tpcdi_fabric", "Databricks secret scope holding the Fabric SPN")
dbutils.widgets.text("batch_timeout_secs",    "3600", "Max wait for the Fabric batch run")

wh_db         = dbutils.widgets.get("wh_db")
scale_factor  = dbutils.widgets.get("scale_factor")
batch_date    = dbutils.widgets.get("batch_date")
workspace_id  = dbutils.widgets.get("fabric_workspace_id")
runner_name   = dbutils.widgets.get("fabric_runner_notebook")
secret_scope  = dbutils.widgets.get("secret_scope")
timeout_secs  = int(dbutils.widgets.get("batch_timeout_secs"))
if not wh_db:
    raise ValueError("wh_db is required")

# COMMAND ----------

import sys, os
_here = os.path.dirname(os.path.abspath("__file__")) if "__file__" in dir() else os.getcwd()
for p in {_here, os.path.dirname(_here)}:
    if p not in sys.path:
        sys.path.insert(0, p)
import _fabric_conn as fab

token = fab.get_token(dbutils=dbutils, secret_scope=secret_scope)
nb = fab.find_item(token, workspace_id, runner_name, item_type="Notebook")
if not nb:
    raise RuntimeError(f"Fabric driver notebook {runner_name!r} not found in workspace {workspace_id}")

# COMMAND ----------

print(f"[fabric] batch {batch_date}: triggering {runner_name} (wh_db={wh_db}, sf={scale_factor})...")
inst = fab.run_and_wait(
    token, workspace_id, nb["id"],
    parameters={"wh_db": wh_db, "scale_factor": scale_factor, "batch_date": batch_date},
    timeout_secs=timeout_secs,
)
# exitValue from batch_runner is "batch_ok:{date}"; wait_for_job already
# raised on a non-Completed status, so reaching here means the batch succeeded.
print(f"[fabric] batch {batch_date} complete: status={inst.get('status')} exit={inst.get('rootActivityId','')}")
