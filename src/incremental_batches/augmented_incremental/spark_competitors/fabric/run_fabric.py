# Databricks notebook source
# Per-batch engine task — the CHILD job's `run_fabric` task (the swap-in for the
# dbt competitors' `dbt_run`). Runs ON Databricks, triggers the Fabric
# `batch_runner` notebook (notebooks/batch_runner.py) via the Job Scheduler REST
# API, and blocks until it completes. batch_runner runs the bronze -> silver/gold
# DAG for this batch_date via runMultiple in one Fabric Spark session.
#
# Ordered after `simulate_filedrops_fabric` in the child, which has already
# dropped this batch's files where Fabric reads them.
#
# enable_nee is the ONLY difference between the Fabric Spark and Fabric NEE
# variants: batch_runner sets spark.native.enabled from it for the session, so
# both variants run byte-identical notebooks.

dbutils.widgets.text("wh_db",                "",  "wh_db prefix; working schema = {wh_db}_{scale_factor}")
dbutils.widgets.dropdown("scale_factor",     "10", ["10","100","1000","5000","10000","20000"])
dbutils.widgets.text("batch_date",           "",  "This batch's date (from the parent for_each loop)")
dbutils.widgets.text("fabric_workspace_id",  "",  "Fabric workspace id")
dbutils.widgets.text("tenant_id",            "",  "Entra tenant id")
dbutils.widgets.text("client_id",            "",  "Service principal application (client) id")
dbutils.widgets.text("client_secret_secret", "",  "UC secret path of the SP client secret (catalog.schema.key)")
dbutils.widgets.text("batch_timeout_secs",   "3600", "Max wait for the Fabric batch run")
dbutils.widgets.dropdown("enable_nee",       "true", ["true", "false"],
                         "'true' = Native Execution Engine, 'false' = plain Spark (same code)")

wh_db         = dbutils.widgets.get("wh_db")
scale_factor  = dbutils.widgets.get("scale_factor")
batch_date    = dbutils.widgets.get("batch_date")
workspace_id  = dbutils.widgets.get("fabric_workspace_id")
tenant_id     = dbutils.widgets.get("tenant_id")
client_id     = dbutils.widgets.get("client_id")
client_secret_secret = dbutils.widgets.get("client_secret_secret")
timeout_secs  = int(dbutils.widgets.get("batch_timeout_secs"))
enable_nee    = dbutils.widgets.get("enable_nee")

_required = dict(wh_db=wh_db, batch_date=batch_date, fabric_workspace_id=workspace_id,
                 tenant_id=tenant_id, client_id=client_id,
                 client_secret_secret=client_secret_secret)
_missing = [k for k, v in _required.items() if not v]
if _missing:
    raise ValueError(f"missing required params: {_missing}")

# COMMAND ----------

import os, sys
# _fabric_conn.py sits next to this notebook in the repo.
_nb = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
_here = os.path.dirname(_nb if _nb.startswith("/Workspace") else "/Workspace" + _nb)
if _here not in sys.path:
    sys.path.insert(0, _here)
import _fabric_conn as fab

token = fab.get_token(dbutils=dbutils, tenant_id=tenant_id, client_id=client_id,
                      client_secret_secret=client_secret_secret)
nb = fab.find_item(token, workspace_id, "batch_runner", item_type="Notebook")
if not nb:
    raise RuntimeError(f"Fabric notebook 'batch_runner' not found in workspace {workspace_id} "
                       f"(the parent's setup_fabric task deploys it)")

# COMMAND ----------

print(f"[fabric] batch {batch_date}: triggering batch_runner "
      f"(wh_db={wh_db}, sf={scale_factor}, enable_nee={enable_nee})...")
inst = fab.run_and_wait(
    token, workspace_id, nb["id"],
    parameters={"wh_db": wh_db, "scale_factor": scale_factor, "batch_date": batch_date,
                "enable_nee": enable_nee},
    timeout_secs=timeout_secs,
)
# wait_for_job already raised on a non-Completed status, so reaching here
# means the batch succeeded.
print(f"[fabric] batch {batch_date} complete: status={inst.get('status')}")
