# Databricks notebook source
# MAGIC %md
# MAGIC # Augmented Incremental — Fabric setup (Databricks-side orchestration)
# MAGIC
# MAGIC The PARENT job's `setup_fabric` task. Direct port of the Cluster Jobs
# MAGIC `setup.py`, split across the two platforms the way the Fabric variant
# MAGIC requires:
# MAGIC
# MAGIC | Cluster `setup.py` step | Fabric equivalent | Where it runs |
# MAGIC |---|---|---|
# MAGIC | clone 20 staging tables into the run schema | **materialize** (DEEP CLONE staging → OneLake, once per SF) + **per-run SHALLOW CLONE** within OneLake | materialize here (Databricks); SHALLOW clone in `notebooks/setup.py` (Fabric) |
# MAGIC | create 6 bronze tables, reset checkpoints | same | `notebooks/setup.py` (Fabric) |
# MAGIC | reset Autoloader batch dir | `simulate_file_drops` owns raw file dirs | (not here) |
# MAGIC | emit `batch_date_ls` | same | here (Databricks) |
# MAGIC
# MAGIC Why the split: Fabric Spark can't read the UC-managed staging tables in
# MAGIC place, so the **materialize** brings them into OneLake once per SF via a
# MAGIC distributed DEEP CLONE (no pandas/driver collection). Thereafter the
# MAGIC per-run reset is a zero-copy SHALLOW CLONE *within* OneLake, run on the
# MAGIC Fabric side. The materialize is idempotent — it self-bootstraps
# MAGIC `staging_sf{sf}` in OneLake and no-ops when it already exists (analogous
# MAGIC to the Cluster setup cloning from an already-built staging schema).

# COMMAND ----------

import concurrent.futures
import time

# COMMAND ----------

dbutils.widgets.text("catalog",                   "main",  "Databricks catalog holding tpcdi_incremental_staging_{sf} (tpc-di ws uses main)")
dbutils.widgets.dropdown("scale_factor",          "10", ["10","100","1000","5000","10000","20000"])
dbutils.widgets.text("wh_db",                     "",   "wh_db prefix; Fabric working schema = {wh_db}_{scale_factor}")
dbutils.widgets.text("fabric_workspace_id",       "4f119fd7-d1f7-48bc-be77-6c41c782b541", "pmt_fabric_ws GUID")
dbutils.widgets.text("fabric_lakehouse_id",       "3f5b1c43-a52c-4a2e-90d7-4de7504e6122", "tpcdi_fabric lakehouse GUID (OneLake materialize target)")
dbutils.widgets.text("fabric_setup_notebook",     "setup", "Fabric notebook display name for the per-run reset (notebooks/setup.py)")
dbutils.widgets.text("secret_scope",              "tpcdi_fabric", "Databricks secret scope holding the Fabric SPN (tenant_id/client_id/client_secret)")
dbutils.widgets.dropdown("force_materialize",     "false", ["true","false"], "Re-DEEP-CLONE staging → OneLake even if it already exists")
dbutils.widgets.text("incremental_batches_to_run","365", "Number of batches the for_each loop runs")

catalog        = dbutils.widgets.get("catalog")
scale_factor   = dbutils.widgets.get("scale_factor")
wh_db          = dbutils.widgets.get("wh_db")
workspace_id   = dbutils.widgets.get("fabric_workspace_id")
lakehouse_id   = dbutils.widgets.get("fabric_lakehouse_id")
setup_nb_name  = dbutils.widgets.get("fabric_setup_notebook")
secret_scope   = dbutils.widgets.get("secret_scope")
force_mat      = dbutils.widgets.get("force_materialize").strip().lower() == "true"
n_batches      = max(1, min(365, int(dbutils.widgets.get("incremental_batches_to_run").strip())))

if not wh_db:
    raise ValueError("wh_db is required")

sf             = scale_factor
staging_db     = f"{catalog}.tpcdi_incremental_staging_{sf}"
staging_schema = f"staging_sf{sf}"          # OneLake-side schema name (under the lakehouse)
onelake_acct   = "onelake.dfs.fabric.microsoft.com"
print(f"materialize src = {staging_db}")
print(f"materialize dst = abfss://{workspace_id}@{onelake_acct}/{lakehouse_id}/Tables/{staging_schema}/")

# COMMAND ----------

# Same 20-table membership as the Cluster setup.py: 12 reference (SHALLOW there)
# + 8 dim/fact (DEEP there). The materialize DEEP-CLONEs all 20 into OneLake so
# Fabric's per-run SHALLOW clones have a native source; the SHALLOW-vs-DEEP
# distinction is a Cluster read-path optimization that doesn't carry over
# (Fabric Delta only has SHALLOW CLONE, applied per run in notebooks/setup.py).
STAGING_TABLES = [
    "taxrate", "dimdate", "industry", "tradetype", "dimbroker", "financial",
    "companyyeareps", "dimsecurity", "statustype", "dimcompany", "dimtime",
    "currentaccountbalances",
    "dimcustomer", "dimaccount", "dimtrade", "factwatches", "factholdings",
    "factmarkethistory", "bronzedailymarket", "factcashbalances",
]

# COMMAND ----------

# MAGIC %md
# MAGIC # 1. Materialize staging → OneLake (idempotent, once per SF)
# MAGIC
# MAGIC Distributed Delta DEEP CLONE onto the OneLake ABFS path — no pandas, no
# MAGIC driver collection. Requires a standard/UC-enabled cluster (serverless
# MAGIC can't set `fs.azure.*` for the Spark→OneLake ABFS write); the interactive
# MAGIC cluster the job pins is UC-enabled + SINGLE_USER (needed to read `main`).

# COMMAND ----------

# OneLake OAuth via the Fabric SPN (secret scope). Bare-GUID ABFS path — the
# `.lakehouse` suffix triggers 400 FriendlyNameSupportDisabled in GUID mode.
tenant = dbutils.secrets.get(secret_scope, "tenant_id")
spark.conf.set(f"fs.azure.account.auth.type.{onelake_acct}", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{onelake_acct}",
               "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{onelake_acct}",
               dbutils.secrets.get(secret_scope, "client_id"))
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{onelake_acct}",
               dbutils.secrets.get(secret_scope, "client_secret"))
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{onelake_acct}",
               f"https://login.microsoftonline.com/{tenant}/oauth2/token")

def _onelake_path(t):
    return f"abfss://{workspace_id}@{onelake_acct}/{lakehouse_id}/Tables/{staging_schema}/{t}"

def _exists(t):
    try:
        spark.sql(f"DESCRIBE DETAIL delta.`{_onelake_path(t)}`")
        return True
    except Exception:
        return False

def materialize(t):
    # NOTE (gotcha): if a stale OneLake path exists with a different columnMapping
    # mode, CREATE OR REPLACE fails DELTA_UNSUPPORTED_COLUMN_MAPPING_MODE_CHANGE.
    # DEEP CLONE from staging (mode='name') is self-consistent across re-runs, so
    # force just re-clones; a mode mismatch would only surface from a foreign
    # writer and is fixed by deleting the path via the DFS API first.
    t0 = time.time()
    spark.sql(f"CREATE OR REPLACE TABLE delta.`{_onelake_path(t)}` DEEP CLONE {staging_db}.{t}")
    return f"{t:24s} {time.time()-t0:5.1f}s"

todo = STAGING_TABLES if force_mat else [t for t in STAGING_TABLES if not _exists(t)]
if not todo:
    print(f"[materialize] OneLake {staging_schema} already complete ({len(STAGING_TABLES)} tables) — skipping")
else:
    print(f"[materialize] DEEP CLONE {len(todo)} table(s) → OneLake {staging_schema} "
          f"(force={force_mat}): {', '.join(todo)}")
    t_all = time.time()
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(todo)) as ex:
        for fut in concurrent.futures.as_completed(ex.submit(materialize, t) for t in todo):
            print(f"[clone] {fut.result()}")
    print(f"[materialize] done in {time.time()-t_all:.1f}s")

# COMMAND ----------

# MAGIC %md
# MAGIC # 2. Trigger the Fabric-side per-run reset and block until it completes
# MAGIC
# MAGIC `notebooks/setup.py` runs IN Fabric: DROP+CREATE the working schema,
# MAGIC SHALLOW CLONE the 20 tables from OneLake `staging_sf{sf}`, create the 6
# MAGIC empty bronze tables, and reset the streaming checkpoints (Fabric-side).

# COMMAND ----------

# _fabric_conn.py sits alongside this notebook in the repo; add its dir to path.
import sys, os
_here = os.path.dirname(os.path.abspath("__file__")) if "__file__" in dir() else os.getcwd()
for p in {_here, os.path.dirname(_here)}:
    if p not in sys.path:
        sys.path.insert(0, p)
import _fabric_conn as fab

token = fab.get_token(dbutils=dbutils, secret_scope=secret_scope)
nb = fab.find_item(token, workspace_id, setup_nb_name, item_type="Notebook")
if not nb:
    raise RuntimeError(f"Fabric notebook {setup_nb_name!r} not found in workspace {workspace_id}")

print(f"[fabric] triggering {setup_nb_name} (wh_db={wh_db}, sf={sf})...")
inst = fab.run_and_wait(
    token, workspace_id, nb["id"],
    parameters={"wh_db": wh_db, "scale_factor": sf, "staging_schema": staging_schema},
    default_lakehouse_id=lakehouse_id,
    timeout_secs=3600,
)
print(f"[fabric] setup complete: status={inst.get('status')}")

# COMMAND ----------

# MAGIC %md
# MAGIC # 3. Emit batch_date_ls for the parent for_each loop

# COMMAND ----------

# One calendar date per batch starting 2016-07-06 (AUG_FILES_DATE_START) —
# identical to the Cluster setup.py.
from datetime import date, timedelta
d0 = date(2016, 7, 6)
batch_date_ls = [(d0 + timedelta(days=i)).isoformat() for i in range(n_batches)]
print(f"Emitting {len(batch_date_ls)} batch dates ({batch_date_ls[0]} → {batch_date_ls[-1]})")
dbutils.jobs.taskValues.set(key="batch_date_ls", value=batch_date_ls)
