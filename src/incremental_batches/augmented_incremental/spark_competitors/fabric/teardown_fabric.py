# Databricks notebook source
# Cleanup task for the Fabric Spark / NEE parent job, gated by
# delete_tables_when_finished. Removes this run's lakehouse schema
# ({wh_db}_{sf}) and its OneLake file-drop zone. The once-per-SF OneLake
# staging (staging_sf{sf}) and the Fabric items (pool, environment, notebooks)
# are kept so the next run reuses them.
#
# Runs on classic compute: the OneLake deletes go through fs.azure.* confs.

dbutils.widgets.text("wh_db",                "",  "wh_db prefix; run schema = {wh_db}_{scale_factor}")
dbutils.widgets.dropdown("scale_factor",     "10", ["10","100","1000","5000","10000","20000"])
dbutils.widgets.text("fabric_workspace_id",  "",  "Fabric workspace id")
dbutils.widgets.text("fabric_lakehouse_id",  "",  "Lakehouse id")
dbutils.widgets.text("tenant_id",            "",  "Entra tenant id")
dbutils.widgets.text("client_id",            "",  "Service principal application (client) id")
dbutils.widgets.text("client_secret_secret", "",  "UC secret path of the SP client secret (catalog.schema.key)")

wh_db         = dbutils.widgets.get("wh_db")
scale_factor  = dbutils.widgets.get("scale_factor")
workspace_id  = dbutils.widgets.get("fabric_workspace_id")
lakehouse_id  = dbutils.widgets.get("fabric_lakehouse_id")
tenant_id     = dbutils.widgets.get("tenant_id")
client_id     = dbutils.widgets.get("client_id")
client_secret_secret = dbutils.widgets.get("client_secret_secret")

_required = dict(wh_db=wh_db, fabric_workspace_id=workspace_id, fabric_lakehouse_id=lakehouse_id,
                 tenant_id=tenant_id, client_id=client_id,
                 client_secret_secret=client_secret_secret)
_missing = [k for k, v in _required.items() if not v]
if _missing:
    raise ValueError(f"missing required params: {_missing}")

run_schema = f"{wh_db}_{scale_factor}"

# COMMAND ----------

import os, sys
_nb = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
_here = os.path.dirname(_nb if _nb.startswith("/Workspace") else "/Workspace" + _nb)
if _here not in sys.path:
    sys.path.insert(0, _here)
import _fabric_conn as fab

fab.onelake_conf(spark, dbutils, tenant_id=tenant_id, client_id=client_id,
                 client_secret_secret=client_secret_secret)

root = f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{lakehouse_id}"
# Tables/<schema>/ holds the run schema's Delta tables; deleting the folder
# drops them from the lakehouse. Files/... holds this run's daily CSV drops.
for path in (f"{root}/Tables/{run_schema}",
             f"{root}/Files/augmented_incremental/_dailybatches/{run_schema}"):
    try:
        dbutils.fs.rm(path, recurse=True)
        print(f"[ok] removed {path}")
    except Exception as e:
        print(f"[warn] remove failed (may not exist): {path}: {type(e).__name__}: {e}")

print(f"Teardown complete. OneLake staging_sf{scale_factor} and the Fabric items are preserved.")
