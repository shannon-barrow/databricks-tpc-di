# Databricks notebook source
# Per-batch file drop — Fabric Spark/NEE variant. Runs ON Databricks (classic
# compute — the OneLake write needs fs.azure.* confs), the CHILD job's first
# task, ordered before run_fabric.
#
# Port of augmented_incremental/simulate_filedrops.py. Identical logic — find the
# single pre-staged part file per dataset for this batch_date and drop it,
# renamed to {Dataset}.{file_ext} — EXCEPT the destination is **OneLake Files**
# (where the Fabric ingest_bronze reads it) instead of the UC volume. Source part
# files still come from the UC volume staging tree (Databricks-generated); we
# copy across to OneLake.
#
# Layout mirrors the Databricks original: one file per dataset in a per-batch
# subdir `{batches_dir}/{batch_date}/{Dataset}.{file_ext}`; ingest_bronze reads
# exactly that batch_date's subdir.

import os
import concurrent.futures
import requests

# COMMAND ----------

dbutils.widgets.dropdown("scale_factor", "10", ["10", "100", "1000", "5000", "10000", "20000"])
dbutils.widgets.text("tpcdi_directory", "/Volumes/main/tpcdi_raw_data/tpcdi_volume/")
dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("batch_date", "")
dbutils.widgets.text("wh_db", "")
dbutils.widgets.text("file_ext", "txt")
dbutils.widgets.text("fabric_workspace_id", "", "Fabric workspace id")
dbutils.widgets.text("fabric_lakehouse_id", "", "Lakehouse id holding the daily file drops")
dbutils.widgets.text("tenant_id", "", "Entra tenant id")
dbutils.widgets.text("client_id", "", "Service principal application (client) id")
dbutils.widgets.text("client_secret_secret", "", "UC secret path of the SP client secret (catalog.schema.key)")

catalog         = dbutils.widgets.get("catalog")
scale_factor    = dbutils.widgets.get("scale_factor")
tpcdi_directory = dbutils.widgets.get("tpcdi_directory")
batch_date      = dbutils.widgets.get("batch_date")
wh_db           = dbutils.widgets.get("wh_db")
file_ext        = dbutils.widgets.get("file_ext").strip()
workspace_id    = dbutils.widgets.get("fabric_workspace_id")
lakehouse_id    = dbutils.widgets.get("fabric_lakehouse_id")
tenant_id       = dbutils.widgets.get("tenant_id")
client_id       = dbutils.widgets.get("client_id")
client_secret_secret = dbutils.widgets.get("client_secret_secret")
_required = dict(batch_date=batch_date, wh_db=wh_db, fabric_workspace_id=workspace_id,
                 fabric_lakehouse_id=lakehouse_id, tenant_id=tenant_id, client_id=client_id,
                 client_secret_secret=client_secret_secret)
_missing = [k for k, v in _required.items() if not v]
if _missing:
    raise ValueError(f"missing required params: {_missing}")

# stage_to_files writes Spark CSV part files (*.csv); the benchmark wants *.txt.
read_file_ext = "csv" if file_ext == "txt" else file_ext

tgt_db      = f"{wh_db}_{scale_factor}"
staging_dir = f"{tpcdi_directory}augmented_incremental/_staging/sf={scale_factor}"   # UC volume (source)

# OneLake Files destination (bare GUIDs, no `.lakehouse` — GUID mode).
acct         = "onelake.dfs.fabric.microsoft.com"
onelake_root = f"abfss://{workspace_id}@{acct}/{lakehouse_id}/Files"
batches_dir  = f"{onelake_root}/augmented_incremental/_dailybatches/{tgt_db}"

DATASETS = [
    "Customer", "Account", "Trade", "CashTransaction",
    "HoldingHistory", "DailyMarket", "WatchHistory",
]

# COMMAND ----------

# OneLake OAuth as the SP, on both the session conf and the JVM Hadoop conf
# (dbutils.fs uses the latter for the cross-filesystem copy into OneLake).
import sys
_nb = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
_here = os.path.dirname(_nb if _nb.startswith("/Workspace") else "/Workspace" + _nb)
if _here not in sys.path:
    sys.path.insert(0, _here)
import _fabric_conn as fab
fab.onelake_conf(spark, dbutils, tenant_id=tenant_id, client_id=client_id,
                 client_secret_secret=client_secret_secret)

# COMMAND ----------

# MAGIC %md
# MAGIC # Clear prior day's files, create this batch's dir
# MAGIC Fabric checkpoints track processed files by full path, so removing the
# MAGIC prior day's files is safe and keeps OneLake bounded over the 365-day run.

# COMMAND ----------

try:
    dbutils.fs.rm(batches_dir, recurse=True)
except Exception as e:
    print(f"[batches_dir] nothing to clear ({type(e).__name__})")
dbutils.fs.mkdirs(f"{batches_dir}/{batch_date}")

# COMMAND ----------

# Each stage_files notebook wrote {staging_dir}/{Dataset}/_pdate={date}/part-*.{ext}.
# repartition(_pdate) → single part file per date. Copy that one file into the
# OneLake watch dir as {Dataset}.{file_ext} (copy, not move — staging tree stays
# intact for re-runs). Sparse datasets may have no _pdate= dir for a date — skip.
def collect_one(dataset):
    src_dir = f"{staging_dir}/{dataset}/_pdate={batch_date}"
    try:
        entries = dbutils.fs.ls(src_dir)
    except Exception:
        return []
    parts = [e for e in entries if e.name.endswith(f".{read_file_ext}")]
    if not parts:
        return []
    if len(parts) > 1:
        raise RuntimeError(
            f"{dataset} {batch_date}: expected 1 .{read_file_ext} file after "
            f"repartition(_pdate), got {len(parts)}: {[e.name for e in parts]}")
    return [(parts[0].path, f"{batches_dir}/{batch_date}/{dataset}.{file_ext}")]

cp_pairs = []
for ds in DATASETS:
    cp_pairs.extend(collect_one(ds))

print(f"Copying {len(cp_pairs)} files for {batch_date} → OneLake {tgt_db}")

def do_cp(pair):
    src, target = pair
    dbutils.fs.cp(src, target)
    return f"{src} → {target}"

with concurrent.futures.ThreadPoolExecutor(
        max_workers=min(8, max(1, len(cp_pairs)))) as executor:
    futures = [executor.submit(do_cp, p) for p in cp_pairs]
    for future in concurrent.futures.as_completed(futures):
        try: print(future.result())
        except requests.ConnectTimeout: print("ConnectTimeout.")
