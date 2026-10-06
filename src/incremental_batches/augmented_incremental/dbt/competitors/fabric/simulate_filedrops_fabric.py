# Databricks notebook source
# Per-batch task: copy the day's pre-staged .txt files from the UC volume
# `_staging/sf=N/` tree into OneLake Files, where the Fabric DW bronze models
# read them via OPENROWSET(BULK). Fabric analogue of simulate_filedrops_rs.py,
# but the destination is OneLake (not the UC volume) so the warehouse can read
# it in place.
#
# Dest OneLake path (abfss; the OPENROWSET https URL in run_dbt points at the
# same bytes):
#   abfss://{ws}@onelake.dfs.fabric.microsoft.com/{lh}/Files/augmented_incremental/
#     _dailybatches/{wh_db}_{sf}/{batch_date}/{Dataset}.txt
#
# Writes OneLake via dbutils.fs.cp with the SP OneLake OAuth set on BOTH
# spark.conf and the JVM hadoop conf (dbutils.fs uses the JVM conf) — the same
# approach the fabric_ss simulate uses.

import os, concurrent.futures

# COMMAND ----------

dbutils.widgets.dropdown("scale_factor", "10", ["10","100","1000","5000","10000","20000"])
dbutils.widgets.text("tpcdi_directory", "/Volumes/main/tpcdi_raw_data/tpcdi_volume/")
dbutils.widgets.text("batch_date",      "")
dbutils.widgets.text("wh_db",           "")
dbutils.widgets.text("file_ext",        "txt")
dbutils.widgets.text("fabric_workspace_id", "e0a370b6-0279-4bc2-92ef-0a3f001e068b")
dbutils.widgets.text("fabric_lakehouse_id", "54963c1f-bb06-4acd-8db3-b8b5056e81c3")
dbutils.widgets.text("tenant_id",       "9f37a392-f0ae-4280-9796-f1864a10effc")

scale_factor    = dbutils.widgets.get("scale_factor")
tpcdi_directory = dbutils.widgets.get("tpcdi_directory")
batch_date      = dbutils.widgets.get("batch_date")
wh_db           = dbutils.widgets.get("wh_db")
file_ext        = dbutils.widgets.get("file_ext").strip()
ws_id           = dbutils.widgets.get("fabric_workspace_id")
lh_id           = dbutils.widgets.get("fabric_lakehouse_id")
tenant_id       = dbutils.widgets.get("tenant_id")

# Spark CSV files end in .csv on disk regardless of the bronze file_ext we want.
read_file_ext = "csv" if file_ext == "txt" else file_ext
run_schema  = f"{wh_db}_{scale_factor}".lower()
staging_dir = f"{tpcdi_directory}augmented_incremental/_staging/sf={scale_factor}"
onelake_base = (f"abfss://{ws_id}@onelake.dfs.fabric.microsoft.com/{lh_id}"
                f"/Files/augmented_incremental/_dailybatches/{run_schema}")
batch_dir = f"{onelake_base}/{batch_date}"

DATASETS = ["Customer", "Account", "Trade", "CashTransaction",
            "HoldingHistory", "DailyMarket", "WatchHistory"]

# COMMAND ----------

# OneLake OAuth (SP) on spark.conf AND the JVM hadoop conf — dbutils.fs.cp uses
# the JVM conf, so both must be set for the write to authenticate.
_oauth = {
    "fs.azure.account.auth.type.onelake.dfs.fabric.microsoft.com": "OAuth",
    "fs.azure.account.oauth.provider.type.onelake.dfs.fabric.microsoft.com":
        "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    "fs.azure.account.oauth2.client.id.onelake.dfs.fabric.microsoft.com":
        dbutils.secrets.get("tpcdi_fabric", "client_id"),
    "fs.azure.account.oauth2.client.secret.onelake.dfs.fabric.microsoft.com":
        dbutils.secrets.get("tpcdi_fabric", "client_secret"),
    "fs.azure.account.oauth2.client.endpoint.onelake.dfs.fabric.microsoft.com":
        f"https://login.microsoftonline.com/{tenant_id}/oauth2/token",
}
_hconf = spark._jsc.hadoopConfiguration()
for k, v in _oauth.items():
    spark.conf.set(k, v)
    _hconf.set(k, v)

# COMMAND ----------

# Clear the prior day's OneLake dir so OPENROWSET can't pick up stale files,
# then recreate for this batch.
try:
    dbutils.fs.rm(batch_dir, recurse=True)
except Exception:
    pass
dbutils.fs.mkdirs(batch_dir)

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
        raise RuntimeError(f"{dataset} {batch_date}: expected 1 .{read_file_ext} "
                           f"file, got {len(parts)}")
    return [(parts[0].path, f"{batch_dir}/{dataset}.{file_ext}")]

cp_pairs = []
for ds in DATASETS:
    cp_pairs.extend(collect_one(ds))
print(f"Copying {len(cp_pairs)} files for {batch_date} -> {batch_dir}")

def do_cp(pair):
    src, tgt = pair
    dbutils.fs.cp(src, tgt)
    return f"{src} -> {tgt}"

with concurrent.futures.ThreadPoolExecutor(max_workers=min(8, max(1, len(cp_pairs)))) as ex:
    for fut in concurrent.futures.as_completed([ex.submit(do_cp, p) for p in cp_pairs]):
        print(fut.result())
