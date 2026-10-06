# Databricks notebook source
# Per-batch file drop — Fabric variant. Runs ON Databricks (interactive cluster
# 0905-212234-wq3cjc8j), the CHILD job's first task, ordered before run_fabric.
#
# Port of augmented_incremental/simulate_filedrops.py. Identical logic — find the
# single pre-staged part file per dataset for this batch_date and drop it into the
# streaming watch dir, renamed to {Dataset}.{file_ext} — EXCEPT the destination is
# **OneLake Files** (where the Fabric ingest_bronze readStream watches) instead of
# the UC volume Autoloader dir. Source part files still come from the UC external
# volume staging tree (Databricks-generated); we copy across to OneLake.
#
# Layout mirrors the Databricks original (agreed design): one file per dataset in a
# per-batch subdir `{batches_dir}/{batch_date}/{Dataset}.{file_ext}`. The Fabric
# native file source lists the watch dir per trigger and picks up the new subdir's
# file; pathGlobFilter matches the leaf filename. Per-batch subdir = unique path per
# day, so the checkpoint (full-path identity, fileNameOnly=false) reprocesses it.

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
dbutils.widgets.text("fabric_workspace_id", "4f119fd7-d1f7-48bc-be77-6c41c782b541", "pmt_fabric_ws GUID")
dbutils.widgets.text("fabric_lakehouse_id", "3f5b1c43-a52c-4a2e-90d7-4de7504e6122", "tpcdi_fabric lakehouse GUID")
dbutils.widgets.text("secret_scope", "tpcdi_fabric", "Databricks secret scope holding the Fabric SPN")

catalog         = dbutils.widgets.get("catalog")
scale_factor    = dbutils.widgets.get("scale_factor")
tpcdi_directory = dbutils.widgets.get("tpcdi_directory")
batch_date      = dbutils.widgets.get("batch_date")
wh_db           = dbutils.widgets.get("wh_db")
file_ext        = dbutils.widgets.get("file_ext").strip()
workspace_id    = dbutils.widgets.get("fabric_workspace_id")
lakehouse_id    = dbutils.widgets.get("fabric_lakehouse_id")
secret_scope    = dbutils.widgets.get("secret_scope")
if not batch_date:
    raise ValueError("batch_date is required")

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

# OneLake OAuth via the Fabric SPN. Set on BOTH the session conf (for any Spark
# access) and the JVM Hadoop conf so dbutils.fs (FileSystem) honors it for the
# cross-filesystem copy into abfss OneLake.
tenant = dbutils.secrets.get(secret_scope, "tenant_id")
_oauth = {
    f"fs.azure.account.auth.type.{acct}": "OAuth",
    f"fs.azure.account.oauth.provider.type.{acct}":
        "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    f"fs.azure.account.oauth2.client.id.{acct}": dbutils.secrets.get(secret_scope, "client_id"),
    f"fs.azure.account.oauth2.client.secret.{acct}": dbutils.secrets.get(secret_scope, "client_secret"),
    f"fs.azure.account.oauth2.client.endpoint.{acct}":
        f"https://login.microsoftonline.com/{tenant}/oauth2/token",
}
_hadoop = spark._jsc.hadoopConfiguration()
for k, v in _oauth.items():
    spark.conf.set(k, v)
    _hadoop.set(k, v)

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
