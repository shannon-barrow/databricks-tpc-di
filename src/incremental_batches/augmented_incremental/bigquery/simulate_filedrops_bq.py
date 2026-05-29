# Databricks notebook source
# Per-batch task: copies the day's pre-staged .txt files from `_staging/sf=N/`
# to the per-(wh_db, sf, batch_date) directory under the UC external volume
# (which maps 1:1 to GCS). Then CREATE OR REPLACE 7 BigQuery external tables
# in `{bq_project}.{wh_db}_sf{sf}_bronze` pointing at the day's directory so
# the dbt bronze models can SELECT from them.
#
# Adapter of the Snowflake-side `simulate_filedrops_sf.py`. Two differences:
#   - GCS path resolution from the UC external volume mount.
#   - Phase D inline: BQ has no implicit STAGE / file-glob magic, so we
#     materialize the day's external-table URIs explicitly each batch.

import os
import concurrent.futures
import requests

# COMMAND ----------

# MAGIC %pip install --quiet google-cloud-bigquery

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import os
import concurrent.futures
import requests

# COMMAND ----------

dbutils.widgets.dropdown("scale_factor","10", ["10","100","1000","5000","10000","20000"])
dbutils.widgets.text("tpcdi_directory", "/Volumes/main/tpcdi_raw_data/tpcdi_volume/")
dbutils.widgets.text("catalog",         "databricks-sandbox-perfeng",
                     "Treated as BQ project — same meaning as `catalog` everywhere else")
dbutils.widgets.text("batch_date",      "")
dbutils.widgets.text("wh_db",           "")
dbutils.widgets.text("secret_scope",    "tpcdi_bigquery")
dbutils.widgets.text("file_ext",        "txt")
dbutils.widgets.text("bq_location",     "us-central1")
dbutils.widgets.text("gcs_volume_prefix", "gs://shannon-tpcdi/tpcdi/",
                     "GCS URI that maps 1:1 to /Volumes/{catalog}/tpcdi_raw_data/tpcdi_volume/")

bq_project        = dbutils.widgets.get("catalog")
scale_factor      = dbutils.widgets.get("scale_factor")
tpcdi_directory   = dbutils.widgets.get("tpcdi_directory")
batch_date        = dbutils.widgets.get("batch_date")
wh_db             = dbutils.widgets.get("wh_db")
secret_scope      = dbutils.widgets.get("secret_scope")
file_ext          = dbutils.widgets.get("file_ext").strip()
bq_location       = dbutils.widgets.get("bq_location")
gcs_volume_prefix = dbutils.widgets.get("gcs_volume_prefix").rstrip("/") + "/"

read_file_ext   = "csv" if file_ext == "txt" else file_ext
batches_dir     = f"{tpcdi_directory}augmented_incremental/_dailybatches/{wh_db}_sf{scale_factor}"
staging_dir     = f"{tpcdi_directory}augmented_incremental/_staging/sf={scale_factor}"
bronze_dataset  = f"{wh_db}_sf{scale_factor}_bronze"
# GCS URI for the day's directory (BQ external tables read from gs://, not UC).
day_dir_gcs     = (f"{batches_dir}/{batch_date}/").replace(
    tpcdi_directory.rstrip("/") + "/", gcs_volume_prefix)

DATASETS = [
    "Customer", "Account", "Trade", "CashTransaction",
    "HoldingHistory", "DailyMarket", "WatchHistory",
]

# COMMAND ----------

# Phase C: clear prior day's files + copy this day's parts.
if os.path.exists(batches_dir):
    dbutils.fs.rm(batches_dir, recurse=True)
dbutils.fs.mkdirs(f"{batches_dir}/{batch_date}")

# COMMAND ----------

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

print(f"Copying {len(cp_pairs)} files for {batch_date}")

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

# COMMAND ----------

# Phase D: CREATE OR REPLACE 7 BQ external tables pointing at the day's
# directory. Each dbt bronze model then `SELECT * FROM` these.

# MAGIC %run ./_bq_conn

# COMMAND ----------

from google.cloud import bigquery

client = bq_connect(
    project=bq_project,
    location=bq_location,
    secret_scope=secret_scope,
    query_label={
        "wh_db":        wh_db,
        "scale_factor": scale_factor,
        "batch_date":   batch_date,
        "task":         "simulate_filedrops_bq",
    },
)

# Ensure the bronze dataset exists (idempotent — first batch creates it).
ds = bigquery.Dataset(f"{bq_project}.{bronze_dataset}")
ds.location = bq_location
client.create_dataset(ds, exists_ok=True)

# Per-dataset external-table schema. Columns + types mirror the bronze
# dbt model declarations on the Databricks side. Pipe-delimited CSV with
# no header (matches spark datagen output).
DATASET_SCHEMAS = {
    "Customer": [
        ("cdc_flag", "STRING"), ("cdc_dsn", "INT64"), ("customerid", "INT64"),
        ("taxid", "STRING"), ("status", "STRING"), ("lastname", "STRING"),
        ("firstname", "STRING"), ("middleinitial", "STRING"), ("gender", "STRING"),
        ("tier", "INT64"), ("dob", "DATE"),
        ("addressline1", "STRING"), ("addressline2", "STRING"),
        ("postalcode", "STRING"), ("city", "STRING"), ("stateprov", "STRING"),
        ("country", "STRING"),
        ("c_ctry_1", "STRING"), ("c_area_1", "STRING"), ("c_local_1", "STRING"), ("c_ext_1", "STRING"),
        ("c_ctry_2", "STRING"), ("c_area_2", "STRING"), ("c_local_2", "STRING"), ("c_ext_2", "STRING"),
        ("c_ctry_3", "STRING"), ("c_area_3", "STRING"), ("c_local_3", "STRING"), ("c_ext_3", "STRING"),
        ("email1", "STRING"), ("email2", "STRING"),
        ("lcl_tx_id", "STRING"), ("nat_tx_id", "STRING"),
        ("update_dt", "DATE"),
    ],
    "Account": [
        ("cdc_flag", "STRING"), ("cdc_dsn", "INT64"), ("accountid", "INT64"),
        ("brokerid", "INT64"), ("customerid", "INT64"),
        ("accountdesc", "STRING"), ("taxstatus", "INT64"), ("status", "STRING"),
        ("update_dt", "DATE"),
    ],
    "Trade": [
        ("cdc_flag", "STRING"), ("cdc_dsn", "INT64"), ("tradeid", "INT64"),
        ("t_dts", "TIMESTAMP"), ("status", "STRING"), ("t_tt_id", "STRING"),
        ("cashflag", "INT64"), ("t_s_symb", "STRING"), ("quantity", "INT64"),
        ("bidprice", "FLOAT64"), ("t_ca_id", "INT64"), ("executedby", "STRING"),
        ("tradeprice", "FLOAT64"), ("fee", "FLOAT64"), ("commission", "FLOAT64"),
        ("tax", "FLOAT64"), ("event_dt", "DATE"),
    ],
    "CashTransaction": [
        ("cdc_flag", "STRING"), ("cdc_dsn", "INT64"), ("accountid", "INT64"),
        ("ct_dts", "TIMESTAMP"), ("ct_amt", "FLOAT64"), ("ct_name", "STRING"),
        ("event_dt", "DATE"),
    ],
    "HoldingHistory": [
        ("cdc_flag", "STRING"), ("cdc_dsn", "INT64"), ("hh_h_t_id", "INT64"),
        ("hh_t_id", "INT64"), ("hh_before_qty", "INT64"), ("hh_after_qty", "INT64"),
        ("event_dt", "DATE"),
    ],
    "DailyMarket": [
        ("cdc_flag", "STRING"), ("cdc_dsn", "INT64"), ("dm_date", "DATE"),
        ("dm_s_symb", "STRING"), ("dm_close", "FLOAT64"), ("dm_high", "FLOAT64"),
        ("dm_low", "FLOAT64"), ("dm_vol", "INT64"),
    ],
    "WatchHistory": [
        ("cdc_flag", "STRING"), ("cdc_dsn", "INT64"), ("w_c_id", "INT64"),
        ("w_s_symb", "STRING"), ("w_dts", "TIMESTAMP"), ("w_action", "STRING"),
        ("event_dt", "DATE"),
    ],
}

def _refresh_external(dataset_name: str, cols: list[tuple[str, str]]):
    src_uri = f"{day_dir_gcs}{dataset_name}.{file_ext}"
    bq_table = f"`{bq_project}.{bronze_dataset}.{dataset_name}`"
    cols_ddl = ", ".join(f"{n} {t}" for n, t in cols)
    sql = f"""
    CREATE OR REPLACE EXTERNAL TABLE {bq_table} (
      {cols_ddl}
    )
    OPTIONS (
      format = 'CSV',
      uris = ['{src_uri}'],
      field_delimiter = '|',
      skip_leading_rows = 0,
      ignore_unknown_values = false
    )
    """
    client.query(sql).result()
    return dataset_name

for ds_name, schema in DATASET_SCHEMAS.items():
    _refresh_external(ds_name, schema)
    print(f"[bq external] {bq_project}.{bronze_dataset}.{ds_name}")
print(f"[done] {batch_date}: {len(cp_pairs)} files copied, {len(DATASET_SCHEMAS)} BQ external tables refreshed.")
