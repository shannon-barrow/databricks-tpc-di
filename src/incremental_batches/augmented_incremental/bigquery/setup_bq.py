# Databricks notebook source
# Per-run BigQuery setup. Dispatches SQL to BQ from a Databricks task (no
# BQ compute beyond zero-copy CLONEs + tiny DDL).
#
# Sequence:
#   1. CREATE OR REPLACE BQ dataset {bq_project}.{wh_db}_sf{sf}
#   2. CLONE 22 historical/reference tables from
#      {bq_project}.tpcdi_staging_sf{sf} (BigQuery CLONE TABLE — zero-copy,
#      metadata-only)
#   3. Pre-create the 7 empty bronze + account_updates target tables
#      (dbt-managed targets dbt populates per batch)
#   4. Emit batch_date_ls task value for the parent's for_each loop
#
# Self-bootstrapping: if {bq_project}.tpcdi_staging_sf{sf} doesn't exist
# yet for this scale factor, this notebook calls seed_staging_py to populate
# it (Delta → parquet → bq load). No separate one-time notebook required.
#
# Auth: reads SA key JSON from `{secret_scope}.sa_json` (see ./_bq_conn.py).

# COMMAND ----------

# MAGIC %pip install --quiet google-cloud-bigquery

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("catalog",        "databricks-sandbox-perfeng", "BigQuery project (treated as `catalog`)")
dbutils.widgets.text("wh_db",          "", "wh_db prefix; final dataset = {wh_db}_sf{scale_factor}")
dbutils.widgets.dropdown("scale_factor","10", ["10","100","1000","5000","10000","20000"])
dbutils.widgets.text("tpcdi_directory","/Volumes/main/tpcdi_raw_data/tpcdi_volume/",
                     "UC external volume root the per-batch files land under")
dbutils.widgets.text("secret_scope",   "tpcdi_bigquery", "Databricks secret scope holding sa_json")
dbutils.widgets.text("bq_location",    "us-central1", "BQ dataset location (must match GCS bucket region)")
dbutils.widgets.text("databricks_catalog", "main",
                     "Databricks catalog where tpcdi_incremental_staging_{sf} lives — used by bootstrap seed only")
dbutils.widgets.text("gcs_volume_prefix", "gs://shannon-tpcdi/tpcdi/",
                     "GCS URI that maps 1:1 to /Volumes/{catalog}/tpcdi_raw_data/tpcdi_volume/")
dbutils.widgets.text("incremental_batches_to_run", "365", "Number of batches the for_each loop runs")

bq_project       = dbutils.widgets.get("catalog")
wh_db            = dbutils.widgets.get("wh_db")
scale_factor     = dbutils.widgets.get("scale_factor")
secret_scope     = dbutils.widgets.get("secret_scope")
bq_location      = dbutils.widgets.get("bq_location")
databricks_catalog = dbutils.widgets.get("databricks_catalog")
gcs_volume_prefix  = dbutils.widgets.get("gcs_volume_prefix")
incremental_n    = int(dbutils.widgets.get("incremental_batches_to_run"))

if not wh_db:
    raise ValueError("wh_db is required")

target_dataset  = f"{wh_db}_sf{scale_factor}"
staging_dataset = f"tpcdi_staging_sf{scale_factor}"
print(f"target  = {bq_project}.{target_dataset}")
print(f"staging = {bq_project}.{staging_dataset} (clone source — self-bootstrapped if missing)")

# COMMAND ----------

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
        "task":         "setup_bq",
    },
)
print(f"[ok] connected to BigQuery project={bq_project} location={bq_location}")

# COMMAND ----------

# Self-bootstrap: ensure tpcdi_staging_sf{sf} exists with all expected
# tables. No-op if already complete. The check is conservative — if any of
# the 22 expected tables is missing, re-run the seed via dbutils.notebook.run.
STAGING_TABLES = [
    "taxrate", "dimdate", "industry", "tradetype", "dimbroker",
    "dimsecurity", "statustype", "dimcompany", "dimtime", "financial",
    "companyyeareps", "currentaccountbalances", "dimaccount",
    "dimcustomer", "dimtrade", "factwatches", "factcashbalances",
    "factholdings", "factmarkethistory", "bronzedailymarket",
    "cashtransactionhistorical", "batchdate",
]

def _staging_complete() -> bool:
    try:
        rows = client.query(
            f"SELECT table_name FROM `{bq_project}.{staging_dataset}.INFORMATION_SCHEMA.TABLES`"
        ).result()
        present = {r["table_name"] for r in rows}
    except Exception as e:
        print(f"[bootstrap] staging dataset missing or unqueryable ({type(e).__name__}: {e})")
        return False
    missing = set(STAGING_TABLES) - present
    if missing:
        print(f"[bootstrap] staging missing {len(missing)} tables: {sorted(missing)}")
        return False
    return True

if not _staging_complete():
    print(f"[bootstrap] seeding {bq_project}.{staging_dataset} via seed_staging_py...")
    dbutils.notebook.run(
        "./seed_staging_py",
        timeout_seconds=0,
        arguments={
            "catalog":           databricks_catalog,
            "scale_factor":      scale_factor,
            "bq_project":        bq_project,
            "bq_location":       bq_location,
            "secret_scope":      secret_scope,
            "gcs_volume_prefix": gcs_volume_prefix,
        },
    )
    if not _staging_complete():
        raise RuntimeError("seed_staging_py finished but staging still incomplete")
print(f"[ok] staging dataset {bq_project}.{staging_dataset} is intact")

# COMMAND ----------

# 1. CREATE OR REPLACE the per-run BQ dataset.
ds = bigquery.Dataset(f"{bq_project}.{target_dataset}")
ds.location = bq_location
# delete_contents=True so CREATE OR REPLACE semantics — a re-run of the
# same wh_db starts from a clean slate. Mirrors the Snowflake
# CREATE OR REPLACE SCHEMA behavior.
try:
    client.delete_dataset(f"{bq_project}.{target_dataset}",
                          delete_contents=True, not_found_ok=True)
except Exception as e:
    print(f"[warn] delete_dataset failed ({type(e).__name__}: {e}) — continuing")
client.create_dataset(ds, exists_ok=False)
print(f"[ok] dataset {bq_project}.{target_dataset} ready")

# COMMAND ----------

# 2. CLONE 22 historical/reference tables from the staging dataset.
# BigQuery CLONE TABLE is zero-copy + time-travel-based; identical
# semantics to Snowflake CLONE. Cheap, no compute consumed.
#
# Parallel — BQ jobs are independent and the API supports concurrent
# submissions. Mirrors the snowflake parallel CLONE pattern.
import concurrent.futures as _cf
import time as _time

def _clone_one(table_name: str) -> tuple[str, float]:
    t0 = _time.time()
    src = f"`{bq_project}.{staging_dataset}.{table_name}`"
    dst = f"`{bq_project}.{target_dataset}.{table_name}`"
    job = client.query(f"CREATE OR REPLACE TABLE {dst} CLONE {src}")
    job.result()
    return (table_name, _time.time() - t0)

t_clone = _time.time()
print(f"[parallel] cloning {len(STAGING_TABLES)} tables (8 concurrent)...")
with _cf.ThreadPoolExecutor(max_workers=8) as ex:
    futures = {ex.submit(_clone_one, t): t for t in STAGING_TABLES}
    for f in _cf.as_completed(futures):
        try:
            name, wall = f.result()
            print(f"[clone] {name:30s} {wall:5.2f}s")
        except Exception as e:
            name = futures[f]
            print(f"[FAIL] {name:30s}  {type(e).__name__}: {e}")
            raise
print(f"[parallel] clones done in {_time.time() - t_clone:.1f}s")

# COMMAND ----------

# 3. Pre-create the 7 dbt-managed target tables (empty; dbt fills them
# per batch via the bronze models that SELECT from BQ external tables).
# Schemas mirror the snowflake side (NUMBER → INT64, FLOAT → FLOAT64,
# STRING/TIMESTAMP/DATE unchanged).
BRONZE_DDLS = {
    "bronzeaccount": (
        "cdc_flag STRING, cdc_dsn INT64, accountid INT64, brokerid INT64, "
        "customerid INT64, accountdesc STRING, taxstatus INT64, status STRING, "
        "update_dt DATE"
    ),
    "bronzecashtransaction": (
        "cdc_flag STRING, cdc_dsn INT64, accountid INT64, ct_dts TIMESTAMP, "
        "ct_amt FLOAT64, ct_name STRING, event_dt DATE"
    ),
    "bronzecustomer": (
        "cdc_flag STRING, cdc_dsn INT64, customerid INT64, taxid STRING, "
        "status STRING, lastname STRING, firstname STRING, middleinitial STRING, "
        "gender STRING, tier INT64, dob DATE, addressline1 STRING, "
        "addressline2 STRING, postalcode STRING, city STRING, stateprov STRING, "
        "country STRING, c_ctry_1 STRING, c_area_1 STRING, c_local_1 STRING, "
        "c_ext_1 STRING, c_ctry_2 STRING, c_area_2 STRING, c_local_2 STRING, "
        "c_ext_2 STRING, c_ctry_3 STRING, c_area_3 STRING, c_local_3 STRING, "
        "c_ext_3 STRING, email1 STRING, email2 STRING, lcl_tx_id STRING, "
        "nat_tx_id STRING, update_dt DATE"
    ),
    "bronzeholdings": (
        "cdc_flag STRING, cdc_dsn INT64, hh_h_t_id INT64, hh_t_id INT64, "
        "hh_before_qty INT64, hh_after_qty INT64, event_dt DATE"
    ),
    "bronzetrade": (
        "cdc_flag STRING, cdc_dsn INT64, tradeid INT64, t_dts TIMESTAMP, "
        "status STRING, t_tt_id STRING, cashflag INT64, t_s_symb STRING, "
        "quantity INT64, bidprice FLOAT64, t_ca_id INT64, executedby STRING, "
        "tradeprice FLOAT64, fee FLOAT64, commission FLOAT64, tax FLOAT64, "
        "event_dt DATE"
    ),
    "bronzewatches": (
        "cdc_flag STRING, cdc_dsn INT64, w_c_id INT64, w_s_symb STRING, "
        "w_dts TIMESTAMP, w_action STRING, event_dt DATE"
    ),
    "account_updates_from_customer": (
        "cdc_flag STRING, cdc_dsn INT64, accountid INT64, brokerid INT64, "
        "customerid INT64, accountdesc STRING, taxstatus INT64, status STRING, "
        "update_dt DATE"
    ),
}
for name, schema_sql in BRONZE_DDLS.items():
    client.query(
        f"CREATE OR REPLACE TABLE `{bq_project}.{target_dataset}.{name}` "
        f"({schema_sql})"
    ).result()
    print(f"[ddl] {name}")
print(f"[ok] target tables ready under {bq_project}.{target_dataset}")

# COMMAND ----------

# 4. Create 7 BigQuery external tables (one per dataset) with wildcard URIs
# pointing at the per-(wh_db, sf) dailybatches root. Each batch,
# simulate_filedrops_bq clears the dailybatches dir and writes just the
# current day's 7 .txt files, so the wildcard `.../*/Dataset.txt` resolves
# to exactly one file at any time. BQ rescans URIs at query time — no
# per-batch refresh needed. This is the BQ equivalent of Snowflake's
# implicit `@stage/{wh_db}/{batch_date}/Dataset.txt` path pattern.
bronze_dataset = f"{target_dataset}_bronze"
client.create_dataset(
    bigquery.Dataset(f"{bq_project}.{bronze_dataset}"), exists_ok=True
)
# Column schemas mirror the bronze model declarations on the Databricks +
# Snowflake sides. Databricks→BQ type mapping: BIGINT/INT/TINYINT→INT64,
# DOUBLE→FLOAT64; STRING/DATE/TIMESTAMP unchanged. Pipe-delimited CSV with
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
# Wildcard URI per dataset. Resolves to whatever {batch_date}/Dataset.txt
# is currently present under the dailybatches dir (always exactly one,
# because simulate_filedrops_bq clears the prior day's dir before writing).
dailybatches_root_gcs = (
    f"{gcs_volume_prefix.rstrip('/')}/augmented_incremental/_dailybatches/"
    f"{target_dataset}/"
)
for name, cols in DATASET_SCHEMAS.items():
    src_uri = f"{dailybatches_root_gcs}*/{name}.txt"
    cols_ddl = ", ".join(f"{n} {t}" for n, t in cols)
    sql = f"""
    CREATE OR REPLACE EXTERNAL TABLE `{bq_project}.{bronze_dataset}.{name}` (
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
    print(f"[external] {bq_project}.{bronze_dataset}.{name} → {src_uri}")
print(f"[ok] 7 external tables created in {bq_project}.{bronze_dataset}")

# COMMAND ----------

# 5. Emit batch_date_ls — match setup_dbt.py / setup_sf.py exactly:
# AUG_FILES_DATE_START is hardcoded to 2016-07-06.
import datetime as dt
incr_start = dt.date(2016, 7, 6)
batches = [(incr_start + dt.timedelta(days=i)).isoformat() for i in range(incremental_n)]
dbutils.jobs.taskValues.set("batch_date_ls", batches)
print(f"emitted batch_date_ls: {len(batches)} dates, first={batches[0]}, last={batches[-1]}")

# COMMAND ----------

print("[done] BigQuery setup complete.")
