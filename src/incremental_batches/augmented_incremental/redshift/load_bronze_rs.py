# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# dependencies = [
#   "psycopg2-binary",
# ]
# ///
# Per-batch task: COPY the day's files from S3 (via UC external volume) into
# Redshift bronze tables. Runs after simulate_filedrops_rs lands the files,
# and before run_dbt fires the silver+gold transformations.
#
# Why this is its own task (vs. external/Spectrum tables): Spectrum-style
# external table queries on Redshift Serverless pay full RPU per scan and
# kill DISTKEY-based join co-location. COPY into a native table is the
# Redshift idiomatic equivalent of BigQuery's "LOAD DATA FROM FILES" — pay
# once per batch, scan many times cheaply.
#
# Each per-batch COPY does a TRUNCATE + INSERT under the hood. The bronze
# tables are CREATE'd empty by setup_rs, so first batch is plain INSERT.

import os
import datetime
import time

# COMMAND ----------

dbutils.widgets.text("database",        "dev",  "Redshift database")
dbutils.widgets.text("wh_db",           "",     "wh_db prefix")
dbutils.widgets.dropdown("scale_factor","10",   ["10","100","1000","5000","10000","20000"])
dbutils.widgets.text("batch_date",      "",     "ISO date for this batch")
dbutils.widgets.text("secret_scope",    "tpcdi_redshift")
dbutils.widgets.text("s3_volume_prefix","s3://tpcds-datasets/shannon_tpcdi/",
                     "S3 prefix matching the Databricks UC external volume root")
dbutils.widgets.text("file_ext",        "txt",  "Bronze file extension (matches simulate_filedrops_rs)")
dbutils.widgets.text("aws_region",      "us-west-2", "Region for COPY")

database      = dbutils.widgets.get("database")
wh_db         = dbutils.widgets.get("wh_db")
scale_factor  = dbutils.widgets.get("scale_factor")
batch_date    = dbutils.widgets.get("batch_date")
secret_scope  = dbutils.widgets.get("secret_scope")
s3_prefix     = dbutils.widgets.get("s3_volume_prefix").rstrip("/")
file_ext      = dbutils.widgets.get("file_ext").strip()
aws_region    = dbutils.widgets.get("aws_region")

if not (wh_db and batch_date):
    raise ValueError("wh_db and batch_date are required")

target_schema = f"{wh_db}_{scale_factor}".lower()
bronze_schema = f"{target_schema}_bronze"
target_id     = target_schema
s3_batch_dir  = f"{s3_prefix}/augmented_incremental/_dailybatches/{target_id}/{batch_date}"
print(f"COPY targets  = {database}.{bronze_schema}.<bronze*>")
print(f"COPY source   = {s3_batch_dir}/<Dataset>.{file_ext}")

# COMMAND ----------

# MAGIC %run ./_rs_conn

# COMMAND ----------

conn = rs_connect(
    database=database, secret_scope=secret_scope,
    query_group={"wh_db": wh_db, "scale_factor": scale_factor,
                 "batch_date": batch_date, "task": "load_bronze_rs"},
)
iam_role = rs_iam_role(secret_scope=secret_scope)
print(f"[ok] connected to {database} (IAM role for COPY: {iam_role})")

# COMMAND ----------

# Dataset → (bronze table, column list).
# The column list is in the same order as Spark's CSV writer emits, which
# matches the bronze table DDL in setup_rs.py.
COPIES = [
    ("Customer",        "bronzecustomer",
     "(cdc_flag, cdc_dsn, customerid, taxid, status, lastname, firstname, "
     "middleinitial, gender, tier, dob, addressline1, addressline2, "
     "postalcode, city, stateprov, country, "
     "c_ctry_1, c_area_1, c_local_1, c_ext_1, "
     "c_ctry_2, c_area_2, c_local_2, c_ext_2, "
     "c_ctry_3, c_area_3, c_local_3, c_ext_3, "
     "email1, email2, lcl_tx_id, nat_tx_id, update_dt)"),

    ("Account",         "bronzeaccount",
     "(cdc_flag, cdc_dsn, accountid, brokerid, customerid, "
     "accountdesc, taxstatus, status, update_dt)"),

    ("Trade",           "bronzetrade",
     "(cdc_flag, cdc_dsn, tradeid, t_dts, status, t_tt_id, cashflag, "
     "t_s_symb, quantity, bidprice, t_ca_id, executedby, tradeprice, "
     "fee, commission, tax, event_dt)"),

    ("CashTransaction", "bronzecashtransaction",
     "(cdc_flag, cdc_dsn, accountid, ct_dts, ct_amt, ct_name, event_dt)"),

    ("HoldingHistory",  "bronzeholdings",
     "(cdc_flag, cdc_dsn, hh_h_t_id, hh_t_id, hh_before_qty, hh_after_qty, event_dt)"),

    ("DailyMarket",     "bronzedailymarket",
     "(cdc_flag, cdc_dsn, dm_date, dm_s_symb, dm_close, dm_high, dm_low, dm_vol)"),

    ("WatchHistory",    "bronzewatches",
     "(cdc_flag, cdc_dsn, w_c_id, w_s_symb, w_dts, w_action, event_dt)"),
]

# COMMAND ----------

# Issue all 7 COPYs sequentially on a single connection — bronze loads are
# fast (small per-batch delta) and Redshift's COPY is already heavily
# parallelized across compute slices. No need for connection-level concurrency.

def _copy_one(cur, dataset: str, table: str, cols: str) -> tuple[str, int, float]:
    s3_uri = f"{s3_batch_dir}/{dataset}.{file_ext}"
    sql = f"""
        COPY {bronze_schema}.{table} {cols}
        FROM '{s3_uri}'
        IAM_ROLE '{iam_role}'
        DELIMITER '|'
        REGION '{aws_region}'
        TIMEFORMAT 'YYYY-MM-DD HH:MI:SS'
        DATEFORMAT 'YYYY-MM-DD'
        EMPTYASNULL
        BLANKSASNULL
        ACCEPTINVCHARS
        COMPUPDATE OFF
        STATUPDATE OFF
    """
    t0 = time.time()
    cur.execute(sql)
    # Capture row count from STL_LOAD_COMMITS / pg_last_copy_count() —
    # the latter is the standard PG-style introspection that Redshift supports.
    cur.execute("SELECT pg_last_copy_count()")
    rowcount = cur.fetchone()[0]
    return (dataset, rowcount, time.time() - t0)

with conn.cursor() as cur:
    for dataset, table, cols in COPIES:
        try:
            ds, n, wall = _copy_one(cur, dataset, table, cols)
            print(f"[copy] {ds:18s} → {table:22s}  rows={n:>10,d}  wall={wall:5.2f}s")
        except Exception as e:
            # Surface COPY errors via SYS_LOAD_ERROR_DETAIL — Serverless's
            # canonical load-error view. STL_LOAD_ERRORS is the provisioned
            # equivalent.
            print(f"[FAIL] {dataset} → {table}: {type(e).__name__}: {e}")
            try:
                with conn.cursor() as ecur:
                    ecur.execute(
                        "SELECT colname, error_message, raw_field_value "
                        "FROM SYS_LOAD_ERROR_DETAIL "
                        f"WHERE file_name LIKE '%{dataset}.{file_ext}%' "
                        "ORDER BY start_time DESC LIMIT 5"
                    )
                    for r in ecur.fetchall():
                        print(f"  [load_err] col={r[0]}  msg={r[1]}  raw={r[2]}")
            except Exception as e2:
                print(f"  [load_err lookup failed] {type(e2).__name__}: {e2}")
            raise

conn.close()
print(f"[done] load_bronze_rs batch={batch_date} complete.")
