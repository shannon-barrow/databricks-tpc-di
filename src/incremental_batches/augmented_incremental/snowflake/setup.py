# Databricks notebook source
# Snowflake-side setup for the TPC-DI augmented incremental benchmark.
# Mirrors the dbt variant's setup_dbt.py — creates the per-run database
# + schema, sets up the external stage pointing at the same S3 prefix
# the simulate_filedrops volume writes to, and pre-creates the empty
# target tables with the right clustering for the benchmark.
#
# Auth: reads creds from the `tpcdi_snowflake` Databricks secret scope
# (see ./_sf_conn.py). The notebook runs on an interactive cluster so
# the secret/conn lifecycle is per-cluster, not per-batch.

# COMMAND ----------

dbutils.widgets.text("catalog",         "TPCDI_TEST",          "Snowflake database name (var('catalog') equivalent)")
dbutils.widgets.text("wh_db",           "",                    "wh_db prefix (Snowflake schema name = wh_db_<sf>)")
dbutils.widgets.dropdown("scale_factor","10", ["10","100","1000","5000","10000","20000"], "scale_factor")
dbutils.widgets.text("tpcdi_directory", "/Volumes/main/tpcdi_raw_data/tpcdi_volume/", "Databricks Volume root (mirror for stage path)")
dbutils.widgets.text("s3_stage_url",    "",                    "S3 URL the external stage will point at (s3://bucket/prefix/)")
dbutils.widgets.text("storage_integration_name", "TPCDI_S3_INT", "Snowflake storage integration to attach to the stage")
dbutils.widgets.text("incremental_batches_to_run", "365",      "Number of batches the for_each loop will run")
dbutils.widgets.text("benchmark_start_date", "2015-07-06",     "Start of the prior-year backfill window")

catalog       = dbutils.widgets.get("catalog")
wh_db         = dbutils.widgets.get("wh_db")
scale_factor  = dbutils.widgets.get("scale_factor")
s3_stage_url  = dbutils.widgets.get("s3_stage_url").strip()
storage_int   = dbutils.widgets.get("storage_integration_name").strip()
incremental_batches_to_run = int(dbutils.widgets.get("incremental_batches_to_run"))

if not wh_db:
    raise ValueError("wh_db is required")

target_schema = f"{wh_db}_{scale_factor}"
print(f"target = {catalog}.{target_schema}")
print(f"stage  = {s3_stage_url or '(none — bronze models will fail until set)'}")

# COMMAND ----------

# MAGIC %run ./_sf_conn

# COMMAND ----------

conn = sf_connect(database=catalog)
cur = conn.cursor()

# --- 1. Database + schema (idempotent) ---
cur.execute(f"CREATE DATABASE IF NOT EXISTS {catalog}")
cur.execute(f"USE DATABASE {catalog}")
cur.execute(f"CREATE OR REPLACE SCHEMA {catalog}.{target_schema}")
cur.execute(f"USE SCHEMA {catalog}.{target_schema}")
print(f"[ok] schema {catalog}.{target_schema} ready")

# --- 2. External stage on the S3 prefix that simulate_filedrops writes to ---
if s3_stage_url and storage_int:
    cur.execute(f"""
        CREATE OR REPLACE STAGE tpcdi_stage
        URL = '{s3_stage_url}'
        STORAGE_INTEGRATION = {storage_int}
        FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = '|' SKIP_HEADER = 0 EMPTY_FIELD_AS_NULL = TRUE)
        DIRECTORY = (ENABLE = TRUE)
    """)
    print(f"[ok] stage tpcdi_stage -> {s3_stage_url} (via {storage_int})")
else:
    print("[skip] stage not created — pass s3_stage_url + storage_integration_name to enable")

# --- 3. Pre-create the empty target tables with the right cluster keys ---
# Mirrors setup_dbt.py's "setup-owns-layout" pattern. dbt models are
# expected to write into these pre-existing tables; the CLUSTER BY here
# avoids per-batch ALTER TABLE noise from the adapter.
def _ddl(name: str, schema_sql: str, cluster_by: str | None = None):
    cluster_clause = f"\nCLUSTER BY ({cluster_by})" if cluster_by else ""
    cur.execute(f"CREATE OR REPLACE TABLE {catalog}.{target_schema}.{name} ({schema_sql}){cluster_clause}")
    print(f"[ok] table {name}")

# Bronze (one row per landed file row; clustered on the batch date col)
_ddl("bronzeaccount", """
    cdc_flag STRING, cdc_dsn NUMBER, accountid NUMBER, brokerid NUMBER,
    customerid NUMBER, accountdesc STRING, taxstatus NUMBER,
    status STRING, update_dt DATE
""", cluster_by="update_dt")
_ddl("bronzecashtransaction", """
    cdc_flag STRING, cdc_dsn NUMBER, accountid NUMBER, ct_dts TIMESTAMP,
    ct_amt FLOAT, ct_name STRING, event_dt DATE
""", cluster_by="event_dt")
_ddl("bronzecustomer", """
    cdc_flag STRING, cdc_dsn NUMBER, customerid NUMBER, taxid STRING,
    status STRING, lastname STRING, firstname STRING, middleinitial STRING,
    gender STRING, tier NUMBER, dob DATE,
    addressline1 STRING, addressline2 STRING, postalcode STRING,
    city STRING, stateprov STRING, country STRING,
    c_ctry_1 STRING, c_area_1 STRING, c_local_1 STRING, c_ext_1 STRING,
    c_ctry_2 STRING, c_area_2 STRING, c_local_2 STRING, c_ext_2 STRING,
    c_ctry_3 STRING, c_area_3 STRING, c_local_3 STRING, c_ext_3 STRING,
    email1 STRING, email2 STRING, lcl_tx_id STRING, nat_tx_id STRING,
    update_dt DATE
""", cluster_by="update_dt")
_ddl("bronzedailymarket", """
    cdc_flag STRING, cdc_dsn NUMBER, dm_date DATE, dm_s_symb STRING,
    dm_close FLOAT, dm_high FLOAT, dm_low FLOAT, dm_vol NUMBER
""", cluster_by="dm_date")
_ddl("bronzeholdings", """
    cdc_flag STRING, cdc_dsn NUMBER, hh_h_t_id NUMBER, hh_t_id NUMBER,
    hh_before_qty NUMBER, hh_after_qty NUMBER, event_dt DATE
""", cluster_by="event_dt")
_ddl("bronzetrade", """
    cdc_flag STRING, cdc_dsn NUMBER, tradeid NUMBER, t_dts TIMESTAMP,
    status STRING, t_tt_id STRING, cashflag NUMBER, t_s_symb STRING,
    quantity NUMBER, bidprice FLOAT, t_ca_id NUMBER, executedby STRING,
    tradeprice FLOAT, fee FLOAT, commission FLOAT, tax FLOAT,
    event_dt DATE
""", cluster_by="event_dt")
_ddl("bronzewatches", """
    cdc_flag STRING, cdc_dsn NUMBER, w_c_id NUMBER, w_s_symb STRING,
    w_dts TIMESTAMP, w_action STRING, event_dt DATE
""", cluster_by="event_dt")
_ddl("account_updates_from_customer", """
    cdc_flag STRING, cdc_dsn NUMBER, accountid NUMBER, brokerid NUMBER,
    customerid NUMBER, accountdesc STRING, taxstatus NUMBER,
    status STRING, update_dt DATE
""", cluster_by="update_dt")

# Silver / Gold (clustering keys mirror the Databricks Liquid layouts)
_ddl("dimcustomer", """
    sk_customerid NUMBER(38,0), customerid NUMBER, taxid STRING, status STRING,
    lastname STRING, firstname STRING, middleinitial STRING, gender STRING,
    tier NUMBER, dob DATE,
    addressline1 STRING, addressline2 STRING, postalcode STRING,
    city STRING, stateprov STRING, country STRING,
    phone1 STRING, phone2 STRING, phone3 STRING, email1 STRING, email2 STRING,
    nationaltaxratedesc STRING, nationaltaxrate FLOAT,
    localtaxratedesc STRING,    localtaxrate FLOAT,
    effectivedate DATE, enddate DATE, iscurrent BOOLEAN
""", cluster_by="effectivedate")
_ddl("dimaccount", """
    sk_accountid NUMBER(38,0), accountid NUMBER, sk_brokerid NUMBER,
    sk_customerid NUMBER(38,0),
    accountdesc STRING, taxstatus NUMBER, status STRING,
    iscurrent BOOLEAN, effectivedate DATE, enddate DATE
""", cluster_by="effectivedate")
_ddl("dimtrade", """
    tradeid NUMBER, sk_brokerid NUMBER,
    sk_createdateid NUMBER, sk_createtimeid NUMBER,
    sk_closedateid  NUMBER, sk_closetimeid  NUMBER,
    status STRING, type STRING, cashflag BOOLEAN,
    sk_securityid NUMBER, sk_companyid NUMBER,
    quantity NUMBER, bidprice FLOAT,
    sk_customerid NUMBER(38,0), sk_accountid NUMBER(38,0),
    executedby STRING, tradeprice FLOAT, fee FLOAT, commission FLOAT, tax FLOAT
""", cluster_by="sk_closedateid")
_ddl("factwatches", """
    sk_customerid NUMBER(38,0), sk_securityid NUMBER,
    customerid NUMBER, symbol STRING,
    sk_dateid_dateplaced NUMBER, sk_dateid_dateremoved NUMBER,
    removed BOOLEAN
""", cluster_by="sk_dateid_dateremoved")
_ddl("currentaccountbalances", """
    ct_date DATE, accountid NUMBER,
    current_account_cash NUMBER(15,2),
    latest_batch BOOLEAN
""")
_ddl("factcashbalances", """
    sk_customerid NUMBER(38,0), sk_accountid NUMBER(38,0),
    sk_dateid NUMBER, cash NUMBER(15,2)
""", cluster_by="sk_dateid")
_ddl("factmarkethistory", """
    sk_securityid NUMBER, sk_companyid NUMBER,
    sk_dateid NUMBER,
    peratio FLOAT, yield FLOAT,
    fiftytwoweekhigh FLOAT, sk_fiftytwoweekhighdate NUMBER,
    fiftytwoweeklow  FLOAT, sk_fiftytwoweeklowdate  NUMBER,
    closeprice FLOAT, dayhigh FLOAT, daylow FLOAT, volume NUMBER
""", cluster_by="sk_dateid")
_ddl("factholdings", """
    tradeid NUMBER, currenttradeid NUMBER,
    sk_customerid NUMBER(38,0), sk_accountid NUMBER(38,0),
    sk_securityid NUMBER, sk_companyid NUMBER,
    sk_dateid NUMBER, sk_timeid NUMBER,
    currentprice FLOAT, currentholding NUMBER
""", cluster_by="sk_dateid")

print(f"[ok] all 16 target tables ready under {catalog}.{target_schema}")

# COMMAND ----------

# --- 4. Build the batch_date_ls task value for the for_each_task loop ---
# Mirrors augmented_dbt's setup behaviour. Same iso-date list spanning
# (benchmark_start_date + 365 days) onward, capped at incremental_batches_to_run.
import datetime as dt
start = dt.date.fromisoformat(dbutils.widgets.get("benchmark_start_date"))
incr_start = start + dt.timedelta(days=365)  # AUG_FILES_DATE_START
batches = [(incr_start + dt.timedelta(days=i)).isoformat()
           for i in range(incremental_batches_to_run)]
dbutils.jobs.taskValues.set("batch_date_ls", batches)
print(f"emitted batch_date_ls: {len(batches)} dates, "
      f"first={batches[0]}, last={batches[-1]}")

# COMMAND ----------

cur.close()
conn.close()
print("[done] Snowflake setup complete.")
