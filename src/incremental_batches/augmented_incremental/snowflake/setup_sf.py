# Databricks notebook source
# Per-run Snowflake setup. Dispatches SQL to Snowflake from a Databricks
# interactive cluster (no Snowflake compute used here aside from the
# instant zero-copy CLONEs + DDL). Replaces the older `setup.py` which
# CREATE-OR-REPLACE'd target tables; this version CLONEs from a one-time
# `STAGING_SF{sf}` schema seeded by `seed_staging.py`.
#
# Sequence:
#   1. CREATE OR REPLACE SCHEMA TPCDI_TEST.{wh_db}_{sf}
#   2. CLONE 12 reference + dimension tables from TPCDI_TEST.STAGING_SF{sf}
#   3. Pre-create the 16 bronze/silver/gold target tables with CLUSTER BY
#   4. Emit batch_date_ls task value for the parent's for_each loop
#
# Auth: reads creds from the {secret_scope} Databricks secret scope (see
# ./_sf_conn.py).

# COMMAND ----------

dbutils.widgets.text("catalog",         "TPCDI_TEST", "Snowflake database (treated as `catalog`)")
dbutils.widgets.text("wh_db",           "",           "wh_db prefix; final schema = {wh_db}_{scale_factor}")
dbutils.widgets.dropdown("scale_factor","10", ["10","100","1000","5000","10000","20000"], "scale_factor")
dbutils.widgets.text("tpcdi_directory", "/Volumes/main/tpcdi_raw_data/tpcdi_benchmarking/",
                     "UC external volume root the per-batch files land under")
dbutils.widgets.text("snowflake_stage", "TPCDI_STAGE", "Snowflake stage name (no @)")
dbutils.widgets.text("secret_scope",    "tpcdi_snowflake", "Databricks secret scope")
dbutils.widgets.text("incremental_batches_to_run", "365", "Number of batches the for_each loop runs")
dbutils.widgets.text("benchmark_start_date",       "2015-07-06", "Start of the prior-year backfill window")

catalog          = dbutils.widgets.get("catalog")
wh_db            = dbutils.widgets.get("wh_db")
scale_factor     = dbutils.widgets.get("scale_factor")
secret_scope     = dbutils.widgets.get("secret_scope")
incremental_n    = int(dbutils.widgets.get("incremental_batches_to_run"))

if not wh_db:
    raise ValueError("wh_db is required")

target_schema    = f"{wh_db}_{scale_factor}"
staging_schema   = f"STAGING_SF{scale_factor}"
print(f"target  = {catalog}.{target_schema}")
print(f"staging = {catalog}.{staging_schema} (cloned per-run; seeded once by seed_staging.py)")

# COMMAND ----------

# MAGIC %run ./_sf_conn

# COMMAND ----------

conn = sf_connect(database=catalog, secret_scope=secret_scope)
cur  = conn.cursor()

cur.execute(f"CREATE DATABASE IF NOT EXISTS {catalog}")
cur.execute(f"USE DATABASE {catalog}")
cur.execute(f"CREATE OR REPLACE SCHEMA {catalog}.{target_schema}")
cur.execute(f"USE SCHEMA {catalog}.{target_schema}")
print(f"[ok] schema {catalog}.{target_schema} ready")

# COMMAND ----------

# 1. CLONE reference + dimension tables from the staging schema
STAGING_TABLES = [
    "taxrate", "dimdate", "industry", "tradetype", "dimbroker",
    "dimsecurity", "statustype", "dimcompany", "dimtime", "financial",
    "companyyeareps", "currentaccountbalances", "dimaccount",
]
for t in STAGING_TABLES:
    cur.execute(
        f"CREATE OR REPLACE TABLE {catalog}.{target_schema}.{t} "
        f"CLONE {catalog}.{staging_schema}.{t}"
    )
    print(f"[clone] {t}")

# COMMAND ----------

# 2. Pre-create bronze/silver/gold target tables (empty; dbt fills them).
#    Same DDL as the previous setup.py, kept here for self-containment.
def _ddl(name, schema_sql, cluster_by=None):
    cluster_clause = f"\nCLUSTER BY ({cluster_by})" if cluster_by else ""
    cur.execute(f"CREATE OR REPLACE TABLE {catalog}.{target_schema}.{name} ({schema_sql}){cluster_clause}")
    print(f"[ddl] {name}")

_ddl("bronzeaccount", "cdc_flag STRING, cdc_dsn NUMBER, accountid NUMBER, brokerid NUMBER, customerid NUMBER, accountdesc STRING, taxstatus NUMBER, status STRING, update_dt DATE", "update_dt")
_ddl("bronzecashtransaction", "cdc_flag STRING, cdc_dsn NUMBER, accountid NUMBER, ct_dts TIMESTAMP, ct_amt FLOAT, ct_name STRING, event_dt DATE", "event_dt")
_ddl("bronzecustomer", "cdc_flag STRING, cdc_dsn NUMBER, customerid NUMBER, taxid STRING, status STRING, lastname STRING, firstname STRING, middleinitial STRING, gender STRING, tier NUMBER, dob DATE, addressline1 STRING, addressline2 STRING, postalcode STRING, city STRING, stateprov STRING, country STRING, c_ctry_1 STRING, c_area_1 STRING, c_local_1 STRING, c_ext_1 STRING, c_ctry_2 STRING, c_area_2 STRING, c_local_2 STRING, c_ext_2 STRING, c_ctry_3 STRING, c_area_3 STRING, c_local_3 STRING, c_ext_3 STRING, email1 STRING, email2 STRING, lcl_tx_id STRING, nat_tx_id STRING, update_dt DATE", "update_dt")
_ddl("bronzedailymarket", "cdc_flag STRING, cdc_dsn NUMBER, dm_date DATE, dm_s_symb STRING, dm_close FLOAT, dm_high FLOAT, dm_low FLOAT, dm_vol NUMBER", "dm_date")
_ddl("bronzeholdings", "cdc_flag STRING, cdc_dsn NUMBER, hh_h_t_id NUMBER, hh_t_id NUMBER, hh_before_qty NUMBER, hh_after_qty NUMBER, event_dt DATE", "event_dt")
_ddl("bronzetrade", "cdc_flag STRING, cdc_dsn NUMBER, tradeid NUMBER, t_dts TIMESTAMP, status STRING, t_tt_id STRING, cashflag NUMBER, t_s_symb STRING, quantity NUMBER, bidprice FLOAT, t_ca_id NUMBER, executedby STRING, tradeprice FLOAT, fee FLOAT, commission FLOAT, tax FLOAT, event_dt DATE", "event_dt")
_ddl("bronzewatches", "cdc_flag STRING, cdc_dsn NUMBER, w_c_id NUMBER, w_s_symb STRING, w_dts TIMESTAMP, w_action STRING, event_dt DATE", "event_dt")
_ddl("account_updates_from_customer", "cdc_flag STRING, cdc_dsn NUMBER, accountid NUMBER, brokerid NUMBER, customerid NUMBER, accountdesc STRING, taxstatus NUMBER, status STRING, update_dt DATE", "update_dt")
_ddl("dimcustomer", "sk_customerid NUMBER(38,0), customerid NUMBER, taxid STRING, status STRING, lastname STRING, firstname STRING, middleinitial STRING, gender STRING, tier NUMBER, dob DATE, addressline1 STRING, addressline2 STRING, postalcode STRING, city STRING, stateprov STRING, country STRING, phone1 STRING, phone2 STRING, phone3 STRING, email1 STRING, email2 STRING, nationaltaxratedesc STRING, nationaltaxrate FLOAT, localtaxratedesc STRING, localtaxrate FLOAT, effectivedate DATE, enddate DATE, iscurrent BOOLEAN", "effectivedate")
_ddl("dimtrade", "tradeid NUMBER, sk_brokerid NUMBER, sk_createdateid NUMBER, sk_createtimeid NUMBER, sk_closedateid NUMBER, sk_closetimeid NUMBER, status STRING, type STRING, cashflag BOOLEAN, sk_securityid NUMBER, sk_companyid NUMBER, quantity NUMBER, bidprice FLOAT, sk_customerid NUMBER(38,0), sk_accountid NUMBER(38,0), executedby STRING, tradeprice FLOAT, fee FLOAT, commission FLOAT, tax FLOAT", "sk_closedateid")
_ddl("factwatches", "sk_customerid NUMBER(38,0), sk_securityid NUMBER, customerid NUMBER, symbol STRING, sk_dateid_dateplaced NUMBER, sk_dateid_dateremoved NUMBER, removed BOOLEAN", "sk_dateid_dateremoved")
_ddl("factcashbalances", "sk_customerid NUMBER(38,0), sk_accountid NUMBER(38,0), sk_dateid NUMBER, cash NUMBER(15,2)", "sk_dateid")
_ddl("factmarkethistory", "sk_securityid NUMBER, sk_companyid NUMBER, sk_dateid NUMBER, peratio FLOAT, yield FLOAT, fiftytwoweekhigh FLOAT, sk_fiftytwoweekhighdate NUMBER, fiftytwoweeklow FLOAT, sk_fiftytwoweeklowdate NUMBER, closeprice FLOAT, dayhigh FLOAT, daylow FLOAT, volume NUMBER", "sk_dateid")
_ddl("factholdings", "tradeid NUMBER, currenttradeid NUMBER, sk_customerid NUMBER(38,0), sk_accountid NUMBER(38,0), sk_securityid NUMBER, sk_companyid NUMBER, sk_dateid NUMBER, sk_timeid NUMBER, currentprice FLOAT, currentholding NUMBER", "sk_dateid")
# Note: dimaccount target is CLONED above (the SCD2 history pre-batch-start
# is in STAGING_SF{sf}); the per-batch silver/dimaccount dbt model writes
# back into the cloned table — that's intentional, matches Databricks side.
print(f"[ok] target tables ready under {catalog}.{target_schema}")

# COMMAND ----------

# 3. Emit batch_date_ls — match setup_dbt.py exactly: AUG_FILES_DATE_START
# is hardcoded to 2016-07-06. (Computing it as benchmark_start + 365 days
# lands on 2016-07-05 because 2016 is a leap year — the resulting batch
# falls one day before any source data exists.)
import datetime as dt
incr_start = dt.date(2016, 7, 6)  # AUG_FILES_DATE_START, see tpcdi_gen/config.py
batches = [(incr_start + dt.timedelta(days=i)).isoformat() for i in range(incremental_n)]
dbutils.jobs.taskValues.set("batch_date_ls", batches)
print(f"emitted batch_date_ls: {len(batches)} dates, first={batches[0]}, last={batches[-1]}")

# COMMAND ----------

cur.close()
conn.close()
print("[done] Snowflake setup complete.")
