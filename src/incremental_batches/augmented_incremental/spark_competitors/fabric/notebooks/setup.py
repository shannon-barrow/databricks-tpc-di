# Fabric Spark NEE notebook — per-run setup (runs IN Fabric, triggered by the
# Databricks setup_fabric task via REST).
#
# BATCH port of the fabric_ss setup. Same TABLES-ONLY scope, but NO structured-streaming
# checkpoints exist in the NEE batch variant, so the checkpoint reset step is dropped.
# See PORT_NOTES.md §7.  MUST run on the NEE-accelerated environment (tpcdi_fabric_nee).
#
# What setup does (all table ops):
#   1. DROP+CREATE the working schema.
#   2. SHALLOW CLONE the 20 staging tables from OneLake-native staging_sf{sf}
#      (12 reference + 8 dim/fact). bronzedailymarket is among these — its clone is the
#      prior-year SEED that FactMarketHistory's 52-week window reads; ingest_bronze then
#      APPENDS each batch's day to it.
#   3. CREATE OR REPLACE the 6 empty bronze ingest tables (liquid-clustered); ingest_bronze
#      fills them per batch via INSERT OVERWRITE (current-batch-only).
#
# Attach a schema-enabled default lakehouse.

# --- PARAMETER CELL (Fabric injects overrides) ---
wh_db            = ""
scale_factor     = "10"
staging_schema   = ""            # OneLake-native staging schema (default: staging_sf{sf})
# -------------------------------------------------

sf         = scale_factor
tgt_db     = f"{wh_db}_{sf}"
staging_db = staging_schema or f"staging_sf{sf}"

# Same membership as the fabric_ss setup (all SHALLOW in Fabric).
shallow_tbls = [
    "taxrate", "dimdate", "industry", "tradetype", "dimbroker", "financial",
    "companyyeareps", "dimsecurity", "statustype", "dimcompany", "dimtime",
    "currentaccountbalances",
]
deep_tbls = [
    "dimcustomer", "dimaccount", "dimtrade", "factwatches", "factholdings",
    "factmarkethistory", "bronzedailymarket", "factcashbalances",
]

import time

# 1. build target schema (DROP+CREATE)
spark.sql(f"DROP SCHEMA IF EXISTS {tgt_db} CASCADE")
spark.sql(f"CREATE SCHEMA {tgt_db}")
print(f"[ok] schema {tgt_db} reset")

# 2. clone the 20 tables from OneLake staging (all SHALLOW), then ANALYZE for stats,
# IN PARALLEL (matches the Databricks setup + the fabric_ss setup). Without ANALYZE the
# CBO estimates the stat-less clones at ~KiB and broadcasts an 8.2 GiB side in the silver
# MERGEs at SF=20000 (hard 8 GiB cap). ANALYZE ... FOR ALL COLUMNS *is* supported on
# Fabric; it needs the current schema set (a 2-part name fails to resolve for ANALYZE) and
# the catalog-stats injection flag (set here + in batch_runner).
spark.conf.set("spark.microsoft.delta.stats.injection.catalog.enabled", "true")
spark.sql(f"USE {tgt_db}")

import concurrent.futures
def _clone_and_analyze(t):
    spark.sql(f"CREATE OR REPLACE TABLE {tgt_db}.{t} SHALLOW CLONE {staging_db}.{t}")
    spark.sql(f"ANALYZE TABLE {t} COMPUTE STATISTICS FOR ALL COLUMNS")
    return t

t_clone = time.time()
_tbls = shallow_tbls + deep_tbls
with concurrent.futures.ThreadPoolExecutor(max_workers=len(_tbls)) as _ex:
    _futs = [_ex.submit(_clone_and_analyze, t) for t in _tbls]
    for _f in concurrent.futures.as_completed(_futs):
        print(f"[clone+analyze] {_f.result()}")
print(f"[done] {len(_tbls)} clone+analyze (parallel) in {time.time()-t_clone:.1f}s")

# 3. (re)create the 6 empty bronze ingest tables, liquid-clustered.
# delta.dataSkippingNumIndexedCols=34 is REQUIRED: Delta clustering demands stats on the
# CLUSTER BY column, but bronzecustomer's update_dt is its 34th column — past Delta's
# default 32-col stats window — so without this the CREATE fails
# DELTA_CLUSTERING_COLUMN_MISSING_STATS on Fabric Runtime 2.0 (Delta 4.2).
bronze_ddl = {
    "bronzeaccount":         ("cdc_flag STRING, cdc_dsn BIGINT, accountid BIGINT, brokerid BIGINT, customerid BIGINT, accountdesc STRING, taxstatus TINYINT, status STRING, update_dt DATE", "update_dt"),
    "bronzecashtransaction": ("cdc_flag STRING, cdc_dsn BIGINT, accountid BIGINT, ct_dts TIMESTAMP, ct_amt DOUBLE, ct_name STRING, event_dt DATE", "event_dt"),
    "bronzecustomer":        ("cdc_flag STRING, cdc_dsn BIGINT, customerid BIGINT, taxid STRING, status STRING, lastname STRING, firstname STRING, middleinitial STRING, gender STRING, tier TINYINT, dob DATE, addressline1 STRING, addressline2 STRING, postalcode STRING, city STRING, stateprov STRING, country STRING, c_ctry_1 STRING, c_area_1 STRING, c_local_1 STRING, c_ext_1 STRING, c_ctry_2 STRING, c_area_2 STRING, c_local_2 STRING, c_ext_2 STRING, c_ctry_3 STRING, c_area_3 STRING, c_local_3 STRING, c_ext_3 STRING, email1 STRING, email2 STRING, lcl_tx_id STRING, nat_tx_id STRING, update_dt DATE", "update_dt"),
    "bronzeholdings":        ("cdc_flag STRING, cdc_dsn BIGINT, hh_h_t_id BIGINT, hh_t_id BIGINT, hh_before_qty INT, hh_after_qty INT, event_dt DATE", "event_dt"),
    "bronzetrade":           ("cdc_flag STRING, cdc_dsn BIGINT, tradeid BIGINT, t_dts TIMESTAMP, status STRING, t_tt_id STRING, cashflag TINYINT, t_s_symb STRING, quantity INT, bidprice DOUBLE, t_ca_id BIGINT, executedby STRING, tradeprice DOUBLE, fee DOUBLE, commission DOUBLE, tax DOUBLE, event_dt DATE", "event_dt"),
    "bronzewatches":         ("cdc_flag STRING, cdc_dsn BIGINT, w_c_id BIGINT, w_s_symb STRING, w_dts TIMESTAMP, w_action STRING, event_dt DATE", "event_dt"),
}
for t, (cols, cl) in bronze_ddl.items():
    spark.sql(f"CREATE OR REPLACE TABLE {tgt_db}.{t} ({cols}) CLUSTER BY ({cl}) "
              f"TBLPROPERTIES ('delta.dataSkippingNumIndexedCols' = '34')")
    print(f"[bronze] {t}")

notebookutils.notebook.exit("setup_ok")
