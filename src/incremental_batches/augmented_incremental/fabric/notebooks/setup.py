# Fabric Spark notebook — per-run setup (runs IN Fabric, triggered by the
# Databricks setup_fabric task via REST).
#
# Port of the Databricks Cluster setup.py, scoped to what setup should own:
#   **TABLES ONLY.** No file/dir operations here.
#     - `simulate_file_drops` owns the raw file dirs (clears + writes per batch).
#     - Structured-streaming checkpoints are the Fabric streams' persistent state
#       store; a separate one-time Fabric-side reset clears them at run start.
#
# What setup does (all table ops):
#   1. DROP+CREATE the working schema.
#   2. SHALLOW CLONE the 20 staging tables from OneLake-native staging_sf{sf}
#      (12 reference + 8 dim/fact — DEEP on Databricks, all SHALLOW here since
#      Fabric Delta has SHALLOW CLONE only). Clone source is materialized into
#      OneLake by setup_fabric's inline bootstrap (Databricks reads UC, writes
#      OneLake) — Fabric can't read the UC-managed source itself.
#   3. CREATE OR REPLACE the 6 empty bronze ingest tables (liquid-clustered).
#
# Attach a schema-enabled default lakehouse (tpcdi_fabric).

# --- PARAMETER CELL (Fabric injects overrides) ---
wh_db            = ""
scale_factor     = "10"
staging_schema   = ""            # OneLake-native staging schema (default: staging_sf{sf})
checkpoint_root  = "Files/augmented_incremental/_checkpoints"   # in the attached lakehouse's OneLake
# -------------------------------------------------

sf         = scale_factor
tgt_db     = f"{wh_db}_{sf}"
staging_db = staging_schema or f"staging_sf{sf}"

# Same membership as the Databricks setup (12 SHALLOW reference + 8 DEEP dim/fact);
# all SHALLOW in Fabric.
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

# 2. clone the 20 tables from OneLake staging (all SHALLOW), then ANALYZE for stats.
# ANALYZE ... COMPUTE STATISTICS FOR ALL COLUMNS *is* supported on Fabric (the earlier
# "unsupported" note was wrong). It is REQUIRED, not optional: a SHALLOW CLONE carries no
# optimizer statistics, so without it the CBO estimates these tables at ~KiB and broadcasts
# them in the silver MERGEs — an 8.2 GiB broadcast that blows Spark's hard 8 GiB cap at
# SF=20000 (DimCustomer et al. fail from batch 2 on). Recomputed per run because the clone
# is recreated each run. Mirrors the Databricks setup's clone_table (parity). The catalog-
# stats injection flag must be ON for the CBO to actually use them (also set in batch_runner
# for the query-planning session).
spark.conf.set("spark.microsoft.delta.stats.injection.catalog.enabled", "true")
# ANALYZE needs the current schema set: on Fabric a 2-part `schema.table` name fails to
# resolve for ANALYZE TABLE (SELECT/CLONE tolerate it, ANALYZE does not), so USE the
# schema and reference tables unqualified. USE is session-level, so it's set once here
# and every worker thread below inherits it.
spark.sql(f"USE {tgt_db}")

# Clone + ANALYZE the 20 tables IN PARALLEL (matches the Databricks setup's
# ThreadPoolExecutor over clone_table). SparkSession is thread-safe; the clone uses
# qualified names and the ANALYZE relies on the session-level USE above.
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
# Schemas identical to ingest_bronze.py; CLUSTER BY on the bronze filter date.
# delta.dataSkippingNumIndexedCols=34 is REQUIRED (not Databricks-only): Delta
# clustering demands stats on the CLUSTER BY column, but bronzecustomer's
# update_dt is its 34th column — past Delta's default 32-col stats window — so
# without this the CREATE fails DELTA_CLUSTERING_COLUMN_MISSING_STATS on Fabric
# Runtime 2.0 (Delta 4.2). Same fix as the Databricks setup. The Databricks-only
# delta.autoOptimize.* props stay dropped (Fabric manages layout via V-Order).
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

# 4. Reset the structured-streaming checkpoints — ONCE per run, and CRUCIALLY
# executed here on the FABRIC side (this notebook runs in Fabric), creating the
# dir in the attached lakehouse's OneLake default storage. It must NOT be created
# from Databricks, or it wouldn't be usable as the Fabric streams' state store.
# Clearing it so the availableNow streams reprocess from scratch, matched to the
# freshly-recreated (empty) tables above.
ckpt_dir = f"{checkpoint_root}/{tgt_db}"
try:
    notebookutils.fs.rm(ckpt_dir, True); print(f"[ckpt] cleared {ckpt_dir}")
except Exception as e:
    print(f"[ckpt] nothing to clear ({type(e).__name__})")
notebookutils.fs.mkdirs(ckpt_dir)

notebookutils.notebook.exit("setup_ok")
