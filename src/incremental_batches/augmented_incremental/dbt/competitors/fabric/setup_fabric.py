# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# dependencies = [
#   "pyodbc",
#   "msal",
# ]
# ///
# Per-run Fabric Data Warehouse setup. Runs on a CLASSIC single-user UC cluster
# (needs the msodbcsql18 ODBC driver via the cluster init script AND OneLake
# OAuth to read UC `main` for the one-time staging materialize).
#
# Steps:
#   1. Ensure the Lakehouse OneLake `staging_sf{sf}` Delta tables exist
#      (fabric_staging_bootstrap: DEEP CLONE main.tpcdi_incremental_staging_{sf}
#      -> OneLake if missing; idempotent). Reuses the tables the fabric_ss/nee
#      runs already materialized.
#   2. CREATE the per-run schema {wh_db}_{sf} in the warehouse.
#   3. CTAS the 22 historical dim/fact + reference tables from the Lakehouse into
#      the warehouse via CROSS-DATABASE query ([tpcdi_fabric].[staging_sf{sf}].[t]
#      -> [wh].[{wh_db}_{sf}].[t]). Clustered tables get WITH (CLUSTER BY (...)).
#      (setup-owns-layout: these are the dbt models' pre-created targets, so the
#      SCD2 MERGEs land into a clustered, history-seeded table.)
#   4. Pre-create the 6 streaming bronze tables EMPTY, clustered — dbt appends
#      into them each batch (bronze reads the day's OneLake CSV via OPENROWSET).
#   5. Emit batch_date_ls for the parent's for_each loop.
#
# Fabric DW clustering (preview): WITH (CLUSTER BY (<=4 cols)), set at CREATE
# only, no ALTER. Supported key types incl. date/bigint (all our keys qualify);
# NOT bit/varchar(max). No dataSkippingNumIndexedCols (Delta-only).
#
# ⚠ VERIFY AT SMOKE TIME: the cross-DB source schema. The DEEP CLONE writes to
# OneLake Tables/staging_sf{sf}/<t>; in a schema-enabled lakehouse that surfaces
# to the SQL endpoint as schema [staging_sf{sf}]. If cross-DB resolution fails,
# check the actual schema name via the Lakehouse SQL endpoint and set
# `lakehouse_src_schema`.

# COMMAND ----------

dbutils.widgets.text("wh_db",            "", "wh_db prefix; run schema = {wh_db}_{scale_factor}")
dbutils.widgets.dropdown("scale_factor", "10", ["10","100","1000","5000","10000","20000"])
dbutils.widgets.text("fabric_workspace_id",  "e0a370b6-0279-4bc2-92ef-0a3f001e068b")
dbutils.widgets.text("fabric_lakehouse_id",  "54963c1f-bb06-4acd-8db3-b8b5056e81c3")
dbutils.widgets.text("fabric_lakehouse_name","tpcdi_fabric", "Lakehouse item name (cross-DB source db)")
dbutils.widgets.text("lakehouse_src_schema", "", "Cross-DB schema of the staging tables; default staging_sf{sf}")
dbutils.widgets.text("fabric_wh_host",  "skrtph5o6caeff4w6gdeuehp7q-wzykhydzalbexexpbi7qahqgrm.datawarehouse.fabric.microsoft.com",
                     "Warehouse SQL analytics endpoint")
dbutils.widgets.text("fabric_wh_name",  "tpcdi_fabric_dw", "Warehouse item name (= DW database)")
dbutils.widgets.text("tenant_id",       "9f37a392-f0ae-4280-9796-f1864a10effc")
dbutils.widgets.text("databricks_catalog", "main", "UC catalog holding tpcdi_incremental_staging_{sf}")
dbutils.widgets.text("incremental_batches_to_run", "365")
dbutils.widgets.dropdown("force_reset", "NO", ["NO","YES"],
                         "YES = drop the run schema + re-CTAS everything")

wh_db            = dbutils.widgets.get("wh_db")
scale_factor     = dbutils.widgets.get("scale_factor")
ws_id            = dbutils.widgets.get("fabric_workspace_id")
lh_id            = dbutils.widgets.get("fabric_lakehouse_id")
lh_name          = dbutils.widgets.get("fabric_lakehouse_name")
wh_host          = dbutils.widgets.get("fabric_wh_host")
wh_name          = dbutils.widgets.get("fabric_wh_name")
tenant_id        = dbutils.widgets.get("tenant_id")
db_catalog       = dbutils.widgets.get("databricks_catalog")
incremental_n    = int(dbutils.widgets.get("incremental_batches_to_run"))
force_reset      = dbutils.widgets.get("force_reset").upper() == "YES"
src_schema       = dbutils.widgets.get("lakehouse_src_schema") or f"staging_sf{scale_factor}"

if not wh_db:
    raise ValueError("wh_db is required")

run_schema = f"{wh_db}_{scale_factor}".lower()
print(f"warehouse   = {wh_name} (host {wh_host})")
print(f"run schema  = [{run_schema}]")
print(f"cross-DB    = [{lh_name}].[{src_schema}].<table>")

# COMMAND ----------

# OneLake OAuth (SP) so the Spark DEEP CLONE in the bootstrap can write OneLake.
# Same confs the fabric_ss/nee setup used. Reads UC `main` for the clone source.
for k, v in {
    "fs.azure.account.auth.type.onelake.dfs.fabric.microsoft.com": "OAuth",
    "fs.azure.account.oauth.provider.type.onelake.dfs.fabric.microsoft.com":
        "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    "fs.azure.account.oauth2.client.id.onelake.dfs.fabric.microsoft.com":
        dbutils.secrets.get("tpcdi_fabric", "client_id"),
    "fs.azure.account.oauth2.client.secret.onelake.dfs.fabric.microsoft.com":
        dbutils.secrets.get("tpcdi_fabric", "client_secret"),
    "fs.azure.account.oauth2.client.endpoint.onelake.dfs.fabric.microsoft.com":
        f"https://login.microsoftonline.com/{tenant_id}/oauth2/token",
}.items():
    spark.conf.set(k, v)

# COMMAND ----------

# MAGIC %run ./_fab_conn

# COMMAND ----------

import sys, os
try:
    _nb = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().getOrElse(None)
    _dir = os.path.dirname("/Workspace" + _nb if _nb and not _nb.startswith("/Workspace") else (_nb or ""))
except Exception:
    _dir = os.getcwd()
if _dir and _dir not in sys.path:
    sys.path.insert(0, _dir)
import fabric_staging_bootstrap as bootstrap

# COMMAND ----------

# 1. Ensure the Lakehouse staging tables exist (DEEP CLONE from UC main if missing).
boot = bootstrap.ensure_onelake_staging(
    spark=spark, dbutils=dbutils,
    ws_id=ws_id, lh_id=lh_id,
    src_catalog=db_catalog, scale_factor=scale_factor,
    force=force_reset,
)
print(f"[bootstrap] {boot}")

# COMMAND ----------

# Canonical 22 staging tables + their cluster key (None = unclustered reference).
# DISTKEY dropped (Fabric has no distribution). SORTKEY -> CLUSTER BY (<=4 cols).
CLUSTER_KEY = {
    "dimcustomer":            "enddate",
    "dimaccount":             "enddate",
    "dimtrade":               "sk_closedateid",
    "factwatches":            "sk_dateid_dateremoved",
    "factmarkethistory":      "sk_dateid",
    "factholdings":           "sk_dateid",
    "factcashbalances":       "sk_dateid",
    "bronzedailymarket":      "dm_date",
    # reference / small / low-cardinality -> unclustered
    "currentaccountbalances": None, "dimbroker": None, "dimsecurity": None,
    "dimcompany": None, "dimtime": None, "dimdate": None, "taxrate": None,
    "industry": None, "tradetype": None, "statustype": None, "batchdate": None,
    "financial": None, "companyyeareps": None, "cashtransactionhistorical": None,
}
STAGING_TABLES = sorted(CLUSTER_KEY)

# COMMAND ----------

conn = fab_connect(host=wh_host, database=wh_name, tenant_id=tenant_id,
                   label={"task": "setup_fabric", "wh_db": wh_db, "scale_factor": scale_factor})
cur = conn.cursor()

# 2. Run schema.
if force_reset:
    # Fabric DW has no DROP SCHEMA CASCADE; drop tables first, then schema.
    cur.execute("SELECT table_name FROM INFORMATION_SCHEMA.TABLES WHERE table_schema = ?", run_schema)
    for (t,) in cur.fetchall():
        cur.execute(f"DROP TABLE IF EXISTS [{run_schema}].[{t}]")
    cur.execute(f"DROP SCHEMA IF EXISTS [{run_schema}]")
    print(f"[reset] dropped [{run_schema}]")
cur.execute(f"IF SCHEMA_ID('{run_schema}') IS NULL EXEC('CREATE SCHEMA [{run_schema}]')")
print(f"[ok] schema [{run_schema}] ready")

# COMMAND ----------

# 3. CTAS the 22 tables cross-DB, clustered where applicable. Idempotent: skip a
# table whose row count already matches the Lakehouse source.
import time as _t

def _src_count(t):
    cur.execute(f"SELECT COUNT_BIG(*) FROM [{lh_name}].[{src_schema}].[{t}]")
    return cur.fetchone()[0]

def _tgt_exists(t):
    cur.execute("SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE table_schema=? AND table_name=?",
                run_schema, t)
    return cur.fetchone() is not None

for t in STAGING_TABLES:
    t0 = _t.time()
    if _tgt_exists(t):
        cur.execute(f"SELECT COUNT_BIG(*) FROM [{run_schema}].[{t}]")
        tgt_rows = cur.fetchone()[0]
        if tgt_rows > 0 and tgt_rows == _src_count(t):
            print(f"[ctas] {t:28s} skip ({tgt_rows:,} rows present)")
            continue
        cur.execute(f"DROP TABLE [{run_schema}].[{t}]")
    key = CLUSTER_KEY[t]
    with_clause = f" WITH (CLUSTER BY ([{key}]))" if key else ""
    cur.execute(
        f"CREATE TABLE [{run_schema}].[{t}]{with_clause} AS "
        f"SELECT * FROM [{lh_name}].[{src_schema}].[{t}]"
    )
    print(f"[ctas] {t:28s} {'CLUSTER BY '+key if key else '(unclustered)':22s} {_t.time()-t0:6.1f}s")

# COMMAND ----------

# 4. Pre-create the 6 streaming bronze tables EMPTY + clustered. dbt appends into
# them each batch (fabric bronze reads the OneLake CSV via OPENROWSET). Types
# mirror the Databricks bronze (STRING->VARCHAR, TINYINT->SMALLINT,
# DOUBLE->FLOAT, TIMESTAMP->DATETIME2). Cluster on the batch-date column.
BRONZE_DDL = {
    "bronzecustomer": ("""
        cdc_flag VARCHAR(1), cdc_dsn BIGINT, customerid BIGINT, taxid VARCHAR(20),
        status VARCHAR(10), lastname VARCHAR(40), firstname VARCHAR(40),
        middleinitial VARCHAR(1), gender VARCHAR(1), tier SMALLINT, dob DATE,
        addressline1 VARCHAR(80), addressline2 VARCHAR(80), postalcode VARCHAR(20),
        city VARCHAR(40), stateprov VARCHAR(20), country VARCHAR(30),
        c_ctry_1 VARCHAR(10), c_area_1 VARCHAR(10), c_local_1 VARCHAR(15), c_ext_1 VARCHAR(10),
        c_ctry_2 VARCHAR(10), c_area_2 VARCHAR(10), c_local_2 VARCHAR(15), c_ext_2 VARCHAR(10),
        c_ctry_3 VARCHAR(10), c_area_3 VARCHAR(10), c_local_3 VARCHAR(15), c_ext_3 VARCHAR(10),
        email1 VARCHAR(80), email2 VARCHAR(80), lcl_tx_id VARCHAR(20), nat_tx_id VARCHAR(20),
        update_dt DATE
    """, "update_dt"),
    "bronzeaccount": ("""
        cdc_flag VARCHAR(1), cdc_dsn BIGINT, accountid BIGINT, brokerid BIGINT,
        customerid BIGINT, accountdesc VARCHAR(80), taxstatus SMALLINT,
        status VARCHAR(10), update_dt DATE
    """, "update_dt"),
    "bronzecashtransaction": ("""
        cdc_flag VARCHAR(1), cdc_dsn BIGINT, accountid BIGINT, ct_dts DATETIME2,
        ct_amt FLOAT, ct_name VARCHAR(100), event_dt DATE
    """, "event_dt"),
    "bronzeholdings": ("""
        cdc_flag VARCHAR(1), cdc_dsn BIGINT, hh_h_t_id BIGINT, hh_t_id BIGINT,
        hh_before_qty INT, hh_after_qty INT, event_dt DATE
    """, "event_dt"),
    "bronzetrade": ("""
        cdc_flag VARCHAR(1), cdc_dsn BIGINT, tradeid BIGINT, t_dts DATETIME2,
        status VARCHAR(10), t_tt_id VARCHAR(10), cashflag SMALLINT, t_s_symb VARCHAR(20),
        quantity INT, bidprice FLOAT, t_ca_id BIGINT, executedby VARCHAR(80),
        tradeprice FLOAT, fee FLOAT, commission FLOAT, tax FLOAT, event_dt DATE
    """, "event_dt"),
    "bronzewatches": ("""
        cdc_flag VARCHAR(1), cdc_dsn BIGINT, w_c_id BIGINT, w_s_symb VARCHAR(20),
        w_dts DATETIME2, w_action VARCHAR(10), event_dt DATE
    """, "event_dt"),
}
for tbl, (cols, key) in BRONZE_DDL.items():
    # IF NOT EXISTS preserves any dbt-loaded rows on re-run; force_reset re-makes.
    if force_reset:
        cur.execute(f"DROP TABLE IF EXISTS [{run_schema}].[{tbl}]")
    cur.execute(f"""
        IF OBJECT_ID('[{run_schema}].[{tbl}]') IS NULL
        EXEC('CREATE TABLE [{run_schema}].[{tbl}] ( {cols.strip().rstrip(",")} )
              WITH (CLUSTER BY ([{key}]))')
    """)
    print(f"[bronze-ddl] {tbl:28s} CLUSTER BY {key}")

conn.close()

# COMMAND ----------

# 5. Emit the batch-date list for the parent's for_each loop (start 2016-07-06).
import datetime as dt
start = dt.date(2016, 7, 6)
batches = [(start + dt.timedelta(days=i)).isoformat() for i in range(incremental_n)]
dbutils.jobs.taskValues.set("batch_date_ls", batches)
print(f"emitted batch_date_ls: {len(batches)} dates, first={batches[0]}, last={batches[-1]}")
print("[done] Fabric DW setup complete.")
