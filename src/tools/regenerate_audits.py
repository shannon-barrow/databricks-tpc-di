# Databricks notebook source
# MAGIC %md
# MAGIC # Regenerate audit files from existing generated data
# MAGIC
# MAGIC Standalone notebook that scans the data files already in the volume at
# MAGIC `/Volumes/{catalog}/tpcdi_raw_data/tpcdi_volume/spark_datagen/sf={SF}/`
# MAGIC and rewrites every `*_audit.csv` using DIGen's exact counter semantics.
# MAGIC
# MAGIC Schemas + filename patterns mirror the silver/bronze ingest notebooks
# MAGIC (`incremental_batches/silver/*.sql`, `incremental_batches/bronze/*.sql`)
# MAGIC so we read the files exactly as the benchmark pipeline does.
# MAGIC
# MAGIC Use this when:
# MAGIC - Code changes have shifted what the data generator emits, but you don't
# MAGIC   want to spend ~30+ min regenerating the data itself
# MAGIC - You suspect the committed `static_audits/sf={SF}/*.csv` are stale
# MAGIC - You need to re-derive audit counts to commit a fresh static snapshot

# COMMAND ----------

dbutils.widgets.dropdown("scale_factor", "10",
                         ["10", "100", "1000", "5000", "10000", "20000"],
                         "Scale Factor")
dbutils.widgets.text("catalog", "main", "Target Catalog")

scale_factor = int(dbutils.widgets.get("scale_factor"))
catalog = dbutils.widgets.get("catalog")
volume_path = f"/Volumes/{catalog}/tpcdi_raw_data/tpcdi_volume/spark_datagen/sf={scale_factor}"
print(f"scanning {volume_path}")

# COMMAND ----------

import sys, os
_nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
_workspace_src = f"/Workspace{_nb_path.split('/src')[0]}/src"
sys.path.insert(0, f"{_workspace_src}/tools")

from tpcdi_gen.config import (
    NUM_INCREMENTAL_BATCHES, FIRST_BATCH_DATE, DATE_BEGIN, DATE_END,
    ONE_QUARTER_MS, BROKER_PCT, ScaleConfig)

from datetime import timedelta
import pyspark.sql.functions as F

cfg = ScaleConfig(scale_factor, catalog)
internal_sf = cfg.internal_sf
print(f"internal_sf={internal_sf}, NUM_INCREMENTAL_BATCHES={NUM_INCREMENTAL_BATCHES}")

# COMMAND ----------

# MAGIC %md ## Helpers + schemas
# MAGIC
# MAGIC Schemas verbatim from the benchmark pipeline notebooks:
# MAGIC - `silver/DimCustomer Incremental.sql` — Customer schema (B2/B3)
# MAGIC - `silver/DimAccount Incremental.sql` — Account schema (B2/B3)
# MAGIC - `silver/DimTrade Incremental.sql` — Trade schema (B2/B3)
# MAGIC - `silver/DimTrade Historical.sql` — Trade/TradeHistory/HoldingHistory schemas (B1)
# MAGIC - `silver/FactWatches Historical.sql` / `Incremental.sql` — WatchHistory schemas
# MAGIC - `silver/DimBroker.sql` — HR schema
# MAGIC - `gold/FactCashBalances Historical.sql` — CashTransaction schemas
# MAGIC - `bronze/ingest_dailymarketincremental.sql` — DailyMarket schemas
# MAGIC - `bronze/ingest_prospectincremental.sql` — Prospect schema

# COMMAND ----------

AUDIT_HEADER = "DataSet, BatchID ,Date , Attribute , Value, DValue\n"

def audit_row(dataset: str, batch: int, attribute: str, value) -> str:
    v = "" if value is None else str(value)
    return f"{dataset},{batch},,{attribute},{v},\n"

def write_audit(lines: list, dst: str):
    body = AUDIT_HEADER + "".join(lines)
    dbutils.fs.put(dst, body, overwrite=True)

def batch_path(b: int) -> str:
    return f"{volume_path}/Batch{b}"

def read_csv(path_glob: str, schema: str, sep: str = "|"):
    return spark.read.csv(path_glob, sep=sep, header=False, schema=schema)

# COMMAND ----------

# B1 schemas (no cdc_flag prefix; historical files are full-load)
HR_SCHEMA = ("employeeid BIGINT, managerid BIGINT, employeefirstname STRING, "
             "employeelastname STRING, employeemi STRING, employeejobcode STRING, "
             "employeebranch STRING, employeeoffice STRING, employeephone STRING")

TRADE_HIST_SCHEMA = (
    "t_id BIGINT, t_dts TIMESTAMP, t_st_id STRING, t_tt_id STRING, "
    "t_is_cash TINYINT, t_s_symb STRING, quantity INT, bidprice DOUBLE, "
    "t_ca_id BIGINT, executedby STRING, tradeprice DOUBLE, fee DOUBLE, "
    "commission DOUBLE, tax DOUBLE")

TRADE_HISTORY_HIST_SCHEMA = "tradeid BIGINT, th_dts TIMESTAMP, status STRING"

CASH_TRANS_HIST_SCHEMA = "accountid BIGINT, ct_dts TIMESTAMP, ct_amt DOUBLE, ct_name STRING"

HOLDING_HIST_SCHEMA = "hh_h_t_id BIGINT, hh_t_id BIGINT, hh_before_qty INT, hh_after_qty INT"

WATCH_HIST_SCHEMA = "w_c_id BIGINT, w_s_symb STRING, w_dts TIMESTAMP, w_action STRING"

DAILY_MARKET_HIST_SCHEMA = ("dm_date DATE, dm_s_symb STRING, dm_close DOUBLE, "
                             "dm_high DOUBLE, dm_low DOUBLE, dm_vol INT")

# B2/B3 schemas (cdc_flag/cdc_dsn prefix)
CUSTOMER_INC_SCHEMA = (
    "cdc_flag STRING, cdc_dsn BIGINT, customerid BIGINT, taxid STRING, "
    "status STRING, lastname STRING, firstname STRING, middleinitial STRING, "
    "gender STRING, tier TINYINT, dob DATE, addressline1 STRING, "
    "addressline2 STRING, postalcode STRING, city STRING, stateprov STRING, "
    "country STRING, c_ctry_1 STRING, c_area_1 STRING, c_local_1 STRING, "
    "c_ext_1 STRING, c_ctry_2 STRING, c_area_2 STRING, c_local_2 STRING, "
    "c_ext_2 STRING, c_ctry_3 STRING, c_area_3 STRING, c_local_3 STRING, "
    "c_ext_3 STRING, email1 STRING, email2 STRING, lcl_tx_id STRING, "
    "nat_tx_id STRING")

ACCOUNT_INC_SCHEMA = (
    "cdc_flag STRING, cdc_dsn BIGINT, accountid BIGINT, brokerid BIGINT, "
    "customerid BIGINT, accountdesc STRING, taxstatus TINYINT, status STRING")

TRADE_INC_SCHEMA = (
    "cdc_flag STRING, cdc_dsn BIGINT, tradeid BIGINT, t_dts TIMESTAMP, "
    "status STRING, t_tt_id STRING, cashflag TINYINT, t_s_symb STRING, "
    "quantity INT, bidprice DOUBLE, t_ca_id BIGINT, executedby STRING, "
    "tradeprice DOUBLE, fee DOUBLE, commission DOUBLE, tax DOUBLE")

HOLDING_INC_SCHEMA = ("cdc_flag STRING, cdc_dsn BIGINT, hh_h_t_id BIGINT, "
                       "hh_t_id BIGINT, hh_before_qty INT, hh_after_qty INT")

WATCH_INC_SCHEMA = ("cdc_flag STRING, cdc_dsn BIGINT, w_c_id BIGINT, "
                     "w_s_symb STRING, w_dts TIMESTAMP, w_action STRING")

CASH_TRANS_INC_SCHEMA = ("cdc_flag STRING, cdc_dsn BIGINT, accountid BIGINT, "
                          "ct_dts TIMESTAMP, ct_amt DOUBLE, ct_name STRING")

DAILY_MARKET_INC_SCHEMA = (
    "cdc_flag STRING, cdc_dsn BIGINT, dm_date DATE, dm_s_symb STRING, "
    "dm_close DOUBLE, dm_high DOUBLE, dm_low DOUBLE, dm_vol INT")

PROSPECT_SCHEMA = (
    "agencyid STRING, lastname STRING, firstname STRING, middleinitial STRING, "
    "gender STRING, addressline1 STRING, addressline2 STRING, postalcode STRING, "
    "city STRING, state STRING, country STRING, phone STRING, income STRING, "
    "numbercars INT, numberchildren INT, maritalstatus STRING, age INT, "
    "creditrating INT, ownorrentflag STRING, employer STRING, "
    "numbercreditcards INT, networth INT")

# COMMAND ----------

# MAGIC %md ## Customer audit (Customer-Audit.xml counter logic)
# MAGIC
# MAGIC From `pdgf/config/tpc-di-Audit/Customer-Audit.xml`:
# MAGIC - C_NEW: cdc_flag == "I"
# MAGIC - C_UPDCUST: cdc_flag == "U" AND status != "INAC"
# MAGIC - C_INACT: status == "INAC"
# MAGIC - C_DOB_TO: dob < batch_date - 100 years
# MAGIC - C_DOB_TY: dob > batch_date
# MAGIC - C_TIER_INV: tier in {0, 4-9} OR null/empty/non-numeric

# COMMAND ----------

def compute_customer_audit(batch_id: int) -> dict:
    """Same approach as customer.py:1349-1356 (incremental customer audit) —
    minimal positional schema covers just the 11 columns needed for the
    DIGen counters; column pruning keeps the scan fast."""
    path = f"{batch_path(batch_id)}/Customer_*.txt"
    min_schema = ("cdc_flag STRING, cdc_dsn BIGINT, c_id STRING, c_tax_id STRING, "
                  "c_st_id STRING, c_l_name STRING, c_f_name STRING, c_m_name STRING, "
                  "c_gndr STRING, c_tier STRING, c_dob STRING")
    df = read_csv(path, min_schema)

    batch_date = FIRST_BATCH_DATE + timedelta(days=batch_id - 1)
    century_start = batch_date - timedelta(days=365 * 100)

    aggs = df.select(
        F.sum(F.when(F.col("cdc_flag") == "I", 1).otherwise(0)).alias("c_new"),
        F.sum(F.when((F.col("cdc_flag") == "U") & (F.col("c_st_id") != "INAC"), 1).otherwise(0)).alias("c_updcust"),
        F.sum(F.when(F.col("c_st_id") == "INAC", 1).otherwise(0)).alias("c_inact"),
        F.sum(F.when(F.to_date(F.col("c_dob")).isNotNull() &
                     (F.to_date(F.col("c_dob")) < F.lit(century_start)),
                     1).otherwise(0)).alias("c_dob_to"),
        F.sum(F.when(F.to_date(F.col("c_dob")).isNotNull() &
                     (F.to_date(F.col("c_dob")) > F.lit(batch_date)),
                     1).otherwise(0)).alias("c_dob_ty"),
        F.sum(F.when(F.col("c_tier").isNull() | (~F.col("c_tier").isin("1", "2", "3")),
                     1).otherwise(0)).alias("c_tier_inv"),
    ).collect()[0]
    return {k: (aggs[k] or 0) for k in ("c_new", "c_updcust", "c_inact",
                                         "c_dob_to", "c_dob_ty", "c_tier_inv")}

# COMMAND ----------

# MAGIC %md ## Account audit (Account-Audit.xml counter logic)
# MAGIC - CA_ADDACCT: cdc_flag == "I"
# MAGIC - CA_CLOSEACCT: status == "INAC"
# MAGIC - CA_UPDACCT: cdc_flag == "U" AND status != "INAC"
# MAGIC - CA_ID_HIST: always -1

# COMMAND ----------

def compute_account_audit(batch_id: int) -> dict:
    """Mirrors customer.py:1349 (existing incremental Account audit). DIGen
    Account-Audit.xml semantics:
      CA_ADDACCT  = cdc_flag == "I"
      CA_CLOSEACCT = cdc_flag == "U" AND ca_st_id == "INAC"
      CA_UPDACCT  = cdc_flag == "U" AND ca_st_id == "ACTV"
      CA_ID_HIST  = -1 (sentinel)
    """
    path = f"{batch_path(batch_id)}/Account_*.txt"
    min_schema = ("cdc_flag STRING, cdc_dsn BIGINT, ca_id STRING, ca_b_id STRING, "
                  "ca_c_id STRING, ca_name STRING, ca_tax_st STRING, ca_st_id STRING")
    df = read_csv(path, min_schema)
    aggs = df.select(
        F.sum(F.when(F.col("cdc_flag") == "I", 1).otherwise(0)).alias("ca_addacct"),
        F.sum(F.when((F.col("cdc_flag") == "U") & (F.col("ca_st_id") == "INAC"), 1).otherwise(0)).alias("ca_closeacct"),
        F.sum(F.when((F.col("cdc_flag") == "U") & (F.col("ca_st_id") == "ACTV"), 1).otherwise(0)).alias("ca_updacct"),
    ).collect()[0]
    return {k: (aggs[k] or 0) for k in ("ca_addacct", "ca_closeacct", "ca_updacct")}

# COMMAND ----------

# MAGIC %md ## Trade audit (Trade-Audit.xml counter logic)
# MAGIC
# MAGIC B1 Trade.txt has no CDC prefix; we count distinct trades (one per t_id).
# MAGIC B2/B3 Trade.txt has cdc_flag/cdc_dsn prefix; T_NEW = cdc_flag='I'.

# COMMAND ----------

def compute_trade_audit(batch_id: int) -> dict:
    """Per DIGen TradeAuditVar.java:
      T_Records  = every row in Trade.txt
      T_NEW      = B1: every row; B2/B3: rows with cdc_flag == "I"
      T_Canceled = rows with th_st_id (or status) == "CNCL"
      T_InvalidCommision = rows where commission > qty * tradeprice
      T_InvalidCharge    = rows where charge > qty * tradeprice

    In B1 our Trade.txt has one row per trade (final state). In B2/B3 it
    has CDC-style rows for each status transition.
    """
    path = f"{batch_path(batch_id)}/Trade_*.txt"
    if batch_id == 1:
        # B1 minimal positional schema: only the columns needed for counters
        min_schema = ("t_id BIGINT, t_dts TIMESTAMP, t_st_id STRING, t_tt_id STRING, "
                      "t_is_cash TINYINT, t_s_symb STRING, quantity INT, bidprice DOUBLE, "
                      "t_ca_id BIGINT, executedby STRING, tradeprice DOUBLE, "
                      "fee DOUBLE, commission DOUBLE, tax DOUBLE")
        df = read_csv(path, min_schema)
        aggs = df.select(
            F.count("*").alias("t_records"),
            F.sum(F.when(F.col("t_st_id") == "CNCL", 1).otherwise(0)).alias("t_canceled"),
            F.sum(F.when(F.col("fee") > F.col("quantity") * F.col("tradeprice"), 1).otherwise(0)).alias("t_inv_chrg"),
            F.sum(F.when(F.col("commission") > F.col("quantity") * F.col("tradeprice"), 1).otherwise(0)).alias("t_inv_comm"),
        ).collect()[0]
        return {
            "t_records":  aggs["t_records"]  or 0,
            "t_new":      aggs["t_records"]  or 0,  # B1: every row counts as NEW
            "t_canceled": aggs["t_canceled"] or 0,
            "t_inv_chrg": aggs["t_inv_chrg"] or 0,
            "t_inv_comm": aggs["t_inv_comm"] or 0,
        }
    # B2/B3 minimal positional schema
    min_schema = ("cdc_flag STRING, cdc_dsn BIGINT, t_id BIGINT, t_dts TIMESTAMP, "
                  "t_st_id STRING, t_tt_id STRING, t_is_cash TINYINT, t_s_symb STRING, "
                  "quantity INT, bidprice DOUBLE, t_ca_id BIGINT, executedby STRING, "
                  "tradeprice DOUBLE, fee DOUBLE, commission DOUBLE, tax DOUBLE")
    df = read_csv(path, min_schema)
    aggs = df.select(
        F.count("*").alias("t_records"),
        F.sum(F.when(F.col("cdc_flag") == "I", 1).otherwise(0)).alias("t_new"),
        F.sum(F.when(F.col("t_st_id") == "CNCL", 1).otherwise(0)).alias("t_canceled"),
        F.sum(F.when(F.col("fee") > F.col("quantity") * F.col("tradeprice"), 1).otherwise(0)).alias("t_inv_chrg"),
        F.sum(F.when(F.col("commission") > F.col("quantity") * F.col("tradeprice"), 1).otherwise(0)).alias("t_inv_comm"),
    ).collect()[0]
    return {k: (aggs[k] or 0) for k in ("t_records", "t_new", "t_canceled",
                                         "t_inv_chrg", "t_inv_comm")}

# COMMAND ----------

# MAGIC %md ## TradeHistory + CashTransaction + HoldingHistory + DailyMarket + WatchHistory + Prospect + HR

# COMMAND ----------

def compute_trade_history_audit_b1() -> dict:
    """B1 TradeHistory: TH_Records, TH_TLB/TLS/TMB/TMS, TH_CanceledLTrades."""
    th = read_csv(f"{batch_path(1)}/TradeHistory_*.txt", TRADE_HISTORY_HIST_SCHEMA)
    tr = read_csv(f"{batch_path(1)}/Trade_*.txt", TRADE_HIST_SCHEMA) \
            .groupBy("t_id").agg(F.first("t_tt_id").alias("t_tt_id"))
    th_with_type = th.join(tr.withColumnRenamed("t_id", "tradeid"), on="tradeid", how="left")
    aggs = th_with_type.select(
        F.count("*").alias("th_records"),
        F.sum(F.when(F.col("t_tt_id") == "TLB", 1).otherwise(0)).alias("th_tlb"),
        F.sum(F.when(F.col("t_tt_id") == "TLS", 1).otherwise(0)).alias("th_tls"),
        F.sum(F.when(F.col("t_tt_id") == "TMB", 1).otherwise(0)).alias("th_tmb"),
        F.sum(F.when(F.col("t_tt_id") == "TMS", 1).otherwise(0)).alias("th_tms"),
        F.sum(F.when((F.col("status") == "CNCL") & F.col("t_tt_id").isin("TLB", "TLS"),
                     1).otherwise(0)).alias("th_canceled_l"),
    ).collect()[0]
    return {k: (aggs[k] or 0) for k in ("th_records", "th_tlb", "th_tls",
                                         "th_tmb", "th_tms", "th_canceled_l")}

def compute_cash_transaction_count(batch_id: int) -> int:
    path = f"{batch_path(batch_id)}/CashTransaction_*.txt"
    schema = CASH_TRANS_HIST_SCHEMA if batch_id == 1 else CASH_TRANS_INC_SCHEMA
    return read_csv(path, schema).count()

def compute_holding_history_count(batch_id: int) -> int:
    path = f"{batch_path(batch_id)}/HoldingHistory_*.txt"
    schema = HOLDING_HIST_SCHEMA if batch_id == 1 else HOLDING_INC_SCHEMA
    return read_csv(path, schema).count()

def compute_daily_market_count(batch_id: int) -> int:
    path = f"{batch_path(batch_id)}/DailyMarket_*.txt"
    schema = DAILY_MARKET_HIST_SCHEMA if batch_id == 1 else DAILY_MARKET_INC_SCHEMA
    return read_csv(path, schema).count()

def compute_watch_history_audit(batch_id: int) -> dict:
    path = f"{batch_path(batch_id)}/WatchHistory_*.txt"
    schema = WATCH_HIST_SCHEMA if batch_id == 1 else WATCH_INC_SCHEMA
    df = read_csv(path, schema)
    aggs = df.select(
        F.count("*").alias("wh_records"),
        F.sum(F.when(F.col("w_action") == "ACTV", 1).otherwise(0)).alias("actv"),
        F.sum(F.when(F.col("w_action") == "CNCL", 1).otherwise(0)).alias("cncl"),
    ).collect()[0]
    return {
        "wh_records": aggs["wh_records"] or 0,
        "wh_active":  (aggs["actv"] or 0) - (aggs["cncl"] or 0),  # net active
    }

def compute_prospect_count(batch_id: int) -> int:
    return read_csv(f"{batch_path(batch_id)}/Prospect_*.csv",
                     PROSPECT_SCHEMA, sep=",").count()

def compute_hr_brokers_count() -> int:
    df = read_csv(f"{batch_path(1)}/HR_*.csv", HR_SCHEMA, sep=",")
    return df.filter(F.col("employeejobcode") == "314").count()

# COMMAND ----------

# MAGIC %md ## CustomerMgmt.xml (B1) action counts

# COMMAND ----------

def compute_customermgmt_audit() -> dict:
    """Parse Batch1/CustomerMgmt_*.xml and count ActionType occurrences."""
    df = (spark.read.format("xml")
          .option("rowTag", "Action")
          .option("rootTag", "TPCDI:Actions")
          .load(f"{batch_path(1)}/CustomerMgmt_*.xml"))
    action_col = next((c for c in df.columns if c.lower() in ("_actiontype", "actiontype")), None)
    if action_col is None:
        raise RuntimeError(f"could not find ActionType in CustomerMgmt XML; columns: {df.columns}")
    rows = df.groupBy(action_col).count().collect()
    by_action = {r[action_col]: r["count"] for r in rows}
    return {
        "cm_new":       by_action.get("NEW", 0),
        "cm_addacct":   by_action.get("ADDACCT", 0),
        "cm_updacct":   by_action.get("UPDACCT", 0),
        "cm_closeacct": by_action.get("CLOSEACCT", 0),
        "cm_updcust":   by_action.get("UPDCUST", 0),
        "cm_inact":     by_action.get("INACT", 0),
    }

# COMMAND ----------

# MAGIC %md ## FINWIRE per-record counts (CMP / SEC / FIN)
# MAGIC
# MAGIC FINWIRE files are fixed-width. Per the bronze ingest, the rectype occupies
# MAGIC bytes 16-18 (3 chars) of each line: `CMP`, `SEC`, or `FIN`.

# COMMAND ----------

def compute_finwire_audit() -> dict:
    df = (spark.read.text(f"{batch_path(1)}/FINWIRE*Q*")
          .filter(~F.col("value").endswith("_audit.csv"))
          .withColumn("rec_type", F.substring(F.col("value"), 16, 3)))
    rows = df.groupBy("rec_type").count().collect()
    by_t = {r["rec_type"]: r["count"] for r in rows}
    return {
        "fw_cmp": by_t.get("CMP", 0),
        "fw_sec": by_t.get("SEC", 0),
        "fw_fin": by_t.get("FIN", 0),
    }

# COMMAND ----------

# MAGIC %md ## Run all scans

# COMMAND ----------

print("=== Customer audits (B2/B3) ===")
cust_results = {b: compute_customer_audit(b) for b in range(2, NUM_INCREMENTAL_BATCHES + 2)}
for b, r in cust_results.items(): print(f"  B{b}: {r}")

print("=== Account audits (B2/B3) ===")
acct_results = {b: compute_account_audit(b) for b in range(2, NUM_INCREMENTAL_BATCHES + 2)}
for b, r in acct_results.items(): print(f"  B{b}: {r}")

print("=== Trade audits ===")
trade_results = {b: compute_trade_audit(b) for b in range(1, NUM_INCREMENTAL_BATCHES + 2)}
for b, r in trade_results.items(): print(f"  B{b}: {r}")

print("=== TradeHistory audit (B1) ===")
th_b1 = compute_trade_history_audit_b1()
print(f"  B1: {th_b1}")

print("=== CashTransaction counts ===")
ct_counts = {b: compute_cash_transaction_count(b) for b in range(1, NUM_INCREMENTAL_BATCHES + 2)}
for b, n in ct_counts.items(): print(f"  B{b}: {n}")

print("=== HoldingHistory counts ===")
hh_counts = {b: compute_holding_history_count(b) for b in range(1, NUM_INCREMENTAL_BATCHES + 2)}
for b, n in hh_counts.items(): print(f"  B{b}: {n}")

print("=== DailyMarket counts ===")
dm_counts = {b: compute_daily_market_count(b) for b in range(1, NUM_INCREMENTAL_BATCHES + 2)}
for b, n in dm_counts.items(): print(f"  B{b}: {n}")

print("=== WatchHistory audits ===")
wh_results = {b: compute_watch_history_audit(b) for b in range(1, NUM_INCREMENTAL_BATCHES + 2)}
for b, r in wh_results.items(): print(f"  B{b}: {r}")

print("=== Prospect counts ===")
prospect_counts = {b: compute_prospect_count(b) for b in range(1, NUM_INCREMENTAL_BATCHES + 2)}
for b, n in prospect_counts.items(): print(f"  B{b}: {n}")

print("=== HR brokers ===")
hr_brokers = compute_hr_brokers_count()
print(f"  {hr_brokers}")

# COMMAND ----------

# MAGIC %md ## CustomerMgmt + FINWIRE scans (slow — separated so they can be re-run independently)
# MAGIC
# MAGIC At SF=20000+ the CustomerMgmt XML scan reads ~800 files and takes minutes;
# MAGIC FINWIRE scans ~200 quarterly fixed-width files (~1B rows). Isolating them
# MAGIC means a syntax fix to one of the lighter scans doesn't force re-running
# MAGIC the heavy ones.

# COMMAND ----------

print("=== CustomerMgmt action counts (B1) ===")
cm_b1 = compute_customermgmt_audit()
print(f"  {cm_b1}")

print("=== FINWIRE counts (B1) ===")
fw_b1 = compute_finwire_audit()
print(f"  {fw_b1}")

# COMMAND ----------

# MAGIC %md ## Write audit files
# MAGIC
# MAGIC ⚠ This OVERWRITES every `*_audit.csv` in the volume. Don't run this
# MAGIC unless you're prepared to commit the new values back to
# MAGIC `static_audits/sf={SF}/`.

# COMMAND ----------

# Batch{N}_audit.csv (date metadata, unchanged regardless of data drift)
for b in range(1, NUM_INCREMENTAL_BATCHES + 2):
    last_day = FIRST_BATCH_DATE if b == 1 else FIRST_BATCH_DATE + timedelta(days=b - 1)
    write_audit([
        f"Batch,{b},{DATE_BEGIN.strftime('%Y-%m-%d')},FirstDay,,\n",
        f"Batch,{b},{last_day.strftime('%Y-%m-%d')},LastDay,,\n",
    ], f"{volume_path}/Batch{b}_audit.csv")
print("wrote Batch{1,2,3}_audit.csv")

# COMMAND ----------

# Batch1 per-table audit files
b1 = batch_path(1)

write_audit([audit_row("DimBroker", 1, "HR_BROKERS", hr_brokers)], f"{b1}/HR_audit.csv")

cm_lines = [
    audit_row("DimAccount",  1, "CA_ADDACCT",   cm_b1["cm_addacct"]),
    audit_row("DimAccount",  1, "CA_CLOSEACCT", cm_b1["cm_closeacct"]),
    audit_row("DimAccount",  1, "CA_UPDACCT",   cm_b1["cm_updacct"]),
    audit_row("DimAccount",  1, "CA_ID_HIST",   -1),
    audit_row("DimCustomer", 1, "C_NEW",        cm_b1["cm_new"]),
    audit_row("DimCustomer", 1, "C_UPDCUST",    cm_b1["cm_updcust"]),
    audit_row("DimCustomer", 1, "C_ID_HIST",    0),
    audit_row("DimCustomer", 1, "C_INACT",      cm_b1["cm_inact"]),
    audit_row("DimCustomer", 1, "C_DOB_TO",     0),
    audit_row("DimCustomer", 1, "C_DOB_TY",     0),
    audit_row("DimCustomer", 1, "C_TIER_INV",   0),
]
write_audit(cm_lines, f"{b1}/CustomerMgmt_audit.csv")

trade_lines_b1 = [
    audit_row("DimTrade", 1, "T_Records",          trade_results[1]["t_records"]),
    audit_row("DimTrade", 1, "T_NEW",              trade_results[1]["t_new"]),
    audit_row("DimTrade", 1, "T_CanceledTrades",   trade_results[1]["t_canceled"]),
    audit_row("DimTrade", 1, "T_InvalidCommision", trade_results[1]["t_inv_comm"]),
    audit_row("DimTrade", 1, "T_InvalidCharge",    trade_results[1]["t_inv_chrg"]),
]
write_audit(trade_lines_b1, f"{b1}/Trade_audit.csv")

write_audit([audit_row("FactMarketHistory", 1, "DM_RECORDS", dm_counts[1])],
            f"{b1}/DailyMarket_audit.csv")

write_audit([
    audit_row("FactWatches", 1, "WH_ACTIVE",  wh_results[1]["wh_active"]),
    audit_row("FactWatches", 1, "WH_RECORDS", wh_results[1]["wh_records"]),
], f"{b1}/WatchHistory_audit.csv")

write_audit([audit_row("FactHoldings", 1, "HH_RECORDS", hh_counts[1])],
            f"{b1}/HoldingHistory_audit.csv")

th_lines_b1 = [
    audit_row("DimTradeHistory", 1, "TH_Records",         th_b1["th_records"]),
    audit_row("DimTradeHistory", 1, "TH_TLBTrades",       th_b1["th_tlb"]),
    audit_row("DimTradeHistory", 1, "TH_TLSTrades",       th_b1["th_tls"]),
    audit_row("DimTradeHistory", 1, "TH_TMBTrades",       th_b1["th_tmb"]),
    audit_row("DimTradeHistory", 1, "TH_TMSTrades",       th_b1["th_tms"]),
    audit_row("DimTradeHistory", 1, "TH_CanceledLTrades", th_b1["th_canceled_l"]),
    audit_row("DimTradeHistory", 1, "CT_Records",         ct_counts[1]),
    audit_row("DimTradeHistory", 1, "CT_Trades",          ct_counts[1]),
]
write_audit(th_lines_b1, f"{b1}/TradeHistory_audit.csv")

write_audit([
    audit_row("DimSecurity", 1, "FW_SEC",     fw_b1["fw_sec"]),
    audit_row("DimSecurity", 1, "FW_SEC_DUP", -1),
    audit_row("DimCompany",  1, "FW_CMP",     fw_b1["fw_cmp"]),
    audit_row("DimCompany",  1, "FW_CMP_DUP", -1),
    audit_row("Financial",   1, "FW_FIN",     fw_b1["fw_fin"]),
    audit_row("Financial",   1, "FW_FIN_DUP", -1),
], f"{b1}/FINWIRE_audit.csv")

# Prospect P_C_MATCHING is the count of prospects matching DimCustomer rows.
# It's not directly derivable from Prospect.csv alone — DIGen sets it to
# round(prospect_total * P_MATCH_PCT). Match P_MATCH_PCT=0.25.
p_total_b1 = prospect_counts[1]
p_match_b1 = int(p_total_b1 * 0.25)  # DIGen P_MATCH_PCT
write_audit([
    audit_row("Prospect", 1, "P_RECORDS",    p_total_b1),
    audit_row("Prospect", 1, "P_C_MATCHING", p_match_b1),
    audit_row("Prospect", 1, "P_NEW",        p_total_b1),
], f"{b1}/Prospect_audit.csv")

print("wrote Batch1/*_audit.csv")

# COMMAND ----------

# Batch2/Batch3 per-table audit files
churn_per_batch = max(1, int(0.005 * internal_sf * 1.2))

for b in range(2, NUM_INCREMENTAL_BATCHES + 2):
    bp = batch_path(b)
    cust = cust_results[b]
    write_audit([
        audit_row("DimCustomer", b, "C_NEW",       cust["c_new"]),
        audit_row("DimCustomer", b, "C_UPDCUST",   cust["c_updcust"]),
        audit_row("DimCustomer", b, "C_ID_HIST",   0),
        audit_row("DimCustomer", b, "C_INACT",     cust["c_inact"]),
        audit_row("DimCustomer", b, "C_DOB_TO",    cust["c_dob_to"]),
        audit_row("DimCustomer", b, "C_DOB_TY",    cust["c_dob_ty"]),
        audit_row("DimCustomer", b, "C_TIER_INV",  cust["c_tier_inv"]),
    ], f"{bp}/Customer_audit.csv")

    acct = acct_results[b]
    write_audit([
        audit_row("DimAccount", b, "CA_ADDACCT",   acct["ca_addacct"]),
        audit_row("DimAccount", b, "CA_CLOSEACCT", acct["ca_closeacct"]),
        audit_row("DimAccount", b, "CA_UPDACCT",   acct["ca_updacct"]),
        audit_row("DimAccount", b, "CA_ID_HIST",   -1),
    ], f"{bp}/Account_audit.csv")

    tr = trade_results[b]
    write_audit([
        audit_row("DimTrade", b, "T_Records",          tr["t_records"]),
        audit_row("DimTrade", b, "T_NEW",              tr["t_new"]),
        audit_row("DimTrade", b, "T_CanceledTrades",   tr["t_canceled"]),
        audit_row("DimTrade", b, "T_InvalidCommision", tr["t_inv_comm"]),
        audit_row("DimTrade", b, "T_InvalidCharge",    tr["t_inv_chrg"]),
    ], f"{bp}/Trade_audit.csv")

    wh = wh_results[b]
    write_audit([
        audit_row("FactWatches", b, "WH_ACTIVE",  wh["wh_active"]),
        audit_row("FactWatches", b, "WH_RECORDS", wh["wh_records"]),
    ], f"{bp}/WatchHistory_audit.csv")

    write_audit([audit_row("FactMarketHistory", b, "DM_RECORDS", dm_counts[b])],
                f"{bp}/DailyMarket_audit.csv")

    write_audit([audit_row("FactHoldings", b, "HH_RECORDS", hh_counts[b])],
                f"{bp}/HoldingHistory_audit.csv")

    write_audit([
        audit_row("DimTradeHistory", b, "CT_Records", ct_counts[b]),
        audit_row("DimTradeHistory", b, "CT_Trades",  ct_counts[b]),
    ], f"{bp}/TradeHistory_audit.csv")

    p_match_b = p_match_b1 + int(0.25 * churn_per_batch * (b - 1))
    write_audit([
        audit_row("Prospect", b, "P_RECORDS",    prospect_counts[b]),
        audit_row("Prospect", b, "P_C_MATCHING", p_match_b),
        audit_row("Prospect", b, "P_NEW",        churn_per_batch),
    ], f"{bp}/Prospect_audit.csv")

    print(f"wrote Batch{b}/*_audit.csv")

# COMMAND ----------

print(f"\n=== DONE === Audit files regenerated for sf={scale_factor} at {volume_path}")
print("Next: review outputs in the volume; if good, copy back to repo at")
print(f"  src/tools/tpcdi_gen/static_audits/sf={scale_factor}/")
print("then commit and push.")
