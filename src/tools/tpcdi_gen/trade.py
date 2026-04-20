"""Generate Trade, TradeHistory, CashTransaction, HoldingHistory from TradeSource.

Mirrors DIGen's TradeSourceOutput.split() logic:
- Trade.txt: one row per trade showing status at batch cutoff date
- TradeHistory.txt: one row per status transition before cutoff
- CashTransaction.txt: one row per completed trade (settlement)
- HoldingHistory.txt: one row per completed trade (quantity change)

Trade type: TLB=30%, TLS=30%, TMB=20%, TMS=20%
t_is_cash: 80% margin (0), 20% cash (1) — matching DIGen
Cancellation: only limit orders, 10% cancel rate
t_exec_name: broker full names from HR (first+last), ~14K+ unique
t_chrg/t_comm: 0.01% chance of invalid (1-1000% of trade value)
hh_h_t_id: references original buy trade for sells

Trade Status Model (PNDG / SBMT / CMPT / CNCL)
================================================
Every trade follows a state machine with up to three transitions:

  Limit orders (TLB, TLS):  PNDG -> SBMT -> CMPT   (or PNDG -> SBMT -> CNCL)
  Market orders (TMB, TMS): SBMT -> CMPT

- PNDG (Pending):   Limit order placed but not yet submitted to market.
                     Timestamp = _base_ts (random time in trade date range).
- SBMT (Submitted):  Order submitted to exchange. For limit orders, this happens
                     after a random delay (0-90 days from _base_ts). Market orders
                     start here immediately (_submit_offset = 0).
- CMPT (Completed):  Trade executed. Occurs 0-300 seconds after SBMT.
- CNCL (Cancelled):  Only limit orders can cancel (10% rate). Replaces CMPT.

Temporal Cutoff Logic
=====================
The FIRST_BATCH_DATE acts as the "snapshot date" for Batch 1. A trade's status
in Trade.txt reflects the latest transition that occurred ON OR BEFORE this cutoff:

  - If a limit order's _submit_ts > cutoff:  status = PNDG  (still waiting)
  - If not canceled and _complete_ts > cutoff:  status = SBMT  (submitted but not done)
  - If canceled:  status = CNCL
  - Otherwise:  status = CMPT  (fully completed before cutoff)

The t_dts timestamp is set to the timestamp of whichever transition determined
the current status (e.g., PNDG -> _base_ts, SBMT -> _submit_ts, CMPT -> _complete_ts).

TradeHistory Row Generation
===========================
TradeHistory records every status transition that occurred before the cutoff,
producing 1, 2, or 3 rows per trade:

  Row 1 (all trades):     Initial status. Limit orders get PNDG, market orders get SBMT.
  Row 2 (limit orders):   Only if _submit_ts <= cutoff. Status is SBMT (or CNCL if canceled).
  Row 3 (non-canceled):   Only if _complete_ts <= cutoff. Status is always CMPT.

Examples:
  - Market order completed before cutoff:  2 rows (SBMT, CMPT)
  - Limit order still pending at cutoff:   1 row  (PNDG)
  - Limit order submitted but not done:    2 rows (PNDG, SBMT)
  - Limit order completed:                 3 rows (PNDG, SBMT, CMPT)
  - Limit order canceled:                  2 rows (PNDG, CNCL)

CashTransaction and HoldingHistory
===================================
Both tables are generated ONLY for completed trades (t_st_id == "CMPT"):

CashTransaction:
  - One row per completed trade representing the cash settlement.
  - ct_amt is negative for buys (money leaves account), positive for sells.
  - ct_dts = _cash_ts, which is 0-5 days after completion (capped at cutoff).
  - ct_name is a pseudo-random string (10-100 chars) generated via MD5 hashing.

HoldingHistory:
  - One row per completed trade representing the position change.
  - For buys: hh_h_t_id = t_id (this trade creates a new holding),
              hh_before_qty = 0, hh_after_qty = t_qty.
  - For sells: hh_h_t_id = hash into a prior trade ID (the holding being sold),
               hh_before_qty = t_qty, hh_after_qty = 0.

Broker Names (t_exec_name)
==========================
t_exec_name is the full name of the broker who executed the trade. Broker names
come from the HR employee table: employees with jobcode 314 are brokers. The
_brokers temp view (created during account generation) contains broker IDs. We
reconstruct full names by hashing broker_id through the same hr_family_names and
hr_given_names dictionaries used by HR generation, producing "FirstName LastName"
strings. Each trade picks a broker via hash_key(t_id, seed) % n_brokers.

Valid Account Pool and Closed Account Exclusion
================================================
Not all accounts are valid trade targets. The account pool is sized based on how
many accounts would have been created between CM_BEGIN_DATE and TRADE_BEGIN_DATE
(a fraction of total accounts proportional to elapsed time). From this pool, any
accounts present in the _closed_accounts temp view are excluded via LEFT ANTI join.
The remaining "valid" accounts are indexed sequentially (_va_idx), and each trade
picks an account via hash_key(t_id, seed) % n_valid. This ensures no trade
references a closed account.

Incremental Trades (CDC Format)
===============================
Batches 2+ produce Trade.txt in CDC (Change Data Capture) format with two extra
leading columns: cdc_flag and cdc_dsn (data sequence number).

  - cdc_flag = "I" (Insert): ~55% of rows. These are brand-new trades that did not
    exist in Batch 1. Their t_id starts after the historical trade range
    (trade_total + offset). New limit orders start as PNDG, market orders as SBMT.

  - cdc_flag = "U" (Update): ~45% of rows. These represent status transitions on
    existing trades (t_id drawn from the historical range via hash). Updates move
    trades to CMPT (~93%) or CNCL (~7%).

  - cdc_dsn provides ordering: inserts get DSNs 1..n_new, updates get n_new+1..total.

  Each batch uses a unique seed derived from batch_id to ensure deterministic but
  distinct trade generation across batches.
"""

import concurrent.futures
import math
import threading
from datetime import timedelta
from pyspark.sql import SparkSession, functions as F, Window
from pyspark import StorageLevel
from .config import *
from .utils import write_file, seed_for, hash_key, dict_join, log, disk_cache, safe_unpersist, bulk_copy_all


def generate(spark: SparkSession, cfg, dicts: dict, dbutils) -> dict:
    """Generate all trade-related tables. Returns counts.

    Orchestrates historical (Batch 1) and incremental (Batch 2+) trade generation.
    Historical trades produce four files (Trade, TradeHistory, CashTransaction,
    HoldingHistory). Incremental batches produce only Trade.txt in CDC format.

    Builds shared lookups (valid accounts, broker names, symbol count) once and
    passes them to both historical and incremental generators to avoid redundant
    .collect()/.count() calls that were adding ~4 min per invocation.
    """
    log("[Trade] Starting generation")
    # Read pre-built valid account pool from CustomerMgmt (already cached with sequential IDs).
    # This avoids collecting millions of rows to the driver.
    valid_accts = spark.table("_valid_acct_pool")
    n_valid = valid_accts.count()
    log(f"[Trade] account pool: {n_valid} valid accounts (from _valid_acct_pool)")

    broker_names, n_brokers = _build_broker_names(spark)
    num_sec = spark.table("_symbols").count()

    # Cache broker names — used by historical + each incremental batch.
    broker_names, _broker_cleanup = disk_cache(broker_names, spark, "broker_names",
                                                volume_path=cfg.volume_path, dbutils=dbutils)

    shared = {"valid_accts": valid_accts, "n_valid": n_valid,
              "broker_names": broker_names, "n_brokers": n_brokers, "num_sec": num_sec}

    counts = {}
    hist_result = _gen_historical_trades(spark, cfg, dicts, dbutils, shared)
    counts.update(hist_result["counts"])

    with concurrent.futures.ThreadPoolExecutor(max_workers=NUM_INCREMENTAL_BATCHES) as executor:
        futures = [executor.submit(_gen_incremental_trades, spark, cfg, dicts, batch_id, dbutils, shared)
                   for batch_id in range(2, NUM_INCREMENTAL_BATCHES + 2)]
        for f in futures:
            counts.update(f.result())

    # Release trade_df now that incrementals (which read _ct/_hh_hist_batch{b}
    # temp views that reference it) are done.
    safe_unpersist(hist_result["trade_df"], hist_result["cleanup_info"])
    safe_unpersist(broker_names, _broker_cleanup)
    # Release CustomerMgmt view caches — Trade was the last consumer
    for view_name in ["_closed_accounts", "_created_accounts", "_account_owners", "_valid_acct_pool"]:
        try:
            spark.catalog.uncacheTable(view_name)
        except:
            pass
    log("[Trade] Released all Trade + CustomerMgmt view caches")

    log("[Trade] Generation complete")
    return counts



def _build_broker_names(spark):
    """Build broker full names from HR _brokers view for t_exec_name.

    DIGen uses RandomBrokerGenerator referencing HR employees with jobcode 314.
    The _brokers temp view was created during account generation and contains
    broker_id (which is the HR employeeid) and _idx (sequential index).

    To reconstruct each broker's full name, we hash broker_id through the same
    hr_family_names and hr_given_names dictionaries that the HR generator uses,
    producing "FirstName LastName" strings. This avoids reading HR CSV files
    back and instead regenerates names deterministically from the same seeds.

    Returns:
        (broker_names DataFrame with [_broker_idx, broker_name], count of brokers)
    """
    brokers = spark.table("_brokers")
    # Reconstruct full names by hashing broker_id through the HR name dictionaries.
    # broker_id IS the employeeid from HR; we use the same seeds as HR generation.
    broker_names = (brokers
        .withColumn("_fn_hash", hash_key(F.col("broker_id").cast("long"), seed_for("HR", "fn")))
        .withColumn("_ln_hash", hash_key(F.col("broker_id").cast("long"), seed_for("HR", "ln")))
    )
    broker_names = dict_join(broker_names, "hr_family_names", F.col("_fn_hash"), "_first")
    broker_names = dict_join(broker_names, "hr_given_names", F.col("_ln_hash"), "_last")
    broker_names = (broker_names
        .withColumn("broker_name", F.concat(F.col("_first"), F.lit(" "), F.col("_last")))
        .select(F.col("_idx").alias("_broker_idx"), "broker_name"))

    n_brokers = broker_names.count()
    log(f"[Trade] Broker names: {n_brokers} brokers", "DEBUG")
    return broker_names, n_brokers


def _gen_historical_trades(spark, cfg, dicts, dbutils, shared):
    """Generate Batch 1 (historical) trade data: Trade, TradeHistory, CashTransaction, HoldingHistory.

    Uses pre-built shared lookups (valid_accts, broker_names, num_sec) from generate()
    to avoid redundant .collect()/.count() calls.
    """
    valid_accts = shared["valid_accts"]
    n_valid = shared["n_valid"]
    broker_names = shared["broker_names"]
    n_brokers = shared["n_brokers"]
    num_sec = shared["num_sec"]

    symbols_df = spark.table("_symbols")
    trade_begin_s = int(TRADE_BEGIN_DATE.timestamp())
    trade_range_s = int((TRADE_END_DATE - TRADE_BEGIN_DATE).total_seconds())
    batch_cutoff_s = int(FIRST_BATCH_DATE_END.timestamp())

    # Build base trade DataFrame — one row per trade with all computed attributes.
    # All randomness is deterministic via hash_key(t_id, seed).
    trade_df = (spark.range(0, cfg.trade_total).withColumnRenamed("id", "t_id")
        # --- Trade type: TLB=30%, TLS=30%, TMB=20%, TMS=20% ---
        .withColumn("_tt_rand", hash_key(F.col("t_id"), seed_for("T", "tt")) % 100)
        .withColumn("t_tt_id",
            F.when(F.col("_tt_rand") < 30, F.lit("TLB"))
             .when(F.col("_tt_rand") < 60, F.lit("TLS"))
             .when(F.col("_tt_rand") < 80, F.lit("TMB"))
             .otherwise(F.lit("TMS")))
        # Convenience flags for buy/sell and limit/market classification
        .withColumn("_is_buy", F.col("t_tt_id").isin("TLB", "TMB"))
        .withColumn("_is_limit", F.col("t_tt_id").isin("TLB", "TLS"))
        # --- Timestamps: _base_ts is the initial order placement time ---
        .withColumn("_ts_offset", hash_key(F.col("t_id"), seed_for("T", "ts")) % trade_range_s)
        .withColumn("_base_ts", F.lit(trade_begin_s).cast("long") + F.col("_ts_offset"))
        # t_is_cash: 80% margin (0), 20% cash (1) — DIGen pattern
        .withColumn("t_is_cash", F.when(hash_key(F.col("t_id"), seed_for("T", "cash")) % 100 < 20, F.lit("1")).otherwise(F.lit("0")))
        # --- Security and account assignment via hash into indexed pools ---
        .withColumn("_sym_idx", hash_key(F.col("t_id"), seed_for("T", "sym")) % num_sec)
        .withColumn("_va_idx", hash_key(F.col("t_id"), seed_for("T", "ca")) % F.lit(n_valid))
        # t_qty: 10-10000
        .withColumn("t_qty", (hash_key(F.col("t_id"), seed_for("T", "qty")) % 9991 + 10).cast("string"))
        .withColumn("_qty_val", (hash_key(F.col("t_id"), seed_for("T", "qty")) % 9991 + 10).cast("double"))
        # t_bid_price: 1.01-10.00
        .withColumn("_bid", (hash_key(F.col("t_id"), seed_for("T", "bid")) % 900 + 101) / 100.0)
        .withColumn("t_bid_price", F.format_string("%.2f", F.col("_bid")))
        # t_trade_price: 95-105% of bid price (simulates market slippage)
        .withColumn("_tp", F.col("_bid") * (0.95 + (hash_key(F.col("t_id"), seed_for("T", "tp")) % 11) / 100.0))
        .withColumn("t_trade_price", F.format_string("%.2f", F.col("_tp")))
        .withColumn("_trade_val", F.col("_tp") * F.col("_qty_val"))
        # t_chrg: normally 0.01-0.5% of trade value; 0.01% chance of invalid (1-1000%)
        .withColumn("t_chrg", F.format_string("%.2f",
            F.when(hash_key(F.col("t_id"), seed_for("T", "chrg_inv")) % 10000 < 1,
                F.col("_trade_val") * (hash_key(F.col("t_id"), seed_for("T", "chrg_hi")) % 1000 + 1) / 100.0)
            .otherwise(
                F.col("_trade_val") * (hash_key(F.col("t_id"), seed_for("T", "chrg")) % 50 + 1) / 10000.0)))
        # t_comm: normally 0.01-0.5% of trade value; 0.01% chance of invalid (1-1000%)
        .withColumn("t_comm", F.format_string("%.2f",
            F.when(hash_key(F.col("t_id"), seed_for("T", "comm_inv")) % 10000 < 1,
                F.col("_trade_val") * (hash_key(F.col("t_id"), seed_for("T", "comm_hi")) % 1000 + 1) / 100.0)
            .otherwise(
                F.col("_trade_val") * (hash_key(F.col("t_id"), seed_for("T", "comm")) % 50 + 1) / 10000.0)))
        # t_tax: 5-40% of trade value
        .withColumn("t_tax", F.format_string("%.2f", F.col("_trade_val") * (hash_key(F.col("t_id"), seed_for("T", "tax")) % 36 + 5) / 100.0))
        # --- Cancellation: only limit orders can cancel, at a 10% rate ---
        .withColumn("_is_canceled",
            F.when(F.col("_is_limit"), hash_key(F.col("t_id"), seed_for("T", "cncl")) % 100 < 10).otherwise(F.lit(False)))
        # --- Temporal offsets for the state machine transitions ---
        # Limit orders wait 0-90 days before submission; market orders submit immediately
        .withColumn("_submit_offset",
            F.when(F.col("_is_limit"), hash_key(F.col("t_id"), seed_for("T", "sub")) % 7776000).otherwise(F.lit(0)))
        .withColumn("_submit_ts", F.col("_base_ts") + F.col("_submit_offset"))
        # Completion happens 0-300 seconds after submission
        .withColumn("_complete_ts", F.col("_submit_ts") + (hash_key(F.col("t_id"), seed_for("T", "cmp")) % 300))
        # Cash settlement: 0-5 days after completion.
        # NOT capped at cutoff — trades completing near the cutoff may settle in
        # Batch2/3, which is how DIGen routes incremental CashTransaction files.
        .withColumn("_cash_ts",
            F.col("_complete_ts") + (hash_key(F.col("t_id"), seed_for("T", "ct")) % 432000))
        # Broker name assignment: each trade hashes into the broker pool
        .withColumn("_broker_idx", hash_key(F.col("t_id"), seed_for("T", "exec")) % F.lit(n_brokers))
    )

    # --- Determine trade status at the batch cutoff date ---
    # This is the core temporal cutoff logic: we check each transition timestamp
    # against the cutoff to decide what status the trade has reached by batch time.
    cutoff = F.lit(batch_cutoff_s).cast("long")
    trade_df = trade_df.withColumn("t_st_id",
        # Limit order not yet submitted? -> still PNDG
        F.when(F.col("_is_limit") & (F.col("_submit_ts") > cutoff), F.lit("PNDG"))
        # Submitted but not yet completed (and not canceled)? -> SBMT
         .when(~F.col("_is_canceled") & (F.col("_complete_ts") > cutoff), F.lit("SBMT"))
        # Canceled (only limit orders reach here)? -> CNCL
         .when(F.col("_is_canceled"), F.lit("CNCL"))
        # All transitions complete before cutoff -> CMPT
         .otherwise(F.lit("CMPT")))

    # t_dts = timestamp of the latest status transition that occurred before cutoff.
    # This is the "as-of" timestamp shown in Trade.txt.
    trade_df = trade_df.withColumn("t_dts",
        F.when(F.col("t_st_id") == "PNDG", F.date_format(F.col("_base_ts").cast("timestamp"), "yyyy-MM-dd HH:mm:ss"))
         .when(F.col("t_st_id") == "SBMT", F.date_format(F.col("_submit_ts").cast("timestamp"), "yyyy-MM-dd HH:mm:ss"))
         .when(F.col("t_st_id") == "CNCL", F.date_format(F.col("_submit_ts").cast("timestamp"), "yyyy-MM-dd HH:mm:ss"))
         .otherwise(F.date_format(F.col("_complete_ts").cast("timestamp"), "yyyy-MM-dd HH:mm:ss")))

    # Join symbol with temporal validity check.
    # Trades must only reference securities that existed at the trade date.
    # Convert trade timestamp to a FINWIRE quarter index for comparison with
    # the symbol's creation_quarter and deactivation_quarter.
    fw_begin_s = int(FW_BEGIN_DATE.timestamp())
    quarter_secs = ONE_QUARTER_MS / 1000
    trade_df = trade_df.withColumn("_trade_quarter",
        ((F.col("_base_ts") - F.lit(fw_begin_s)) / F.lit(quarter_secs)).cast("int"))

    # Symbol 0 (always active from Q0) used as fallback for temporal violations
    _sym0 = symbols_df.filter(F.col("_idx") == 0).select("Symbol").collect()[0][0]

    trade_df = trade_df.join(
        F.broadcast(symbols_df.select(
            F.col("_idx").alias("_sym_idx"),
            F.col("Symbol").alias("_sym_name"),
            F.col("creation_quarter").alias("_sym_cq"),
            F.col("deactivation_quarter").alias("_sym_dq"))),
        on="_sym_idx", how="left")

    # If the symbol wasn't active at trade time, fall back to symbol 0.
    # Use strict > for creation_quarter (not >=) because a security created
    # mid-quarter has a SEC posting date partway through that quarter. A trade
    # earlier in the same quarter would have date(create_ts) < ds.effectivedate,
    # failing the DimSecurity temporal join even though the quarter matches.
    trade_df = trade_df.withColumn("t_s_symb",
        F.when((F.col("_trade_quarter") > F.col("_sym_cq")) &
               (F.col("_trade_quarter") < F.col("_sym_dq")),
            F.col("_sym_name"))
         .otherwise(F.lit(_sym0)))

    # Join valid (non-closed) account IDs via the sequential index
    trade_df = trade_df.join(
        valid_accts, on="_va_idx", how="left"
    ).withColumn("t_ca_id", F.col("_valid_ca_id"))


    # Join broker full names for t_exec_name (reconstructed from HR dictionaries)
    trade_df = trade_df.join(
        broker_names, on="_broker_idx", how="left"
    ).withColumn("t_exec_name", F.col("broker_name"))


    # ct_name: pseudo-random transaction description string for CashTransaction.
    # Length is 10-100 chars; content is generated by MD5-hashing the trade ID
    # and translating hex digits to letters (mimics DIGen's random string logic).
    trade_df = trade_df.withColumn("_ct_name_len", (hash_key(F.col("t_id"), seed_for("T", "ctn")) % 91 + 10).cast("int"))
    trade_df = trade_df.withColumn("_ct_name_raw",
        F.translate(F.concat(
            F.md5(F.concat(F.col("t_id").cast("string"), F.lit("ct1"))),
            F.md5(F.concat(F.col("t_id").cast("string"), F.lit("ct2"))),
            F.md5(F.concat(F.col("t_id").cast("string"), F.lit("ct3"))),
            F.md5(F.concat(F.col("t_id").cast("string"), F.lit("ct4")))),
            "0123456789abcdef",
            "ABCDEFGHIJKabcde"))


    # Materialize trade_df via disk_cache so the full DAG (which is expensive to
    # re-run — joins against symbols, broker_names, valid_accts + many hash
    # derivations) executes exactly once. All 4 downstream writes (Trade, TH,
    # CT, HH) then read from this cached copy. On serverless this is a Parquet
    # stage at {volume_path}/_staging/; on classic it's persist(DISK_ONLY).
    trade_df, _trade_df_cleanup = disk_cache(trade_df, spark, "trade_df",
                                              volume_path=cfg.volume_path, dbutils=dbutils)

    # === Write all 4 output tables ===
    def write_trade():
        """Write Trade.txt — one row per trade with status at batch cutoff."""
        out = trade_df.select(
            F.col("t_id").cast("string"), "t_dts", "t_st_id", "t_tt_id", "t_is_cash",
            "t_s_symb", "t_qty", "t_bid_price", "t_ca_id", "t_exec_name",
            "t_trade_price", "t_chrg", "t_comm", "t_tax")
        write_file(out, f"{cfg.batch_path(1)}/Trade.txt", "|", dbutils,
                   scale_factor=cfg.sf)
        return {("Trade", 1): cfg.trade_total}

    def write_trade_history():
        """Write TradeHistory.txt — one row per status transition before cutoff.

        Produces 1, 2, or 3 rows per trade depending on how far the trade
        progressed through its state machine before the batch cutoff:

        Row 1 (th1): Always emitted. Initial status for every trade.
            - Limit orders -> PNDG (order placed, waiting to submit)
            - Market orders -> SBMT (immediately submitted)

        Row 2 (th2): Only for limit orders whose _submit_ts <= cutoff.
            - Non-canceled -> SBMT (order submitted to exchange)
            - Canceled -> CNCL (order canceled instead of executing)

        Row 3 (th3): Only for non-canceled trades whose _complete_ts <= cutoff.
            - Always CMPT (trade executed successfully)

        All three sets are unioned to form the complete TradeHistory.
        """
        cutoff_ts = F.lit(batch_cutoff_s).cast("long")
        # Row 1: First status (PNDG for limit, SBMT for market) — every trade gets this
        th1 = (trade_df
            .withColumn("th_t_id", F.col("t_id").cast("string"))
            .withColumn("th_dts", F.date_format(F.col("_base_ts").cast("timestamp"), "yyyy-MM-dd HH:mm:ss"))
            .withColumn("th_st_id", F.when(F.col("_is_limit"), F.lit("PNDG")).otherwise(F.lit("SBMT")))
            .select("th_t_id", "th_dts", "th_st_id"))
        # Row 2: Submit/Cancel for limit orders (only if the transition happened before cutoff)
        th2 = (trade_df
            .filter(F.col("_is_limit") & (F.col("_submit_ts") <= cutoff_ts))
            .withColumn("th_t_id", F.col("t_id").cast("string"))
            .withColumn("th_dts", F.date_format(F.col("_submit_ts").cast("timestamp"), "yyyy-MM-dd HH:mm:ss"))
            .withColumn("th_st_id", F.when(F.col("_is_canceled"), F.lit("CNCL")).otherwise(F.lit("SBMT")))
            .select("th_t_id", "th_dts", "th_st_id"))
        # Row 3: Completion for non-canceled trades (only if completed before cutoff)
        th3 = (trade_df
            .filter(~F.col("_is_canceled") & (F.col("_complete_ts") <= cutoff_ts))
            .withColumn("th_t_id", F.col("t_id").cast("string"))
            .withColumn("th_dts", F.date_format(F.col("_complete_ts").cast("timestamp"), "yyyy-MM-dd HH:mm:ss"))
            .withColumn("th_st_id", F.lit("CMPT"))
            .select("th_t_id", "th_dts", "th_st_id"))
        th_df = th1.union(th2).union(th3)
        th_count = th_df.count()
        write_file(th_df, f"{cfg.batch_path(1)}/TradeHistory.txt", "|", dbutils,
                   scale_factor=cfg.sf)
        return {("TradeHistory", 1): th_count}

    def write_cash_transaction():
        """Write CashTransaction.txt for Batch1 AND Batch2/3.

        In DIGen, CashTransaction is routed by the cash settlement timestamp:
        - _cash_ts <= cutoff → Batch1
        - _cash_ts > cutoff → incremental batch based on calcBatch(ct_dts)
        This produces the incremental CT files that the pipeline reads.
        """
        # All non-canceled submitted trades will eventually complete and settle.
        # Includes CMPT (settled before cutoff) and SBMT (completing after cutoff).
        will_complete = trade_df.filter(~F.col("_is_canceled"))
        ct_base = (will_complete
            .withColumn("ct_ca_id", F.col("t_ca_id"))
            .withColumn("ct_dts", F.date_format(F.col("_cash_ts").cast("timestamp"), "yyyy-MM-dd HH:mm:ss"))
            .withColumn("ct_amt", F.format_string("%.2f",
                F.when(F.col("_is_buy"), -F.col("_trade_val")).otherwise(F.col("_trade_val"))))
            .withColumn("ct_name", F.substring(F.col("_ct_name_raw"), 1, F.col("_ct_name_len"))))

        # Batch1: settlement strictly before cutoff. Half-open [begin, cutoff) so
        # a timestamp of exactly batch_cutoff_s (2017-07-08 00:00:00) falls into
        # Batch 2, never Batch 1 — otherwise the same (accountid, to_date(ct_dts))
        # would appear in both batches and the FactCashBalances pipeline would
        # emit two rows for one source pair.
        ct_b1 = ct_base.filter(F.col("_cash_ts") < F.lit(batch_cutoff_s).cast("long")).select("ct_ca_id", "ct_dts", "ct_amt", "ct_name")
        ct_count = ct_b1.count()
        write_file(ct_b1, f"{cfg.batch_path(1)}/CashTransaction.txt", "|", dbutils,
                   scale_factor=cfg.sf)

        # Batch2/3: settlement after cutoff — save as temp views for writing in
        # _gen_incremental_trades.
        counts_ct = {("CashTransaction", 1): ct_count}
        for b in range(2, NUM_INCREMENTAL_BATCHES + 2):
            b_start = batch_cutoff_s + (b - 2) * 86400
            b_end = batch_cutoff_s + (b - 1) * 86400
            # Half-open [b_start, b_end) — no boundary overlap with Batch 1 or adjacent batches.
            ct_inc = (ct_base
                .filter((F.col("_cash_ts") >= F.lit(b_start).cast("long")) &
                        (F.col("_cash_ts") < F.lit(b_end).cast("long")))
                .withColumn("cdc_flag", F.lit("I"))
                .withColumn("cdc_dsn", F.monotonically_increasing_id() + 1)
                .select("cdc_flag", "cdc_dsn", "ct_ca_id", "ct_dts", "ct_amt", "ct_name"))
            ct_inc.createOrReplaceTempView(f"_ct_hist_batch{b}")
        return counts_ct

    def write_holding_history():
        """Write HoldingHistory.txt for Batch1 AND Batch2/3.

        In DIGen, HoldingHistory is routed by the completion timestamp:
        - _complete_ts <= cutoff → Batch1
        - _complete_ts > cutoff → incremental batch based on calcBatch(th_dts_cmpt)

        Only CMPT trades produce HoldingHistory. Each row tracks the change
        in holding quantity for the associated security position.

        The hh_h_t_id for sells uses hash_key % t_id to pick a trade ID that is
        guaranteed to be less than the current trade, simulating a reference to
        the original buy trade that created the holding.
        """
        # All non-canceled, submitted trades (will eventually complete).
        # Includes trades marked CMPT at cutoff AND trades marked SBMT at cutoff
        # (submitted before cutoff but completing after — these route to Batch2/3).
        will_complete = trade_df.filter(~F.col("_is_canceled"))
        hh_base = (will_complete
            .withColumn("hh_t_id", F.col("t_id").cast("string"))
            .withColumn("hh_h_t_id",
                F.when(F.col("_is_buy"), F.col("t_id").cast("string"))
                 .otherwise(
                    (hash_key(F.col("t_id"), seed_for("T", "hh_ref")) %
                        F.greatest(F.lit(1), F.col("t_id"))).cast("string")))
            .withColumn("hh_before_qty", F.when(F.col("_is_buy"), F.lit("0")).otherwise(F.col("t_qty")))
            .withColumn("hh_after_qty", F.when(F.col("_is_buy"), F.col("t_qty")).otherwise(F.lit("0"))))

        # Batch1: completed strictly before cutoff (half-open — matches CT logic).
        hh_b1 = hh_base.filter(F.col("_complete_ts") < F.lit(batch_cutoff_s).cast("long")).select("hh_h_t_id", "hh_t_id", "hh_before_qty", "hh_after_qty")
        hh_count = hh_b1.count()
        write_file(hh_b1, f"{cfg.batch_path(1)}/HoldingHistory.txt", "|", dbutils,
                   scale_factor=cfg.sf)

        # Batch2/3: completed after cutoff — save as temp views for writing in
        # _gen_incremental_trades.
        hh_est = int(cfg.trade_total * 0.927)
        counts_hh = {("HoldingHistory", 1): hh_count}
        for b in range(2, NUM_INCREMENTAL_BATCHES + 2):
            b_start = batch_cutoff_s + (b - 2) * 86400
            b_end = batch_cutoff_s + (b - 1) * 86400
            # Half-open [b_start, b_end) — consistent with CT logic.
            hh_inc = (hh_base
                .filter((F.col("_complete_ts") >= F.lit(b_start).cast("long")) &
                        (F.col("_complete_ts") < F.lit(b_end).cast("long")))
                .withColumn("cdc_flag", F.lit("I"))
                .withColumn("cdc_dsn", F.monotonically_increasing_id() + 1)
                .select("cdc_flag", "cdc_dsn", "hh_h_t_id", "hh_t_id", "hh_before_qty", "hh_after_qty"))
            hh_inc.createOrReplaceTempView(f"_hh_hist_batch{b}")
        return counts_hh

    counts = {}

    # All 4 writes in parallel — each reads from the cached trade_df.
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        futures = [
            executor.submit(write_trade),
            executor.submit(write_trade_history),
            executor.submit(write_cash_transaction),
            executor.submit(write_holding_history),
        ]
        for f in futures:
            counts.update(f.result())

    log(f"[Trade] Trade: {counts.get(('Trade',1),0):,}, TH: ~{counts.get(('TradeHistory',1),0):,}, "
        f"CT: ~{counts.get(('CashTransaction',1),0):,}, HH: ~{counts.get(('HoldingHistory',1),0):,}")

    # Per-DIGen Trade_audit.csv: T_NEW / T_CanceledTrades / T_InvalidCharge /
    # T_InvalidCommision. One extra scan over the staged trade_df (cheap —
    # it's already on disk). The invalid-charge/commission filters mirror
    # DIGen's check: count where the actual monetary amount exceeds the
    # gross trade value.
    _inv_row = trade_df.select(
        F.sum(F.when(F.col("t_st_id") == "CNCL", 1).otherwise(0)).alias("cncl"),
        F.sum(F.when(F.col("t_chrg").cast("double") > F.col("_trade_val"), 1).otherwise(0)).alias("bad_chrg"),
        F.sum(F.when(F.col("t_comm").cast("double") > F.col("_trade_val"), 1).otherwise(0)).alias("bad_comm"),
    ).collect()[0]
    counts[("T_CanceledTrades", 1)] = _inv_row["cncl"] or 0
    counts[("T_InvalidCharge", 1)] = _inv_row["bad_chrg"] or 0
    counts[("T_InvalidCommision", 1)] = _inv_row["bad_comm"] or 0  # sic — DIGen typo
    log(f"[Trade] Audit counts — canceled: {counts[('T_CanceledTrades',1)]:,}, "
        f"invalid_charge: {counts[('T_InvalidCharge',1)]:,}, "
        f"invalid_commission: {counts[('T_InvalidCommision',1)]:,}")

    # Kick off a background bulk_copy_all to drain the large Batch1 Trade/TH/CT/HH
    # part files while the incremental batches run. Reduces end-of-run
    # orchestrator copy time substantially since most parts finish copying
    # before the final bulk_copy_all is invoked.
    threading.Thread(target=bulk_copy_all, args=(dbutils, 64, "after Trade historical"),
                     daemon=True).start()

    # Do not unpersist trade_df here — _ct_hist_batch{2,3} and _hh_hist_batch{2,3}
    # temp views are lazy filters over trade_df and are consumed by
    # _gen_incremental_trades. Caller unpersists after incrementals finish.
    return {"counts": counts, "trade_df": trade_df, "cleanup_info": _trade_df_cleanup}


def _gen_incremental_trades(spark, cfg, dicts, batch_id, dbutils, shared):
    """Generate incremental Trade.txt for Batch 2+ with CDC format.

    Uses pre-built shared lookups (valid_accts, broker_names, num_sec) from generate().
    """
    valid_accts = shared["valid_accts"]
    n_valid = shared["n_valid"]
    n_brokers = shared["n_brokers"]
    num_sec = shared["num_sec"]

    symbols_df = spark.table("_symbols")
    bs = seed_for(f"T_B{batch_id}", "base")
    inc_trades = cfg.trade_inc
    bp = cfg.batch_path(batch_id)

    # DIGen incremental: ~55% new (I), ~45% updates (U)
    n_new = cfg.trade_inc_new
    n_update = inc_trades - n_new
    batch_date_str = (FIRST_BATCH_DATE + timedelta(days=batch_id - 1)).strftime("%Y-%m-%d")

    # --- New trades (cdc_flag = "I") ---
    # These are brand-new trades with t_id beyond the historical range.
    # Limit orders start as PNDG, market orders start as SBMT.
    new_df = (spark.range(0, n_new).withColumnRenamed("id", "rid")
        .withColumn("cdc_flag", F.lit("I"))
        .withColumn("cdc_dsn", (F.col("rid") + 1).cast("string"))
        # t_id offset past historical trades: trade_total + (batch-2)*inc + rid
        .withColumn("t_id", (F.col("rid") + cfg.trade_total + (batch_id - 2) * inc_trades).cast("string"))
        .withColumn("t_tt_id",
            F.when(hash_key(F.col("rid"), bs) % 100 < 30, F.lit("TLB"))
             .when(hash_key(F.col("rid"), bs) % 100 < 60, F.lit("TLS"))
             .when(hash_key(F.col("rid"), bs) % 100 < 80, F.lit("TMB"))
             .otherwise(F.lit("TMS")))
        # New trades: limit orders start PNDG, market orders start SBMT
        .withColumn("t_st_id",
            F.when(F.col("t_tt_id").isin("TLB", "TLS"), F.lit("PNDG")).otherwise(F.lit("SBMT")))
        .withColumn("t_dts", F.lit(batch_date_str))
        .withColumn("t_is_cash", F.when(hash_key(F.col("rid"), bs + 10) % 100 < 20, F.lit("1")).otherwise(F.lit("0")))
        .withColumn("_sym_idx", hash_key(F.col("rid"), bs + 1) % num_sec)
        .withColumn("t_qty", (hash_key(F.col("rid"), bs + 2) % 9991 + 10).cast("string"))
        .withColumn("t_bid_price", F.format_string("%.2f", (hash_key(F.col("rid"), bs + 3) % 900 + 101) / 100.0))
        .withColumn("_va_idx", hash_key(F.col("rid"), bs + 4) % F.lit(n_valid))
        .withColumn("_tp", (hash_key(F.col("rid"), bs + 3) % 900 + 101) / 100.0)
        .withColumn("t_trade_price", F.format_string("%.2f", F.col("_tp")))
        .withColumn("t_chrg", F.format_string("%.2f", F.col("_tp") * (hash_key(F.col("rid"), bs + 30) % 50 + 1) / 10000.0))
        .withColumn("t_comm", F.format_string("%.2f", F.col("_tp") * (hash_key(F.col("rid"), bs + 31) % 50 + 1) / 10000.0))
        .withColumn("t_tax", F.format_string("%.2f", F.col("_tp") * (hash_key(F.col("rid"), bs + 32) % 36 + 5) / 100.0))
        .withColumn("_broker_idx", hash_key(F.col("rid"), bs + 5) % F.lit(n_brokers))
    )

    # --- Update rows (cdc_flag = "U") ---
    # These represent status transitions on existing historical trades.
    # t_id is hashed into the historical range [0, trade_total).
    # ~93% complete (CMPT), ~7% cancel (CNCL).
    upd_df = (spark.range(0, n_update).withColumnRenamed("id", "rid")
        .withColumn("cdc_flag", F.lit("U"))
        # DSNs continue after the inserts for correct ordering
        .withColumn("cdc_dsn", (F.col("rid") + n_new + 1).cast("string"))
        # Hash into historical trade IDs — these are updates to existing trades
        .withColumn("t_id", (hash_key(F.col("rid"), bs + 20) % cfg.trade_total).cast("string"))
        .withColumn("t_tt_id",
            F.when(hash_key(F.col("rid"), bs + 21) % 100 < 30, F.lit("TLB"))
             .when(hash_key(F.col("rid"), bs + 21) % 100 < 60, F.lit("TLS"))
             .when(hash_key(F.col("rid"), bs + 21) % 100 < 80, F.lit("TMB"))
             .otherwise(F.lit("TMS")))
        # Updates move trades to terminal status: ~93% CMPT, ~7% CNCL
        .withColumn("t_st_id",
            F.when(hash_key(F.col("rid"), bs + 22) % 100 < 93, F.lit("CMPT")).otherwise(F.lit("CNCL")))
        .withColumn("t_dts", F.lit(batch_date_str))
        .withColumn("t_is_cash", F.when(hash_key(F.col("rid"), bs + 23) % 100 < 20, F.lit("1")).otherwise(F.lit("0")))
        .withColumn("_sym_idx", hash_key(F.col("rid"), bs + 24) % num_sec)
        .withColumn("t_qty", (hash_key(F.col("rid"), bs + 25) % 9991 + 10).cast("string"))
        .withColumn("t_bid_price", F.format_string("%.2f", (hash_key(F.col("rid"), bs + 26) % 900 + 101) / 100.0))
        .withColumn("_va_idx", hash_key(F.col("rid"), bs + 27) % F.lit(n_valid))
        .withColumn("_tp", (hash_key(F.col("rid"), bs + 26) % 900 + 101) / 100.0)
        .withColumn("t_trade_price", F.format_string("%.2f", F.col("_tp")))
        .withColumn("t_chrg", F.format_string("%.2f", F.col("_tp") * (hash_key(F.col("rid"), bs + 33) % 50 + 1) / 10000.0))
        .withColumn("t_comm", F.format_string("%.2f", F.col("_tp") * (hash_key(F.col("rid"), bs + 34) % 50 + 1) / 10000.0))
        .withColumn("t_tax", F.format_string("%.2f", F.col("_tp") * (hash_key(F.col("rid"), bs + 35) % 36 + 5) / 100.0))
        .withColumn("_broker_idx", hash_key(F.col("rid"), bs + 28) % F.lit(n_brokers))
    )

    # Union inserts and updates into a single CDC output
    common_cols = ["cdc_flag", "cdc_dsn", "t_id", "t_dts", "t_st_id", "t_tt_id", "t_is_cash",
                   "_sym_idx", "t_qty", "t_bid_price", "_va_idx", "_tp", "_broker_idx",
                   "t_trade_price", "t_chrg", "t_comm", "t_tax"]
    inc_df = new_df.select(*common_cols).union(upd_df.select(*common_cols))

    # Join symbol with temporal validity (same as historical trades).
    # Incremental trades are at batch_date (~2017-07-08), so most symbols are active,
    # but deactivated symbols still need to be excluded.
    fw_begin_s = int(FW_BEGIN_DATE.timestamp())
    quarter_secs = ONE_QUARTER_MS / 1000
    batch_date = FIRST_BATCH_DATE + timedelta(days=batch_id - 1)
    batch_quarter = int((int(batch_date.timestamp()) - fw_begin_s) / quarter_secs)
    _sym0 = symbols_df.filter(F.col("_idx") == 0).select("Symbol").collect()[0][0]

    inc_df = inc_df.join(
        F.broadcast(symbols_df.select(
            F.col("_idx").alias("_sym_idx"),
            F.col("Symbol").alias("_sym_name"),
            F.col("creation_quarter").alias("_sym_cq"),
            F.col("deactivation_quarter").alias("_sym_dq"))),
        on="_sym_idx", how="left")
    inc_df = inc_df.withColumn("t_s_symb",
        F.when((F.lit(batch_quarter) > F.col("_sym_cq")) &
               (F.lit(batch_quarter) < F.col("_sym_dq")),
            F.col("_sym_name"))
         .otherwise(F.lit(_sym0)))
    # Join valid (non-closed) account
    inc_df = inc_df.join(valid_accts, on="_va_idx", how="left").withColumn("t_ca_id", F.col("_valid_ca_id"))

    # Join broker name from shared cached lookup
    broker_names = shared["broker_names"]
    inc_df = inc_df.join(broker_names, on="_broker_idx", how="left").withColumn("t_exec_name", F.col("broker_name"))

    # Final column selection: CDC columns first, then standard trade columns
    inc_df = inc_df.select(
        "cdc_flag", "cdc_dsn", "t_id", "t_dts", "t_st_id", "t_tt_id", "t_is_cash",
        "t_s_symb", "t_qty", "t_bid_price", "t_ca_id", "t_exec_name",
        "t_trade_price", "t_chrg", "t_comm", "t_tax")

    # Per-DIGen Trade-Audit (incremental batches): T_NEW = cdc_flag='I',
    # T_CanceledTrades = t_st_id='CNCL'. InvalidCharge/Commision are 0 for
    # incremental batches (our gen doesn't inject invalid values here).
    _trade_agg = inc_df.select(
        F.sum(F.when(F.col("cdc_flag") == "I", 1).otherwise(0)).alias("t_new"),
        F.sum(F.when(F.col("t_st_id") == "CNCL", 1).otherwise(0)).alias("t_cncl"),
    ).collect()[0]
    write_file(inc_df, f"{bp}/Trade.txt", "|", dbutils, scale_factor=cfg.sf)

    # CashTransaction and HoldingHistory from historical routing only.
    # DIGen generates all trades in a unified pass and routes CT/HH by timestamp.
    # Our historical generator replicates this — trades completing/settling after
    # the cutoff are routed to Batch2/3 via temp views.
    ct_batch = spark.table(f"_ct_hist_batch{batch_id}")
    hh_batch = spark.table(f"_hh_hist_batch{batch_id}")

    ct_count = ct_batch.count()
    hh_count = hh_batch.count()
    write_file(ct_batch, f"{bp}/CashTransaction.txt", "|", dbutils, scale_factor=cfg.sf)
    write_file(hh_batch, f"{bp}/HoldingHistory.txt", "|", dbutils, scale_factor=cfg.sf)

    log(f"[Trade] Batch{batch_id}: {inc_trades} ({n_new} I, {n_update} U), CT: {ct_count}, HH: {hh_count}")
    return {
        ("Trade", batch_id): inc_trades,
        ("CashTransaction", batch_id): ct_count,
        ("HoldingHistory", batch_id): hh_count,
        ("TI_NEW", batch_id):              _trade_agg["t_new"] or 0,
        ("TI_CanceledTrades", batch_id):   _trade_agg["t_cncl"] or 0,
        ("TI_InvalidCharge", batch_id):    0,
        ("TI_InvalidCommision", batch_id): 0,
    }
