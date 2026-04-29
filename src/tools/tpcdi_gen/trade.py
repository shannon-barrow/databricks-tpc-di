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
from datetime import datetime, timedelta
from pyspark.sql import SparkSession, functions as F, Window
from pyspark import StorageLevel
from .config import *
from .utils import write_file, seed_for, hash_key, dict_join, log, disk_cache, safe_unpersist
from .audit import static_audits_available


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
    # Account pool size is analytical (see Config.n_available_accounts). CA_IDs are sequential 0..n_valid-1 — no view/join needed; Trade uses its hash-derived _va_idx directly as the CA_ID. Combined with the analytical n_brokers below, Trade no longer depends on CustomerMgmt at all, and only waits on HR when we're regenerating audits dynamically.
    n_valid = cfg.n_available_accounts
    log(f"[Trade] account pool: {n_valid} valid accounts (analytical)")

    # n_brokers lets each trade pick a broker index (hash_key % n_brokers). Trade does NOT join to _brokers — t_exec_name is derived inline from _broker_idx via dict_joins on HR name dictionaries. So any n_brokers value that stays stable across the run works; tiny estimate↔exact variance is harmless. Use the analytical estimate when static audits are present (lets Trade start without waiting on HR); use the exact count when we're regenerating audits dynamically to keep audit counts as faithful to the underlying HR data as possible.
    if static_audits_available(cfg):
        n_brokers = cfg.n_brokers_estimate
    else:
        n_brokers = spark.table("_brokers").count()
    num_sec = spark.table("_symbols").count()

    shared = {"n_valid": n_valid, "n_brokers": n_brokers, "num_sec": num_sec}

    counts = {}
    hist_result = _gen_historical_trades(spark, cfg, dicts, dbutils, shared)
    counts.update(hist_result["counts"])

    if not cfg.augmented_incremental:
        with concurrent.futures.ThreadPoolExecutor(max_workers=NUM_INCREMENTAL_BATCHES) as executor:
            futures = [executor.submit(_gen_incremental_trades, spark, cfg, dicts, batch_id, dbutils, shared)
                       for batch_id in range(2, NUM_INCREMENTAL_BATCHES + 2)]
            for f in futures:
                counts.update(f.result())

    # Release trade_df now that incrementals (which read _ct/_hh_hist_batch{b} temp views that reference it) are done.
    safe_unpersist(hist_result["trade_df"], hist_result["cleanup_info"])
    # Release CustomerMgmt view caches — Trade was the last consumer. Serverless rejects UNCACHE TABLE; skip entirely there (views are session-scoped and go away at session end anyway).
    from .utils import _detect_serverless
    if not _detect_serverless(spark):
        for view_name in ["_account_owners"]:
            try:
                spark.catalog.uncacheTable(view_name)
            except:
                pass
    log("[Trade] Released all Trade + CustomerMgmt view caches")

    log("[Trade] Generation complete")
    return counts



def _gen_historical_trades(spark, cfg, dicts, dbutils, shared):
    """Generate Batch 1 (historical) trade data: Trade, TradeHistory, CashTransaction, HoldingHistory.

    Uses pre-built shared lookups (valid_accts, broker_names, num_sec) from generate()
    to avoid redundant .collect()/.count() calls.
    """
    n_valid = shared["n_valid"]
    n_brokers = shared["n_brokers"]
    num_sec = shared["num_sec"]

    symbols_df = spark.table("_symbols")
    trade_begin_s = int(TRADE_BEGIN_DATE.timestamp())
    # Historical trades' _base_ts must land strictly before FIRST_BATCH_DATE — otherwise DimTrade rows for Batch 1 would have CreateDate >= LastDay, failing the automated_audit 'DimTrade date check'. The incremental batches own the post-cutoff window and write their own new trades via _gen_incremental_trades.
    trade_range_s = int((FIRST_BATCH_DATE - TRADE_BEGIN_DATE).total_seconds())
    batch_cutoff_s = int(FIRST_BATCH_DATE_END.timestamp())

    # Build base trade DataFrame — one row per trade with all computed attributes. All randomness is deterministic via hash_key(t_id, seed).
    #
    # Partition sizing: each trade row grows from 8 bytes (just id) to ~hundreds of bytes as 30+ withColumns and the broadcast join against _symbols add fields. Default partitioning at SF=20000 (~24 tasks) was putting ~108M rows/task → 237+ GB spill inside the "range" stage. Tie partition count to scale factor so per-partition row count stays manageable (~1M rows/partition at all scales). At SF=20000: 2500 partitions.
    target_trade_partitions = max(8, cfg.sf // 8)
    trade_df = (spark.range(0, cfg.trade_total, numPartitions=target_trade_partitions)
        .withColumnRenamed("id", "t_id")
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
        # t_chrg / t_comm: 0.01-0.5% of trade value. We intentionally do NOT inject DQ-invalid values (charge/commission > trade value). See project_no_dq_injection memory: DIGen's purposeful DQ injection is out of scope; our audit reports 0 T_InvalidCharge/Commision which matches our ETL emitting 0 alerts.
        .withColumn("t_chrg", F.format_string("%.2f",
            F.col("_trade_val") * (hash_key(F.col("t_id"), seed_for("T", "chrg")) % 50 + 1) / 10000.0))
        .withColumn("t_comm", F.format_string("%.2f",
            F.col("_trade_val") * (hash_key(F.col("t_id"), seed_for("T", "comm")) % 50 + 1) / 10000.0))
        # t_tax: 5-40% of trade value
        .withColumn("t_tax", F.format_string("%.2f", F.col("_trade_val") * (hash_key(F.col("t_id"), seed_for("T", "tax")) % 36 + 5) / 100.0))
        # --- Cancellation: only limit orders can cancel, at a 10% rate ---
        .withColumn("_is_canceled",
            F.when(F.col("_is_limit"), hash_key(F.col("t_id"), seed_for("T", "cncl")) % 100 < 10).otherwise(F.lit(False)))
        # --- Temporal offsets for the state machine transitions ---
        # Limit orders wait 1-90 days before submission; market orders submit immediately. The +1 ensures _submit_ts is strictly greater than _base_ts for limit orders — otherwise TH's th1 (PNDG at _base_ts) and th2 (CNCL/SBMT at _submit_ts) share a timestamp and the DimTrade silver's max_by(struct(th_dts, status)) ties non- deterministically. 'PNDG' sorts after 'CNCL' alphabetically so a canceled trade gets its status picked as PNDG, silently breaking the 'DimTrade canceled trades' audit check at higher scale factors where the 1/7776000 collision probability bites.
        .withColumn("_submit_offset",
            F.when(F.col("_is_limit"), hash_key(F.col("t_id"), seed_for("T", "sub")) % 7776000 + 1).otherwise(F.lit(0)))
        .withColumn("_submit_ts", F.col("_base_ts") + F.col("_submit_offset"))
        # Completion happens 0-300 seconds after submission +1 to the modulo so _complete_ts is strictly greater than _submit_ts. Without this, ~0.3% of trades get complete_ts == submit_ts, which means TH's SBMT row and CMPT row share the same th_dts, and the DimTrade silver's max_by(struct(th_dts, status)) ties non- deterministically — sometimes picking SBMT, leaving sk_closedateid NULL on a supposedly-completed trade. That breaks the 'FactHoldings CurrentTradeID' and 'FactHoldings SK_*' audit checks.
        .withColumn("_complete_ts", F.col("_submit_ts") + (hash_key(F.col("t_id"), seed_for("T", "cmp")) % 300 + 1))
        # Cash settlement: 0-5 days after completion. NOT capped at cutoff — trades completing near the cutoff may settle in Batch2/3, which is how DIGen routes incremental CashTransaction files.
        .withColumn("_cash_ts",
            F.col("_complete_ts") + (hash_key(F.col("t_id"), seed_for("T", "ct")) % 432000))
        # Broker name assignment: each trade hashes into the broker pool
        .withColumn("_broker_idx", hash_key(F.col("t_id"), seed_for("T", "exec")) % F.lit(n_brokers))
    )

    # --- Determine trade status at the batch cutoff date ---
    # This is the core temporal cutoff logic: we check each transition timestamp against the cutoff to decide what status the trade has reached by batch time.
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

    # t_dts = timestamp of the latest status transition that occurred before cutoff. This is the "as-of" timestamp shown in Trade.txt.
    trade_df = trade_df.withColumn("t_dts",
        F.when(F.col("t_st_id") == "PNDG", F.date_format(F.col("_base_ts").cast("timestamp"), "yyyy-MM-dd HH:mm:ss"))
         .when(F.col("t_st_id") == "SBMT", F.date_format(F.col("_submit_ts").cast("timestamp"), "yyyy-MM-dd HH:mm:ss"))
         .when(F.col("t_st_id") == "CNCL", F.date_format(F.col("_submit_ts").cast("timestamp"), "yyyy-MM-dd HH:mm:ss"))
         .otherwise(F.date_format(F.col("_complete_ts").cast("timestamp"), "yyyy-MM-dd HH:mm:ss")))

    # Join symbol with temporal validity check. Trades must only reference securities that existed at the trade date. Convert trade timestamp to a FINWIRE quarter index for comparison with the symbol's creation_quarter and deactivation_quarter.
    fw_begin_s = int(FW_BEGIN_DATE.timestamp())
    quarter_secs = ONE_QUARTER_MS / 1000
    trade_df = trade_df.withColumn("_trade_quarter",
        ((F.col("_base_ts") - F.lit(fw_begin_s)) / F.lit(quarter_secs)).cast("int"))

    # Symbol 0 (always active from Q0) used as fallback for temporal violations
    _sym0 = symbols_df.filter(F.col("_idx") == 0).select("Symbol").collect()[0][0]

    trade_df = trade_df.join(
        symbols_df.select(
            F.col("_idx").alias("_sym_idx"),
            F.col("Symbol").alias("_sym_name"),
            F.col("creation_quarter").alias("_sym_cq"),
            F.col("deactivation_quarter").alias("_sym_dq")),
        on="_sym_idx", how="left")

    # If the symbol wasn't active at trade time, fall back to symbol 0. Use strict > for creation_quarter (not >=) because a security created mid-quarter has a SEC posting date partway through that quarter. A trade earlier in the same quarter would have date(create_ts) < ds.effectivedate, failing the DimSecurity temporal join even though the quarter matches.
    trade_df = trade_df.withColumn("t_s_symb",
        F.when((F.col("_trade_quarter") > F.col("_sym_cq")) &
               (F.col("_trade_quarter") < F.col("_sym_dq")),
            F.col("_sym_name"))
         .otherwise(F.lit(_sym0)))

    # CA_ID is the hash-derived _va_idx directly (sequential 0..n_valid-1).
    trade_df = trade_df.withColumn("t_ca_id", F.col("_va_idx").cast("string"))


    # t_exec_name is computed lazily at write time (inline dict_joins on the already-present _broker_idx column) — see write_trade() below. Avoids staging ~26 GB of broker_name strings at SF=5000 and skips a 1.33B × 7.5M shuffle-hash join that the pre-fix code issued here.

    # ct_name: pseudo-random transaction description string for CashTransaction. Length is 10-100 chars; content is generated by MD5-hashing the trade ID and translating hex digits to letters (mimics DIGen's random string logic).
    trade_df = trade_df.withColumn("_ct_name_len", (hash_key(F.col("t_id"), seed_for("T", "ctn")) % 91 + 10).cast("int"))
    trade_df = trade_df.withColumn("_ct_name_raw",
        F.translate(F.concat(
            F.md5(F.concat(F.col("t_id").cast("string"), F.lit("ct1"))),
            F.md5(F.concat(F.col("t_id").cast("string"), F.lit("ct2"))),
            F.md5(F.concat(F.col("t_id").cast("string"), F.lit("ct3"))),
            F.md5(F.concat(F.col("t_id").cast("string"), F.lit("ct4")))),
            "0123456789abcdef",
            "ABCDEFGHIJKabcde"))


    # Materialize trade_df via disk_cache so the full DAG (which is expensive to re-run — joins against symbols, broker_names, valid_accts + many hash derivations) executes exactly once. All 4 downstream writes (Trade, TH, CT, HH) then read from this cached copy. On serverless this is a Parquet stage at {volume_path}/_staging/; on classic it's persist(DISK_ONLY).
    trade_df, _trade_df_cleanup = disk_cache(trade_df, spark, "trade_df",
                                              volume_path=cfg.volume_path, dbutils=dbutils)

    # === Write all 4 output tables ===
    def write_trade():
        """Write Trade.txt — one row per trade with status at batch cutoff.

        t_exec_name (broker full name) is computed here inline via dict_joins
        on _broker_idx rather than joining a 7.5M-row broker_names table
        (which required a shuffle-hash join at SF=5000+ and inflated the
        trade_df parquet stage by ~26 GB). The HR name dictionaries are
        small (<300 KB combined) and auto-broadcast.
        """
        from .utils import dict_count
        tx = trade_df
        tx = tx.withColumn("_bk_fn_hash", hash_key(F.col("_broker_idx").cast("long"), seed_for("T", "bk_fn")) % F.lit(dict_count("hr_given_names")))
        tx = tx.withColumn("_bk_ln_hash", hash_key(F.col("_broker_idx").cast("long"), seed_for("T", "bk_ln")) % F.lit(dict_count("hr_family_names")))
        tx = dict_join(tx, "hr_given_names",  F.col("_bk_fn_hash"), "_bk_first")
        tx = dict_join(tx, "hr_family_names", F.col("_bk_ln_hash"), "_bk_last")
        tx = tx.withColumn("t_exec_name", F.concat(F.col("_bk_first"), F.lit(" "), F.col("_bk_last")))
        out = tx.select(
            F.col("t_id").cast("string"), "t_dts", "t_st_id", "t_tt_id", "t_is_cash",
            "t_s_symb", "t_qty", "t_bid_price", "t_ca_id", "t_exec_name",
            "t_trade_price", "t_chrg", "t_comm", "t_tax")
        if cfg.augmented_incremental:
            # Align Delta column names with the canonical Trade.txt schema (downstream stage_tables / stage_files SQL is written against the file shape).
            out_p = out.toDF(
                "t_id", "t_dts", "t_st_id", "t_tt_id", "t_is_cash",
                "t_s_symb", "quantity", "bidprice", "t_ca_id", "executedby",
                "tradeprice", "fee", "commission", "tax")
            from .utils import write_delta
            # Filter out post-window rows entirely so they never land in the temp Delta.
            out_p = (out_p
                .filter(F.col("t_dts") < F.lit(AUG_FILES_DATE_END_EXCL))
                .withColumn("stg_target",
                    F.when(F.col("t_dts") < F.lit(AUG_FILES_DATE_START), F.lit("tables"))
                     .otherwise(F.lit("files"))))
            write_delta(out_p, cfg=cfg, dataset="trade",
                        partition_cols=["stg_target"])
        else:
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
        # Row 3: Completion for non-canceled trades (only if completed strictly before cutoff). Must use `<` (not `<=`) to match hh_b1's filter — otherwise a trade with _complete_ts == cutoff produces a CMPT TH row in B1 AND lands in _hh_hist_batch2 (which uses `>= cutoff`), causing the B2 incremental MERGE to INSERT a duplicate DimTrade row (the existing B1 row's sk_closedateid is set, failing the MERGE's `ON t.sk_closedateid IS NULL` predicate). Seen at SF=5000+ as rare (~4 trades) duplicate tradeids breaking DimTrade distinct-keys audit.
        th3 = (trade_df
            .filter(~F.col("_is_canceled") & (F.col("_complete_ts") < cutoff_ts))
            .withColumn("th_t_id", F.col("t_id").cast("string"))
            .withColumn("th_dts", F.date_format(F.col("_complete_ts").cast("timestamp"), "yyyy-MM-dd HH:mm:ss"))
            .withColumn("th_st_id", F.lit("CMPT"))
            .select("th_t_id", "th_dts", "th_st_id"))
        th_df = th1.union(th2).union(th3)
        if cfg.augmented_incremental:
            # Align Delta column names with TradeHistory.txt schema; filter out post-window rows so they never land in the temp Delta.
            th_p = (th_df
                .toDF("tradeid", "th_dts", "status")
                .filter(F.col("th_dts") < F.lit(AUG_FILES_DATE_END_EXCL))
                .withColumn("stg_target",
                    F.when(F.col("th_dts") < F.lit(AUG_FILES_DATE_START), F.lit("tables"))
                     .otherwise(F.lit("files"))))
            from .utils import write_delta
            write_delta(th_p, cfg=cfg, dataset="tradehistory",
                        partition_cols=["stg_target"])
        else:
            write_file(th_df, f"{cfg.batch_path(1)}/TradeHistory.txt", "|", dbutils,
                       scale_factor=cfg.sf)
        # Analytical estimates for DimTradeHistory counts — always returned so the main-path log reports consistent values. Per DIGen's trade-type distribution: TLB/TLS = 30% each (limit orders), TMB/TMS = 20% each. TH rows per trade: limit orders average 2.9 (PNDG + SBMT/CNCL + CMPT if not canceled), market orders 2 (PNDG + CMPT). Canceled = 10% of limits.
        tt = cfg.trade_total
        est = {
            ("TradeHistory", 1):       int(tt * 2.54),
            ("TH_Records", 1):         int(tt * 2.54),
            ("TH_TLBTrades", 1):       int(tt * 0.87),
            ("TH_TLSTrades", 1):       int(tt * 0.87),
            ("TH_TMBTrades", 1):       int(tt * 0.4),
            ("TH_TMSTrades", 1):       int(tt * 0.4),
            ("TH_CanceledLTrades", 1): int(tt * 0.06),
        }
        if static_audits_available(cfg):
            return est  # static audit CSV handles exact values; estimates for log only
        # Dynamic audit regeneration path: compute exact counts (3-5 min scan over ~2-3B TH rows at SF=5000+). Only runs for unknown SFs.
        th_with_type = th_df.join(
            trade_df.select(
                F.col("t_id").cast("string").alias("th_t_id"),
                F.col("t_tt_id"),
                F.col("_is_canceled")),
            on="th_t_id", how="left")
        th_by_type = th_with_type.groupBy("t_tt_id").count().collect()
        tt_counts = {r["t_tt_id"]: r["count"] for r in th_by_type}
        th_total = sum(tt_counts.values())
        th_cncl_count = th_with_type.filter(F.col("th_st_id") == "CNCL").count()
        return {
            ("TradeHistory", 1):       th_total,
            ("TH_Records", 1):         th_total,
            ("TH_TLBTrades", 1):       tt_counts.get("TLB", 0),
            ("TH_TLSTrades", 1):       tt_counts.get("TLS", 0),
            ("TH_TMBTrades", 1):       tt_counts.get("TMB", 0),
            ("TH_TMSTrades", 1):       tt_counts.get("TMS", 0),
            ("TH_CanceledLTrades", 1): th_cncl_count,
        }

    def write_cash_transaction():
        """Write CashTransaction.txt for Batch1 AND Batch2/3.

        In DIGen, CashTransaction is routed by the cash settlement timestamp:
        - _cash_ts <= cutoff → Batch1
        - _cash_ts > cutoff → incremental batch based on calcBatch(ct_dts)
        This produces the incremental CT files that the pipeline reads.
        """
        # All non-canceled submitted trades will eventually complete and settle. Includes CMPT (settled before cutoff) and SBMT (completing after cutoff).
        will_complete = trade_df.filter(~F.col("_is_canceled"))
        ct_base = (will_complete
            .withColumn("ct_ca_id", F.col("t_ca_id"))
            .withColumn("ct_dts", F.date_format(F.col("_cash_ts").cast("timestamp"), "yyyy-MM-dd HH:mm:ss"))
            .withColumn("ct_amt", F.format_string("%.2f",
                F.when(F.col("_is_buy"), -F.col("_trade_val")).otherwise(F.col("_trade_val"))))
            .withColumn("ct_name", F.substring(F.col("_ct_name_raw"), 1, F.col("_ct_name_len"))))

        # Batch1: settlement strictly before cutoff. Half-open [begin, cutoff) so a timestamp of exactly batch_cutoff_s (2017-07-08 00:00:00) falls into Batch 2, never Batch 1 — otherwise the same (accountid, to_date(ct_dts)) would appear in both batches and the FactCashBalances pipeline would emit two rows for one source pair.
        ct_b1 = ct_base.filter(F.col("_cash_ts") < F.lit(batch_cutoff_s).cast("long")).select("ct_ca_id", "ct_dts", "ct_amt", "ct_name")
        if cfg.augmented_incremental:
            # Align Delta column names with CashTransaction.txt schema; filter out post-window rows.
            ct_p = (ct_b1
                .toDF("accountid", "ct_dts", "ct_amt", "ct_name")
                .filter(F.col("ct_dts") < F.lit(AUG_FILES_DATE_END_EXCL))
                .withColumn("stg_target",
                    F.when(F.col("ct_dts") < F.lit(AUG_FILES_DATE_START), F.lit("tables"))
                     .otherwise(F.lit("files"))))
            from .utils import write_delta
            write_delta(ct_p, cfg=cfg, dataset="cashtransaction",
                        partition_cols=["stg_target"])
        else:
            write_file(ct_b1, f"{cfg.batch_path(1)}/CashTransaction.txt", "|", dbutils,
                       scale_factor=cfg.sf)

        # Analytical B1 estimate: CT = one per non-canceled trade that settled before cutoff. ~94% non-canceled × ~99.7% settled-before-cutoff.
        tt = cfg.trade_total
        ct_count_est = int(tt * 0.94 * 0.997)
        counts_ct = {
            ("CashTransaction", 1): ct_count_est,
            ("CT_Records", 1):      ct_count_est,
            ("CT_Trades", 1):       ct_count_est,
        }

        # Batch2/3 incremental temp views (views are required for downstream incremental Trade writes — the temp view creation itself is lazy).
        for b in range(2, NUM_INCREMENTAL_BATCHES + 2):
            b_start = batch_cutoff_s + (b - 2) * 86400
            b_end = batch_cutoff_s + (b - 1) * 86400
            ct_inc = (ct_base
                .filter((F.col("_cash_ts") >= F.lit(b_start).cast("long")) &
                        (F.col("_cash_ts") < F.lit(b_end).cast("long")))
                .withColumn("cdc_flag", F.lit("I"))
                .withColumn("cdc_dsn", F.monotonically_increasing_id() + 1)
                .select("cdc_flag", "cdc_dsn", "ct_ca_id", "ct_dts", "ct_amt", "ct_name"))
            ct_inc.createOrReplaceTempView(f"_ct_hist_batch{b}")

        if static_audits_available(cfg):
            # Estimate incremental CT ≈ trade_inc × non-canceled pct
            ct_inc_est = int(cfg.trade_inc * 0.94)
            for b in range(2, NUM_INCREMENTAL_BATCHES + 2):
                counts_ct[("CT_Records", b)] = ct_inc_est
                counts_ct[("CT_Trades", b)]  = ct_inc_est
            return counts_ct

        # Dynamic audit regeneration — need exact per-batch counts
        ct_count = ct_b1.count()
        counts_ct[("CashTransaction", 1)] = ct_count
        counts_ct[("CT_Records", 1)] = ct_count
        counts_ct[("CT_Trades", 1)] = ct_count
        for b in range(2, NUM_INCREMENTAL_BATCHES + 2):
            ct_inc_count = spark.table(f"_ct_hist_batch{b}").count()
            counts_ct[("CT_Records", b)] = ct_inc_count
            counts_ct[("CT_Trades", b)]  = ct_inc_count
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
        # All non-canceled, submitted trades (will eventually complete). Includes trades marked CMPT at cutoff AND trades marked SBMT at cutoff (submitted before cutoff but completing after — these route to Batch2/3).
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
        # HoldingHistory: route by completion-timestamp into tables (< 2015-07-06) vs files (>=) when augmented. event_dt is included so the stage_files notebook can partition per-day without a join back to trade (the DIGen splitter does that join via max(event_dt) per tradeid; we already have _complete_ts here).
        if cfg.augmented_incremental:
            cutoff_2015 = int(datetime(2015, 7, 6).timestamp())
            cutoff_2017 = int(datetime(2017, 7, 5).timestamp())
            hh_b1 = (hh_base
                .filter((F.col("_complete_ts") < F.lit(batch_cutoff_s).cast("long")) &
                        (F.col("_complete_ts") < F.lit(cutoff_2017).cast("long")))
                .withColumn("event_dt",
                    F.to_date(F.col("_complete_ts").cast("timestamp")))
                .withColumn("stg_target",
                    F.when(F.col("_complete_ts") < F.lit(cutoff_2015).cast("long"), F.lit("tables"))
                     .otherwise(F.lit("files")))
                .select("hh_h_t_id", "hh_t_id", "hh_before_qty", "hh_after_qty",
                        "event_dt", "stg_target"))
            from .utils import write_delta
            write_delta(hh_b1, cfg=cfg, dataset="holdinghistory",
                        partition_cols=["stg_target"])
        else:
            hh_b1 = hh_base.filter(F.col("_complete_ts") < F.lit(batch_cutoff_s).cast("long")).select("hh_h_t_id", "hh_t_id", "hh_before_qty", "hh_after_qty")
            write_file(hh_b1, f"{cfg.batch_path(1)}/HoldingHistory.txt", "|", dbutils,
                       scale_factor=cfg.sf)
        # Analytical estimate: HH = one per non-canceled trade completed before cutoff (same as CT).
        hh_count = int(cfg.trade_total * 0.94 * 0.997) if static_audits_available(cfg) else hh_b1.count()

        # Batch2/3: completed after cutoff — save as temp views for writing in _gen_incremental_trades. Also save SBMT/CNCL transition views (trades whose _submit_ts falls in the batch window — these produce PNDG→SBMT or PNDG→CNCL U rows in incremental Trade.txt, closing the DimTrade T_Records B2/B3 -40% drift vs DIGen.
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

            # Trades (limit orders) with _submit_ts in this batch window. Each emits a PNDG→SBMT (or PNDG→CNCL) U row in Trade.txt. Note: keep (t_id, _is_canceled) columns; other fields are generated fresh in _gen_incremental_trades (matching the existing CMPT-U behavior of using hash-based field values).
            sub_inc = (trade_df
                .filter(F.col("_is_limit") &
                        (F.col("_submit_ts") >= F.lit(b_start).cast("long")) &
                        (F.col("_submit_ts") < F.lit(b_end).cast("long")))
                .select(F.col("t_id"), F.col("_is_canceled")))
            sub_inc.createOrReplaceTempView(f"_t_submit_hist_batch{b}")
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

    # Per-DIGen Trade_audit.csv: T_NEW / T_CanceledTrades / T_InvalidCharge / T_InvalidCommision. Analytical estimates used for the log line below and as defaults for the audit CSV; when no static snapshot is available for this SF we fall back to exact scans over staged trade_df. Canceled = 10% of limit orders = 6% of trade_total (deterministic hash). Invalid charge/commission = 0 (we don't inject DQ per project memory).
    counts[("T_CanceledTrades", 1)]    = int(cfg.trade_total * 0.06)
    counts[("T_InvalidCharge", 1)]     = 0
    counts[("T_InvalidCommision", 1)]  = 0  # sic — DIGen typo

    if not static_audits_available(cfg):
        # Dynamic audit regeneration: exact scans for unknown SFs. Use rounded t_trade_price * t_qty (not raw _trade_val = _tp * _qty) to match the ETL's DimTrade alert check exactly.
        _tp_x_qty = F.col("t_trade_price").cast("double") * F.col("t_qty").cast("double")
        # Exclude trades updated in B2/B3 via the incremental DimTrade MERGE — those overwrite comm/chrg with valid values, so counting them here would over-count T_Invalid*.
        updated_tids = (
            spark.table("_hh_hist_batch2").select(F.col("hh_t_id").alias("_upd_t_id"))
            .unionByName(spark.table("_hh_hist_batch3").select(F.col("hh_t_id").alias("_upd_t_id")))
            .unionByName(spark.table("_t_submit_hist_batch2").select(F.col("t_id").cast("string").alias("_upd_t_id")))
            .unionByName(spark.table("_t_submit_hist_batch3").select(F.col("t_id").cast("string").alias("_upd_t_id")))
            .distinct()
        )
        _surv = trade_df.join(updated_tids,
                              F.col("t_id") == F.col("_upd_t_id"), "left_anti")
        _inv_row = trade_df.select(
            F.sum(F.when(F.col("t_st_id") == "CNCL", 1).otherwise(0)).alias("cncl"),
        ).collect()[0]
        _bad_row = _surv.select(
            F.sum(F.when(F.col("t_chrg").cast("double") > _tp_x_qty, 1).otherwise(0)).alias("bad_chrg"),
            F.sum(F.when(F.col("t_comm").cast("double") > _tp_x_qty, 1).otherwise(0)).alias("bad_comm"),
        ).collect()[0]
        counts[("T_CanceledTrades", 1)]   = _inv_row["cncl"] or 0
        counts[("T_InvalidCharge", 1)]    = _bad_row["bad_chrg"] or 0
        counts[("T_InvalidCommision", 1)] = _bad_row["bad_comm"] or 0

    log(f"[Trade] Audit counts — canceled: {counts[('T_CanceledTrades',1)]:,}, "
        f"invalid_charge: {counts[('T_InvalidCharge',1)]:,}, "
        f"invalid_commission: {counts[('T_InvalidCommision',1)]:,}")

    # Per-dataset async copies are kicked off inside each write_file; no explicit post-historical drain is needed.

    # Do not unpersist trade_df here — _ct_hist_batch{2,3} and _hh_hist_batch{2,3} temp views are lazy filters over trade_df and are consumed by _gen_incremental_trades. Caller unpersists after incrementals finish.
    return {"counts": counts, "trade_df": trade_df, "cleanup_info": _trade_df_cleanup}


def _gen_incremental_trades(spark, cfg, dicts, batch_id, dbutils, shared):
    """Generate incremental Trade.txt for Batch 2+ with CDC format.

    Uses pre-built shared lookups (valid_accts, broker_names, num_sec) from generate().
    """
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
    # Trade incremental t_dts aligns with the BatchDate entry (2017-07-08 for Batch 2, 2017-07-09 for Batch 3). The DimTrade date check uses strict `>` LastDay, so t_dts == LastDay passes. Aligning t_dts with that date lets the ETL's DimTrade Incremental (which joins DimAccount on iscurrent) find the SCD2-updated account version that has the same effectivedate — otherwise the SK_AccountID check fails on 2 rows per batch where incremental Account.txt updated the referenced account.
    batch_date_str = (FIRST_BATCH_DATE + timedelta(days=batch_id - 1)).strftime("%Y-%m-%d")

    # --- New trades (cdc_flag = "I") ---
    # These are brand-new trades with t_id beyond the historical range. Limit orders start as PNDG, market orders start as SBMT.
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
    # U rows represent status transitions on existing historical trades. These MUST be the trades actually transitioning in this batch window — otherwise FactHoldings's join on DimTrade(batchid=N) AND tradeid=hh_t_id yields zero rows because HH's trade_ids don't match the randomly-picked Trade.txt U rows. Source: _hh_hist_batch{N} (CMPT transitions; trades that complete in this batch window) — exactly the set that HoldingHistory.txt and CashTransaction.txt reference. Helper: build a U-row DataFrame from a set of (t_id, status) pairs. All U rows share the same hash-based field generation pattern; only t_id, t_st_id, and t_dts differ per source. t_dts offsets are chosen so DimTrade Incremental's max_by ordering places later-lifecycle transitions AFTER earlier ones for the same trade (a historical limit order that both submits and completes in the same batch window produces both an SBMT U row and a CMPT U row — without distinct timestamps, max_by ties and may pick SBMT, leaving sk_closedateid NULL and breaking FactHoldings SK_*/CurrentTradeID).
    def _build_u_rows(tids_df, status_col, t_dts_str, dsn_offset, bs_offset):
        return (tids_df
            .withColumn("rid", F.row_number().over(Window.orderBy("t_id")) - F.lit(1))
            .withColumn("cdc_flag", F.lit("U"))
            .withColumn("cdc_dsn", (F.col("rid") + F.lit(dsn_offset) + 1).cast("string"))
            .withColumn("t_tt_id",
                F.when(hash_key(F.col("rid"), bs + bs_offset) % 100 < 30, F.lit("TLB"))
                 .when(hash_key(F.col("rid"), bs + bs_offset) % 100 < 60, F.lit("TLS"))
                 .when(hash_key(F.col("rid"), bs + bs_offset) % 100 < 80, F.lit("TMB"))
                 .otherwise(F.lit("TMS")))
            .withColumn("t_st_id", status_col)
            .withColumn("t_dts", F.lit(t_dts_str))
            .withColumn("t_is_cash", F.when(hash_key(F.col("rid"), bs + bs_offset + 2) % 100 < 20, F.lit("1")).otherwise(F.lit("0")))
            .withColumn("_sym_idx", hash_key(F.col("rid"), bs + bs_offset + 3) % num_sec)
            .withColumn("t_qty", (hash_key(F.col("rid"), bs + bs_offset + 4) % 9991 + 10).cast("string"))
            .withColumn("t_bid_price", F.format_string("%.2f", (hash_key(F.col("rid"), bs + bs_offset + 5) % 900 + 101) / 100.0))
            .withColumn("_va_idx", hash_key(F.col("rid"), bs + bs_offset + 6) % F.lit(n_valid))
            .withColumn("_tp", (hash_key(F.col("rid"), bs + bs_offset + 5) % 900 + 101) / 100.0)
            .withColumn("t_trade_price", F.format_string("%.2f", F.col("_tp")))
            .withColumn("t_chrg", F.format_string("%.2f", F.col("_tp") * (hash_key(F.col("rid"), bs + bs_offset + 7) % 50 + 1) / 10000.0))
            .withColumn("t_comm", F.format_string("%.2f", F.col("_tp") * (hash_key(F.col("rid"), bs + bs_offset + 8) % 50 + 1) / 10000.0))
            .withColumn("t_tax", F.format_string("%.2f", F.col("_tp") * (hash_key(F.col("rid"), bs + bs_offset + 9) % 36 + 5) / 100.0))
            .withColumn("_broker_idx", hash_key(F.col("rid"), bs + bs_offset + 10) % F.lit(n_brokers)))

    # t_dts offsets per transition type so max_by in DimTrade Incremental picks CMPT over SBMT for same-batch transitions. Order: I (new trade, midnight) < SBMT U (01s) < CNCL U (02s) < CMPT U (03s) < new-market CMPT U (04s). new-market CMPT has the latest t_dts so its CMPT wins over the I row's SBMT status.
    t_dts_sbmt = f"{batch_date_str} 00:00:01"
    t_dts_cncl = f"{batch_date_str} 00:00:02"
    t_dts_cmpt = f"{batch_date_str} 00:00:03"

    # CMPT transitions: trades completing in batch window (from _hh_hist_batch).
    cmpt_tids = (spark.table(f"_hh_hist_batch{batch_id}")
        .select(F.col("hh_t_id").alias("t_id"))
        .distinct())
    upd_cmpt = _build_u_rows(cmpt_tids, F.lit("CMPT"), t_dts_cmpt,
                             dsn_offset=n_new, bs_offset=21)

    # SBMT / CNCL transitions: limit orders whose _submit_ts falls in this batch window (from _t_submit_hist_batch). Each emits one U row with t_st_id=SBMT (non-canceled) or t_st_id=CNCL (canceled). This closes the DimTrade T_Records B2/B3 -40% drift vs DIGen.
    submit_df = spark.table(f"_t_submit_hist_batch{batch_id}")
    sbmt_tids = submit_df.filter(~F.col("_is_canceled")).select(F.col("t_id"))
    cncl_tids = submit_df.filter(F.col("_is_canceled")).select(F.col("t_id"))
    # DSN offsets keep each source's rids in distinct ranges (n_new is an upper bound of I rows; upd_cmpt gets [n_new, n_new+2M]; sbmt gets [n_new+2M, ...]; cncl gets [n_new+3M, ...]). Exact non-overlap is not required since the ETL doesn't rely on cdc_dsn ordering — only that each row is a distinct CDC event.
    upd_sbmt = _build_u_rows(sbmt_tids, F.lit("SBMT"), t_dts_sbmt,
                             dsn_offset=n_new + 2_000_000, bs_offset=50)
    upd_cncl = _build_u_rows(cncl_tids, F.lit("CNCL"), t_dts_cncl,
                             dsn_offset=n_new + 3_000_000, bs_offset=70)

    # New market orders (TMB/TMS) complete same-day. Emit a CMPT U row per such trade (paired with the SBMT I row already in new_df). These close the FactHoldings HH_RECORDS -46% drift vs DIGen at B2/B3 and the corresponding chunk of T_Records drift. t_dts uses the t_dts_cmpt offset so DimTrade Incremental's max_by deterministically picks this CMPT U row over the SBMT I row (which sits at batch_date midnight).
    new_market_cmpt = (new_df.filter(F.col("t_tt_id").isin("TMB", "TMS"))
        .withColumn("cdc_flag", F.lit("U"))
        .withColumn("t_st_id", F.lit("CMPT"))
        .withColumn("t_dts", F.lit(t_dts_cmpt))
        # Keep all other fields (t_id, symbols, prices, etc.) so the U row reflects the same trade; only cdc_flag/t_st_id/t_dts differ from the SBMT I row. cdc_dsn is offset into a fresh range to keep DSNs locally unique without risking collision with I / other U rows.
        .withColumn("cdc_dsn", (F.col("rid") + F.lit(n_new + 4_000_000) + 1).cast("string")))

    # Union all inserts and updates into a single CDC output.
    common_cols = ["cdc_flag", "cdc_dsn", "t_id", "t_dts", "t_st_id", "t_tt_id", "t_is_cash",
                   "_sym_idx", "t_qty", "t_bid_price", "_va_idx", "_tp", "_broker_idx",
                   "t_trade_price", "t_chrg", "t_comm", "t_tax"]
    inc_df = (new_df.select(*common_cols)
        .union(upd_cmpt.select(*common_cols))
        .union(upd_sbmt.select(*common_cols))
        .union(upd_cncl.select(*common_cols))
        .union(new_market_cmpt.select(*common_cols)))

    # Join symbol with temporal validity (same as historical trades). Incremental trades are at batch_date (~2017-07-08), so most symbols are active, but deactivated symbols still need to be excluded.
    fw_begin_s = int(FW_BEGIN_DATE.timestamp())
    quarter_secs = ONE_QUARTER_MS / 1000
    batch_date = FIRST_BATCH_DATE + timedelta(days=batch_id - 1)
    batch_quarter = int((int(batch_date.timestamp()) - fw_begin_s) / quarter_secs)
    _sym0 = symbols_df.filter(F.col("_idx") == 0).select("Symbol").collect()[0][0]

    inc_df = inc_df.join(
        symbols_df.select(
            F.col("_idx").alias("_sym_idx"),
            F.col("Symbol").alias("_sym_name"),
            F.col("creation_quarter").alias("_sym_cq"),
            F.col("deactivation_quarter").alias("_sym_dq")),
        on="_sym_idx", how="left")
    inc_df = inc_df.withColumn("t_s_symb",
        F.when((F.lit(batch_quarter) > F.col("_sym_cq")) &
               (F.lit(batch_quarter) < F.col("_sym_dq")),
            F.col("_sym_name"))
         .otherwise(F.lit(_sym0)))
    # Join valid (non-closed) account
    inc_df = inc_df.withColumn("t_ca_id", F.col("_va_idx").cast("string"))

    # Derive t_exec_name inline from _broker_idx — same pattern as historical write_trade. HR name dicts auto-broadcast; no intermediate broker_names table needed.
    from .utils import dict_count
    inc_df = inc_df.withColumn("_bk_fn_hash", hash_key(F.col("_broker_idx").cast("long"), seed_for("T", "bk_fn")) % F.lit(dict_count("hr_given_names")))
    inc_df = inc_df.withColumn("_bk_ln_hash", hash_key(F.col("_broker_idx").cast("long"), seed_for("T", "bk_ln")) % F.lit(dict_count("hr_family_names")))
    inc_df = dict_join(inc_df, "hr_given_names",  F.col("_bk_fn_hash"), "_bk_first")
    inc_df = dict_join(inc_df, "hr_family_names", F.col("_bk_ln_hash"), "_bk_last")
    inc_df = inc_df.withColumn("t_exec_name", F.concat(F.col("_bk_first"), F.lit(" "), F.col("_bk_last")))

    # Final column selection: CDC columns first, then standard trade columns
    inc_df = inc_df.select(
        "cdc_flag", "cdc_dsn", "t_id", "t_dts", "t_st_id", "t_tt_id", "t_is_cash",
        "t_s_symb", "t_qty", "t_bid_price", "t_ca_id", "t_exec_name",
        "t_trade_price", "t_chrg", "t_comm", "t_tax")

    # Analytical estimates for Trade-Audit (incremental batches): t_new ≈ cfg.trade_inc_new; t_total = n_new + n_update t_cncl ≈ 10% of limit-order U rows; invalid = 0
    t_new_est = cfg.trade_inc_new
    t_total_est = inc_trades  # n_new + n_update
    t_cncl_est = int(n_update * 0.6 * 0.1)  # limit-order U rows canceled
    write_file(inc_df, f"{bp}/Trade.txt", "|", dbutils, scale_factor=cfg.sf)

    ct_batch = spark.table(f"_ct_hist_batch{batch_id}")
    hh_batch = spark.table(f"_hh_hist_batch{batch_id}")

    # HH rows for new market orders completing same-day.
    new_market_hh = (new_df.filter(F.col("t_tt_id").isin("TMB", "TMS"))
        .withColumn("hh_t_id", F.col("t_id"))
        .withColumn("_is_buy", F.col("t_tt_id") == "TMB")
        .withColumn("hh_h_t_id",
            F.when(F.col("_is_buy"), F.col("t_id"))
             .otherwise((hash_key(F.col("t_id").cast("long"), seed_for("T", "hh_ref")) %
                 F.greatest(F.lit(1), F.col("t_id").cast("long"))).cast("string")))
        .withColumn("hh_before_qty", F.when(F.col("_is_buy"), F.lit("0")).otherwise(F.col("t_qty")))
        .withColumn("hh_after_qty", F.when(F.col("_is_buy"), F.col("t_qty")).otherwise(F.lit("0")))
        .withColumn("cdc_flag", F.lit("I"))
        .withColumn("cdc_dsn_hh", (F.col("rid") + F.lit(10_000_000)).cast("bigint"))
        .select("cdc_flag", F.col("cdc_dsn_hh").alias("cdc_dsn"),
                "hh_h_t_id", "hh_t_id", "hh_before_qty", "hh_after_qty"))

    hh_combined = hh_batch.unionByName(new_market_hh)
    write_file(ct_batch, f"{bp}/CashTransaction.txt", "|", dbutils, scale_factor=cfg.sf)
    write_file(hh_combined, f"{bp}/HoldingHistory.txt", "|", dbutils, scale_factor=cfg.sf)

    # CT/HH batch estimates: one CT per non-canceled trade in batch, similar for HH.
    ct_count = int(inc_trades * 0.94)
    hh_count = int(inc_trades * 0.94)
    log(f"[Trade] Batch{batch_id}: {t_total_est:,} ({t_new_est:,} I, {t_total_est - t_new_est:,} U), CT: ~{ct_count:,}, HH: ~{hh_count:,}")

    if not static_audits_available(cfg):
        # Dynamic audit regeneration — compute exact counts.
        _trade_agg = inc_df.select(
            F.count("*").alias("t_total"),
            F.sum(F.when(F.col("cdc_flag") == "I", 1).otherwise(0)).alias("t_new"),
            F.sum(F.when(F.col("t_st_id") == "CNCL", 1).otherwise(0)).alias("t_cncl"),
        ).collect()[0]
        ct_count = ct_batch.count()
        hh_count = hh_combined.count()
        return {
            ("Trade", batch_id): _trade_agg["t_total"] or 0,
            ("CashTransaction", batch_id): ct_count,
            ("HoldingHistory", batch_id): hh_count,
            ("TI_NEW", batch_id):              _trade_agg["t_new"] or 0,
            ("TI_CanceledTrades", batch_id):   _trade_agg["t_cncl"] or 0,
            ("TI_InvalidCharge", batch_id):    0,
            ("TI_InvalidCommision", batch_id): 0,
        }
    return {
        ("Trade", batch_id): t_total_est,
        ("CashTransaction", batch_id): ct_count,
        ("HoldingHistory", batch_id): hh_count,
        ("TI_NEW", batch_id):              t_new_est,
        ("TI_CanceledTrades", batch_id):   t_cncl_est,
        ("TI_InvalidCharge", batch_id):    0,
        ("TI_InvalidCommision", batch_id): 0,
    }
