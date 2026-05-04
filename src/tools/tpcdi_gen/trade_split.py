"""V6: decomposed Trade generation — base + 4 parallel leaves.

Splits the work that today lives inside ``trade._gen_historical_trades``
across 5 workflow tasks so each Trade-family dataset has its own
repair-run granularity and writes happen in parallel:

  materialize_base(spark, cfg, dbutils, dicts) -> writes
    ``_gen_trade_df`` Delta to ``{wh_db}_{sf}_stage`` with the *minimum*
    cross-task column set. All four leaves read this Delta.

  gen_trade(spark, cfg, dbutils, dicts, base_df) ->
    Trade.txt B1 (+ B2/B3 incrementals in standard mode).

  gen_tradehistory(spark, cfg, dbutils, base_df) ->
    TradeHistory.txt B1 only (TH is historical-only).

  gen_cashtransaction(spark, cfg, dbutils, base_df) ->
    CashTransaction.txt B1 (+ B2/B3 incrementals in standard mode).

  gen_holdinghistory(spark, cfg, dbutils, dicts, base_df) ->
    HoldingHistory.txt B1 (+ B2/B3 incrementals in standard mode, with
    new-market-CMPT HH rows for same-day completions of new TMB/TMS
    trades emitted by the Trade leaf).

Cross-task column set staged in ``_gen_trade_df`` (minimum):
    t_id, t_qty, t_ca_id, t_s_symb,
    _is_canceled, _is_buy, _is_limit,
    _base_ts, _submit_ts, _complete_ts, _cash_ts

Single-consumer columns are re-derived in each leaf from t_id (cheap
hash ops). Notably *not* staged: ``_ct_name_raw`` (~128 chars × 2.4B
trades = ~300 GB at SF=20k — expensive to materialize, cheap to
re-derive in the CT leaf), ``_trade_val``, ``_broker_idx``,
``_qty_val``, intermediate offsets.

Inter-leaf state (the original ``_hh_hist_batch{N}``,
``_ct_hist_batch{N}``, ``_t_submit_hist_batch{N}`` temp views) is
*not* shared — each leaf computes the equivalent filter directly from
``base_df``. This eliminates the inter-leaf dependency that would
otherwise serialize the writes back together.

Trade.py is intentionally left untouched — this is an additive module
the new task notebooks call instead. Once V6 lands and is validated
the orchestrator path in trade.py can be dropped.
"""
from __future__ import annotations

import math
from datetime import datetime, timedelta

from pyspark.sql import SparkSession, functions as F, Window

from .config import (
    TRADE_BEGIN_DATE, FIRST_BATCH_DATE, FIRST_BATCH_DATE_END,
    FW_BEGIN_DATE, ONE_QUARTER_MS, NUM_INCREMENTAL_BATCHES,
    AUG_FILES_DATE_END_EXCL, AUG_FILES_DATE_START,
)
from .utils import (
    write_file, write_delta, seed_for, hash_key, dict_join, dict_count,
    log,
)
from .audit import static_audits_available


# Columns staged in {_stage}._gen_trade_df (the minimum cross-task set).
BASE_COLS = [
    "t_id", "t_qty", "t_ca_id", "t_s_symb",
    "_is_canceled", "_is_buy", "_is_limit",
    "_base_ts", "_submit_ts", "_complete_ts", "_cash_ts",
]


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------
def _stage_schema_fq(cfg) -> str:
    """``{catalog}.{wh_db}_{sf}_stage`` — staging schema for the Delta hand-off."""
    return f"{cfg.catalog}.{cfg.wh_db}_{cfg.sf}_stage"


def _trade_base_table_fq(cfg) -> str:
    return f"{_stage_schema_fq(cfg)}._gen_trade_df"


def _resolve_n_brokers(spark, cfg) -> int:
    """Same logic the orchestrator uses (``trade.generate``): analytical
    estimate when static audits are present, exact ``_brokers`` count
    otherwise. Each leaf re-runs this independently — both branches
    return a stable value so all leaves see the same n_brokers and
    derive identical t_exec_name strings.
    """
    if static_audits_available(cfg):
        return cfg.n_brokers_estimate
    return spark.table("_brokers").count()


# ---------------------------------------------------------------------------
# materialize_base — gen_trade_base task
# ---------------------------------------------------------------------------
def _build_base_df(spark, cfg, n_brokers: int, num_sec: int, n_valid: int):
    """Lift the trade_df build from ``trade._gen_historical_trades`` lines
    195-298 (range -> hash columns -> symbols join -> t_ca_id assignment).

    Stops short of single-consumer derivations (``_ct_name_raw``,
    ``_trade_val``, ``_broker_idx``, ``t_st_id``, ``t_dts``) — those are
    re-derived inside each leaf.
    """
    symbols_df = spark.table("_symbols")
    trade_begin_s = int(TRADE_BEGIN_DATE.timestamp())
    trade_range_s = int((FIRST_BATCH_DATE - TRADE_BEGIN_DATE).total_seconds())

    target_trade_partitions = max(8, cfg.sf // 8)
    trade_df = (spark.range(0, cfg.trade_total, numPartitions=target_trade_partitions)
        .withColumnRenamed("id", "t_id")
        .withColumn("_tt_rand", hash_key(F.col("t_id"), seed_for("T", "tt")) % 100)
        .withColumn("t_tt_id",
            F.when(F.col("_tt_rand") < 30, F.lit("TLB"))
             .when(F.col("_tt_rand") < 60, F.lit("TLS"))
             .when(F.col("_tt_rand") < 80, F.lit("TMB"))
             .otherwise(F.lit("TMS")))
        .withColumn("_is_buy", F.col("t_tt_id").isin("TLB", "TMB"))
        .withColumn("_is_limit", F.col("t_tt_id").isin("TLB", "TLS"))
        .withColumn("_ts_offset", hash_key(F.col("t_id"), seed_for("T", "ts")) % trade_range_s)
        .withColumn("_base_ts", F.lit(trade_begin_s).cast("long") + F.col("_ts_offset"))
        .withColumn("_sym_idx", hash_key(F.col("t_id"), seed_for("T", "sym")) % num_sec)
        .withColumn("_va_idx", hash_key(F.col("t_id"), seed_for("T", "ca")) % F.lit(n_valid))
        .withColumn("t_qty", (hash_key(F.col("t_id"), seed_for("T", "qty")) % 9991 + 10).cast("string"))
        .withColumn("_is_canceled",
            F.when(F.col("_is_limit"), hash_key(F.col("t_id"), seed_for("T", "cncl")) % 100 < 10).otherwise(F.lit(False)))
        .withColumn("_submit_offset",
            F.when(F.col("_is_limit"), hash_key(F.col("t_id"), seed_for("T", "sub")) % 7776000 + 1).otherwise(F.lit(0)))
        .withColumn("_submit_ts", F.col("_base_ts") + F.col("_submit_offset"))
        .withColumn("_complete_ts", F.col("_submit_ts") + (hash_key(F.col("t_id"), seed_for("T", "cmp")) % 300 + 1))
        .withColumn("_cash_ts",
            F.col("_complete_ts") + (hash_key(F.col("t_id"), seed_for("T", "ct")) % 432000))
    )

    # Symbol join — temporal validity check + fallback to symbol 0 when the
    # picked symbol wasn't active at trade time.
    fw_begin_s = int(FW_BEGIN_DATE.timestamp())
    quarter_secs = ONE_QUARTER_MS / 1000
    trade_df = trade_df.withColumn("_trade_quarter",
        ((F.col("_base_ts") - F.lit(fw_begin_s)) / F.lit(quarter_secs)).cast("int"))
    _sym0 = symbols_df.filter(F.col("_idx") == 0).select("Symbol").collect()[0][0]

    trade_df = trade_df.join(
        symbols_df.select(
            F.col("_idx").alias("_sym_idx"),
            F.col("Symbol").alias("_sym_name"),
            F.col("creation_quarter").alias("_sym_cq"),
            F.col("deactivation_quarter").alias("_sym_dq")),
        on="_sym_idx", how="left")
    trade_df = trade_df.withColumn("t_s_symb",
        F.when((F.col("_trade_quarter") > F.col("_sym_cq")) &
               (F.col("_trade_quarter") < F.col("_sym_dq")),
            F.col("_sym_name"))
         .otherwise(F.lit(_sym0)))
    trade_df = trade_df.withColumn("t_ca_id", F.col("_va_idx").cast("string"))
    return trade_df


def materialize_base(spark, cfg, dbutils, dicts=None):
    """Build the trade base and persist the minimum cross-task column set
    to ``{_stage}._gen_trade_df`` Delta.

    Idempotent: overwrites the table on each call. Returns the
    fully-qualified table name plus the resolved ``n_valid``,
    ``n_brokers``, ``num_sec`` constants for the caller's log.
    """
    log("[trade_split] materialize_base — start")
    n_valid = cfg.n_available_accounts
    num_sec = spark.table("_symbols").count()
    n_brokers = _resolve_n_brokers(spark, cfg)
    log(f"[trade_split] n_valid={n_valid:,} n_brokers={n_brokers:,} num_sec={num_sec:,}")

    trade_df = _build_base_df(spark, cfg, n_brokers, num_sec, n_valid)
    base = trade_df.select(*BASE_COLS)

    fq = _trade_base_table_fq(cfg)
    spark.sql(f"DROP TABLE IF EXISTS {fq}")
    cols_ddl = ", ".join(f"{f.name} {f.dataType.simpleString()}"
                         for f in base.schema.fields)
    spark.sql(
        f"CREATE TABLE {fq} ({cols_ddl}) USING DELTA "
        f"TBLPROPERTIES("
        f"'delta.dataSkippingNumIndexedCols'=0, "
        f"'delta.autoOptimize.autoCompact'=False, "
        f"'delta.autoOptimize.optimizeWrite'=False)"
    )
    base.write.mode("append").saveAsTable(fq)
    log(f"[trade_split] materialize_base — wrote {fq}")
    return {"fq": fq, "n_valid": n_valid, "n_brokers": n_brokers, "num_sec": num_sec}


def read_base(spark, cfg):
    """Read ``{_stage}._gen_trade_df`` back as a DataFrame for a leaf."""
    return spark.read.table(_trade_base_table_fq(cfg))


# ---------------------------------------------------------------------------
# gen_trade leaf
# ---------------------------------------------------------------------------
def _add_trade_only_cols(df, cfg, n_brokers: int):
    """Re-derive the columns Trade.txt outputs that aren't staged in base.

    Lifted from ``trade._gen_historical_trades``'s trade_df pipeline +
    ``write_trade`` broker name lookup. Single-consumer, so cheaper to
    re-derive than to stage at SF=20k scale.
    """
    return (df
        .withColumn("_tt_rand", hash_key(F.col("t_id"), seed_for("T", "tt")) % 100)
        .withColumn("t_tt_id",
            F.when(F.col("_tt_rand") < 30, F.lit("TLB"))
             .when(F.col("_tt_rand") < 60, F.lit("TLS"))
             .when(F.col("_tt_rand") < 80, F.lit("TMB"))
             .otherwise(F.lit("TMS")))
        .withColumn("t_is_cash", F.when(hash_key(F.col("t_id"), seed_for("T", "cash")) % 100 < 20, F.lit("1")).otherwise(F.lit("0")))
        .withColumn("_qty_val", F.col("t_qty").cast("double"))
        .withColumn("_bid", (hash_key(F.col("t_id"), seed_for("T", "bid")) % 900 + 101) / 100.0)
        .withColumn("t_bid_price", F.format_string("%.2f", F.col("_bid")))
        .withColumn("_tp", F.col("_bid") * (0.95 + (hash_key(F.col("t_id"), seed_for("T", "tp")) % 11) / 100.0))
        .withColumn("t_trade_price", F.format_string("%.2f", F.col("_tp")))
        .withColumn("_trade_val", F.col("_tp") * F.col("_qty_val"))
        .withColumn("t_chrg", F.format_string("%.2f",
            F.col("_trade_val") * (hash_key(F.col("t_id"), seed_for("T", "chrg")) % 50 + 1) / 10000.0))
        .withColumn("t_comm", F.format_string("%.2f",
            F.col("_trade_val") * (hash_key(F.col("t_id"), seed_for("T", "comm")) % 50 + 1) / 10000.0))
        .withColumn("t_tax", F.format_string("%.2f", F.col("_trade_val") * (hash_key(F.col("t_id"), seed_for("T", "tax")) % 36 + 5) / 100.0))
        .withColumn("_broker_idx", hash_key(F.col("t_id"), seed_for("T", "exec")) % F.lit(n_brokers))
    )


def _add_broker_name(df):
    """Apply HR dictionary joins to derive t_exec_name from _broker_idx."""
    df = df.withColumn("_bk_fn_hash", hash_key(F.col("_broker_idx").cast("long"), seed_for("T", "bk_fn")) % F.lit(dict_count("hr_given_names")))
    df = df.withColumn("_bk_ln_hash", hash_key(F.col("_broker_idx").cast("long"), seed_for("T", "bk_ln")) % F.lit(dict_count("hr_family_names")))
    df = dict_join(df, "hr_given_names", F.col("_bk_fn_hash"), "_bk_first")
    df = dict_join(df, "hr_family_names", F.col("_bk_ln_hash"), "_bk_last")
    df = df.withColumn("t_exec_name", F.concat(F.col("_bk_first"), F.lit(" "), F.col("_bk_last")))
    return df


def gen_trade(spark, cfg, dbutils, dicts, base_df):
    """Read base, derive Trade-only cols, write Trade.txt for B1 (+ B2/B3
    in standard mode). Mirrors ``trade.write_trade`` for B1 and
    ``_gen_incremental_trades`` for B2/B3.
    """
    log("[trade_split] gen_trade — start")
    n_valid = cfg.n_available_accounts
    n_brokers = _resolve_n_brokers(spark, cfg)
    num_sec = spark.table("_symbols").count()

    batch_cutoff_s = int(FIRST_BATCH_DATE_END.timestamp())
    cutoff = F.lit(batch_cutoff_s).cast("long")

    # B1 status + dts derivation (mirrors trade.py:256-271).
    tx = (base_df
        .withColumn("t_st_id",
            F.when(F.col("_is_limit") & (F.col("_submit_ts") > cutoff), F.lit("PNDG"))
             .when(~F.col("_is_canceled") & (F.col("_complete_ts") > cutoff), F.lit("SBMT"))
             .when(F.col("_is_canceled"), F.lit("CNCL"))
             .otherwise(F.lit("CMPT")))
        .withColumn("t_dts",
            F.when(F.col("t_st_id") == "PNDG", F.date_format(F.col("_base_ts").cast("timestamp"), "yyyy-MM-dd HH:mm:ss"))
             .when(F.col("t_st_id") == "SBMT", F.date_format(F.col("_submit_ts").cast("timestamp"), "yyyy-MM-dd HH:mm:ss"))
             .when(F.col("t_st_id") == "CNCL", F.date_format(F.col("_submit_ts").cast("timestamp"), "yyyy-MM-dd HH:mm:ss"))
             .otherwise(F.date_format(F.col("_complete_ts").cast("timestamp"), "yyyy-MM-dd HH:mm:ss"))))
    tx = _add_trade_only_cols(tx, cfg, n_brokers)
    tx = _add_broker_name(tx)

    out = tx.select(
        F.col("t_id").cast("string"), "t_dts", "t_st_id", "t_tt_id", "t_is_cash",
        "t_s_symb", "t_qty", "t_bid_price", "t_ca_id", "t_exec_name",
        "t_trade_price", "t_chrg", "t_comm", "t_tax")

    if cfg.augmented_incremental:
        out_p = out.toDF(
            "t_id", "t_dts", "t_st_id", "t_tt_id", "t_is_cash",
            "t_s_symb", "quantity", "bidprice", "t_ca_id", "executedby",
            "tradeprice", "fee", "commission", "tax")
        out_p = (out_p
            .filter(F.col("t_dts") < F.lit(AUG_FILES_DATE_END_EXCL))
            .withColumn("stg_target",
                F.when(F.col("t_dts") < F.lit(AUG_FILES_DATE_START), F.lit("tables"))
                 .otherwise(F.lit("files"))))
        write_delta(out_p, cfg=cfg, dataset="trade", partition_cols=["stg_target"])
    else:
        write_file(out, f"{cfg.batch_path(1)}/Trade.txt", "|", dbutils,
                   scale_factor=cfg.sf)

    counts = {("Trade", 1): cfg.trade_total}

    # T_CanceledTrades / T_InvalidCharge / T_InvalidCommision
    counts[("T_CanceledTrades", 1)]    = int(cfg.trade_total * 0.06)
    counts[("T_InvalidCharge", 1)]     = 0
    counts[("T_InvalidCommision", 1)]  = 0
    if not static_audits_available(cfg):
        _tp_x_qty = F.col("t_trade_price").cast("double") * F.col("t_qty").cast("double")
        _row = tx.select(
            F.sum(F.when(F.col("t_st_id") == "CNCL", 1).otherwise(0)).alias("cncl"),
            F.sum(F.when(F.col("t_chrg").cast("double") > _tp_x_qty, 1).otherwise(0)).alias("bad_chrg"),
            F.sum(F.when(F.col("t_comm").cast("double") > _tp_x_qty, 1).otherwise(0)).alias("bad_comm"),
        ).collect()[0]
        counts[("T_CanceledTrades", 1)]   = _row["cncl"] or 0
        counts[("T_InvalidCharge", 1)]    = _row["bad_chrg"] or 0
        counts[("T_InvalidCommision", 1)] = _row["bad_comm"] or 0

    # B2/B3 incrementals — augmented mode skips entirely.
    if not cfg.augmented_incremental:
        for batch_id in range(2, NUM_INCREMENTAL_BATCHES + 2):
            counts.update(_gen_trade_incremental(
                spark, cfg, dbutils, base_df,
                batch_id=batch_id, n_valid=n_valid,
                n_brokers=n_brokers, num_sec=num_sec))

    log(f"[trade_split] gen_trade — done, {len(counts)} count entries")
    return counts


def _gen_trade_incremental(spark, cfg, dbutils, base_df, *, batch_id: int,
                            n_valid: int, n_brokers: int, num_sec: int):
    """B2/B3 Trade.txt CDC writer. Mirrors trade._gen_incremental_trades.

    Originally consumed ``_hh_hist_batch{N}`` and ``_t_submit_hist_batch{N}``
    temp views produced by sibling write functions; here we filter
    ``base_df`` directly so the leaf is independent.
    """
    symbols_df = spark.table("_symbols")
    bs = seed_for(f"T_B{batch_id}", "base")
    inc_trades = cfg.trade_inc
    bp = cfg.batch_path(batch_id)
    n_new = cfg.trade_inc_new
    n_update = inc_trades - n_new
    batch_date_str = (FIRST_BATCH_DATE + timedelta(days=batch_id - 1)).strftime("%Y-%m-%d")
    batch_cutoff_s = int(FIRST_BATCH_DATE_END.timestamp())
    b_start = batch_cutoff_s + (batch_id - 2) * 86400
    b_end = batch_cutoff_s + (batch_id - 1) * 86400

    # New trades (cdc_flag='I')
    new_df = (spark.range(0, n_new).withColumnRenamed("id", "rid")
        .withColumn("cdc_flag", F.lit("I"))
        .withColumn("cdc_dsn", (F.col("rid") + 1).cast("string"))
        .withColumn("t_id", (F.col("rid") + cfg.trade_total + (batch_id - 2) * inc_trades).cast("string"))
        .withColumn("t_tt_id",
            F.when(hash_key(F.col("rid"), bs) % 100 < 30, F.lit("TLB"))
             .when(hash_key(F.col("rid"), bs) % 100 < 60, F.lit("TLS"))
             .when(hash_key(F.col("rid"), bs) % 100 < 80, F.lit("TMB"))
             .otherwise(F.lit("TMS")))
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

    t_dts_sbmt = f"{batch_date_str} 00:00:01"
    t_dts_cncl = f"{batch_date_str} 00:00:02"
    t_dts_cmpt = f"{batch_date_str} 00:00:03"

    # CMPT tids — derive from base_df (originally _hh_hist_batch{N}).
    cmpt_tids = (base_df
        .filter(~F.col("_is_canceled"))
        .filter((F.col("_complete_ts") >= F.lit(b_start).cast("long")) &
                (F.col("_complete_ts") < F.lit(b_end).cast("long")))
        .select(F.col("t_id").cast("string").alias("t_id"))
        .distinct())
    upd_cmpt = _build_u_rows(cmpt_tids, F.lit("CMPT"), t_dts_cmpt,
                             dsn_offset=n_new, bs_offset=21)

    # SBMT/CNCL tids — derive from base_df (originally _t_submit_hist_batch{N}).
    submit_in_window = (base_df
        .filter(F.col("_is_limit") &
                (F.col("_submit_ts") >= F.lit(b_start).cast("long")) &
                (F.col("_submit_ts") < F.lit(b_end).cast("long"))))
    sbmt_tids = submit_in_window.filter(~F.col("_is_canceled")).select(F.col("t_id"))
    cncl_tids = submit_in_window.filter(F.col("_is_canceled")).select(F.col("t_id"))
    upd_sbmt = _build_u_rows(sbmt_tids, F.lit("SBMT"), t_dts_sbmt,
                             dsn_offset=n_new + 2_000_000, bs_offset=50)
    upd_cncl = _build_u_rows(cncl_tids, F.lit("CNCL"), t_dts_cncl,
                             dsn_offset=n_new + 3_000_000, bs_offset=70)

    # New market orders complete same-day → CMPT U row alongside the SBMT I.
    new_market_cmpt = (new_df.filter(F.col("t_tt_id").isin("TMB", "TMS"))
        .withColumn("cdc_flag", F.lit("U"))
        .withColumn("t_st_id", F.lit("CMPT"))
        .withColumn("t_dts", F.lit(t_dts_cmpt))
        .withColumn("cdc_dsn", (F.col("rid") + F.lit(n_new + 4_000_000) + 1).cast("string")))

    common_cols = ["cdc_flag", "cdc_dsn", "t_id", "t_dts", "t_st_id", "t_tt_id", "t_is_cash",
                   "_sym_idx", "t_qty", "t_bid_price", "_va_idx", "_tp", "_broker_idx",
                   "t_trade_price", "t_chrg", "t_comm", "t_tax"]
    inc_df = (new_df.select(*common_cols)
        .union(upd_cmpt.select(*common_cols))
        .union(upd_sbmt.select(*common_cols))
        .union(upd_cncl.select(*common_cols))
        .union(new_market_cmpt.select(*common_cols)))

    # Symbol join + temporal validity (same as historical trades).
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
    inc_df = inc_df.withColumn("t_ca_id", F.col("_va_idx").cast("string"))
    inc_df = _add_broker_name(inc_df)
    inc_df = inc_df.select(
        "cdc_flag", "cdc_dsn", "t_id", "t_dts", "t_st_id", "t_tt_id", "t_is_cash",
        "t_s_symb", "t_qty", "t_bid_price", "t_ca_id", "t_exec_name",
        "t_trade_price", "t_chrg", "t_comm", "t_tax")
    write_file(inc_df, f"{bp}/Trade.txt", "|", dbutils, scale_factor=cfg.sf)

    t_new_est = cfg.trade_inc_new
    t_total_est = inc_trades
    t_cncl_est = int(n_update * 0.6 * 0.1)
    counts = {
        ("Trade", batch_id): t_total_est,
        ("TI_NEW", batch_id):              t_new_est,
        ("TI_CanceledTrades", batch_id):   t_cncl_est,
        ("TI_InvalidCharge", batch_id):    0,
        ("TI_InvalidCommision", batch_id): 0,
    }
    if not static_audits_available(cfg):
        agg = inc_df.select(
            F.count("*").alias("t_total"),
            F.sum(F.when(F.col("cdc_flag") == "I", 1).otherwise(0)).alias("t_new"),
            F.sum(F.when(F.col("t_st_id") == "CNCL", 1).otherwise(0)).alias("t_cncl"),
        ).collect()[0]
        counts[("Trade", batch_id)]            = agg["t_total"] or 0
        counts[("TI_NEW", batch_id)]            = agg["t_new"]   or 0
        counts[("TI_CanceledTrades", batch_id)] = agg["t_cncl"]  or 0
    return counts


# ---------------------------------------------------------------------------
# gen_tradehistory leaf
# ---------------------------------------------------------------------------
def gen_tradehistory(spark, cfg, dbutils, base_df):
    """TradeHistory.txt B1 only — TH is historical-only."""
    log("[trade_split] gen_tradehistory — start")
    cutoff_ts = F.lit(int(FIRST_BATCH_DATE_END.timestamp())).cast("long")
    th1 = (base_df
        .withColumn("th_t_id", F.col("t_id").cast("string"))
        .withColumn("th_dts", F.date_format(F.col("_base_ts").cast("timestamp"), "yyyy-MM-dd HH:mm:ss"))
        .withColumn("th_st_id", F.when(F.col("_is_limit"), F.lit("PNDG")).otherwise(F.lit("SBMT")))
        .select("th_t_id", "th_dts", "th_st_id"))
    th2 = (base_df
        .filter(F.col("_is_limit") & (F.col("_submit_ts") <= cutoff_ts))
        .withColumn("th_t_id", F.col("t_id").cast("string"))
        .withColumn("th_dts", F.date_format(F.col("_submit_ts").cast("timestamp"), "yyyy-MM-dd HH:mm:ss"))
        .withColumn("th_st_id", F.when(F.col("_is_canceled"), F.lit("CNCL")).otherwise(F.lit("SBMT")))
        .select("th_t_id", "th_dts", "th_st_id"))
    th3 = (base_df
        .filter(~F.col("_is_canceled") & (F.col("_complete_ts") < cutoff_ts))
        .withColumn("th_t_id", F.col("t_id").cast("string"))
        .withColumn("th_dts", F.date_format(F.col("_complete_ts").cast("timestamp"), "yyyy-MM-dd HH:mm:ss"))
        .withColumn("th_st_id", F.lit("CMPT"))
        .select("th_t_id", "th_dts", "th_st_id"))
    th_df = th1.union(th2).union(th3)

    if cfg.augmented_incremental:
        th_p = (th_df
            .toDF("tradeid", "th_dts", "status")
            .filter(F.col("th_dts") < F.lit(AUG_FILES_DATE_END_EXCL))
            .withColumn("stg_target",
                F.when(F.col("th_dts") < F.lit(AUG_FILES_DATE_START), F.lit("tables"))
                 .otherwise(F.lit("files"))))
        write_delta(th_p, cfg=cfg, dataset="tradehistory", partition_cols=["stg_target"])
    else:
        write_file(th_df, f"{cfg.batch_path(1)}/TradeHistory.txt", "|", dbutils,
                   scale_factor=cfg.sf)

    tt = cfg.trade_total
    counts = {
        ("TradeHistory", 1):       int(tt * 2.54),
        ("TH_Records", 1):         int(tt * 2.54),
        ("TH_TLBTrades", 1):       int(tt * 0.87),
        ("TH_TLSTrades", 1):       int(tt * 0.87),
        ("TH_TMBTrades", 1):       int(tt * 0.4),
        ("TH_TMSTrades", 1):       int(tt * 0.4),
        ("TH_CanceledLTrades", 1): int(tt * 0.06),
    }
    if static_audits_available(cfg):
        log(f"[trade_split] gen_tradehistory — done, estimates")
        return counts

    # Dynamic-audit path: exact counts.
    th_with_type = th_df.join(
        base_df.select(
            F.col("t_id").cast("string").alias("th_t_id"),
            hash_key(F.col("t_id"), seed_for("T", "tt")).alias("_tt_rand_raw"),
            F.col("_is_canceled")),
        on="th_t_id", how="left")
    # Re-derive t_tt_id from the staged t_id (cheap, single-pass).
    th_with_type = th_with_type.withColumn("_tt_rand", F.col("_tt_rand_raw") % 100)
    th_with_type = th_with_type.withColumn("t_tt_id",
        F.when(F.col("_tt_rand") < 30, F.lit("TLB"))
         .when(F.col("_tt_rand") < 60, F.lit("TLS"))
         .when(F.col("_tt_rand") < 80, F.lit("TMB"))
         .otherwise(F.lit("TMS")))
    th_by_type = th_with_type.groupBy("t_tt_id").count().collect()
    tt_counts = {r["t_tt_id"]: r["count"] for r in th_by_type}
    th_total = sum(tt_counts.values())
    th_cncl_count = th_with_type.filter(F.col("th_st_id") == "CNCL").count()
    counts.update({
        ("TradeHistory", 1):       th_total,
        ("TH_Records", 1):         th_total,
        ("TH_TLBTrades", 1):       tt_counts.get("TLB", 0),
        ("TH_TLSTrades", 1):       tt_counts.get("TLS", 0),
        ("TH_TMBTrades", 1):       tt_counts.get("TMB", 0),
        ("TH_TMSTrades", 1):       tt_counts.get("TMS", 0),
        ("TH_CanceledLTrades", 1): th_cncl_count,
    })
    log(f"[trade_split] gen_tradehistory — done, exact counts")
    return counts


# ---------------------------------------------------------------------------
# gen_cashtransaction leaf
# ---------------------------------------------------------------------------
def _add_ct_derived(df):
    """Re-derive CT-only fields (_trade_val for ct_amt, _ct_name_raw +
    _ct_name_len for ct_name) from t_id + t_qty.
    """
    return (df
        .withColumn("_qty_val", F.col("t_qty").cast("double"))
        .withColumn("_bid", (hash_key(F.col("t_id"), seed_for("T", "bid")) % 900 + 101) / 100.0)
        .withColumn("_tp", F.col("_bid") * (0.95 + (hash_key(F.col("t_id"), seed_for("T", "tp")) % 11) / 100.0))
        .withColumn("_trade_val", F.col("_tp") * F.col("_qty_val"))
        .withColumn("_ct_name_len", (hash_key(F.col("t_id"), seed_for("T", "ctn")) % 91 + 10).cast("int"))
        .withColumn("_ct_name_raw",
            F.translate(F.concat(
                F.md5(F.concat(F.col("t_id").cast("string"), F.lit("ct1"))),
                F.md5(F.concat(F.col("t_id").cast("string"), F.lit("ct2"))),
                F.md5(F.concat(F.col("t_id").cast("string"), F.lit("ct3"))),
                F.md5(F.concat(F.col("t_id").cast("string"), F.lit("ct4")))),
                "0123456789abcdef",
                "ABCDEFGHIJKabcde")))


def gen_cashtransaction(spark, cfg, dbutils, base_df):
    """CashTransaction.txt B1 (+ B2/B3 incrementals in standard mode)."""
    log("[trade_split] gen_cashtransaction — start")
    batch_cutoff_s = int(FIRST_BATCH_DATE_END.timestamp())

    will_complete = base_df.filter(~F.col("_is_canceled"))
    ct_base = _add_ct_derived(will_complete)
    ct_base = (ct_base
        .withColumn("ct_ca_id", F.col("t_ca_id"))
        .withColumn("ct_dts", F.date_format(F.col("_cash_ts").cast("timestamp"), "yyyy-MM-dd HH:mm:ss"))
        .withColumn("ct_amt", F.format_string("%.2f",
            F.when(F.col("_is_buy"), -F.col("_trade_val")).otherwise(F.col("_trade_val"))))
        .withColumn("ct_name", F.substring(F.col("_ct_name_raw"), 1, F.col("_ct_name_len"))))

    ct_b1 = ct_base.filter(F.col("_cash_ts") < F.lit(batch_cutoff_s).cast("long")).select("ct_ca_id", "ct_dts", "ct_amt", "ct_name")
    if cfg.augmented_incremental:
        ct_p = (ct_b1
            .toDF("accountid", "ct_dts", "ct_amt", "ct_name")
            .filter(F.col("ct_dts") < F.lit(AUG_FILES_DATE_END_EXCL))
            .withColumn("stg_target",
                F.when(F.col("ct_dts") < F.lit(AUG_FILES_DATE_START), F.lit("tables"))
                 .otherwise(F.lit("files"))))
        write_delta(ct_p, cfg=cfg, dataset="cashtransaction", partition_cols=["stg_target"])
    else:
        write_file(ct_b1, f"{cfg.batch_path(1)}/CashTransaction.txt", "|", dbutils,
                   scale_factor=cfg.sf)

    tt = cfg.trade_total
    ct_count_est = int(tt * 0.94 * 0.997)
    counts = {
        ("CashTransaction", 1): ct_count_est,
        ("CT_Records", 1):      ct_count_est,
        ("CT_Trades", 1):       ct_count_est,
    }

    # B2/B3 incrementals — augmented skips.
    if not cfg.augmented_incremental:
        for b in range(2, NUM_INCREMENTAL_BATCHES + 2):
            b_start = batch_cutoff_s + (b - 2) * 86400
            b_end = batch_cutoff_s + (b - 1) * 86400
            ct_inc = (ct_base
                .filter((F.col("_cash_ts") >= F.lit(b_start).cast("long")) &
                        (F.col("_cash_ts") < F.lit(b_end).cast("long")))
                .withColumn("cdc_flag", F.lit("I"))
                .withColumn("cdc_dsn", F.monotonically_increasing_id() + 1)
                .select("cdc_flag", "cdc_dsn", "ct_ca_id", "ct_dts", "ct_amt", "ct_name"))
            write_file(ct_inc, f"{cfg.batch_path(b)}/CashTransaction.txt", "|", dbutils,
                       scale_factor=cfg.sf)
            ct_inc_est = int(cfg.trade_inc * 0.94)
            counts[("CT_Records", b)] = ct_inc_est
            counts[("CT_Trades", b)]  = ct_inc_est
            if not static_audits_available(cfg):
                ct_inc_count = ct_inc.count()
                counts[("CT_Records", b)] = ct_inc_count
                counts[("CT_Trades", b)]  = ct_inc_count

    if not static_audits_available(cfg):
        ct_count = ct_b1.count()
        counts[("CashTransaction", 1)] = ct_count
        counts[("CT_Records", 1)]      = ct_count
        counts[("CT_Trades", 1)]       = ct_count

    log(f"[trade_split] gen_cashtransaction — done, {len(counts)} entries")
    return counts


# ---------------------------------------------------------------------------
# gen_holdinghistory leaf
# ---------------------------------------------------------------------------
def gen_holdinghistory(spark, cfg, dbutils, dicts, base_df):
    """HoldingHistory.txt B1 (+ B2/B3 incrementals + new-market HH for the
    same-day-completing TMB/TMS new trades).
    """
    log("[trade_split] gen_holdinghistory — start")
    batch_cutoff_s = int(FIRST_BATCH_DATE_END.timestamp())
    n_valid = cfg.n_available_accounts
    n_brokers = _resolve_n_brokers(spark, cfg)
    num_sec = spark.table("_symbols").count()

    will_complete = base_df.filter(~F.col("_is_canceled"))
    hh_base = (will_complete
        .withColumn("hh_t_id", F.col("t_id").cast("string"))
        .withColumn("hh_h_t_id",
            F.when(F.col("_is_buy"), F.col("t_id").cast("string"))
             .otherwise(
                (hash_key(F.col("t_id"), seed_for("T", "hh_ref")) %
                    F.greatest(F.lit(1), F.col("t_id"))).cast("string")))
        .withColumn("hh_before_qty", F.when(F.col("_is_buy"), F.lit("0")).otherwise(F.col("t_qty")))
        .withColumn("hh_after_qty", F.when(F.col("_is_buy"), F.col("t_qty")).otherwise(F.lit("0"))))

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
        write_delta(hh_b1, cfg=cfg, dataset="holdinghistory",
                    partition_cols=["stg_target"])
    else:
        hh_b1 = (hh_base
            .filter(F.col("_complete_ts") < F.lit(batch_cutoff_s).cast("long"))
            .select("hh_h_t_id", "hh_t_id", "hh_before_qty", "hh_after_qty"))
        write_file(hh_b1, f"{cfg.batch_path(1)}/HoldingHistory.txt", "|", dbutils,
                   scale_factor=cfg.sf)

    hh_count = (int(cfg.trade_total * 0.94 * 0.997)
                if static_audits_available(cfg) else hh_b1.count())
    counts = {("HoldingHistory", 1): hh_count}

    # B2/B3 incrementals — augmented skips.
    if not cfg.augmented_incremental:
        for b in range(2, NUM_INCREMENTAL_BATCHES + 2):
            b_start = batch_cutoff_s + (b - 2) * 86400
            b_end = batch_cutoff_s + (b - 1) * 86400
            bs = seed_for(f"T_B{b}", "base")
            n_new = cfg.trade_inc_new

            hh_inc = (hh_base
                .filter((F.col("_complete_ts") >= F.lit(b_start).cast("long")) &
                        (F.col("_complete_ts") < F.lit(b_end).cast("long")))
                .withColumn("cdc_flag", F.lit("I"))
                .withColumn("cdc_dsn", F.monotonically_increasing_id() + 1)
                .select("cdc_flag", "cdc_dsn", "hh_h_t_id", "hh_t_id", "hh_before_qty", "hh_after_qty"))

            # Same new_df build as gen_trade leaf — derive new market HH rows.
            new_df = (spark.range(0, n_new).withColumnRenamed("id", "rid")
                .withColumn("t_id", (F.col("rid") + cfg.trade_total + (b - 2) * cfg.trade_inc).cast("string"))
                .withColumn("t_tt_id",
                    F.when(hash_key(F.col("rid"), bs) % 100 < 30, F.lit("TLB"))
                     .when(hash_key(F.col("rid"), bs) % 100 < 60, F.lit("TLS"))
                     .when(hash_key(F.col("rid"), bs) % 100 < 80, F.lit("TMB"))
                     .otherwise(F.lit("TMS")))
                .withColumn("t_qty", (hash_key(F.col("rid"), bs + 2) % 9991 + 10).cast("string"))
            )
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

            hh_combined = hh_inc.unionByName(new_market_hh)
            write_file(hh_combined, f"{cfg.batch_path(b)}/HoldingHistory.txt", "|", dbutils,
                       scale_factor=cfg.sf)

            counts[("HoldingHistory", b)] = (
                int(cfg.trade_inc * 0.94) if static_audits_available(cfg)
                else hh_combined.count())

    log(f"[trade_split] gen_holdinghistory — done, {len(counts)} entries")
    return counts
