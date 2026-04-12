"""Generate DailyMarket table.

Mirrors DIGen's DailyMarketBlackBox:
- One record per active security per calendar day (including weekends/holidays)
- Securities only appear after their FINWIRE creation quarter
- INAC (delisted) securities are excluded after their deactivation
- dm_close: random uniform 0.50-1000.00
- dm_high: dm_close × random(1.0, 1.5) — always >= dm_close
- dm_low: dm_close × random(0.5, 1.0) — always <= dm_close
- dm_vol: random uniform 1,000-1,000,000,000
- Incremental: CDC format with cdc_flag='I', cdc_dsn

Approach: Sequence + Explode
-----------------------------
Instead of a cross-join between all symbols and all dates (which would materialize
the full Cartesian product upfront and cause memory pressure), we:
  1. Attach a per-symbol date range [start_date, end_date] based on its creation
     and deactivation quarters, clamped to the DailyMarket window.
  2. Use F.sequence() to generate a date array per row (one row = one symbol).
  3. Use F.explode() to fan each symbol's date array into individual (symbol, date) rows.
This lets Spark generate dates lazily per partition and avoids shuffling a massive
cross-join. Repartitioning BEFORE explode distributes the sequence generation across
executors so no single partition is stuck expanding all 540M+ rows (at SF=1000).

Price Generation Formulas
--------------------------
All prices are deterministic via hash-based seeding (day_id, sym_id, salt):
  - dm_close = (abs(hash) % 99951) / 100 + 0.50  => range [0.50, 1000.00]
  - dm_high  = dm_close * (1.0 + abs(hash) % 501 / 1000)  => [1.0x, 1.5x] of close
  - dm_low   = dm_close * (0.5 + abs(hash) % 501 / 1000)  => [0.5x, 1.0x] of close
  - dm_vol   = abs(hash) % 999_999_001 + 1000  => range [1000, 1_000_000_000]

Temporal Filtering
-------------------
Each security has a creation_quarter (when its SEC record appeared in FINWIRE) and
a deactivation_quarter (when it was delisted, or fw_quarters+1 if never delisted).
These quarter IDs are converted to calendar dates via _quarter_to_date_expr. A
security only produces DailyMarket rows for dates within
[max(creation_date, DM_BEGIN), min(deactivation_date, DM_END)].
For incremental batches, the same temporal filter applies: only securities whose
creation_date <= inc_date < deactivation_date produce a row for that day.
"""

from datetime import timedelta
from pyspark.sql import SparkSession, functions as F
from .config import *
from .utils import write_file, seed_for, hash_key


def generate(spark: SparkSession, cfg, dbutils) -> dict:
    """Generate DailyMarket for all batches.

    Uses sequence + explode instead of cross-join for efficiency:
    1. For each symbol, generate a date array from max(creation_date, DM_BEGIN) to min(deact_date, DM_END)
    2. Explode the array to get one row per (symbol, date)
    3. Generate random market data values per row

    Args:
        spark: Active SparkSession.
        cfg: ScaleConfig with sf, dm_days, batch_path(), etc.
        dbutils: Databricks dbutils for file I/O.

    Returns:
        dict mapping (table_name, batch_id) to row counts for audit reporting.
    """
    return _gen_daily_market(spark, cfg, dbutils)


def _quarter_to_date_expr(quarter_col):
    """Convert a FINWIRE quarter_id (integer) to a calendar date.

    FINWIRE quarters are sequential integers starting at 0 for FW_BEGIN_DATE.
    Each quarter spans ONE_QUARTER_MS milliseconds. This function computes:
        date = FW_BEGIN_DATE + quarter_id * ONE_QUARTER_MS
    The result is a Spark date column used to determine when a security was
    created or deactivated, enabling temporal filtering of DailyMarket rows.
    """
    fw_begin_s = int(FW_BEGIN_DATE.timestamp())
    quarter_s = ONE_QUARTER_MS // 1000
    return (F.lit(fw_begin_s).cast("long") + quarter_col.cast("long") * F.lit(quarter_s)).cast("timestamp").cast("date")


def _gen_daily_market(spark, cfg, dbutils):
    """Core DailyMarket generation for historical (batch 1) and incremental batches.

    Historical batch: generates the full date range [DM_BEGIN, DM_END] for every
    active symbol, producing one row per (symbol, date) pair.

    Incremental batches (2..N): each batch covers a single day. Only symbols
    active on that day produce a row, written in CDC format with cdc_flag='I'.
    """
    dm_begin_date = DM_BEGIN_DATE.strftime("%Y-%m-%d")
    dm_end_date = DM_END_DATE.strftime("%Y-%m-%d")

    # _symbols temp view was created by finwire.generate() in Wave 1.
    # Contains: Symbol, creation_quarter, deactivation_quarter, _idx
    symbols_df = spark.table("_symbols")
    num_sec = symbols_df.count()

    # Build symbols with creation/deactivation dates converted from quarter IDs
    # to calendar dates, then clamp to the DailyMarket date window.
    syms_df = (symbols_df
        .select(
            F.col("_idx").alias("sym_id"),
            F.col("Symbol").alias("dm_s_symb"),
            F.col("creation_quarter"),
            F.col("deactivation_quarter"))
        .withColumn("_create_date", _quarter_to_date_expr(F.col("creation_quarter")))
        .withColumn("_deact_date", _quarter_to_date_expr(F.col("deactivation_quarter")))
        # Clamp to DM date range: symbol active on [max(create, DM_BEGIN), min(deact-1, DM_END)]
        # Deactivation date is exclusive — the security is NOT active on the deactivation day.
        # Use date_sub to get the last active day before deactivation.
        .withColumn("_start_date", F.greatest(F.col("_create_date"), F.lit(dm_begin_date).cast("date")))
        .withColumn("_end_date", F.least(F.date_sub(F.col("_deact_date"), 1), F.lit(dm_end_date).cast("date")))
        # Only include symbols that have at least one valid day
        .filter(F.col("_start_date") <= F.col("_end_date"))
    )

    # --- Sequence + Explode pattern ---
    # Generate a date array per symbol using F.sequence(), then explode into rows.
    # Repartition BEFORE explode so the sequence generation runs in parallel across
    # executors. Without this, a single partition would expand all rows sequentially.
    # Partition count scales with SF: e.g. SF=1000 -> 250 partitions.
    num_partitions = max(2, int(0.25 * cfg.sf))
    dm_df = (syms_df
        .repartition(num_partitions)
        .withColumn("_dates", F.sequence(F.col("_start_date"), F.col("_end_date"), F.expr("INTERVAL 1 DAY")))
        .select("sym_id", "dm_s_symb", F.explode("_dates").alias("dm_date"))
        .withColumn("dm_date", F.date_format("dm_date", "yyyy-MM-dd"))
        # day_id = days since DM_BEGIN; used as part of the hash seed for deterministic prices
        .withColumn("day_id", F.datediff(F.col("dm_date"), F.lit(dm_begin_date)))
    )

    # --- Price generation ---
    # Each price field uses a distinct hash salt so (day_id, sym_id) produces
    # independent pseudo-random values for close, high, low, and volume.
    # dm_close is the base; dm_high and dm_low are derived as multipliers of dm_close.
    dm_df = (dm_df
        .withColumn("_seed_c", F.hash(F.col("day_id"), F.col("sym_id"), F.lit(seed_for("DM", "c"))).cast("long"))
        # dm_close: uniform in [0.50, 1000.00] (99951 discrete values at 0.01 granularity)
        .withColumn("_close", F.abs(F.col("_seed_c")) % 99951 / 100.0 + 0.50)
        .withColumn("dm_close", F.format_string("%.2f", F.col("_close")))
        # dm_high: close * [1.0, 1.5] — guaranteed >= close
        .withColumn("dm_high", F.format_string("%.2f",
            F.col("_close") * (1.0 + F.abs(F.hash(F.col("day_id"), F.col("sym_id"), F.lit(seed_for("DM", "h"))).cast("long")) % 501 / 1000.0)))
        # dm_low: close * [0.5, 1.0] — guaranteed <= close
        .withColumn("dm_low", F.format_string("%.2f",
            F.col("_close") * (0.5 + F.abs(F.hash(F.col("day_id"), F.col("sym_id"), F.lit(seed_for("DM", "l"))).cast("long")) % 501 / 1000.0)))
        # dm_vol: uniform in [1000, 1_000_000_000]
        .withColumn("dm_vol",
            (F.abs(F.hash(F.col("day_id"), F.col("sym_id"), F.lit(seed_for("DM", "v"))).cast("long")) % 999999001 + 1000).cast("string"))
        .select("dm_date", "dm_s_symb", "dm_close", "dm_high", "dm_low", "dm_vol")
    )

    # Write batch 1 (historical): pipe-delimited flat file
    estimated_total = cfg.dm_days * num_sec  # upper bound before deactivation filtering
    write_file(dm_df, f"{cfg.batch_path(1)}/DailyMarket.txt", "|", dbutils,
               scale_factor=cfg.sf)
    counts = {("DailyMarket", 1): estimated_total}
    print(f"  DailyMarket: estimated ~{estimated_total:,} historical ({cfg.dm_days} days × ~{num_sec} syms, actual may be lower due to deactivations)")

    # --- Incremental batches (batch 2, 3, ...) ---
    # Each incremental batch covers a single calendar day. Only securities whose
    # creation_date <= inc_date < deactivation_date are included. Output is in
    # CDC format: cdc_flag='I' (insert), cdc_dsn = sequential row number.
    for batch_id in range(2, NUM_INCREMENTAL_BATCHES + 2):
        inc_date = (FIRST_BATCH_DATE + timedelta(days=batch_id - 2)).strftime("%Y-%m-%d")

        # Filter symbols active on this date (temporal filtering)
        active_syms = (symbols_df
            .select(F.col("_idx").alias("sym_id"), F.col("Symbol").alias("dm_s_symb"),
                    F.col("creation_quarter"), F.col("deactivation_quarter"))
            .withColumn("_create_date", _quarter_to_date_expr(F.col("creation_quarter")))
            .withColumn("_deact_date", _quarter_to_date_expr(F.col("deactivation_quarter")))
            .filter(
                (F.col("_create_date") <= F.lit(inc_date).cast("date")) &
                (F.col("_deact_date") > F.lit(inc_date).cast("date")))
        )

        # Generate one row per active symbol for this single day, using the same
        # price formulas but with different hash salts ("inc_c", "inc_h", etc.)
        # to produce different values than the historical batch.
        inc_df = (active_syms
            .withColumn("cdc_flag", F.lit("I"))
            .withColumn("cdc_dsn", F.col("sym_id").cast("long") + 1)
            .withColumn("dm_date", F.lit(inc_date))
            .withColumn("day_id", F.datediff(F.lit(inc_date), F.lit(dm_begin_date)))
            .withColumn("_seed_c", F.hash(F.col("day_id"), F.col("sym_id"), F.lit(seed_for("DM", "inc_c"))).cast("long"))
            .withColumn("_close", F.abs(F.col("_seed_c")) % 99951 / 100.0 + 0.50)
            .withColumn("dm_close", F.format_string("%.2f", F.col("_close")))
            .withColumn("dm_high", F.format_string("%.2f",
                F.col("_close") * (1.0 + F.abs(F.hash(F.col("day_id"), F.col("sym_id"), F.lit(seed_for("DM", "inc_h"))).cast("long")) % 501 / 1000.0)))
            .withColumn("dm_low", F.format_string("%.2f",
                F.col("_close") * (0.5 + F.abs(F.hash(F.col("day_id"), F.col("sym_id"), F.lit(seed_for("DM", "inc_l"))).cast("long")) % 501 / 1000.0)))
            .withColumn("dm_vol",
                (F.abs(F.hash(F.col("day_id"), F.col("sym_id"), F.lit(seed_for("DM", "inc_v"))).cast("long")) % 999999001 + 1000).cast("string"))
            .select("cdc_flag", "cdc_dsn", "dm_date", "dm_s_symb", "dm_close", "dm_high", "dm_low", "dm_vol")
        )
        write_file(inc_df, f"{cfg.batch_path(batch_id)}/DailyMarket.txt", "|", dbutils,
               scale_factor=cfg.sf)
        counts[("DailyMarket", batch_id)] = num_sec

    return counts
