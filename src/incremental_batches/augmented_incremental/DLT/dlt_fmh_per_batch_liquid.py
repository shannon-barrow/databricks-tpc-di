# Databricks notebook source
# MAGIC %md
# MAGIC # FactMarketHistory — SDP foreachBatch variant (liquid pipeline only)
# MAGIC
# MAGIC Standalone Python notebook used as part of the SF=10 SDP_FMH_Dev
# MAGIC pipeline. Pairs with `dlt_incremental_liquid_fmhdev.sql`, which has
# MAGIC the FMH and `factmarkethistorystg` sections commented out — this
# MAGIC notebook owns the incremental FMH writes via `@dp.foreach_batch_sink`.
# MAGIC
# MAGIC **Why a sink instead of a streaming-table flow?**
# MAGIC The classic SDP `factmarkethistory` flow joins `STREAM(bronzedailymarket)`
# MAGIC to a side `factmarkethistorystg` MV that materializes a per-symbol
# MAGIC date-sorted array. That works for the common 1-day-per-batch case
# MAGIC but skews when a batch covers multiple days (e.g. catch-up after a
# MAGIC gap) — every row in the batch ends up sharing the same lookback
# MAGIC anchor.
# MAGIC
# MAGIC `@dp.foreach_batch_sink` lets us run a small Python loop per micro-
# MAGIC batch: enumerate the distinct `dm_date` values that arrived, and
# MAGIC fire one `INSERT INTO factmarkethistory REPLACE USING (sk_dateid)`
# MAGIC per day with that day's correct 365-day rolling window. Each day
# MAGIC gets its own correctly-windowed compute, idempotent via the
# MAGIC `REPLACE USING (sk_dateid)` predicate.
# MAGIC
# MAGIC **Wiring**: `@dp.append_flow(target="factmarkethistory_per_batch_sink")`
# MAGIC reads `STREAM(bronzedailymarket)` and routes the rows to the sink.
# MAGIC The sink writes directly to the existing `factmarkethistory` Delta
# MAGIC table (declared + seeded by `dlt_historical_liquid` during the
# MAGIC historical phase).

# COMMAND ----------

from pyspark import pipelines as dp
from pyspark.sql import functions as F

scale_factor = spark.conf.get("scale_factor")
catalog      = spark.conf.get("catalog")
wh_db        = spark.conf.get("wh_db")

target_schema = f"{wh_db}_{scale_factor}"
fmh_target    = f"{catalog}.{target_schema}.factmarkethistory"
bronze_table  = f"{catalog}.{target_schema}.bronzedailymarket"
staging_db    = f"{catalog}.tpcdi_incremental_staging_{scale_factor}"

# COMMAND ----------

# foreachBatch sink. Runs once per pipeline update microbatch.
#
# microbatch_df = the rows the @dp.append_flow streamed in (today's
#                 bronzedailymarket arrivals, possibly multiple days if
#                 the pipeline caught up after a gap).
# batch_id      = monotonic per-update integer; not needed for our logic.
#
# For each distinct dm_date in the microbatch, run one
# INSERT INTO ... REPLACE USING (sk_dateid). The 365-day rolling window
# is computed against the *static* bronzedailymarket (which holds the
# pre-staged year + every prior batch's arrivals), giving a correctly
# anchored lookback per day even when the microbatch covers multiple
# days.
@dp.foreach_batch_sink(name="factmarkethistory_per_batch_sink")
def fmh_per_batch_sink(microbatch_df, batch_id):
    sess = microbatch_df.sparkSession

    # Skip pre-window rows (already covered by the historical INSERT INTO
    # ONCE flow during set_pipeline_historical / run_historical_pipeline).
    # Without this filter, the first incremental update would emit FMH
    # for the backfilled year and double-write what the historical seed
    # already wrote.
    days = (microbatch_df
            .filter(F.col("dm_date") >= F.lit("2016-07-06"))
            .select("dm_date").distinct()
            .orderBy("dm_date").collect())

    for row in days:
        d = row.dm_date  # python date
        sess.sql(f"""
            INSERT INTO {fmh_target}
            REPLACE USING (sk_dateid)
            WITH sym_min_max AS (
              SELECT
                dm_s_symb,
                min_by(struct(dm_low, dm_date), dm_low)   AS fiftytwoweeklow,
                max_by(struct(dm_high, dm_date), dm_high) AS fiftytwoweekhigh
              FROM {bronze_table}
              WHERE dm_date BETWEEN date_sub(date('{d}'), 365) AND date('{d}')
              GROUP BY ALL
            ),
            eps AS (
              -- companyyeareps is partitioned by qtr_start_date. Filtering
              -- on quarter()/year() of qtr_start_date doesn't push down to
              -- partition pruning — Spark can't reason through the
              -- function calls. Compute the actual qtr_start_date for
              -- the batch's quarter and use direct equality so the
              -- optimizer prunes to one (or two) partitions.
              SELECT sk_companyid, prev_year_basic_eps
              FROM {staging_db}.companyyeareps
              WHERE qtr_start_date = make_date(
                year(date('{d}')),
                1 + 3 * (quarter(date('{d}')) - 1),
                1
              )
            )
            SELECT
              s.sk_securityid,
              s.sk_companyid,
              bigint(date_format(dm.dm_date, 'yyyyMMdd')) AS sk_dateid,
              try_divide(dm.dm_close, f.prev_year_basic_eps) AS peratio,
              try_divide(s.dividend, dm.dm_close) / 100      AS yield,
              agg.fiftytwoweekhigh.dm_high  AS fiftytwoweekhigh,
              bigint(date_format(agg.fiftytwoweekhigh.dm_date, 'yyyyMMdd')) AS sk_fiftytwoweekhighdate,
              agg.fiftytwoweeklow.dm_low    AS fiftytwoweeklow,
              bigint(date_format(agg.fiftytwoweeklow.dm_date, 'yyyyMMdd'))  AS sk_fiftytwoweeklowdate,
              dm.dm_close AS closeprice,
              dm.dm_high  AS dayhigh,
              dm.dm_low   AS daylow,
              dm.dm_vol   AS volume
            FROM {bronze_table} dm
            JOIN sym_min_max agg
              ON dm.dm_s_symb = agg.dm_s_symb
            JOIN {staging_db}.dimsecurity s
              ON s.symbol = dm.dm_s_symb
             AND dm.dm_date >= s.effectivedate
             AND dm.dm_date <  s.enddate
            LEFT JOIN eps f
              ON f.sk_companyid = s.sk_companyid
            WHERE dm.dm_date = date('{d}')
        """)

# COMMAND ----------

# Stream router: reads bronzedailymarket as a streaming source and
# routes each microbatch's rows to the sink above. The flow function
# returns a streaming DataFrame; SDP wraps it in a foreachBatch writer
# that hands the DataFrame off to fmh_per_batch_sink.
@dp.append_flow(target="factmarkethistory_per_batch_sink")
def fmh_per_batch_flow():
    return spark.readStream.table("bronzedailymarket")
