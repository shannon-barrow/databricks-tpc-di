# Databricks notebook source
# FactMarketHistory incremental phase. Defines factmarkethistory as a
# streaming table via @dlt.table(replace_where=...) — the only API surface
# that supports REPLACE WHERE in declarative pipelines. @dlt.materialized_view
# does not accept replace_where, and a SQL CREATE OR REFRESH STREAMING TABLE
# can't be parameterized with per-batch REPLACE WHERE predicates.
#
# Why this exists as a separate notebook (split out of dlt_incremental.sql):
#   - DLT/SDP doesn't allow the same dataset to be declared in two languages
#     across a swap, AND we need REPLACE WHERE (Python-only) for FMH so
#     batches are idempotent and reprocess-safe (auto-retry, manual repair).
#   - The library swap in update_pipeline_notebook.py treats
#     {dlt_incremental.sql, dlt_incremental_fmh.py} as the incremental set
#     and dlt_historical.sql as the historical set; the parent job swaps
#     between them.
#
# Per-batch contract:
#   simulate_filedrops.py wipes batches_dir and creates exactly one
#   {batch_date}/ subdir before each pipeline update. We read that one
#   subdir name to derive target_date — no widget/conf plumbing required.
#
# Override: set pipeline configuration `factmarkethistory.target_date` to a
# YYYY-MM-DD literal to reprocess a specific date (sticks until cleared).

import dlt
from pyspark.sql.functions import col

scale_factor    = spark.conf.get("scale_factor")
catalog         = spark.conf.get("catalog")
wh_db           = spark.conf.get("wh_db")
tpcdi_directory = spark.conf.get("tpcdi_directory")
staging_db      = f"tpcdi_incremental_staging_{scale_factor}"
batches_dir     = f"{tpcdi_directory}augmented_incremental/_dailybatches/{wh_db}_{scale_factor}"

# COMMAND ----------

override = (spark.conf.get("factmarkethistory.target_date", "") or "").strip()
if override:
    target_date_str = override
    source = "override"
else:
    entries = dbutils.fs.ls(batches_dir)
    subdirs = [e.name.rstrip("/") for e in entries if e.isDir()]
    if len(subdirs) != 1:
        raise RuntimeError(
            f"factmarkethistory_incremental: expected exactly one batch_date subdir "
            f"under {batches_dir}, found {len(subdirs)}: {subdirs}. "
            f"simulate_filedrops should have wiped the dir and created a single "
            f"{{batch_date}}/ subdir before triggering the pipeline."
        )
    target_date_str = subdirs[0]
    source = "batches_dir subdir"
target_sk_dateid = int(target_date_str.replace("-", ""))

print(f"factmarkethistory target_date={target_date_str}  sk_dateid={target_sk_dateid}  source={source}")

# COMMAND ----------

@dlt.table(
    name="factmarkethistory",
    cluster_by=["sk_dateid"],
    comment=f"FactMarketHistory incremental flow — REPLACE WHERE sk_dateid={target_sk_dateid} (target_date={target_date_str}).",
    table_properties={"delta.dataSkippingNumIndexedCols": "34"},
    replace_where=col("sk_dateid") == target_sk_dateid,
)
def factmarkethistory():
    return spark.sql(f"""
        WITH sym_min_max AS (
          SELECT
            dm_s_symb,
            min_by(struct(dm_low,  dm_date), dm_low)  AS fiftytwoweeklow,
            max_by(struct(dm_high, dm_date), dm_high) AS fiftytwoweekhigh
          FROM bronzedailymarket
          WHERE dm_date >  date_sub(date('{target_date_str}'), 365)
            AND dm_date <= date('{target_date_str}')
          GROUP BY ALL
        )
        SELECT
          s.sk_securityid,
          s.sk_companyid,
          bigint(date_format(dm.dm_date, 'yyyyMMdd'))                       AS sk_dateid,
          try_divide(dm.dm_close, f.prev_year_basic_eps)                     AS peratio,
          (try_divide(s.dividend, dm.dm_close)) / 100                         AS yield,
          agg.fiftytwoweekhigh.dm_high                                       AS fiftytwoweekhigh,
          bigint(date_format(agg.fiftytwoweekhigh.dm_date, 'yyyyMMdd'))      AS sk_fiftytwoweekhighdate,
          agg.fiftytwoweeklow.dm_low                                         AS fiftytwoweeklow,
          bigint(date_format(agg.fiftytwoweeklow.dm_date,  'yyyyMMdd'))      AS sk_fiftytwoweeklowdate,
          dm.dm_close                                                         AS closeprice,
          dm.dm_high                                                          AS dayhigh,
          dm.dm_low                                                           AS daylow,
          dm.dm_vol                                                           AS volume
        FROM bronzedailymarket dm
        JOIN sym_min_max agg ON dm.dm_s_symb = agg.dm_s_symb
        JOIN {catalog}.{staging_db}.dimsecurity s
          ON  s.symbol      = dm.dm_s_symb
          AND dm.dm_date   >= s.effectivedate
          AND dm.dm_date    < s.enddate
        LEFT JOIN {catalog}.{staging_db}.companyyeareps f
          ON  f.sk_companyid     = s.sk_companyid
          AND quarter(dm.dm_date) = quarter(f.qtr_start_date)
          AND year(dm.dm_date)    = year(f.qtr_start_date)
        WHERE dm.dm_date = date('{target_date_str}')
    """)
