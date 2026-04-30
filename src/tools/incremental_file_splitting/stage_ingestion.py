# Databricks notebook source
from glob import glob

# COMMAND ----------

dbutils.widgets.dropdown("scale_factor", "10", ["10", "100", "1000", "5000", "10000", "20000"])
dbutils.widgets.text("tpcdi_directory", "/Volumes/tpcdi/tpcdi_raw_data/tpcdi_volume/")
dbutils.widgets.text("catalog", "tpcdi")

catalog = dbutils.widgets.get("catalog")
scale_factor = dbutils.widgets.get("scale_factor")
tpcdi_directory = dbutils.widgets.get("tpcdi_directory")

tables_ls = [
    ["rawaccount", "update_dt", "Account.txt", "csv"],
    ["rawcashtransaction", "event_dt", "CashTransaction.txt", "csv"],
    ["rawcustomer", "update_dt", "Customer.txt", "csv"],
    ["rawdailymarket", "dm_date", "DailyMarket.txt", "csv"],
    ["rawholdings", "event_dt", "HoldingHistory.txt", "csv"],
    ["rawtrade", "event_dt", "Trade.txt", "csv"],
    ["rawwatches", "event_dt", "WatchHistory.txt", "csv"],
]

staging_dir = f"{tpcdi_directory}augmented_incremental/sf={scale_factor}"

# COMMAND ----------

df = spark.sql(f"""
    SELECT DISTINCT CAST(update_dt AS STRING) update_dt
    FROM {catalog}.tpcdi_raw_data.rawcustomer{scale_factor}
    WHERE update_dt >= '2015-07-06'
    ORDER BY update_dt
""")

dates_ls = [row.update_dt for row in df.collect()]

# COMMAND ----------

for date in dates_ls:
  for tbl, part_col, filename, fmt in tables_ls:
    df = spark.sql(f"""
      SELECT *
      FROM {catalog}.tpcdi_raw_data.{tbl}{scale_factor}
      WHERE {part_col} = '{date}'
    """)
    if fmt == 'csv':
      df.coalesce(1).write.options(header=False, delimiter="|").mode("overwrite").csv(f"{staging_dir}/{tbl}/{date}")
    elif fmt == 'parquet':
      df.coalesce(1).write.mode("overwrite").parquet(f"{staging_dir}/{tbl}/{date}")

# COMMAND ----------

for date in dates_ls:
  for tbl, part_col, filename, fmt in tables_ls:
    for file in glob(f"{staging_dir}/{tbl}/{date}/part-00000-*.{fmt}"):
      dbutils.fs.cp(file, f"{tpcdi_directory}augmented_incremental/_staging/sf={scale_factor}/{date}/{filename}")