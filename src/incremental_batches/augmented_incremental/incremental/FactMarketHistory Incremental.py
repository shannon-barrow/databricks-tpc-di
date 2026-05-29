# Databricks notebook source
sf_ls = ["10", "100", "1000", "5000", "10000", "20000"]
dbutils.widgets.dropdown("scale_factor", sf_ls[0], sf_ls)
dbutils.widgets.text("tpcdi_directory", "/Volumes/tpcdi/tpcdi_raw_data/tpcdi_volume/")
dbutils.widgets.text("catalog", "tpcdi")
dbutils.widgets.text("wh_db", "")
dbutils.widgets.text("batch_date", "")

catalog         = dbutils.widgets.get("catalog")
scale_factor    = dbutils.widgets.get("scale_factor")
tpcdi_directory = dbutils.widgets.get("tpcdi_directory")
wh_db           = dbutils.widgets.get("wh_db")
batch_date      = dbutils.widgets.get("batch_date")
tgt_db          = f"{wh_db}_{scale_factor}"
table           = "factmarkethistory"
src_table       = f"{catalog}.{tgt_db}.bronzedailymarket"
tgt_table       = f"{catalog}.{tgt_db}.{table}"
checkpoint_dir  = f"{tpcdi_directory}augmented_incremental/_checkpoints/{tgt_db}/{table}"

# COMMAND ----------

# ONLY Excecute this code if you need to restart the stream over

# dbutils.fs.rm(f"{checkpoint_dir}", recurse=True)
# dbutils.fs.mkdirs(f"{checkpoint_dir}")
# spark.sql(f"RESTORE TABLE {tgt_table} TO VERSION AS OF 0")

# COMMAND ----------

def upsertToDelta(microBatchOutputDF, batch_id):
  # bronzedailymarket is pre-seeded by setup.py with the prior year of
  # DM data ([2015-07-06, 2016-07-05]). On the FIRST streaming microbatch
  # the Delta stream emits that initial commit's rows; without this filter
  # we'd recompute (and REPLACE USING) FMH for the pre-window sk_dateids
  # already covered by the FactMarketHistoryHistorical clone — wasteful
  # and would make batch 1 ~365× heavier than steady-state batches.
  from pyspark.sql import functions as F
  microBatchOutputDF.filter(F.col("dm_date") >= "2016-07-06").createOrReplaceTempView("bronzedailymarket")
  microBatchOutputDF.sparkSession.sql(f"""
    INSERT INTO {tgt_table}
    REPLACE USING (sk_dateid)
    with sym_min_max as (
      SELECT 
        dm_s_symb, 
        min_by(struct(dm_low, dm_date), dm_low) fiftytwoweeklow,
        max_by(struct(dm_high, dm_date), dm_high) fiftytwoweekhigh
      FROM {src_table}
      where dm_date > date_sub('{batch_date}', 365)
      group by all
    )
    SELECT /*+ BROADCAST(f) */
      s.sk_securityid,
      s.sk_companyid,
      bigint(date_format(dm.dm_date, 'yyyyMMdd')) sk_dateid,
      try_divide(dm.dm_close, f.prev_year_basic_eps) AS peratio,
      (try_divide(s.dividend, dm.dm_close)) / 100 yield,
      agg.fiftytwoweekhigh.dm_high fiftytwoweekhigh,
      bigint(date_format(agg.fiftytwoweekhigh.dm_date, 'yyyyMMdd')) sk_fiftytwoweekhighdate,
      agg.fiftytwoweeklow.dm_low fiftytwoweeklow,
      bigint(date_format(agg.fiftytwoweeklow.dm_date, 'yyyyMMdd')) sk_fiftytwoweeklowdate,
      dm.dm_close closeprice,
      dm.dm_high dayhigh,
      dm.dm_low daylow,
      dm.dm_vol volume        
    FROM bronzedailymarket dm
    JOIN sym_min_max agg
      ON 
        dm.dm_s_symb = agg.dm_s_symb
    JOIN {catalog}.{tgt_db}.dimsecurity s 
      ON 
        s.symbol = dm.dm_s_symb
        AND dm.dm_date >= s.effectivedate 
        AND dm.dm_date < s.enddate
    LEFT JOIN {catalog}.{tgt_db}.companyyeareps f 
      ON 
        f.sk_companyid = s.sk_companyid
        AND quarter(dm.dm_date) = quarter(f.qtr_start_date)
        AND year(dm.dm_date) = year(f.qtr_start_date)
  """)

# COMMAND ----------

(spark.readStream.table(src_table)
  .writeStream
  .option("checkpointLocation", checkpoint_dir)
  .trigger(availableNow=True)
  .foreachBatch(upsertToDelta)
  .outputMode("append")
  .start()
)