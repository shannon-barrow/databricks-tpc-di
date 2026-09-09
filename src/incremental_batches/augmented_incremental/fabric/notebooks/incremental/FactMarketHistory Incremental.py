# Fabric Spark notebook (ported from augmented_incremental/incremental/FactMarketHistory Incremental.py)
# Silver/gold incremental transform — structured streaming + foreachBatch MERGE.
# Ports 1:1 from Databricks; only the parameter source, catalog naming, and
# checkpoint path change for Fabric. Attach a schema-enabled default lakehouse.

# --- PARAMETER CELL (Fabric injects overrides) ---
scale_factor    = "10"
wh_db           = ""
batch_date      = ""       # this batch's date; the 52-week lookback window keys off it
checkpoint_base = "Files/augmented_incremental/_checkpoints"
# -------------------------------------------------

sf_ls = ["10", "100", "1000", "5000", "10000", "20000"]

tgt_db          = f"{wh_db}_{scale_factor}"
table           = "factmarkethistory"
src_table       = f"{tgt_db}.bronzedailymarket"
tgt_table       = f"{tgt_db}.{table}"
checkpoint_dir  = f"{checkpoint_base}/{tgt_db}/{table}"

# COMMAND ----------

# ONLY Excecute this code if you need to restart the stream over

# notebookutils.fs.rm(f"{checkpoint_dir}", recurse=True)
# notebookutils.fs.mkdirs(f"{checkpoint_dir}")
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
  _spark = microBatchOutputDF.sparkSession
  # Fabric Spark 4.1 has no `INSERT INTO ... REPLACE USING` (Databricks-only). Build this
  # batch's rows, then DELETE matching sk_dateid + INSERT — same selective-overwrite
  # semantics, portable Delta 4.2 SQL.
  _src = _spark.sql(f"""
    with sym_min_max as (
      SELECT 
        dm_s_symb, 
        min_by(struct(dm_low, dm_date), dm_low) fiftytwoweeklow,
        max_by(struct(dm_high, dm_date), dm_high) fiftytwoweekhigh
      FROM {src_table}
      where dm_date > date_sub('{batch_date}', 365)
      group by all
    )
    -- No BROADCAST(f) hint: companyyeareps is ~950M rows at SF=20000. Databricks/Photon
    -- AQE silently demotes an over-threshold broadcast hint, but Fabric Spark 4.1 honors
    -- it literally — collecting the 950M-row build side to the driver blows
    -- spark.driver.maxResultSize (Spark_User_Driver_MaxResultSizeExceeded). Let AQE pick
    -- the join strategy against autoBroadcastJoinThreshold instead.
    SELECT
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
    JOIN {tgt_db}.dimsecurity s 
      ON 
        s.symbol = dm.dm_s_symb
        AND dm.dm_date >= s.effectivedate 
        AND dm.dm_date < s.enddate
    LEFT JOIN {tgt_db}.companyyeareps f 
      ON 
        f.sk_companyid = s.sk_companyid
        AND quarter(dm.dm_date) = quarter(f.qtr_start_date)
        AND year(dm.dm_date) = year(f.qtr_start_date)
  """)
  _src.createOrReplaceTempView("_fmh_src")
  # Delta on Fabric rejects subqueries in DELETE — use a literal IN-list of this batch's dateids.
  _dateids = [int(r[0]) for r in _src.select("sk_dateid").distinct().collect() if r[0] is not None]
  if _dateids:
      _spark.sql(f"DELETE FROM {tgt_table} WHERE sk_dateid IN ({','.join(map(str, _dateids))})")
  _spark.sql(f"INSERT INTO {tgt_table} SELECT * FROM _fmh_src")

# COMMAND ----------

(spark.readStream.table(src_table)
  .writeStream
  .option("checkpointLocation", checkpoint_dir)
  .trigger(availableNow=True)
  .foreachBatch(upsertToDelta)
  .outputMode("append")
  .start()
).awaitTermination()   # Fabric: block until the availableNow stream finishes (see ingest_bronze)
