# Fabric Spark notebook (ported from augmented_incremental/incremental/FactWatches Incremental.py)
# Silver/gold incremental transform — structured streaming + foreachBatch MERGE.
# Ports 1:1 from Databricks; only the parameter source, catalog naming, and
# checkpoint path change for Fabric. Attach a schema-enabled default lakehouse.

# --- PARAMETER CELL (Fabric injects overrides) ---
scale_factor    = "10"
wh_db           = ""
checkpoint_base = "Files/augmented_incremental/_checkpoints"
# -------------------------------------------------

sf_ls = ["10", "100", "1000", "5000", "10000", "20000"]

tgt_db          = f"{wh_db}_{scale_factor}"
table           = "factwatches"
src_table       = f"{tgt_db}.bronzewatches"
tgt_table       = f"{tgt_db}.{table}"
checkpoint_dir  = f"{checkpoint_base}/{tgt_db}/{table}"

# COMMAND ----------

# ONLY Excecute this code if you need to restart the stream over

# notebookutils.fs.rm(f"{checkpoint_dir}", recurse=True)
# notebookutils.fs.mkdirs(f"{checkpoint_dir}")
# spark.sql(f"RESTORE TABLE {tgt_table} TO VERSION AS OF 0")

# COMMAND ----------

def upsertToDelta(microBatchOutputDF, batch_id):
  microBatchOutputDF.createOrReplaceTempView("bronzewatches")
  microBatchOutputDF.sparkSession.sql(f"""
    with w as (
      SELECT 
        w_c_id customerid,
        w_s_symb symbol,        
        date(min(if(w_action != 'CNCL', w_dts, cast(null as timestamp)))) dateplaced,
        date(max(if(w_action = 'CNCL', w_dts, cast(null as timestamp)))) dateremoved
      FROM bronzewatches
      group by all
    ),
    stage as (
      SELECT
        c.sk_customerid sk_customerid,
        s.sk_securityid sk_securityid,
        w.customerid,
        w.symbol,
        bigint(date_format(w.dateplaced, 'yyyyMMdd')) sk_dateid_dateplaced,
        bigint(date_format(w.dateremoved, 'yyyyMMdd')) sk_dateid_dateremoved,
        nvl2(w.dateremoved, True, False) removed
      from w
      JOIN {tgt_db}.dimsecurity s 
        ON 
          s.symbol = w.symbol
          AND s.iscurrent
      JOIN {tgt_db}.dimcustomer c 
        ON
          w.customerid = c.customerid
          AND c.iscurrent
    )
    MERGE INTO {tgt_table} t
    USING stage s
    ON
      !t.removed
      AND t.symbol = s.symbol
      AND t.customerid = s.customerid
      AND t.sk_dateid_dateremoved IS NULL
    WHEN MATCHED THEN UPDATE SET
      t.sk_dateid_dateremoved = s.sk_dateid_dateremoved,
      t.removed = True
    WHEN NOT MATCHED THEN 
    INSERT (sk_customerid, sk_securityid, customerid, symbol, sk_dateid_dateplaced, sk_dateid_dateremoved, removed)
    VALUES (sk_customerid, sk_securityid, customerid, symbol, sk_dateid_dateplaced, sk_dateid_dateremoved, removed)
    """)

# COMMAND ----------

(spark.readStream.table(src_table)
  .writeStream
  .option("checkpointLocation", checkpoint_dir)
  .trigger(availableNow=True)
  .foreachBatch(upsertToDelta)
  .outputMode("update")
  .start()
).awaitTermination()   # Fabric: block until the availableNow stream finishes (see ingest_bronze)
