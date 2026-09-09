# Fabric Spark notebook (ported from augmented_incremental/incremental/FactHoldings Incremental.py)
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
table           = "factholdings"
src_table       = f"{tgt_db}.bronzeholdings"
tgt_table       = f"{tgt_db}.{table}"
checkpoint_dir  = f"{checkpoint_base}/{tgt_db}/{table}"

# COMMAND ----------

# ONLY Excecute this code if you need to restart the stream over

# notebookutils.fs.rm(f"{checkpoint_dir}", recurse=True)
# notebookutils.fs.mkdirs(f"{checkpoint_dir}")
# spark.sql(f"RESTORE TABLE {tgt_table} TO VERSION AS OF 0")

# COMMAND ----------

def upsertToDelta(microBatchOutputDF, batch_id):
  # Query shape intentionally mirrors the SDP factholdings_incremental flow
  # so the cross-variant comparison stays apples-to-apples — the
  # sk_closedateid predicate lives in the ON clause and references the
  # per-row h.event_dt rather than a constant. Spark constant-folds
  # bronzeholdings's per-microbatch event_dt into the join's sk_closedateid
  # filter, letting dimtrade prune via its Liquid CLUSTER BY (sk_closedateid).
  microBatchOutputDF.createOrReplaceTempView("bronzeholdings")
  microBatchOutputDF.sparkSession.sql(f"""
    INSERT INTO {tgt_table}
    with h as (
      SELECT
        h.hh_h_t_id tradeid,
        h.hh_t_id currenttradeid,
        h.hh_after_qty currentholding,
        h.event_dt
      FROM bronzeholdings h
    )
    SELECT
      h.tradeid,
      currenttradeid,
      t.sk_customerid,
      t.sk_accountid,
      t.sk_securityid,
      t.sk_companyid,
      t.sk_closedateid sk_dateid,
      t.sk_closetimeid sk_timeid,
      t.tradeprice currentprice,
      currentholding
    FROM h
    JOIN {tgt_db}.dimtrade t
      ON t.tradeid = h.tradeid
     AND t.sk_closedateid = bigint(date_format(h.event_dt, 'yyyyMMdd'))
    """)

# COMMAND ----------

(spark.readStream.table(src_table)
  .writeStream
  .option("checkpointLocation", checkpoint_dir)
  .trigger(availableNow=True)
  .foreachBatch(upsertToDelta)
  .outputMode("append")
  .start()
).awaitTermination()   # Fabric: block until the availableNow stream finishes (see ingest_bronze)
