# Fabric Spark notebook (ported from augmented_incremental/incremental/currentaccountbalances Incremental.py)
# Silver/gold incremental transform — structured streaming + foreachBatch MERGE.
# Ports 1:1 from Databricks; only the parameter source, catalog naming, and
# checkpoint path change for Fabric. Attach a schema-enabled default lakehouse.

# --- PARAMETER CELL (Fabric injects overrides) ---
scale_factor    = "10"
wh_db           = ""
checkpoint_base = "Files/augmented_incremental/_checkpoints"
# -------------------------------------------------

# NOTE: the manual spark.sql.autoBroadcastJoinThreshold=250MB bump (ported from
# Databricks) was REMOVED — on Fabric Spark it drove AQE to broadcast an 8.2 GiB build
# side in the silver MERGEs at SF=20000, hitting Spark's hard 8 GiB broadcast cap. Use
# Fabric's default threshold (10 MB).

# COMMAND ----------

sf_ls = ["10", "100", "1000", "5000", "10000", "20000"]

tgt_db          = f"{wh_db}_{scale_factor}"
table           = "currentaccountbalances"
src_table       = f"{tgt_db}.bronzecashtransaction"
tgt_table       = f"{tgt_db}.{table}"
checkpoint_dir  = f"{checkpoint_base}/{tgt_db}/{table}"

# COMMAND ----------

# ONLY Excecute this code if you need to restart the stream over

# notebookutils.fs.rm(f"{checkpoint_dir}", recurse=True)
# notebookutils.fs.mkdirs(f"{checkpoint_dir}")
# spark.sql(f"RESTORE TABLE {tgt_table} TO VERSION AS OF 0")

# COMMAND ----------

def upsertToDelta(microBatchOutputDF, batch_id):
  microBatchOutputDF.createOrReplaceTempView("bronzecashtransaction")
  microBatchOutputDF.sparkSession.sql(f"""
    INSERT OVERWRITE {tgt_table}
    with c as (
      SELECT 
        to_date(ct_dts) ct_date,
        accountid,
        ct_amt,
        True latest_batch
      FROM bronzecashtransaction
      UNION ALL 
      SELECT 
        ct_date,
        accountid,
        current_account_cash,
        False latest_batch
      FROM {tgt_table}
    )
    SELECT
      max(ct_date) ct_date,
      accountid,
      cast(sum(ct_amt) as DECIMAL(15,2)) current_account_cash,
      max(latest_batch) latest_batch
    FROM c
    GROUP BY ALL
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
