# Fabric Spark notebook (ported from augmented_incremental/bronze/account_updates_from_customer.py)
# Bronze-layer transform — structured streaming + foreachBatch INSERT.
# Streams bronzecustomer and, for each 'U' (update) row, derives the matching
# account row (joined to the current DimAccount SCD2 record) and appends it into
# bronzeaccount so DimAccount picks up customer-driven account changes.
# Ports 1:1 from Databricks; only the parameter source, catalog naming, and
# checkpoint path change for Fabric. Attach a schema-enabled default lakehouse.

# --- PARAMETER CELL (Fabric injects overrides) ---
scale_factor    = "10"
wh_db           = ""
checkpoint_base = "Files/augmented_incremental/_checkpoints"
# -------------------------------------------------

tgt_db          = f"{wh_db}_{scale_factor}"
src_table       = f"{tgt_db}.bronzecustomer"
tgt_table       = f"{tgt_db}.bronzeaccount"
# Matches the Databricks setup's checkpoint subdir name for this stream.
checkpoint_dir  = f"{checkpoint_base}/{tgt_db}/bronzeaccountcustomer"

# COMMAND ----------

# ONLY Execute this code if you need to restart the stream over

# notebookutils.fs.rm(f"{checkpoint_dir}", recurse=True)
# notebookutils.fs.mkdirs(f"{checkpoint_dir}")
# spark.sql(f"RESTORE TABLE {tgt_table} TO VERSION AS OF 1")

# COMMAND ----------

def customeraccountupdates(microBatchOutputDF, batch_id):
  microBatchOutputDF.createOrReplaceTempView("bronzecustomer")
  microBatchOutputDF.sparkSession.sql(f"""
    INSERT INTO {tgt_table}
    SELECT
      "cust_update" cdc_flag,
      -1 cdc_dsn,
      a.accountid,
      a.sk_brokerid brokerid,
      c.customerid,
      a.accountdesc,
      a.taxstatus,
      a.status,
      c.update_dt
    FROM bronzecustomer c
    JOIN {tgt_db}.DimAccount a
      ON
        c.customerid = substring(cast(a.sk_customerid as string), 9)
        and a.iscurrent
        and c.update_dt > a.effectivedate
    WHERE
      cdc_flag = 'U'
  """)

# COMMAND ----------

# awaitTermination() is REQUIRED on Fabric: start() returns immediately and a
# triggered notebook reaches a terminal state without it — the batch_runner DAG
# would report this activity done before the append finishes.
q = (spark.readStream
  .table(src_table)
  .writeStream
  .option("checkpointLocation", checkpoint_dir)
  .trigger(availableNow=True)
  .foreachBatch(customeraccountupdates)
  .outputMode("append")
  .start()
)
q.awaitTermination()
