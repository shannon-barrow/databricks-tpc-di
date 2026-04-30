# Databricks notebook source
# Serverless rejects most spark.conf.set() calls with CONFIG_NOT_AVAILABLE — wrap in try/except so the runtime's own defaults take over there.
try:
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 262144000)
    spark.conf.set("spark.databricks.adaptive.autoBroadcastJoinThreshold", 262144000)
except Exception:
    pass

# COMMAND ----------

sf_ls = ["10", "100", "1000", "5000", "10000", "20000"]
dbutils.widgets.dropdown("scale_factor", sf_ls[0], sf_ls)
dbutils.widgets.text("tpcdi_directory", "/Volumes/tpcdi/tpcdi_raw_data/tpcdi_volume/")
dbutils.widgets.text("catalog", "tpcdi")
dbutils.widgets.text("wh_db", "")

catalog         = dbutils.widgets.get("catalog")
scale_factor    = dbutils.widgets.get("scale_factor")
tpcdi_directory = dbutils.widgets.get("tpcdi_directory")
wh_db           = dbutils.widgets.get("wh_db")
tgt_db          = f"{wh_db}_{scale_factor}"
table           = "currentaccountbalances"
src_table       = f"{catalog}.{tgt_db}.bronzecashtransaction"
tgt_table       = f"{catalog}.{tgt_db}.{table}"
checkpoint_dir  = f"{tpcdi_directory}augmented_incremental/_checkpoints/{tgt_db}/{table}"

# COMMAND ----------

# ONLY Excecute this code if you need to restart the stream over

# dbutils.fs.rm(f"{checkpoint_dir}", recurse=True)
# dbutils.fs.mkdirs(f"{checkpoint_dir}")
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
)