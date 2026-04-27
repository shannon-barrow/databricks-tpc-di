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
table           = "factholdings"
src_table       = f"{catalog}.{tgt_db}.bronzeholdings"
tgt_table       = f"{catalog}.{tgt_db}.{table}"
checkpoint_dir  = f"{tpcdi_directory}augmented_incremental/_checkpoints/{tgt_db}/{table}"

# COMMAND ----------

# ONLY Excecute this code if you need to restart the stream over

# dbutils.fs.rm(f"{checkpoint_dir}", recurse=True)
# dbutils.fs.mkdirs(f"{checkpoint_dir}")
# spark.sql(f"RESTORE TABLE {tgt_table} TO VERSION AS OF 0")

# COMMAND ----------

def upsertToDelta(microBatchOutputDF, batch_id):
  microBatchOutputDF.createOrReplaceTempView("bronzeholdings")
  microBatchOutputDF.sparkSession.sql(f"""
    INSERT INTO {tgt_table}
    with h as (
      SELECT
        h.hh_h_t_id tradeid,
        h.hh_t_id currenttradeid,
        h.hh_after_qty currentholding
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
    JOIN {catalog}.{tgt_db}.dimtrade t 
      ON 
        t.tradeid = h.tradeid
    WHERE t.sk_closedateid = bigint(date_format('{batch_date}', 'yyyyMMdd'))
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