# Databricks notebook source
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
table           = "factwatches"
src_table       = f"{catalog}.{tgt_db}.bronzewatches"
tgt_table       = f"{catalog}.{tgt_db}.{table}"
checkpoint_dir  = f"{tpcdi_directory}augmented_incremental/_checkpoints/{tgt_db}/{table}"

# COMMAND ----------

# ONLY Excecute this code if you need to restart the stream over

# dbutils.fs.rm(f"{checkpoint_dir}", recurse=True)
# dbutils.fs.mkdirs(f"{checkpoint_dir}")
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
      JOIN {catalog}.{tgt_db}.dimsecurity s 
        ON 
          s.symbol = w.symbol
          AND s.iscurrent
      JOIN {catalog}.{tgt_db}.dimcustomer c 
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
)