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
src_table       = f"{catalog}.{tgt_db}.bronzecustomer"
tgt_table       = f"{catalog}.{tgt_db}.bronzeaccount"
checkpoint_dir  = f"{tpcdi_directory}augmented_incremental/_checkpoints/{tgt_db}/bronzeaccountcustomer"

# COMMAND ----------

# ONLY Excecute this code if you need to restart the stream over

# dbutils.fs.rm(f"{checkpoint_dir}", recurse=True)
# dbutils.fs.mkdirs(f"{checkpoint_dir}")
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
    JOIN {catalog}.{tgt_db}.DimAccount a
      ON 
        c.customerid = substring(cast(a.sk_customerid as string), 9)
        and a.iscurrent
        and c.update_dt > a.effectivedate
    WHERE
      cdc_flag = 'U'
  """)

# COMMAND ----------

(spark.readStream
  .table(src_table)
  .writeStream
  .option("checkpointLocation", checkpoint_dir)
  .trigger(availableNow=True)
  .foreachBatch(customeraccountupdates)
  .outputMode("append")
  .start()
)