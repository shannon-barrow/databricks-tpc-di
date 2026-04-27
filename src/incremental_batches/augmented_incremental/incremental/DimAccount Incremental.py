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
table           = "dimaccount"
src_table       = f"{catalog}.{tgt_db}.bronzeaccount"
tgt_table       = f"{catalog}.{tgt_db}.{table}"
checkpoint_dir  = f"{tpcdi_directory}augmented_incremental/_checkpoints/{tgt_db}/{table}"

# COMMAND ----------

# ONLY Excecute this code if you need to restart the stream over

# dbutils.fs.rm(f"{checkpoint_dir}", recurse=True)
# dbutils.fs.mkdirs(f"{checkpoint_dir}")
# spark.sql(f"RESTORE TABLE {tgt_table} TO VERSION AS OF 0")

# COMMAND ----------

def upsertToDelta(microBatchOutputDF, batch_id):
  microBatchOutputDF.createOrReplaceTempView("bronzeaccount")
  microBatchOutputDF.sparkSession.sql(f"""
    with accounts as (
      SELECT * except(cdc_dsn)
      FROM bronzeaccount a
      QUALIFY row_number() over (partition by update_dt, accountid order by cdc_flag desc) = 1
    ),
    all_incr_updates as (
      SELECT
        bigint(concat(date_format(a.update_dt, 'yyyyMMdd'), a.accountid)) sk_accountid,
        accountid,
        brokerid sk_brokerid,
        dc.sk_customerid,
        accountdesc,
        taxstatus,
        decode(a.status, 
          'ACTV',	'Active',
          'CMPT','Completed',
          'CNCL','Canceled',
          'PNDG','Pending',
          'SBMT','Submitted',
          'INAC','Inactive',
          a.status) status,
        true iscurrent,
        update_dt effectivedate,
        date('9999-12-31') enddate
      FROM accounts a
      JOIN {catalog}.{tgt_db}.dimcustomer dc
        ON 
          dc.iscurrent
          and dc.customerid = a.customerid
    ),
    matched_accts as (
      SELECT
        s.*
      FROM all_incr_updates s
      JOIN {catalog}.{tgt_db}.DimAccount t
        ON s.accountid = t.accountid
      WHERE t.iscurrent
    )
    MERGE INTO {tgt_table} t USING (
      SELECT
        CAST(NULL AS BIGINT) AS mergeKey,
        *
      FROM all_incr_updates
      UNION ALL
      SELECT 
        accountid mergeKey,
        *
      FROM matched_accts
    ) s 
    ON t.accountid = s.mergeKey AND t.iscurrent
    WHEN MATCHED AND t.iscurrent THEN UPDATE SET
      t.iscurrent = false,
      t.enddate = s.effectivedate
    WHEN NOT MATCHED THEN INSERT (sk_accountid, accountid, sk_brokerid, sk_customerid, accountdesc, TaxStatus, status, iscurrent, effectivedate, enddate)
    VALUES (sk_accountid, accountid, sk_brokerid, sk_customerid, accountdesc, TaxStatus, status, iscurrent, effectivedate, enddate)
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