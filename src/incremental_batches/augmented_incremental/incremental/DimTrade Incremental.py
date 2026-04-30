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
table           = "dimtrade"
src_table       = f"{catalog}.{tgt_db}.bronzetrade"
tgt_table       = f"{catalog}.{tgt_db}.{table}"
checkpoint_dir  = f"{tpcdi_directory}augmented_incremental/_checkpoints/{tgt_db}/{table}"

# COMMAND ----------

# ONLY Excecute this code if you need to restart the stream over

# dbutils.fs.rm(f"{checkpoint_dir}", recurse=True)
# dbutils.fs.mkdirs(f"{checkpoint_dir}")
# spark.sql(f"RESTORE TABLE {tgt_table} TO VERSION AS OF 0")

# COMMAND ----------

def upsertToDelta(microBatchOutputDF, batch_id):
  microBatchOutputDF.createOrReplaceTempView("bronzetrade")
  microBatchOutputDF.sparkSession.sql(f"""
    with trades as (
      SELECT
        tradeid,
        min(case when cdc_flag = "I" then t_dts end) create_ts,
        max_by(
          struct(
            t_dts,
            status,
            t_tt_id,
            cashflag,
            t_s_symb,
            quantity,
            bidprice,
            t_ca_id,
            executedby,
            tradeprice,
            fee,
            commission,
            tax
          ),
          t_dts
        ) current_record
      FROM bronzetrade
      group by tradeid
    ),
    current_trades as (
      SELECT
        tradeid,
        create_ts,
        CASE
          WHEN current_record.status IN ("CMPT", "CNCL") THEN current_record.t_dts 
          END close_ts,
        decode(current_record.status, 
          'ACTV',	'Active',
          'CMPT','Completed',
          'CNCL','Canceled',
          'PNDG','Pending',
          'SBMT','Submitted',
          'INAC','Inactive') status,
        decode(current_record.t_tt_id,
          'TMB', 'Market Buy',
          'TMS', 'Market Sell',
          'TSL', 'Stop Loss',
          'TLS', 'Limit Sell',
          'TLB', 'Limit Buy'
        ) type,
        if(current_record.cashflag = 1, TRUE, FALSE) cashflag,
        current_record.t_s_symb,
        current_record.quantity,
        current_record.bidprice,
        current_record.t_ca_id,
        current_record.executedby,
        current_record.tradeprice,
        current_record.fee,
        current_record.commission,
        current_record.tax,
        current_record.t_dts
      FROM trades t
    ),
    final as (
      SELECT 
        t.tradeid,
        sk_brokerid,
        bigint(date_format(create_ts, 'yyyyMMdd')) sk_createdateid,
        bigint(date_format(create_ts, 'HHmmss')) sk_createtimeid,
        bigint(date_format(close_ts, 'yyyyMMdd')) sk_closedateid,
        bigint(date_format(close_ts, 'HHmmss')) sk_closetimeid,
        t.status,
        t.type,
        cashflag,
        sk_securityid,
        sk_companyid,
        t.quantity,
        t.bidprice,
        sk_customerid,
        sk_accountid,
        t.executedby,
        t.tradeprice,
        t.fee,
        t.commission,
        t.tax
      FROM current_trades t
      JOIN {catalog}.{tgt_db}.dimsecurity ds
        ON 
          ds.symbol = t.t_s_symb
          AND date(t_dts) >= ds.effectivedate 
          AND date(t_dts) < ds.enddate
      JOIN {catalog}.{tgt_db}.dimaccount da
        ON 
          t.t_ca_id = da.accountid 
          AND da.iscurrent
    )
    MERGE INTO {tgt_table} t
    USING final s
      ON 
        t.tradeid = s.tradeid
        AND t.sk_closedateid is null
    WHEN MATCHED THEN UPDATE SET
      sk_closedateid = s.sk_closedateid,
      sk_closetimeid = s.sk_closetimeid,
      status = s.status,
      type = s.type,
      cashflag = s.cashflag,
      quantity = s.quantity,
      bidprice = s.bidprice,
      executedby = s.executedby,
      tradeprice = s.tradeprice,
      fee = s.fee,
      commission = s.commission,
      tax = s.tax
    WHEN NOT MATCHED THEN INSERT (tradeid, sk_brokerid, sk_createdateid, sk_createtimeid, sk_closedateid, sk_closetimeid, status, type, cashflag, sk_securityid, sk_companyid, quantity, bidprice, sk_customerid, sk_accountid, executedby, tradeprice, fee, commission, tax)
    VALUES (tradeid, sk_brokerid, sk_createdateid, sk_createtimeid, sk_closedateid, sk_closetimeid, status, type, cashflag, sk_securityid, sk_companyid, quantity, bidprice, sk_customerid, sk_accountid, executedby, tradeprice, fee, commission, tax)
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