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
table           = "factcashbalances"
src_table       = f"{catalog}.{tgt_db}.currentaccountbalances"
tgt_table       = f"{catalog}.{tgt_db}.{table}"
checkpoint_dir  = f"{tpcdi_directory}augmented_incremental/_checkpoints/{tgt_db}/{table}"

# COMMAND ----------

# ONLY Excecute this code if you need to restart the full pipeline over again
# spark.sql(f"RESTORE TABLE {tgt_table} TO VERSION AS OF 0")

# COMMAND ----------

# Switched from INSERT OVERWRITE (with partitionOverwriteMode='dynamic') to
# REPLACE WHERE for parity with the dbt port and to use the modern Delta
# selective-overwrite primitive. REPLACE WHERE works compute-independently
# (cluster, serverless, warehouse) without the spark.session config tweak.
spark.sql(f"""
  INSERT INTO {tgt_table}
  REPLACE WHERE sk_dateid = bigint(date_format('{batch_date}','yyyyMMdd'))
  SELECT
    a.sk_customerid,
    a.sk_accountid,
    bigint(date_format(c.ct_date, 'yyyyMMdd')) sk_dateid,
    c.current_account_cash
  FROM {src_table} c
  JOIN {catalog}.{tgt_db}.dimaccount a
    ON
      c.accountid = a.accountid
      AND a.iscurrent
  where c.latest_batch
""")