# Databricks notebook source
df = spark.sql("""
    SELECT table_name 
    FROM system.information_schema.tables 
    WHERE table_catalog = 'main' 
    AND table_schema = 'tpcdi_raw_data'
""")

table_names = [row.table_name for row in df.collect()]

# COMMAND ----------

try:
    spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
except Exception:
    pass  # serverless: VACUUM RETAIN 0 will fail on serverless anyway; a 7-day retention isn't worth aborting the optimize loop over.
for tbl in table_names:
  spark.sql(f"alter table main.tpcdi_raw_data.{tbl} set tblproperties ('delta.targetFileSize' = '1GB')")
  spark.sql(f"optimize main.tpcdi_raw_data.{tbl}")
  spark.sql(f"analyze table main.tpcdi_raw_data.{tbl} compute statistics for all columns")
  spark.sql(f"VACUUM main.tpcdi_raw_data.{tbl} RETAIN 0 HOURS")