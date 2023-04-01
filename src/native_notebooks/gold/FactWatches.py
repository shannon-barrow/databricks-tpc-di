# Databricks notebook source
import json

with open("../../tools/traditional_config.json", "r") as json_conf:
  table_conf = json.load(json_conf)['views']['WatchHistory']
user_name = spark.sql("select current_user()").collect()[0][0].split("@")[0].replace(".","_")

dbutils.widgets.text("wh_db", f"{user_name}_TPCDI",'Root name of Target Warehouse')
dbutils.widgets.text("tpcdi_directory", "/tmp/tpcdi/", "Directory where Raw Files are located")
dbutils.widgets.text("scale_factor", "10", "Scale factor")
dbutils.widgets.text("batch_id", "1", "Batch ID (1,2,3)")

batch_id = dbutils.widgets.get("batch_id")
wh_db = f"{dbutils.widgets.get('wh_db')}_wh"
staging_db = f"{dbutils.widgets.get('wh_db')}_stage"
scale_factor = dbutils.widgets.get("scale_factor")
tpcdi_directory = dbutils.widgets.get("tpcdi_directory")
files_directory = f"{tpcdi_directory}sf={scale_factor}"
tgt_cols = "sk_customerid, sk_securityid, sk_dateid_dateplaced, sk_dateid_dateremoved, batchid"

# COMMAND ----------

if(batch_id == '1'):
  spark.read.csv(
    f"{files_directory}/{table_conf['path']}/{table_conf['filename']}", 
    schema=table_conf['raw_schema'], 
    sep=table_conf['sep'], 
    header=table_conf['header'], 
    inferSchema=False).createOrReplaceTempView("Watch_src")
else:
  spark.sql(f"""
  CREATE OR REPLACE TEMPORARY VIEW Watch_src AS SELECT
    * except(cdc_flag, cdc_dsn)
  FROM {staging_db}.WatchIncremental
  WHERE batchid = cast({batch_id} as int)
""")

# COMMAND ----------

spark.sql(f"""
  CREATE OR REPLACE TEMPORARY VIEW Watch_date AS SELECT 
    w_c_id customerid,
    w_s_symb symbol,
    d.sk_dateid,
    d.datevalue recorddate,
    w_action
  FROM Watch_src
  JOIN {wh_db}.DimDate d
      ON d.datevalue = date(w_dts) 
""")

# COMMAND ----------

spark.sql(f"""
  CREATE OR REPLACE TEMPORARY VIEW Watch_tmp AS SELECT
    -- Need to keep the orig values of the ACTV record, only need to change dateremoved on CNCL record
    nvl(actv.customerid, cncl.customerid) customerid,
    nvl(actv.symbol, cncl.symbol) symbol,
    actv.recorddate,
    actv.sk_dateid sk_dateid_dateplaced, 
    cncl.sk_dateid sk_dateid_dateremoved
  FROM (SELECT * FROM Watch_date WHERE w_action = 'ACTV') actv
  FULL OUTER JOIN (SELECT * FROM Watch_date WHERE w_action = 'CNCL') cncl 
    ON
      actv.customerid = cncl.customerid
      AND actv.symbol = cncl.symbol
""")

# COMMAND ----------

# Historical needs to handle effective/end dates. Otherwise just need currently active record
cust_join = "w.recorddate >= c.effectivedate AND w.recorddate < c.enddate" if batch_id == '1' else "c.iscurrent"
sec_join = "w.recorddate >= s.effectivedate AND w.recorddate < s.enddate" if batch_id == '1' else "s.iscurrent"

spark.sql(f"""
  CREATE OR REPLACE TEMPORARY VIEW Watch_actv AS SELECT
    c.sk_customerid sk_customerid,
    s.sk_securityid sk_securityid,
    sk_dateid_dateplaced,
    sk_dateid_dateremoved,
    cast({batch_id} as int) batchid
  FROM Watch_tmp w
  JOIN {staging_db}.DimCustomerStg c 
    ON
      w.customerid = c.customerid
      AND {cust_join}
  JOIN {wh_db}.DimSecurity s 
    ON 
      s.symbol = w.symbol
      AND {sec_join}
  WHERE w.sk_dateid_dateplaced IS NOT NULL -- This limits these results to only the records in the batch where at least there was a watch created in the batch, otherwise if it is null then it is a CANCEL record removing a past batch's watch
""")

# COMMAND ----------

spark.sql(f"""
  CREATE OR REPLACE TEMPORARY VIEW Watch_cncl AS SELECT
    fw.sk_customerid, 
    fw.sk_securityid, 
    cast(null as bigint) sk_dateid_dateplaced, 
    w.sk_dateid_dateremoved,
    cast({batch_id} as int) batchid
  FROM (
    SELECT
      sk_customerid,
      sk_securityid
    FROM {wh_db}.FactWatches
    WHERE isnull(sk_dateid_dateremoved) -- Active watches
  ) fw
  JOIN {staging_db}.DimCustomerStg c 
    ON fw.sk_customerid = c.sk_customerid
  JOIN {wh_db}.DimSecurity s 
    ON fw.sk_securityid = s.sk_securityid
  JOIN (
    SELECT 
      customerid,
      symbol,
      sk_dateid_dateremoved
    FROM Watch_tmp
    WHERE isnull(sk_dateid_dateplaced) -- Canceled Watches
  ) w
  ON
    w.customerid = c.customerid
    AND  w.symbol = s.symbol
  """)

# COMMAND ----------

if(batch_id == '1'):
  spark.sql(f"""
    INSERT OVERWRITE {wh_db}.FactWatches ({tgt_cols})
    SELECT {tgt_cols} FROM Watch_actv
  """)
else:
  spark.sql(f"""
    MERGE INTO {wh_db}.FactWatches t
    USING (
      SELECT
        sk_customerid AS merge_sk_customerid,
        sk_securityid AS merge_sk_securityid,
        wc.*
      FROM Watch_cncl wc
      UNION ALL
      SELECT 
        CAST(NULL AS BIGINT) AS merge_sk_customerid,
        CAST(NULL AS BIGINT) AS merge_sk_securityid,
        wa.*
      FROM Watch_actv wa
      WHERE sk_dateid_dateplaced IS NOT NULL) s
    ON 
      t.sk_customerid = s.merge_sk_customerid
      AND t.sk_securityid = s.merge_sk_securityid
      AND isnull(t.sk_dateid_dateremoved)
    WHEN MATCHED THEN UPDATE SET
      t.sk_dateid_dateremoved = s.sk_dateid_dateremoved
      ,t.batchid = s.batchid
    WHEN NOT MATCHED THEN 
    INSERT ({tgt_cols})
    VALUES ({tgt_cols})
  """)
