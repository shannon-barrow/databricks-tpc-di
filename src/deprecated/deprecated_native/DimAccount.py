# Databricks notebook source
# MAGIC %md
# MAGIC # Incremental DimAccount

# COMMAND ----------

from pyspark.sql.functions import lit
dbutils.widgets.text("env",'a01','Name of the environment')
dbutils.widgets.text("files_directory", "/tmp/tpc-di", "Directory where Files are located")
dbutils.widgets.text("scale_factor", "10", "Scale factor")
dbutils.widgets.text("batch_id", "1", "Batch ID (1,2,3)")

env = dbutils.widgets.get("env")
files_directory = dbutils.widgets.get("files_directory")
batch_id = dbutils.widgets.get("batch_id")
scale_factor = dbutils.widgets.get("scale_factor")
wh_db = f"{env}_tpcdi_warehouse"
spark.conf.set("spark.sql.shuffle.partitions", sc.defaultParallelism)

# COMMAND ----------

accountSchema = """
  cdc_flag STRING COMMENT 'Denotes insert or update',
  cdc_dsn BIGINT COMMENT 'Database Sequence Number',
  accountid BIGINT COMMENT 'Customer account identifier',
  ca_b_id BIGINT COMMENT 'Identifier of the managing broker',
  ca_c_id BIGINT COMMENT 'Owning customer identifier',
  accountDesc STRING COMMENT 'Name of customer account',
  TaxStatus TINYINT COMMENT 'Tax status of this account',
  ca_st_id STRING COMMENT 'Customer status type identifier'
"""

# COMMAND ----------

spark.read.csv(f'{files_directory}/{scale_factor}/Batch{batch_id}/Account.txt', schema=accountSchema, sep='|', header=False, inferSchema=False) \
  .withColumn('batchid', lit(f'{batch_id}').cast('int')) \
  .createOrReplaceTempView('AccountView')

# COMMAND ----------

# MAGIC %md
# MAGIC # The Updated DimCustomer Surrogate Key Values Need to be carried to DimAccount
# MAGIC * Surrogate Keys need referential integrity
# MAGIC * Code has to go get DimCustoer current batch, match the CURRENT batches customerid to the PREVIOUS CURRENT customerid, take the previous sk_customerid
# MAGIC * This is because the DimAccount table does not have a customerid, only the sk_customerid. So we need the OLD active surrogate key so we can update it to the NEW surrogate key

# COMMAND ----------

spark.sql(f"""
  CREATE OR REPLACE TEMPORARY VIEW UpdateDimAccountView AS
  SELECT 
    c.accountid,
    c.sk_brokerid,
    a.sk_customerid,
    c.status,    
    c.accountdesc,
    c.taxstatus,
    a.effectivedate,
    a.enddate
  FROM (
    SELECT 
      sk_customerid, 
      customerid,
      effectivedate,
      enddate
    FROM {wh_db}.DimCustomer a
    WHERE iscurrent AND a.batchid = {batch_id}) a 
  JOIN (
    SELECT 
      sk_customerid, 
      customerid,
      enddate
    FROM {wh_db}.DimCustomer a
    WHERE 
      a.batchid < {batch_id}
      AND !iscurrent) b
  ON 
    a.customerid = b.customerid
    AND b.enddate = a.effectivedate  
  JOIN {wh_db}.DimAccount c
    ON 
      b.sk_customerid = c.sk_customerid
      AND c.iscurrent
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC --cache table UpdateDimAccountView

# COMMAND ----------

spark.sql(f"""
  CREATE OR REPLACE TEMPORARY VIEW DimAccountView AS
  SELECT
    nvl(a.accountid, b.accountid) accountid,
    nvl(a.sk_brokerid, b.sk_brokerid) sk_brokerid,
    nvl(a.sk_customerid, b.sk_customerid) sk_customerid,
    nvl(a.status, b.status) status,
    nvl(a.accountdesc, b.accountdesc) accountdesc,
    nvl(a.TaxStatus, b.TaxStatus) TaxStatus,
    true iscurrent,
    {batch_id} batchid,
    nvl(a.effectivedate, b.effectivedate) effectivedate,
    nvl(a.enddate, b.enddate) enddate
  FROM (
    SELECT
      accountid,
      b.sk_brokerid,
      dc.sk_customerid,
      st_name as status,
      accountDesc,
      TaxStatus,
      bd.batchdate effectivedate,
      to_date('9999-12-31') enddate
    FROM AccountView a
    JOIN {wh_db}.batchdate bd
      ON a.batchid = bd.batchid
    JOIN {wh_db}.StatusType st 
      ON a.CA_ST_ID = st.st_id
    JOIN (
      SELECT customerid, sk_customerid
      FROM {wh_db}.DimCustomer 
      WHERE iscurrent) dc
      ON dc.customerid = a.ca_c_id
    LEFT JOIN {wh_db}.DimBroker b 
        ON a.ca_b_id = b.brokerid) a
  FULL OUTER JOIN UpdateDimAccountView b
    ON a.accountid = b.accountid
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- cache table DimAccountView;
# MAGIC -- uncache table UpdateDimAccountView;

# COMMAND ----------

spark.sql(f"""
  CREATE OR REPLACE TEMPORARY VIEW MatchedAccountsView AS
  SELECT
    CAST(NULL AS BIGINT) mergeKey,
    s.accountid,
    nvl(s.sk_brokerid, t.sk_brokerid) sk_brokerid,
    s.sk_customerid,
    s.status,
    nvl(s.accountdesc, t.accountdesc) accountdesc,
    nvl(s.taxstatus, t.taxstatus) taxstatus,
    true iscurrent,
    {batch_id} batchid,
    s.effectivedate,
    s.enddate    
  FROM DimAccountView s
  JOIN {wh_db}.DimAccount t
    ON s.accountid = t.accountid
  WHERE
    t.iscurrent;
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- cache table MatchedAccountsView;

# COMMAND ----------

spark.sql(f"""
  MERGE INTO {wh_db}.DimAccount t USING (
    SELECT
      dav.accountid AS mergeKey,
      dav.*
    FROM
      DimAccountView dav
    UNION ALL
    SELECT
      *
    FROM
      MatchedAccountsView) s 
    ON t.accountid = s.mergeKey
  WHEN MATCHED AND t.iscurrent THEN UPDATE SET
    t.iscurrent = false,
    t.enddate = s.effectivedate
  WHEN NOT MATCHED THEN INSERT (
    accountid,
    sk_brokerid,
    sk_customerid,
    status,
    accountdesc,
    taxstatus,
    iscurrent,
    batchid,
    effectivedate,
    enddate
  )
  VALUES (
    s.accountid,
    s.sk_brokerid,
    s.sk_customerid,
    s.status,
    s.accountdesc,
    s.taxstatus,
    s.iscurrent,
    s.batchid,
    s.effectivedate,
    s.enddate
  )
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- uncache table DimAccountView;
# MAGIC -- uncache table MatchedAccountsView;
