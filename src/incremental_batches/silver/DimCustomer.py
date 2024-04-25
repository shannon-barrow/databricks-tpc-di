# Databricks notebook source
# MAGIC %md
# MAGIC # DimCustomer
# MAGIC * Read from the staging table, join to Prospect table, then write to DimCustomer

# COMMAND ----------

# MAGIC %md
# MAGIC # Setup

# COMMAND ----------

import string

user_name = spark.sql("select lower(regexp_replace(split(current_user(), '@')[0], '(\\\W+)', ' '))").collect()[0][0]
default_catalog = 'tpcdi' if spark.conf.get('spark.databricks.unityCatalog.enabled') == 'true' else 'hive_metastore'
default_wh = f"{string.capwords(user_name).replace(' ','_')}_TPCDI"

dbutils.widgets.text("catalog", default_catalog, 'Target Catalog')
dbutils.widgets.text("wh_db", default_wh,'Target Database')
dbutils.widgets.text("batch_id", "1", "Batch ID (1,2,3)")

catalog = dbutils.widgets.get("catalog")
wh_db = f"{dbutils.widgets.get('wh_db')}"
staging_db = f"{dbutils.widgets.get('wh_db')}_stage"
batch_id = dbutils.widgets.get("batch_id")
spark.sql(f"USE CATALOG {catalog}")

# COMMAND ----------

spark.sql(f"""
  INSERT OVERWRITE {wh_db}.DimCustomer 
  SELECT
    sk_customerid,
    c.customerid,
    c.taxid,
    c.status,
    c.lastname,
    c.firstname,
    c.middleinitial,
    if(c.gender IN ('M', 'F'), c.gender, 'U') gender,
    c.tier,
    c.dob,
    c.addressline1,
    c.addressline2,
    c.postalcode,
    c.city,
    c.stateprov,
    c.country,
    c.phone1,
    c.phone2,
    c.phone3,
    c.email1,
    c.email2,
    r_nat.TX_NAME as nationaltaxratedesc,
    r_nat.TX_RATE as nationaltaxrate,
    r_lcl.TX_NAME as localtaxratedesc,
    r_lcl.TX_RATE as localtaxrate,
    p.agencyid,
    p.creditrating,
    p.networth,
    p.marketingnameplate,
    c.iscurrent,
    c.batchid,
    c.effectivedate,
    c.enddate
  FROM {staging_db}.DimCustomerStg c
  JOIN {wh_db}.TaxRate r_lcl 
    ON c.LCL_TX_ID = r_lcl.TX_ID
  JOIN {wh_db}.TaxRate r_nat 
    ON c.NAT_TX_ID = r_nat.TX_ID
  LEFT JOIN {wh_db}.Prospect p 
    on upper(p.lastname) = upper(c.lastname)
    and upper(p.firstname) = upper(c.firstname)
    and upper(p.addressline1) = upper(c.addressline1)
    and upper(nvl(p.addressline2, '')) = upper(nvl(c.addressline2, ''))
    and upper(p.postalcode) = upper(c.postalcode);
""")

# COMMAND ----------

spark.sql(f"ANALYZE TABLE {wh_db}.DimCustomer COMPUTE STATISTICS FOR ALL COLUMNS")
