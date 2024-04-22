# Databricks notebook source
# MAGIC %md
# MAGIC # CustomerMgmt is NOT a required table, but it's source XML file is used as source data for multiple downstream tables
# MAGIC * Downstream tables include historical Customer and Account Data
# MAGIC * Parse the XML once and store it as a source for both historical Customer and Account Data

# COMMAND ----------

# MAGIC %md
# MAGIC # Setup

# COMMAND ----------

import json
import string

spark.conf.set("spark.databricks.sql.nativeXmlDataSourcePreview.enabled", "true")

with open("../../tools/traditional_config.json", "r") as json_conf:
  table_conf = json.load(json_conf)['views']['CustomerMgmt']
  
user_name = spark.sql("select lower(regexp_replace(split(current_user(), '@')[0], '(\\\W+)', ' '))").collect()[0][0]
default_catalog = 'tpcdi' if spark.conf.get('spark.databricks.unityCatalog.enabled') == 'true' else 'hive_metastore'
default_wh = f"{string.capwords(user_name).replace(' ','_')}_TPCDI"

dbutils.widgets.text("catalog", default_catalog, 'Target Catalog')
dbutils.widgets.text("wh_db", default_wh,'Target Database')
dbutils.widgets.text("tpcdi_directory", "/Volumes/tpcdi/tpcdi_raw_data/tpcdi_volume/", "Directory where Raw Files are located")
dbutils.widgets.text("scale_factor", "10", "Scale factor")

catalog = dbutils.widgets.get("catalog")
wh_db = f"{dbutils.widgets.get('wh_db')}"
staging_db = f"{dbutils.widgets.get('wh_db')}_stage"
scale_factor = dbutils.widgets.get("scale_factor")
tpcdi_directory = dbutils.widgets.get("tpcdi_directory")
files_directory = f"{tpcdi_directory}sf={scale_factor}"

# COMMAND ----------

if catalog != 'hive_metastore':
  catalog_exists = spark.sql(f"SELECT count(*) FROM system.information_schema.tables WHERE table_catalog = '{catalog}'").first()[0] > 0
  if not catalog_exists:
    spark.sql(f"""CREATE CATALOG IF NOT EXISTS {catalog}""")
    spark.sql(f"""GRANT ALL PRIVILEGES ON CATALOG {catalog} TO `account users`""")
spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog}.{staging_db}")

# COMMAND ----------

# DBTITLE 1,Read the XML file and create a temp view on the raw data as all string values
xml_lib = 'com.databricks.spark.xml' if int(scale_factor) > 100 else 'xml'
spark.read.format(xml_lib) \
  .option('rowTag', table_conf['rowTag']) \
  .option('inferSchema', 'false') \
  .load(f"""{files_directory}/{table_conf['path']}/{table_conf['filename']}""") \
  .createOrReplaceTempView("v_CustomerMgmt")

# COMMAND ----------

# DBTITLE 1,Now insert into CustomerMgmt table with nested values parsed and data types applied
spark.sql(f"""
  CREATE TABLE IF NOT EXISTS {catalog}.{staging_db}.CustomerMgmt PARTITIONED BY (ActionType) AS SELECT 
    cast(Customer._C_ID as BIGINT) customerid, 
    cast(Customer.Account._CA_ID as BIGINT) accountid,
    cast(Customer.Account.CA_B_ID as BIGINT) brokerid, 
    nullif(Customer._C_TAX_ID, '') taxid,
    nullif(Customer.Account.CA_NAME, '') accountdesc, 
    cast(Customer.Account._CA_TAX_ST as TINYINT) taxstatus,
    decode(_ActionType,
      "NEW","Active",
      "ADDACCT","Active",
      "UPDACCT","Active",
      "UPDCUST","Active",
      "CLOSEACCT","Inactive",
      "INACT","Inactive") status,
    nullif(Customer.Name.C_L_NAME, '') lastname, 
    nullif(Customer.Name.C_F_NAME, '') firstname, 
    nullif(Customer.Name.C_M_NAME, '') middleinitial, 
    nullif(upper(Customer._C_GNDR), '') gender,
    cast(Customer._C_TIER as TINYINT) tier, 
    cast(Customer._C_DOB as DATE) dob,
    nullif(Customer.Address.C_ADLINE1, '') addressline1, 
    nullif(Customer.Address.C_ADLINE2, '') addressline2, 
    nullif(Customer.Address.C_ZIPCODE, '') postalcode,
    nullif(Customer.Address.C_CITY, '') city, 
    nullif(Customer.Address.C_STATE_PROV, '') stateprov,
    nullif(Customer.Address.C_CTRY, '') country, 
    nvl2(
      nullif(Customer.ContactInfo.C_PHONE_1.C_LOCAL, ''),
      concat(
          nvl2(nullif(Customer.ContactInfo.C_PHONE_1.C_CTRY_CODE, ''), '+' || Customer.ContactInfo.C_PHONE_1.C_CTRY_CODE || ' ', ''),
          nvl2(nullif(Customer.ContactInfo.C_PHONE_1.C_AREA_CODE, ''), '(' || Customer.ContactInfo.C_PHONE_1.C_AREA_CODE || ') ', ''),
          Customer.ContactInfo.C_PHONE_1.C_LOCAL,
          nvl(Customer.ContactInfo.C_PHONE_1.C_EXT, '')),
      cast(null as string)) phone1,
    nvl2(
      nullif(Customer.ContactInfo.C_PHONE_2.C_LOCAL, ''),
      concat(
          nvl2(nullif(Customer.ContactInfo.C_PHONE_2.C_CTRY_CODE, ''), '+' || Customer.ContactInfo.C_PHONE_2.C_CTRY_CODE || ' ', ''),
          nvl2(nullif(Customer.ContactInfo.C_PHONE_2.C_AREA_CODE, ''), '(' || Customer.ContactInfo.C_PHONE_2.C_AREA_CODE || ') ', ''),
          Customer.ContactInfo.C_PHONE_2.C_LOCAL,
          nvl(Customer.ContactInfo.C_PHONE_2.C_EXT, '')),
      cast(null as string)) phone2,
    nvl2(
      nullif(Customer.ContactInfo.C_PHONE_3.C_LOCAL, ''),
      concat(
          nvl2(nullif(Customer.ContactInfo.C_PHONE_3.C_CTRY_CODE, ''), '+' || Customer.ContactInfo.C_PHONE_3.C_CTRY_CODE || ' ', ''),
          nvl2(nullif(Customer.ContactInfo.C_PHONE_3.C_AREA_CODE, ''), '(' || Customer.ContactInfo.C_PHONE_3.C_AREA_CODE || ') ', ''),
          Customer.ContactInfo.C_PHONE_3.C_LOCAL,
          nvl(Customer.ContactInfo.C_PHONE_3.C_EXT, '')),
      cast(null as string)) phone3,
    nullif(Customer.ContactInfo.C_PRIM_EMAIL, '') email1,
    nullif(Customer.ContactInfo.C_ALT_EMAIL, '') email2,
    nullif(Customer.TaxInfo.C_LCL_TX_ID, '') lcl_tx_id, 
    nullif(Customer.TaxInfo.C_NAT_TX_ID, '') nat_tx_id, 
    to_timestamp(_ActionTS) update_ts,
    _ActionType ActionType
  FROM v_CustomerMgmt
""")

# COMMAND ----------

spark.sql(f"ANALYZE TABLE {catalog}.{staging_db}.CustomerMgmt COMPUTE STATISTICS FOR ALL COLUMNS")
