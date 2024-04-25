# Databricks notebook source
# MAGIC %md
# MAGIC # Prospect
# MAGIC **Prospect gets a full snapshot in every batch which is handled as SCD Type 1.**
# MAGIC * Need to keep latest record if any change occurred. Otherwise, if no change occurs, only update the recordbatchid.
# MAGIC * This is made slightly more complicated since we need to join to DimCustomer to find out if the prospect is also a customer - which necessitates the DimCustomer staging table

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

batch_id = dbutils.widgets.get("batch_id")
catalog = dbutils.widgets.get("catalog")
wh_db = f"{dbutils.widgets.get('wh_db')}"
staging_db = f"{dbutils.widgets.get('wh_db')}_stage"
spark.sql(f"USE CATALOG {catalog}")

# COMMAND ----------

# MAGIC %md
# MAGIC # Determine if Historical Or Incremental Load
# MAGIC * If Batch 1 (historical), read from ProspectIncremental table (which is loaded by streaming autoloader job), handle joins and business logic, then Insert Overwrite into Prospect table
# MAGIC * If Batch 2 or 3 (incremental), read from ProspectIncremental table (which is loaded by streaming autoloader job) then handle a merge into Prospect table
# MAGIC * Shorten some of the queries for readability by assigning some queries to variables

# COMMAND ----------

spark.sql(f"""
  INSERT OVERWRITE {wh_db}.Prospect
  SELECT 
    agencyid,
    recdate.sk_dateid sk_recorddateid,
    origdate.sk_dateid sk_updatedateid,
    p.batchid,
    nvl2(c.lastname, True, False) iscustomer, 
    p.lastname,
    p.firstname,
    p.middleinitial,
    p.gender,
    p.addressline1,
    p.addressline2,
    p.postalcode,
    city,
    state,
    country,
    phone,
    income,
    numbercars,
    numberchildren,
    maritalstatus,
    age,
    creditrating,
    ownorrentflag,
    employer,
    numbercreditcards,
    networth,
    if(
      isnotnull(
        if(networth > 1000000 or income > 200000,"HighValue+","") || 
        if(numberchildren > 3 or numbercreditcards > 5,"Expenses+","") ||
        if(age > 45, "Boomer+", "") ||
        if(income < 50000 or creditrating < 600 or networth < 100000, "MoneyAlert+","") ||
        if(numbercars > 3 or numbercreditcards > 7, "Spender+","") ||
        if(age < 25 and networth > 1000000, "Inherited+","")),
      left(
        if(networth > 1000000 or income > 200000,"HighValue+","") || 
        if(numberchildren > 3 or numbercreditcards > 5,"Expenses+","") ||
        if(age > 45, "Boomer+", "") ||
        if(income < 50000 or creditrating < 600 or networth < 100000, "MoneyAlert+","") ||
        if(numbercars > 3 or numbercreditcards > 7, "Spender+","") ||
        if(age < 25 and networth > 1000000, "Inherited+",""),
        length(
          if(networth > 1000000 or income > 200000,"HighValue+","") || 
          if(numberchildren > 3 or numbercreditcards > 5,"Expenses+","") ||
          if(age > 45, "Boomer+", "") ||
          if(income < 50000 or creditrating < 600 or networth < 100000, "MoneyAlert+","") ||
          if(numbercars > 3 or numbercreditcards > 7, "Spender+","") ||
          if(age < 25 and networth > 1000000, "Inherited+",""))
        -1),
      NULL) marketingnameplate
  FROM (
    SELECT 
      * FROM (
      SELECT
        agencyid,
        max(batchid) recordbatchid,
        lastname,
        firstname,
        middleinitial,
        gender,
        addressline1,
        addressline2,
        postalcode,
        city,
        state,
        country,
        phone,
        income,
        numbercars,
        numberchildren,
        maritalstatus,
        age,
        creditrating,
        ownorrentflag,
        employer,
        numbercreditcards,
        networth,
        min(batchid) batchid
      FROM {staging_db}.ProspectIncremental p
      WHERE batchid <= cast({batch_id} as int)
      GROUP BY
        agencyid,
        lastname,
        firstname,
        middleinitial,
        gender,
        addressline1,
        addressline2,
        postalcode,
        city,
        state,
        country,
        phone,
        income,
        numbercars,
        numberchildren,
        maritalstatus,
        age,
        creditrating,
        ownorrentflag,
        employer,
        numbercreditcards,
        networth)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY agencyid ORDER BY batchid DESC) = 1) p
  JOIN (
    SELECT 
      sk_dateid,
      batchid
    FROM {wh_db}.BatchDate b 
    JOIN {wh_db}.DimDate d 
      ON b.batchdate = d.datevalue) recdate
    ON p.recordbatchid = recdate.batchid
  JOIN (
    SELECT 
      sk_dateid,
      batchid
    FROM {wh_db}.BatchDate b 
    JOIN {wh_db}.DimDate d 
      ON b.batchdate = d.datevalue) origdate
    ON p.batchid = origdate.batchid
  LEFT JOIN (
    SELECT 
      lastname,
      firstname,
      addressline1,
      addressline2,
      postalcode
    FROM {staging_db}.DimCustomerStg
    WHERE iscurrent) c
    ON 
      upper(p.LastName) = upper(c.lastname)
      and upper(p.FirstName) = upper(c.firstname)
      and upper(p.AddressLine1) = upper(c.addressline1)
      and upper(nvl(p.addressline2, '')) = upper(nvl(c.addressline2, ''))
      and upper(p.PostalCode) = upper(c.postalcode)
""")

# COMMAND ----------

spark.sql(f"ANALYZE TABLE {wh_db}.Prospect COMPUTE STATISTICS FOR ALL COLUMNS")
