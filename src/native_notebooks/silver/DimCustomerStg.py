# Databricks notebook source
# MAGIC %md
# MAGIC # DimCustomerStg
# MAGIC **DimCustomer Table needs a Staging table first.**
# MAGIC * Since all updates only show the changed column, it needs to coalesce with previous record.  
# MAGIC * Any other tables needing data from the Customer table then needs to wait to get the data until AFTER this fully realized record(s) have been created
# MAGIC * This includes: 
# MAGIC   * Prospect (Customer and Prospect need data from the other table and join on name/address - which you cannot get until data is coalesced)
# MAGIC   * Account table needs each change of Customer record to get the surrogate key of the customer record. This only occurs once a Customer record gets updated
# MAGIC 
# MAGIC ### Staging Customer table unions the historical to the incremental
# MAGIC 1) Window results by customerid and order by the update timestamp 
# MAGIC 2) Then coalesce the current row to the last row and ignore nulls

# COMMAND ----------

# MAGIC %md
# MAGIC # Setup

# COMMAND ----------

user_name = spark.sql("select current_user()").collect()[0][0].split("@")[0].replace(".","_")
dbutils.widgets.text("wh_db", f"{user_name}_TPCDI",'Root name of Target Warehouse')
dbutils.widgets.text("batch_id", "1", "Batch ID (1,2,3)")
staging_db = f"{dbutils.widgets.get('wh_db')}_stage"
wh_db = f"{dbutils.widgets.get('wh_db')}_wh"
batch_id = dbutils.widgets.get("batch_id")

# COMMAND ----------

# MAGIC %md
# MAGIC # Determine if Historical Or Incremental Load
# MAGIC * If Batch 1 (historical), read from CustomerMgmt table, satisfy SCD Type 2 by using window/coalesce, then Insert Overwrite into DimCustomerStg
# MAGIC * If Batch 2 or 3 (incremental), read from CustomerIncremental (which is loaded by streaming autoloader job) then handle a merge into DimCustomerStg
# MAGIC * Shorten some of the queries for readability by assigning some queries to variables

# COMMAND ----------

tgt_cols = "customerid, taxid, status, lastname, firstname, middleinitial, gender, tier, dob, addressline1, addressline2, postalcode, city, stateprov, country, phone1, phone2, phone3, email1, email2, lcl_tx_id, nat_tx_id, batchid, iscurrent, effectivedate, enddate"

# COMMAND ----------

hist_query = f"""SELECT
  customerid,
  coalesce(taxid, last_value(taxid) IGNORE NULLS OVER (
      PARTITION BY customerid
      ORDER BY update_ts)) taxid,
  status,
  coalesce(lastname, last_value(lastname) IGNORE NULLS OVER (
      PARTITION BY customerid
      ORDER BY update_ts)) lastname,
  coalesce(firstname, last_value(firstname) IGNORE NULLS OVER (
      PARTITION BY customerid
      ORDER BY update_ts)) firstname,
  coalesce(middleinitial, last_value(middleinitial) IGNORE NULLS OVER (
      PARTITION BY customerid
      ORDER BY update_ts)) middleinitial,
  coalesce(gender, last_value(gender) IGNORE NULLS OVER (
      PARTITION BY customerid
      ORDER BY update_ts)) gender,
  coalesce(tier, last_value(tier) IGNORE NULLS OVER (
      PARTITION BY customerid
      ORDER BY update_ts)) tier,
  coalesce(dob, last_value(dob) IGNORE NULLS OVER (
      PARTITION BY customerid
      ORDER BY update_ts)) dob,
  coalesce(addressline1, last_value(addressline1) IGNORE NULLS OVER (
      PARTITION BY customerid
      ORDER BY update_ts)) addressline1,
  coalesce(addressline2, last_value(addressline2) IGNORE NULLS OVER (
      PARTITION BY customerid
      ORDER BY update_ts)) addressline2,
  coalesce(postalcode, last_value(postalcode) IGNORE NULLS OVER (
      PARTITION BY customerid
      ORDER BY update_ts)) postalcode,
  coalesce(CITY, last_value(CITY) IGNORE NULLS OVER (
      PARTITION BY customerid
      ORDER BY update_ts)) CITY,
  coalesce(stateprov, last_value(stateprov) IGNORE NULLS OVER (
      PARTITION BY customerid
      ORDER BY update_ts)) stateprov,
  coalesce(country, last_value(country) IGNORE NULLS OVER (
      PARTITION BY customerid
      ORDER BY update_ts)) country,
  coalesce(phone1, last_value(phone1) IGNORE NULLS OVER (
      PARTITION BY customerid
      ORDER BY update_ts)) phone1,
  coalesce(phone2, last_value(phone2) IGNORE NULLS OVER (
      PARTITION BY customerid
      ORDER BY update_ts)) phone2,
  coalesce(phone3, last_value(phone3) IGNORE NULLS OVER (
      PARTITION BY customerid
      ORDER BY update_ts)) phone3,
  coalesce(email1, last_value(email1) IGNORE NULLS OVER (
      PARTITION BY customerid
      ORDER BY update_ts)) email1,
  coalesce(email2, last_value(email2) IGNORE NULLS OVER (
      PARTITION BY customerid
      ORDER BY update_ts)) email2,
  coalesce(LCL_TX_ID, last_value(LCL_TX_ID) IGNORE NULLS OVER (
      PARTITION BY customerid
      ORDER BY update_ts)) LCL_TX_ID,
  coalesce(NAT_TX_ID, last_value(NAT_TX_ID) IGNORE NULLS OVER (
      PARTITION BY customerid
      ORDER BY update_ts)) NAT_TX_ID,
  1 batchid,
  nvl2(lead(update_ts) OVER (PARTITION BY customerid ORDER BY update_ts), false, true) iscurrent,
  date(update_ts) effectivedate,
  coalesce(lead(date(update_ts)) OVER (PARTITION BY customerid ORDER BY update_ts), date('9999-12-31')) enddate
FROM {staging_db}.CustomerMgmt c
WHERE ActionType in ('NEW', 'INACT', 'UPDCUST')
"""

# COMMAND ----------

incr_query = f"""SELECT
  c.customerid,
  nullif(c.taxid, '') taxid,
  nullif(s.st_name, '') as status,
  nullif(c.lastname, '') lastname,
  nullif(c.firstname, '') firstname,
  nullif(c.middleinitial, '') middleinitial,
  gender,
  c.tier,
  c.dob,
  nullif(c.addressline1, '') addressline1,
  nullif(c.addressline2, '') addressline2,
  nullif(c.postalcode, '') postalcode,
  nullif(c.city, '') city,
  nullif(c.stateprov, '') stateprov,
  nullif(c.country, '') country,
  CASE
    WHEN isnull(c_local_1) then c_local_1
    ELSE concat(
      nvl2(c_ctry_1, '+' || c_ctry_1 || ' ', ''),
      nvl2(c_area_1, '(' || c_area_1 || ') ', ''),
      c_local_1,
      nvl(c_ext_1, '')) END as phone1,
  CASE
    WHEN isnull(c_local_2) then c_local_2
    ELSE concat(
      nvl2(c_ctry_2, '+' || c_ctry_2 || ' ', ''),
      nvl2(c_area_2, '(' || c_area_2 || ') ', ''),
      c_local_2,
      nvl(c_ext_2, '')) END as phone2,
  CASE
    WHEN isnull(c_local_3) then c_local_3
    ELSE concat(
      nvl2(c_ctry_3, '+' || c_ctry_3 || ' ', ''),
      nvl2(c_area_3, '(' || c_area_3 || ') ', ''),
      c_local_3,
      nvl(c_ext_3, '')) END as phone3,
  nullif(c.email1, '') email1,
  nullif(c.email2, '') email2,
  c.LCL_TX_ID, 
  c.NAT_TX_ID,
  c.batchid,
  true iscurrent,
  bd.batchdate effectivedate, 
  date('9999-12-31') enddate
FROM {staging_db}.CustomerIncremental c
JOIN {wh_db}.BatchDate bd
  ON c.batchid = bd.batchid
JOIN {wh_db}.StatusType s 
  ON c.status = s.st_id
WHERE c.batchid = cast({batch_id } as int)
"""

# COMMAND ----------

# DBTITLE 1,Now load, depending whether its historical or incremental
if batch_id == '1':
  spark.sql(f"""
    INSERT INTO {staging_db}.DimCustomerStg ({tgt_cols})
    SELECT *
    FROM ({hist_query})
    WHERE effectivedate < enddate
  """)
else:
  spark.sql(f"""
    MERGE INTO {staging_db}.DimCustomerStg t USING (
      SELECT
        s.customerid AS mergeKey,
        s.*
      FROM ({incr_query}) s
      JOIN {staging_db}.DimCustomerStg t
        ON s.customerid = t.customerid
      WHERE t.iscurrent
      UNION ALL
      SELECT
        cast(null as bigint) AS mergeKey,
        *
      FROM ({incr_query})
    ) s 
      ON 
        t.customerid = s.mergeKey
        AND t.iscurrent
    WHEN MATCHED THEN UPDATE SET
      t.iscurrent = false,
      t.enddate = s.effectivedate
    WHEN NOT MATCHED THEN INSERT ({tgt_cols})
    VALUES ({tgt_cols})
  """)

# COMMAND ----------

spark.sql(f"ANALYZE TABLE {staging_db}.DimCustomerStg COMPUTE STATISTICS FOR ALL COLUMNS")
