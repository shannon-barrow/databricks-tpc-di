# Databricks notebook source
# MAGIC %md
# MAGIC # DimAccount

# COMMAND ----------

# MAGIC %md
# MAGIC **This table was made more complicated because it needs to carry over update DimCustomer surrogate keys**
# MAGIC * A customer has a 1->Many relationship with Accounts.
# MAGIC * Therefore, a change to customer will lead to a different current SK of that customer
# MAGIC * This leads to an update to that existing customer's account records to point to latest Customer SK
# MAGIC * But the current start/end date of customer records, SK of customer record, and the start/end date of Account records are not known until the staging tables are created for each

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
tgt_cols = "accountid, sk_brokerid, sk_customerid, accountdesc, TaxStatus, status, batchid, effectivedate, enddate"

# COMMAND ----------

# MAGIC %md
# MAGIC # Determine if Historical Or Incremental Load
# MAGIC * If Batch 1 (historical), read from CustomerMgmt table, satisfy SCD Type 2 by using window/coalesce, then Insert Overwrite into DimAccount
# MAGIC * If Batch 2 or 3 (incremental), read from AccountIncremental (which is loaded by streaming autoloader job) then handle a merge into DimAccount
# MAGIC * Shorten some of the queries for readability by assigning some queries to variables

# COMMAND ----------

hist_acct_query = f"""SELECT * FROM (
  SELECT
    accountid,
    customerid,
    coalesce(accountdesc, last_value(accountdesc) IGNORE NULLS OVER (
      PARTITION BY accountid ORDER BY update_ts)) accountdesc,
    coalesce(taxstatus, last_value(taxstatus) IGNORE NULLS OVER (
      PARTITION BY accountid ORDER BY update_ts)) taxstatus,
    coalesce(brokerid, last_value(brokerid) IGNORE NULLS OVER (
      PARTITION BY accountid ORDER BY update_ts)) brokerid,
    coalesce(status, last_value(status) IGNORE NULLS OVER (
      PARTITION BY accountid ORDER BY update_ts)) status,
    date(update_ts) effectivedate,
    nvl(lead(date(update_ts)) OVER (PARTITION BY accountid ORDER BY update_ts), date('9999-12-31')) enddate
  FROM {staging_db}.CustomerMgmt
  WHERE ActionType NOT IN ('UPDCUST', 'INACT'))
WHERE effectivedate < enddate
"""

# COMMAND ----------

cust_updates_query = f"""
  SELECT 
    a.accountid,
    a.sk_brokerid,
    ci.sk_customerid,
    a.status,    
    a.accountdesc,
    a.taxstatus,
    ci.effectivedate,
    ci.enddate
  FROM (
    SELECT 
      sk_customerid, 
      customerid,
      effectivedate,
      enddate
    FROM {staging_db}.DimCustomerStg
    WHERE 
      iscurrent 
      AND batchid = cast({batch_id} as int)) ci
  JOIN (
    SELECT 
      sk_customerid, 
      customerid,
      enddate
    FROM {staging_db}.DimCustomerStg
    WHERE 
      !iscurrent
      AND batchid < cast({batch_id} as int)) ch
  ON 
    ci.customerid = ch.customerid
    AND ch.enddate = ci.effectivedate  
  JOIN {wh_db}.DimAccount a
    ON 
      ch.sk_customerid = a.sk_customerid
      AND a.iscurrent
"""

# COMMAND ----------

incr_acct_query = f"""
  SELECT
    nvl(a.accountid, b.accountid) accountid,
    nvl(a.sk_brokerid, b.sk_brokerid) sk_brokerid,
    nvl(a.sk_customerid, b.sk_customerid) sk_customerid,
    nvl(a.status, b.status) status,
    nvl(a.accountdesc, b.accountdesc) accountdesc,
    nvl(a.TaxStatus, b.TaxStatus) TaxStatus,
    cast({batch_id} as int) batchid,
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
      date('9999-12-31') enddate,
      a.batchid
    FROM {staging_db}.accountincremental a
    JOIN {wh_db}.batchdate bd
      ON a.batchid = bd.batchid
    JOIN {wh_db}.StatusType st 
      ON a.CA_ST_ID = st.st_id
    JOIN (
      SELECT customerid, sk_customerid
      FROM {staging_db}.DimCustomerStg
      WHERE iscurrent) dc
      ON dc.customerid = a.ca_c_id
    LEFT JOIN {wh_db}.DimBroker b 
      ON a.ca_b_id = b.brokerid
    WHERE a.batchid = cast({batch_id} as int)
  ) a
  FULL OUTER JOIN ({cust_updates_query}) b
    ON a.accountid = b.accountid
"""

# COMMAND ----------

matched_accts_query = f"""
  SELECT
    s.accountid mergeKey,
    s.accountid,
    nvl(s.sk_brokerid, t.sk_brokerid) sk_brokerid,
    s.sk_customerid,
    s.status,
    nvl(s.accountdesc, t.accountdesc) accountdesc,
    nvl(s.taxstatus, t.taxstatus) taxstatus,
    s.batchid,
    s.effectivedate,
    s.enddate    
  FROM ({incr_acct_query}) s
  JOIN {wh_db}.DimAccount t
    ON s.accountid = t.accountid
  WHERE t.iscurrent
"""

# COMMAND ----------

if batch_id == '1':
  spark.sql(f"""
    INSERT OVERWRITE {wh_db}.DimAccount ({tgt_cols})
    SELECT
      a.accountid,
      b.sk_brokerid,
      a.sk_customerid,
      a.accountdesc,
      a.TaxStatus,
      a.status,
      1 batchid,
      a.effectivedate,
      a.enddate
    FROM (
      SELECT
        a.* except(effectivedate, enddate, customerid),
        c.sk_customerid,
        if(a.effectivedate < c.effectivedate, c.effectivedate, a.effectivedate) effectivedate,
        if(a.enddate > c.enddate, c.enddate, a.enddate) enddate
      FROM ({hist_acct_query}) a
      FULL OUTER JOIN (
        SELECT 
          sk_customerid, 
          customerid,
          effectivedate,
          enddate
        FROM {staging_db}.DimCustomerStg
        WHERE batchid = cast({batch_id} as int)
      ) c 
        ON 
          a.customerid = c.customerid
          AND c.enddate > a.effectivedate
          AND c.effectivedate < a.enddate  
    ) a
    LEFT JOIN {wh_db}.DimBroker b 
      ON a.brokerid = b.brokerid;""")
else:
  spark.sql(f"""
    MERGE INTO {wh_db}.DimAccount t USING (
      SELECT
        CAST(NULL AS BIGINT) AS mergeKey,
        dav.*
      FROM ({incr_acct_query}) dav
      UNION ALL
      SELECT *
      FROM ({matched_accts_query})) s 
      ON t.accountid = s.mergeKey
    WHEN MATCHED AND t.iscurrent THEN UPDATE SET
      t.iscurrent = false,
      t.enddate = s.effectivedate
    WHEN NOT MATCHED THEN INSERT ({tgt_cols})
    VALUES ({tgt_cols})
  """)
