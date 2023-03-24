# Databricks notebook source
# MAGIC %md
# MAGIC # DimCustomer AND Prospect Processed in this Notebook
# MAGIC There are dependencies between the tables that make it easier to process together then write to the target table

# COMMAND ----------

from pyspark.sql.functions import lit
dbutils.widgets.text("env",'a01','Name of the environment')
dbutils.widgets.text("files_directory", "/tmp/tpc-di", "Directory where Files are located")
dbutils.widgets.text("scale_factor", "10", "Scale factor")
dbutils.widgets.text("batch_id", "2", "Batch ID (1,2,3)")

env = dbutils.widgets.get("env")
files_directory = dbutils.widgets.get("files_directory")
batch_id = dbutils.widgets.get("batch_id")
scale_factor = dbutils.widgets.get("scale_factor")
wh_db = f"{env}_tpcdi_warehouse"

spark.conf.set("spark.sql.shuffle.partitions", sc.defaultParallelism)

# COMMAND ----------

# DBTITLE 1,Define Schema for Upcoming Views
prospectRawSchema = """
  agencyid STRING COMMENT 'Unique identifier from agency',
  lastname STRING COMMENT 'Last name',
  firstname STRING COMMENT 'First name',
  middleinitial STRING COMMENT 'Middle initial',
  gender STRING COMMENT '‘M’ or ‘F’ or ‘U’',
  addressline1 STRING COMMENT 'Postal address',
  addressline2 STRING COMMENT 'Postal address',
  postalcode STRING COMMENT 'Postal code',
  city STRING COMMENT 'City',
  state STRING COMMENT 'State or province',
  country STRING COMMENT 'Postal country',
  phone STRING COMMENT 'Telephone number',
  income STRING COMMENT 'Annual income',
  numbercars INT COMMENT 'Cars owned',
  numberchildren INT COMMENT 'Dependent children',
  maritalstatus STRING COMMENT '‘S’ or ‘M’ or ‘D’ or ‘W’ or ‘U’',
  age INT COMMENT 'Current age',
  creditrating INT COMMENT 'Numeric rating',
  ownorrentflag STRING COMMENT '‘O’ or ‘R’ or ‘U’',
  employer STRING COMMENT 'Name of employer',
  numbercreditcards INT COMMENT 'Credit cards',
  networth INT COMMENT 'Estimated total net worth'
"""

customerViewSchema = """
  cdc_flag STRING COMMENT 'Denotes insert or update',
  cdc_dsn BIGINT COMMENT 'Database Sequence Number',
  customerid BIGINT COMMENT 'Customer identifier',
  taxid STRING COMMENT 'Customer’s tax identifier',
  status STRING COMMENT 'Customer status type identifier',
  lastname STRING COMMENT 'Primary Customers last name.',
  firstname STRING COMMENT 'Primary Customers first name.',
  middleinitial STRING COMMENT 'Primary Customers middle initial',
  gender STRING COMMENT 'Gender of the primary customer',
  tier TINYINT COMMENT 'Customer tier',
  dob DATE COMMENT 'Customer’s date of birth, as YYYY-MM-DD.',
  addressline1 STRING COMMENT 'Address Line 1',
  addressline2 STRING COMMENT 'Address Line 2',
  postalcode STRING COMMENT 'Zip or postal code',
  city STRING COMMENT 'City',
  stateprov STRING COMMENT 'State or province',
  country STRING COMMENT 'Country',
  c_ctry_1 STRING COMMENT 'Country code for Customers phone 1.',
  c_area_1 STRING COMMENT 'Area code for customer’s phone 1.',
  c_local_1 STRING COMMENT 'Local number for customer’s phone 1.',
  c_ext_1 STRING COMMENT 'Extension number for Customer’s phone 1.',
  c_ctry_2 STRING COMMENT 'Country code for Customers phone 2.',
  c_area_2 STRING COMMENT 'Area code for Customer’s phone 2.',
  c_local_2 STRING COMMENT 'Local number for Customer’s phone 2.',
  c_ext_2 STRING COMMENT 'Extension number for Customer’s phone 2.',
  c_ctry_3 STRING COMMENT 'Country code for Customers phone 3.',
  c_area_3 STRING COMMENT 'Area code for Customer’s phone 3.',
  c_local_3 STRING COMMENT 'Local number for Customer’s phone 3.',
  c_ext_3 STRING COMMENT 'Extension number for Customer’s phone 3.',
  email1 STRING COMMENT 'Customers e-mail address 1.',
  email2 STRING COMMENT 'Customers e-mail address 2.',
  lcl_tx_id STRING COMMENT 'Customers local tax rate',
  nat_tx_id STRING COMMENT 'Customers national tax rate'
"""

# COMMAND ----------

# MAGIC %md
# MAGIC # Start by Preprocessing DimCustomer

# COMMAND ----------

spark.read.csv(f'{files_directory}/{scale_factor}/Batch{batch_id}/Customer.txt', schema=customerViewSchema, sep='|', header=False, inferSchema=False) \
  .withColumn('batchid', lit(f'{batch_id}').cast('int')) \
  .createOrReplaceTempView('CustomerRawView')

# COMMAND ----------

spark.sql(f"""
  CREATE OR REPLACE TEMPORARY VIEW DimCustomerView AS
  SELECT
    c.customerid,
    nullif(c.taxid, '') taxid,
    nullif(s.st_name, '') as status,
    nullif(c.lastname, '') lastname,
    nullif(c.firstname, '') firstname,
    nullif(c.middleinitial, '') middleinitial,
    CASE
      WHEN UPPER(c.gender) IN ('M', 'F') THEN UPPER(c.gender)
      ELSE 'U'
      END AS gender,
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
        nvl(c_ext_1, '')
      )
    END as phone1,
    CASE
      WHEN isnull(c_local_2) then c_local_2
      ELSE concat(
        nvl2(c_ctry_2, '+' || c_ctry_2 || ' ', ''),
        nvl2(c_area_2, '(' || c_area_2 || ') ', ''),
        c_local_2,
        nvl(c_ext_2, '')
      )
    END as phone2,
    CASE
      WHEN isnull(c_local_3) then c_local_3
      ELSE concat(
        nvl2(c_ctry_3, '+' || c_ctry_3 || ' ', ''),
        nvl2(c_area_3, '(' || c_area_3 || ') ', ''),
        c_local_3,
        nvl(c_ext_3, '')
      )
    END as phone3,
    nullif(c.email1, '') email1,
    nullif(c.email2, '') email2,
    r_nat.TX_NAME as nationaltaxratedesc,
    r_nat.TX_RATE as nationaltaxrate,
    r_lcl.TX_NAME as localtaxratedesc,
    r_lcl.TX_RATE as localtaxrate,
    True iscurrent,
    c.batchid,
    bd.batchdate effectivedate,
    to_date('9999-12-31') enddate
  FROM
    CustomerRawView c
  JOIN {wh_db}.batchdate bd
    ON c.batchid = bd.batchid
  JOIN {wh_db}.StatusType s 
    ON c.status = s.st_id
  JOIN {wh_db}.TaxRate r_lcl 
    ON c.LCL_TX_ID = r_lcl.TX_ID
  JOIN {wh_db}.TaxRate r_nat 
    ON c.NAT_TX_ID = r_nat.TX_ID;
""")

# COMMAND ----------

# MAGIC %md
# MAGIC # Next Preprocess Prospect for Prospect Table batch load and also for joining to DimCustomer

# COMMAND ----------

spark.read.csv(f'{files_directory}/{scale_factor}/Batch{batch_id}/Prospect.csv', schema=prospectRawSchema, header=False, inferSchema=False) \
  .withColumn('batchid', lit(f'{batch_id}').cast('int')) \
  .createOrReplaceTempView('ProspectRawView')

# COMMAND ----------

# MAGIC %md
# MAGIC ## The CTE in the Following Code Gets Current Customers from DimCustomers AND New DimCustomer Preprocessed View

# COMMAND ----------

spark.sql(f"""
  CREATE OR REPLACE TEMPORARY VIEW ProspectView AS
  WITH CustomersCTE AS(
      SELECT customerid, upper(FirstName || LastName || AddressLine1 || nvl(addressline2, '') || PostalCode) current_custs
      FROM {wh_db}.DimCustomer
      WHERE iscurrent
      UNION
      SELECT customerid, upper(FirstName || LastName || AddressLine1 || nvl(addressline2, '') || PostalCode) current_custs
      FROM DimCustomerView
    )
  SELECT 
    agencyid,
    sk_dateid sk_recorddateid,
    sk_dateid sk_updatedateid,
    p.batchid,
    nvl2(c.customerid, True, False) iscustomer,
    p.lastname,
    p.firstname,
    p.middleinitial,
    CASE
      WHEN UPPER(p.gender) IN ('M', 'F') THEN UPPER(p.gender)
      ELSE 'U'
      END AS gender,
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
        if(networth > 1000000 or income > 200000,'HighValue+','') || 
        if(numberchildren > 3 or numbercreditcards > 5,'Expenses+','') ||
        if(age > 45, 'Boomer+', '') ||
        if(income < 50000 or creditrating < 600 or networth < 100000, 'MoneyAlert+','') ||
        if(numbercars > 3 or numbercreditcards > 7, 'Spender+','') ||
        if(age < 25 and networth > 1000000, 'Inherited+','')),
      left(
        if(networth > 1000000 or income > 200000,'HighValue+','') || 
        if(numberchildren > 3 or numbercreditcards > 5,'Expenses+','') ||
        if(age > 45, 'Boomer+', '') ||
        if(income < 50000 or creditrating < 600 or networth < 100000, 'MoneyAlert+','') ||
        if(numbercars > 3 or numbercreditcards > 7, 'Spender+','') ||
        if(age < 25 and networth > 1000000, 'Inherited+',''),
        length(
          if(networth > 1000000 or income > 200000,'HighValue+','') || 
          if(numberchildren > 3 or numbercreditcards > 5,'Expenses+','') ||
          if(age > 45, 'Boomer+', '') ||
          if(income < 50000 or creditrating < 600 or networth < 100000, 'MoneyAlert+','') ||
          if(numbercars > 3 or numbercreditcards > 7, 'Spender+','') ||
          if(age < 25 and networth > 1000000, 'Inherited+','')
          )
        -1),
      NULL
      ) marketingnameplate
  FROM ProspectRawView p
  JOIN (
    SELECT 
      sk_dateid,
      batchid
    FROM {wh_db}.BatchDate b 
    JOIN {wh_db}.DimDate d 
      ON b.batchdate = d.datevalue) b
    ON p.batchid = b.batchid
  LEFT JOIN CustomersCTE c
    ON upper(FirstName || LastName || AddressLine1 || nvl(addressline2, '') || PostalCode) = current_custs
""")

# COMMAND ----------

# MAGIC %md
# MAGIC # Merge Into Prospect
# MAGIC * When matching, only update IF any columns have changed
# MAGIC * Most records in each batch are just duplicates since each batch is a full load of whole table

# COMMAND ----------

spark.sql(f"""
  MERGE INTO {wh_db}.Prospect t
  USING ProspectView s
  ON t.agencyid = s.agencyid
  WHEN MATCHED 
    AND (
      COALESCE(s.LastName, '') != COALESCE(t.LastName,'')
      OR COALESCE(s.FirstName, '') != COALESCE(t.FirstName,'')
      OR COALESCE(s.MiddleInitial, '') != COALESCE(t.MiddleInitial,'')
      OR s.Gender != t.Gender
      OR COALESCE(s.AddressLine1, '') != COALESCE(t.AddressLine1,'')
      OR COALESCE(s.AddressLine2, '') != COALESCE(t.AddressLine2,'')
      OR COALESCE(s.PostalCode, '') != COALESCE(t.PostalCode,'')
      OR COALESCE(s.City, '') != COALESCE(t.City,'')
      OR COALESCE(s.State, '') != COALESCE(t.State,'')
      OR COALESCE(s.Country, '') != COALESCE(t.Country,'')
      OR COALESCE(s.Phone, '') != COALESCE(t.Phone,'')
      OR COALESCE(s.Income, '') != COALESCE(t.Income,'')
      OR COALESCE(s.NumberCars,0) != COALESCE(t.NumberCars,0) 
      OR COALESCE(s.NumberChildren,0) != COALESCE(t.NumberChildren,0) 
      OR COALESCE(s.MaritalStatus, '') != COALESCE(t.MaritalStatus,'')
      OR COALESCE(s.Age,0) != COALESCE(t.Age,0) 
      OR COALESCE(s.CreditRating,0) != COALESCE(t.CreditRating,0) 
      OR COALESCE(s.OwnOrRentFlag, '') != COALESCE(t.OwnOrRentFlag,'')
      OR COALESCE(s.Employer, '') != COALESCE(t.Employer,'')
      OR COALESCE(s.NumberCreditCards,0) != COALESCE(t.NumberCreditCards,0) 
      OR COALESCE(s.NetWorth,0) != COALESCE(t.NetWorth,0) 
    )
    THEN UPDATE SET
      sk_recorddateid = s.sk_recorddateid,
      sk_updatedateid = s.sk_updatedateid
      ,iscustomer = s.iscustomer
      ,LastName = s.LastName 
      ,FirstName= s.FirstName
      ,MiddleInitial = s.MiddleInitial 
      ,Gender = s.Gender 
      ,AddressLine1 = s.AddressLine1 
      ,AddressLine2 = s.AddressLine2 
      ,PostalCode = s.PostalCode 
      ,City = s.City 
      ,State = s.State 
      ,Country= s.Country
      ,Phone = s.Phone 
      ,Income = s.Income 
      ,NumberCars = s.NumberCars 
      ,NumberChildren = s.NumberChildren 
      ,MaritalStatus = s.MaritalStatus 
      ,Age = s.Age 
      ,CreditRating= s.CreditRating
      ,OwnOrRentFlag= s.OwnOrRentFlag
      ,Employer = s.Employer 
      ,NumberCreditCards = s.NumberCreditCards 
      ,NetWorth = s.NetWorth
      ,marketingnameplate = s.marketingnameplate,
      batchid = s.batchid
  WHEN MATCHED THEN UPDATE SET
    sk_recorddateid = s.sk_recorddateid
  WHEN NOT MATCHED THEN INSERT *;
""")

# COMMAND ----------

# MAGIC %md
# MAGIC # Now Merge DimCustomer

# COMMAND ----------

# MAGIC %md
# MAGIC ## First join back to Prospect table to get the newly processed Prospect Data

# COMMAND ----------

spark.sql(f"""
  CREATE OR REPLACE TEMPORARY VIEW CustomerSourceView AS
  SELECT
    c.customerid,
    c.taxid,
    c.status,
    c.lastname,
    c.firstname,
    c.middleinitial,
    c.gender,
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
    c.nationaltaxratedesc,
    c.nationaltaxrate,
    c.localtaxratedesc,
    c.localtaxrate,
    p.agencyid,
    p.creditrating,
    p.networth,
    p.marketingnameplate,
    c.iscurrent,
    c.batchid,
    c.effectivedate,
    c.enddate
  FROM
    DimCustomerView c
  LEFT JOIN {wh_db}.Prospect p 
    on upper(p.lastname) = upper(c.lastname)
    and upper(p.firstname) = upper(c.firstname)
    and upper(p.addressline1) = upper(c.addressline1)
    and upper(nvl(p.addressline2,'')) = upper(nvl(c.addressline2,''))
    and upper(p.postalcode) = upper(c.postalcode);
""")

# COMMAND ----------

# MAGIC %md
# MAGIC # Now Merge into DimCustomer, there are a couple steps to do first though

# COMMAND ----------

# MAGIC %md
# MAGIC ## Need to BACKFILL the UPDATED PROSPECT data into EXISTING iscurrent DimCustomer Records
# MAGIC * This is for ONLY previous batches DimCustomer data since new DimCustomer batch already has updated Prospect data
# MAGIC * To put all into a single join, need to get updated prospects, get matched customers for new customer batch, and full outer join these 2 datasets.  Keep the updated customers by default (they already have the new prospect data) if no new customer data is there, keep the prospect update info

# COMMAND ----------

spark.sql(f"""
  CREATE OR REPLACE TEMPORARY VIEW ProspectUpdatesView AS
  SELECT
    customerid,
    p.creditrating,
    p.networth,
    p.marketingnameplate
  FROM ProspectView p
  JOIN (
    SELECT 
      customerid, 
      agencyid,
      creditrating,
      networth,
      marketingnameplate
    FROM {wh_db}.DimCustomer 
    WHERE agencyid is not null AND iscurrent) dc
    ON p.agencyid = dc.agencyid
  WHERE
    COALESCE(dc.CreditRating,0) != COALESCE(p.CreditRating,0) 
    OR COALESCE(dc.NetWorth,0) != COALESCE(p.NetWorth,0) 
    OR COALESCE(dc.MarketingNameplate, '') != COALESCE(p.MarketingNameplate,'')
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Breaking out the MATCHED segment of the "UNION ALL" in the merge command below
# MAGIC * It is a lot of redundant coalescing since we need to keep previous values for the matched records

# COMMAND ----------

spark.sql(f"""
  CREATE OR REPLACE TEMPORARY VIEW MatchedCustomersView AS
  SELECT
    s.customerid,
    nvl(s.taxid, t.taxid) taxid,
    s.status,
    nvl(s.lastname, t.lastname) lastname,
    nvl(s.firstname, t.firstname) firstname,
    nvl(s.middleinitial, t.middleinitial) middleinitial,
    nvl(s.gender, t.gender) gender,
    nvl(s.tier, t.tier) tier,
    nvl(s.dob, t.dob) dob,
    nvl(s.addressline1, t.addressline1) addressline1,
    nvl(s.addressline2, t.addressline2) addressline2,
    nvl(s.postalcode, t.postalcode) postalcode,
    nvl(s.city, t.city) city,
    nvl(s.stateprov, t.stateprov) stateprov,
    nvl(s.country, t.country) country,
    nvl(s.phone1, t.phone1) phone1,
    nvl(s.phone2, t.phone2) phone2,
    nvl(s.phone3, t.phone3) phone3,
    nvl(s.email1, t.email1) email1,
    nvl(s.email2, t.email2) email2,
    s.nationaltaxratedesc,
    s.nationaltaxrate,
    s.localtaxratedesc,
    s.localtaxrate,
    s.agencyid,
    s.creditrating,
    s.networth,
    s.marketingnameplate,
    s.iscurrent,
    s.batchid,
    s.effectivedate,
    s.enddate
  FROM
    CustomerSourceView s
  JOIN {wh_db}.DimCustomer t
    ON s.customerid = t.customerid
  WHERE
    t.iscurrent;
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Now full outer join updated customers and updated prospects - keep necessary values from each

# COMMAND ----------

spark.sql(f"""
  CREATE OR REPLACE TEMPORARY VIEW AllUpdatesView AS
  SELECT
    nvl2(c.customerid, cast(null as bigint), p.customerid) mergeKey,
    c.customerid,
    taxid,
    status,
    lastname,
    firstname,
    middleinitial,
    gender,
    tier,
    dob,
    addressline1,
    addressline2,
    postalcode,
    city,
    stateprov,
    country,
    phone1,
    phone2,
    phone3,
    email1,
    email2,
    nationaltaxratedesc,
    nationaltaxrate,
    localtaxratedesc,
    localtaxrate,
    agencyid,
    nvl(c.creditrating, p.creditrating) creditrating,
    nvl(c.networth, p.networth) networth,
    nvl(c.marketingnameplate, p.marketingnameplate) marketingnameplate,
    iscurrent,
    batchid,
    effectivedate,
    enddate
  FROM
    MatchedCustomersView c
  FULL OUTER JOIN ProspectUpdatesView p
    ON c.customerid = p.customerid
""")

# COMMAND ----------

spark.sql(f"""
  MERGE INTO {wh_db}.DimCustomer t USING (
    SELECT
      dcv.customerid AS mergeKey,
      dcv.*
    FROM
      CustomerSourceView dcv
    UNION ALL
    SELECT
      *
    FROM
      AllUpdatesView) s 
    ON 
      t.customerid = s.mergeKey
      AND t.iscurrent
  WHEN MATCHED AND isnull(s.customerid) THEN UPDATE SET
    t.creditrating = s.creditrating,
    t.networth = s.networth,
    t.marketingnameplate = s.marketingnameplate
  WHEN MATCHED AND s.customerid IS NOT NULL THEN UPDATE SET
    t.iscurrent = false,
    t.enddate = s.effectivedate
  WHEN NOT MATCHED THEN INSERT (
    customerid,
    taxid,
    status,
    lastname,
    firstname,
    middleinitial,
    gender,
    tier,
    dob,
    addressline1,
    addressline2,
    postalcode,
    city,
    stateprov,
    country,
    phone1,
    phone2,
    phone3,
    email1,
    email2,
    nationaltaxratedesc,
    nationaltaxrate,
    localtaxratedesc,
    localtaxrate,
    agencyid,
    creditrating,
    networth,
    marketingnameplate,
    iscurrent,
    batchid,
    effectivedate,
    enddate
  )
  VALUES (
    s.customerid,
    s.taxid,
    s.status,
    s.lastname,
    s.firstname,
    s.middleinitial,
    s.gender,
    s.tier,
    s.dob,
    s.addressline1,
    s.addressline2,
    s.postalcode,
    s.city,
    s.stateprov,
    s.country,
    s.phone1,
    s.phone2,
    s.phone3,
    s.email1,
    s.email2,
    s.nationaltaxratedesc,
    s.nationaltaxrate,
    s.localtaxratedesc,
    s.localtaxrate,
    s.agencyid,
    s.creditrating,
    s.networth,
    s.marketingnameplate,
    s.iscurrent,
    s.batchid,
    s.effectivedate,
    s.enddate
  )
""")
