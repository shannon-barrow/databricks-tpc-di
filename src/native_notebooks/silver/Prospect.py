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

user_name = spark.sql("select current_user()").collect()[0][0].split("@")[0].replace(".","_")
dbutils.widgets.text("wh_db", f"{user_name}_TPCDI",'Root name of Target Warehouse')
dbutils.widgets.text("batch_id", "1", "Batch ID (1,2,3)")
staging_db = f"{dbutils.widgets.get('wh_db')}_stage"
wh_db = f"{dbutils.widgets.get('wh_db')}_wh"
batch_id = dbutils.widgets.get("batch_id")

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

# prospect_query = f"""SELECT
#   agencyid,
#   sk_dateid sk_recorddateid,
#   sk_dateid sk_updatedateid,
#   p.batchid,
#   nvl2(c.lastname, True, False) iscustomer, 
#   p.lastname,
#   p.firstname,
#   p.middleinitial,
#   p.gender,
#   p.addressline1,
#   p.addressline2,
#   p.postalcode,
#   city,
#   state,
#   country,
#   phone,
#   income,
#   numbercars,
#   numberchildren,
#   maritalstatus,
#   age,
#   creditrating,
#   ownorrentflag,
#   employer,
#   numbercreditcards,
#   networth,
#   if(
#     isnotnull(
#       if(networth > 1000000 or income > 200000,"HighValue+","") || 
#       if(numberchildren > 3 or numbercreditcards > 5,"Expenses+","") ||
#       if(age > 45, "Boomer+", "") ||
#       if(income < 50000 or creditrating < 600 or networth < 100000, "MoneyAlert+","") ||
#       if(numbercars > 3 or numbercreditcards > 7, "Spender+","") ||
#       if(age < 25 and networth > 1000000, "Inherited+","")),
#     left(
#       if(networth > 1000000 or income > 200000,"HighValue+","") || 
#       if(numberchildren > 3 or numbercreditcards > 5,"Expenses+","") ||
#       if(age > 45, "Boomer+", "") ||
#       if(income < 50000 or creditrating < 600 or networth < 100000, "MoneyAlert+","") ||
#       if(numbercars > 3 or numbercreditcards > 7, "Spender+","") ||
#       if(age < 25 and networth > 1000000, "Inherited+",""),
#       length(
#         if(networth > 1000000 or income > 200000,"HighValue+","") || 
#         if(numberchildren > 3 or numbercreditcards > 5,"Expenses+","") ||
#         if(age > 45, "Boomer+", "") ||
#         if(income < 50000 or creditrating < 600 or networth < 100000, "MoneyAlert+","") ||
#         if(numbercars > 3 or numbercreditcards > 7, "Spender+","") ||
#         if(age < 25 and networth > 1000000, "Inherited+",""))
#       -1),
#     NULL) marketingnameplate
#   FROM {staging_db}.ProspectIncremental p
#   JOIN (
#     SELECT 
#       sk_dateid,
#       batchid
#     FROM {wh_db}.BatchDate b 
#     JOIN {wh_db}.DimDate d 
#       ON b.batchdate = d.datevalue
#     WHERE batchid = {batch_id}) b
#     ON p.batchid = b.batchid
#   LEFT JOIN (
#     SELECT 
#       lastname,
#       firstname,
#       addressline1,
#       addressline2,
#       postalcode
#     FROM {staging_db}.DimCustomerStg
#     WHERE iscurrent
#   ) c
#     ON 
#       upper(p.LastName) = upper(c.lastname)
#       and upper(p.FirstName) = upper(c.firstname)
#       and upper(p.AddressLine1) = upper(c.addressline1)
#       and upper(nvl(p.addressline2, '')) = upper(nvl(c.addressline2, ''))
#       and upper(p.PostalCode) = upper(c.postalcode)
#   WHERE p.batchid = {batch_id}
# """

# if batch_id == '1': 
#   spark.sql(f"INSERT INTO {wh_db}.Prospect {prospect_query}")
# else:
#   spark.sql(f"""
#     MERGE INTO {wh_db}.Prospect t
#     USING ({prospect_query}) s
#     ON t.agencyid = s.agencyid
#     WHEN MATCHED 
#       AND (
#         COALESCE(s.LastName, '') != COALESCE(t.LastName,'')
#         OR COALESCE(s.FirstName, '') != COALESCE(t.FirstName,'')
#         OR COALESCE(s.MiddleInitial, '') != COALESCE(t.MiddleInitial,'')
#         OR s.Gender != t.Gender
#         OR COALESCE(s.AddressLine1, '') != COALESCE(t.AddressLine1,'')
#         OR COALESCE(s.AddressLine2, '') != COALESCE(t.AddressLine2,'')
#         OR COALESCE(s.PostalCode, '') != COALESCE(t.PostalCode,'')
#         OR COALESCE(s.City, '') != COALESCE(t.City,'')
#         OR COALESCE(s.State, '') != COALESCE(t.State,'')
#         OR COALESCE(s.Country, '') != COALESCE(t.Country,'')
#         OR COALESCE(s.Phone, '') != COALESCE(t.Phone,'')
#         OR COALESCE(s.Income, '') != COALESCE(t.Income,'')
#         OR COALESCE(s.NumberCars,0) != COALESCE(t.NumberCars,0) 
#         OR COALESCE(s.NumberChildren,0) != COALESCE(t.NumberChildren,0) 
#         OR COALESCE(s.MaritalStatus, '') != COALESCE(t.MaritalStatus,'')
#         OR COALESCE(s.Age,0) != COALESCE(t.Age,0) 
#         OR COALESCE(s.CreditRating,0) != COALESCE(t.CreditRating,0) 
#         OR COALESCE(s.OwnOrRentFlag, '') != COALESCE(t.OwnOrRentFlag,'')
#         OR COALESCE(s.Employer, '') != COALESCE(t.Employer,'')
#         OR COALESCE(s.NumberCreditCards,0) != COALESCE(t.NumberCreditCards,0) 
#         OR COALESCE(s.NetWorth,0) != COALESCE(t.NetWorth,0) 
#       )
#       THEN UPDATE SET
#         sk_recorddateid = s.sk_recorddateid,
#         sk_updatedateid = s.sk_updatedateid
#         ,iscustomer = s.iscustomer
#         ,LastName = s.LastName 
#         ,FirstName= s.FirstName
#         ,MiddleInitial = s.MiddleInitial 
#         ,Gender = s.Gender 
#         ,AddressLine1 = s.AddressLine1 
#         ,AddressLine2 = s.AddressLine2 
#         ,PostalCode = s.PostalCode 
#         ,City = s.City 
#         ,State = s.State 
#         ,Country= s.Country
#         ,Phone = s.Phone 
#         ,Income = s.Income 
#         ,NumberCars = s.NumberCars 
#         ,NumberChildren = s.NumberChildren 
#         ,MaritalStatus = s.MaritalStatus 
#         ,Age = s.Age 
#         ,CreditRating= s.CreditRating
#         ,OwnOrRentFlag= s.OwnOrRentFlag
#         ,Employer = s.Employer 
#         ,NumberCreditCards = s.NumberCreditCards 
#         ,NetWorth = s.NetWorth
#         ,marketingnameplate = s.marketingnameplate,
#         batchid = s.batchid
#     WHEN MATCHED THEN UPDATE SET
#       sk_recorddateid = s.sk_recorddateid
#     WHEN NOT MATCHED THEN INSERT *;
#   """)

# COMMAND ----------

spark.sql(f"ANALYZE TABLE {wh_db}.Prospect COMPUTE STATISTICS")
