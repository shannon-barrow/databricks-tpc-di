# Databricks notebook source
# MAGIC %md 
# MAGIC # FactMarketHistory

# COMMAND ----------

# MAGIC %md
# MAGIC When populating fields of the FactMarketHistory table:
# MAGIC 
# MAGIC * SK_SecurityID is obtained from DimSecurity by matching the associated security’s current record DM_S_SYMB with Symbol, for the date indicated by DM_DATE, to return the SK_SecurityID. The match is guaranteed to succeed due to the referential integrity of the OLTP database. The dependency of FactMarketHistory on DimSecurity requires that any update to a company’s DimSecurity records must be completed before updates to the FactMarketHistory records.
# MAGIC * SK_CompanyID is obtained from DimSecurity by matching DM_S_SYMB with Symbol, for the date indicated by DM_DATE, to return the SK_CompanyID. The match is guaranteed to succeed due to the referential integrity of the OLTP database. The dependency of FactMarketHistory on DimSecurity requires that any update to a company’s DimSecurity records must be completed before updates to the FactMarketHistory records.
# MAGIC * PERatio is calculated by dividing DM_CLOSE (the closing price for a security on a given day) by the sum of the company’s quarterly earnings per share (“eps”) over the previous 4 quarters prior to DM_DATE. Company quarterly earnings per share data is provided by the FINWIRE data source in the EPS field of the ‘FIN’ record type. If there are no earnings for this company, NULL is assigned to PERatio and an alert condition is raised as described below.  Over the course of the previous 4 quarters, attributes of securities and companies may have changed. As a result, there may be more than one surrogate key value used for a security and/or company in the target data warehouse tables during that time period.
# MAGIC * Yield is calculated by dividing the security’s dividend by DM_CLOSE (the closing price for a security on a given day), then multiplying by 100 to obtain the percentage. The dividend is obtained from DimSecurity by matching DM_S_SYMB with Symbol, where DM_DATE is in the range given by EffectiveDate and EndDate, to return the Dividend field.

# COMMAND ----------

# MAGIC %md
# MAGIC # Setup

# COMMAND ----------

user_name = spark.sql("select current_user()").collect()[0][0].split("@")[0].replace(".","_")

dbutils.widgets.text("wh_db", f"{user_name}_TPCDI",'Root name of Target Warehouse')
dbutils.widgets.text("batch_id", "1", "Batch ID (1,2,3)")

batch_id = dbutils.widgets.get("batch_id")
wh_db = f"{dbutils.widgets.get('wh_db')}_wh"
staging_db = f"{dbutils.widgets.get('wh_db')}_stage"

# COMMAND ----------

# MAGIC %md
# MAGIC # This load is for historical and incremental loads
# MAGIC * Preprocessed DailyMarket data is waiting in stage but need to join to Security, Company, and Financial
# MAGIC * Broke the dailymarket and company/financial steps into separate notebooks to ensure they began earlier and operated in parallel before later dependent tables were ready

# COMMAND ----------

# Historical needs to handle effective/end dates. Otherwise just need currently active record
sec_join = "dmh.dm_date >= s.effectivedate AND dmh.dm_date < s.enddate" if batch_id == '1' else "s.iscurrent"

spark.sql(f"""
  INSERT INTO {wh_db}.FactMarketHistory
  SELECT
    s.sk_securityid,
    s.sk_companyid,
    sk_dateid,
    dmh.dm_close / sum_fi_basic_eps AS peratio,
    (s.dividend / dmh.dm_close) / 100 yield,
    fiftytwoweekhigh,
    sk_fiftytwoweekhighdate,
    fiftytwoweeklow,
    sk_fiftytwoweeklowdate,
    dm_close closeprice,
    dm_high dayhigh,
    dm_low daylow,
    dm_vol volume,
    dmh.batchid
  FROM {staging_db}.DailyMarketStg dmh
  JOIN {wh_db}.DimSecurity s 
    ON 
      s.symbol = dmh.dm_s_symb
      AND {sec_join}
  LEFT JOIN {staging_db}.CompanyFinancialsStg f 
    ON 
      f.sk_companyid = s.sk_companyid
      AND quarter(dmh.dm_date) = quarter(fi_qtr_start_date)
      AND year(dmh.dm_date) = year(fi_qtr_start_date)
  WHERE dmh.batchid = {batch_id};
""")
