# Databricks notebook source
# MAGIC %md
# MAGIC # DailyMarket

# COMMAND ----------

# MAGIC %md
# MAGIC * As of February 2023, Photon does NOT support the UNBOUNDED PRECEDING WINDOW statement needed for the business logic of this table (previous year high/low amount and date for each stock symbol)
# MAGIC * Therefore, expect this table to take the longest execution when run in Photon runtime as it comes out of Photon - we can revisit when this functionality is added to Photon AFTER DBR 13.1
# MAGIC * Additionally, the logic looks funky as there is not fast native way to retrieve the amount AND date for the previous year low/high values. The fastest execution I have found is the one below (tried a few others but they were slower - even if the code was more concise)  
# MAGIC 
# MAGIC **Steps**
# MAGIC 1) Union the historical and incremental DailyMarket tables
# MAGIC 2) Find out the previous year min/max for each symbol. Store in temp staging table since this needs multiple self-joins (calculate it once)
# MAGIC 3) Join table to itself to find each of the min and the max - each time making sure the amount is within the 1 year before the date of the stock symbol
# MAGIC 4) Use WINDOW function to only select the FIRST time the amount occurred in the year before (this satisfies the case when the amount happens multiple times over previous year)  
# MAGIC 
# MAGIC **Business Logic: When populating fields of the DailyMarketStg table:**  
# MAGIC 
# MAGIC * ClosePrice, DayHigh, DayLow, and Volume are copied from DM_CLOSE, DM_HIGH, DM_LOW, and DM_VOL respectively.
# MAGIC * SK_DateID is obtained from DimDate by matching DM_DATE with DateValue to return the SK_DateID. The match is guaranteed to succeed because DimDate has been populated with date information for all dates relevant to the benchmark.
# MAGIC * FiftyTwoWeekHigh and SK_FiftyTwoWeekHighDate are determined by finding the highest price over the last year (approximately 52 weeks) for a given security. The FactMarketHistory table itself can be used for this comparison. FiftyTwoWeekHigh is set to the highest DM_HIGH value for any date in the range from DM_DATE back to but not including the same date one year earlier. SK_FiftyTwoWeekHighDate is assigned the earliest date in the date range upon which this DM_HIGH value occurred.  Over the course of the year, the surrogate key value for a security may have changed. It is not sufficient to simply compare records that share the same SK_SecurityID value.
# MAGIC * FiftyTwoWeekLow and SK_FiftyTwoWeekLowDate are determined by finding the lowest price over the last year (approximately 52 weeks) for a given security. The FactMarketHistory table itself can be used for this comparison. FiftyTwoWeekLow is set to the lowest DM_LOW value for any date in the range from DM_DATE back to but not including the same date one year earlier. SK_FiftyTwoWeekLowDate is assigned the earliest date in the date range upon which this DM_LOW value occurred.  Over the course of the year, the surrogate key value for a security may have changed. It is not sufficient to simply compare records that share the same SK_SecurityID value.  
# MAGIC 
# MAGIC Note: The terms “52 week high” and “52 week low” are common in financial reporting, and in general practice seem to mean “over the last year”. This benchmark follows the one-year interpretation. Therefore in a summary generated on July 4, 2014, the date range to use is 2013-07-05 to 2014-07-04.  This is why the 364 days window is used below instead of 365 or 1 YEAR since the SQL code is INCLUSIVE

# COMMAND ----------

# MAGIC %md
# MAGIC # Setup

# COMMAND ----------

import json

with open("../../tools/traditional_config.json", "r") as json_conf:
  table_conf = json.load(json_conf)['views']['DailyMarketHistorical']
user_name = spark.sql("select current_user()").collect()[0][0].split("@")[0].replace(".","_")

dbutils.widgets.text("wh_db", f"{user_name}_TPCDI",'Root name of Target Warehouse')
dbutils.widgets.text("tpcdi_directory", "/tmp/tpcdi/", "Directory where Raw Files are located")
dbutils.widgets.text("scale_factor", "10", "Scale factor")

wh_db = f"{dbutils.widgets.get('wh_db')}_wh"
staging_db = f"{dbutils.widgets.get('wh_db')}_stage"
scale_factor = dbutils.widgets.get("scale_factor")
tpcdi_directory = dbutils.widgets.get("tpcdi_directory")
files_directory = f"{tpcdi_directory}sf={scale_factor}"

# COMMAND ----------

# MAGIC %md
# MAGIC # This load is for historical and incremental loads
# MAGIC * When it is historical load (batchid = 1) reads from temp view of large raw CSV files.  
# MAGIC * Incremental (batches 2 and 3) are being loaded via autoloader into staging DB dailymarketincremental table
# MAGIC * Union these 2 together to get all dailymarket records

# COMMAND ----------

spark.read.csv(
  f"{files_directory}/{table_conf['path']}/{table_conf['filename']}", 
  schema=table_conf['raw_schema'], 
  sep=table_conf['sep'], 
  header=table_conf['header'], 
  inferSchema=False).createOrReplaceTempView("v_dmh")

# COMMAND ----------

spark.sql(f"""
  CREATE OR REPLACE TEMPORARY VIEW tempDailyMarket AS SELECT
    sk_dateid,
    min(dm_low) OVER (
      PARTITION BY dm_s_symb
      ORDER BY dm_date ASC ROWS BETWEEN 364 PRECEDING AND 0 FOLLOWING --CURRENT ROW
    ) fiftytwoweeklow,
    max(dm_high) OVER (
      PARTITION by dm_s_symb
      ORDER BY dm_date ASC ROWS BETWEEN 364 PRECEDING AND 0 FOLLOWING --CURRENT ROW
    ) fiftytwoweekhigh,
    dmh.*
  FROM (
    SELECT *, 1 batchid FROM v_dmh 
    UNION ALL
    SELECT * except(cdc_flag, cdc_dsn) FROM {staging_db}.dailymarketincremental 
  ) dmh
  JOIN {wh_db}.DimDate d 
    ON d.datevalue = dm_date
""")

# COMMAND ----------

spark.sql(f"""
  CREATE OR REPLACE TABLE {staging_db}.DailyMarketStg PARTITIONED BY (batchid) AS SELECT * FROM (
    SELECT 
      a.*,
      b.sk_dateid AS sk_fiftytwoweeklowdate,
      c.sk_dateid AS sk_fiftytwoweekhighdate
    FROM tempDailyMarket a
    JOIN tempDailyMarket b 
      ON
        a.dm_s_symb = b.dm_s_symb
        AND a.fiftytwoweeklow = b.dm_low
        AND b.dm_date between add_months(a.dm_date, -12) AND a.dm_date
    JOIN tempDailyMarket c 
      ON 
        a.dm_s_symb = c.dm_s_symb
        AND a.fiftytwoweekhigh = c.dm_high
        AND c.dm_date between add_months(a.dm_date, -12) AND a.dm_date
  ) dmh
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY dm_s_symb, dm_date 
    ORDER BY sk_fiftytwoweeklowdate, sk_fiftytwoweekhighdate) = 1
""")

# COMMAND ----------

spark.sql(f"ANALYZE TABLE {staging_db}.DailyMarketStg COMPUTE STATISTICS FOR ALL COLUMNS")

# COMMAND ----------

# DBTITLE 1,This one too SLOWWWW! NON-PHOTON VERSION! Photon window limited for the moment - can't do min_by/max_by & regular runtime inefficient with those functions
# spark.sql(f"""
#   CREATE OR REPLACE TABLE {staging_db}.TESTINGDailyMarketStg PARTITIONED BY (batchid) AS SELECT
#     dm_date,
#     dm_s_symb,
#     dm_close,
#     dm_high,
#     dm_low, 
#     dm_vol,
#     batchid,
#     sk_dateid,
#     fiftytwoweeklow.dm_low fiftytwoweeklow,
#     fiftytwoweekhigh.dm_high fiftytwoweekhigh,
#     fiftytwoweeklow.sk_dateid sk_fiftytwoweeklowdate,
#     fiftytwoweekhigh.sk_dateid sk_fiftytwoweekhighdate
#   FROM (
#     SELECT
#       sk_dateid,
#       min_by(struct(dm_low, sk_dateid), dm_low) OVER (
#         PARTITION BY dm_s_symb
#         ORDER BY dm_date ASC ROWS BETWEEN 364 PRECEDING AND CURRENT ROW) fiftytwoweeklow,
#       max_by(struct(dm_high, sk_dateid), dm_high) OVER (
#         PARTITION by dm_s_symb
#         ORDER BY dm_date ASC ROWS BETWEEN 364 PRECEDING AND CURRENT ROW) fiftytwoweekhigh,
#       dmh.*
#     FROM (
#       SELECT *, 1 batchid FROM v_dmh 
#       UNION ALL
#       SELECT * except(cdc_flag, cdc_dsn) FROM {staging_db}.dailymarketincremental 
#     ) dmh
#     JOIN {wh_db}.DimDate d 
#       ON d.datevalue = dm_date)
# """)
