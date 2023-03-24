# Databricks notebook source
# MAGIC %md
# MAGIC # Setup

# COMMAND ----------

user_name = spark.sql("select current_user()").collect()[0][0].split("@")[0].replace(".","_")
dbutils.widgets.text("wh_db", f"{user_name}_TPCDI",'Root name of Target Warehouse')
wh_db = f"{dbutils.widgets.get('wh_db')}_wh"
staging_db = f"{dbutils.widgets.get('wh_db')}_stage"

# COMMAND ----------

# MAGIC %md
# MAGIC # Create View on FinWire Then Write into Financial

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TEMPORARY VIEW FinwireFinView AS
SELECT 
  * except(conameorcik),
  nvl(string(cast(conameorcik as bigint)), conameorcik) conameorcik
FROM (
  SELECT
    to_timestamp(substring(value, 1, 15), 'yyyyMMdd-HHmmss') AS PTS,
    cast(substring(value, 19, 4) AS INT) AS fi_year,
    cast(substring(value, 23, 1) AS INT) AS fi_qtr,
    to_date(substring(value, 24, 8), 'yyyyMMdd') AS fi_qtr_start_date,
    --to_date(substring(value, 32, 8), 'yyyyMMdd') AS PostingDate,
    cast(substring(value, 40, 17) AS DOUBLE) AS fi_revenue,
    cast(substring(value, 57, 17) AS DOUBLE) AS fi_net_earn,
    cast(substring(value, 74, 12) AS DOUBLE) AS fi_basic_eps,
    cast(substring(value, 86, 12) AS DOUBLE) AS fi_dilut_eps,
    cast(substring(value, 98, 12) AS DOUBLE) AS fi_margin,
    cast(substring(value, 110, 17) AS DOUBLE) AS fi_inventory,
    cast(substring(value, 127, 17) AS DOUBLE) AS fi_assets,
    cast(substring(value, 144, 17) AS DOUBLE) AS fi_liability,
    cast(substring(value, 161, 13) AS BIGINT) AS fi_out_basic,
    cast(substring(value, 174, 13) AS BIGINT) AS fi_out_dilut,
    trim(substring(value, 187, 60)) AS conameorcik
  FROM {staging_db}.FinWire
  WHERE rectype = 'FIN'
)
""")

# COMMAND ----------

spark.sql(f"""
  INSERT OVERWRITE {wh_db}.Financial
  SELECT 
    sk_companyid,
    fi_year,
    fi_qtr,
    fi_qtr_start_date,
    fi_revenue,
    fi_net_earn,
    fi_basic_eps,
    fi_dilut_eps,
    fi_margin,
    fi_inventory,
    fi_assets,
    fi_liability,
    fi_out_basic,
    fi_out_dilut
  FROM FinwireFinView f  
  JOIN (
    SELECT 
      sk_companyid,
      name conameorcik,
      EffectiveDate,
      EndDate
    FROM {wh_db}.DimCompany
    UNION ALL
    SELECT 
      sk_companyid,
      cast(companyid as string) conameorcik,
      EffectiveDate,
      EndDate
    FROM {wh_db}.DimCompany
  ) dc 
  ON
    f.conameorcik = dc.conameorcik 
    AND date(PTS) >= dc.effectivedate 
    AND date(PTS) < dc.enddate
""")

# COMMAND ----------

spark.sql(f"ANALYZE TABLE {wh_db}.Financial COMPUTE STATISTICS")
