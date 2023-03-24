# Databricks notebook source
# MAGIC %md
# MAGIC # CompanyFinancialsStg
# MAGIC * This is just a staging table to remove need to recompute the query below multiple times. It does not change after the initial batch1 load of the 2 source tables Financial and DimCompany

# COMMAND ----------

user_name = spark.sql("select current_user()").collect()[0][0].split("@")[0].replace(".","_")

dbutils.widgets.text("wh_db", f"{user_name}_TPCDI",'Root name of Target Warehouse')
wh_db = f"{dbutils.widgets.get('wh_db')}_wh"
staging_db = f"{dbutils.widgets.get('wh_db')}_stage"

# COMMAND ----------

spark.sql(f"""
  CREATE OR REPLACE TABLE {staging_db}.CompanyFinancialsStg AS SELECT
    sk_companyid,
    fi_qtr_start_date,
    sum(fi_basic_eps) OVER (PARTITION BY companyid ORDER BY fi_qtr_start_date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) - fi_basic_eps sum_fi_basic_eps
  FROM {wh_db}.Financial
  JOIN {wh_db}.DimCompany
    USING (sk_companyid)
""")

# COMMAND ----------

spark.sql(f"ANALYZE TABLE {staging_db}.CompanyFinancialsStg COMPUTE STATISTICS FOR ALL COLUMNS")
