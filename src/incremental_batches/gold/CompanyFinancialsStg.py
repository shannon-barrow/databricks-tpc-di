# Databricks notebook source
# MAGIC %md
# MAGIC # CompanyFinancialsStg
# MAGIC * This is just a staging table to remove need to recompute the query below multiple times. It does not change after the initial batch1 load of the 2 source tables Financial and DimCompany

# COMMAND ----------

import string

user_name = spark.sql("select lower(regexp_replace(split(current_user(), '@')[0], '(\\\W+)', ' '))").collect()[0][0]
default_catalog = 'tpcdi' if spark.conf.get('spark.databricks.unityCatalog.enabled') == 'true' else 'hive_metastore'
default_wh = f"{string.capwords(user_name).replace(' ','_')}_TPCDI"

dbutils.widgets.text("catalog", default_catalog, 'Target Catalog')
dbutils.widgets.text("wh_db", default_wh,'Target Database')

catalog = dbutils.widgets.get("catalog")
wh_db = f"{dbutils.widgets.get('wh_db')}"
staging_db = f"{dbutils.widgets.get('wh_db')}_stage"
spark.sql(f"USE CATALOG {catalog}")

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
