# Databricks notebook source
# MAGIC %md
# MAGIC # Stage Files — Account
# MAGIC
# MAGIC Reads `tpcdi_raw_data.customermgmt{sf}` filtered to:
# MAGIC `stg_target='files'` AND `ActionType NOT IN ('UPDCUST','INACT')`
# MAGIC (the account-touching action types — UPDCUST/INACT are customer-only).
# MAGIC
# MAGIC Mirrors the DIGen `incremental_file_splitting/Account.sql` output schema.
# MAGIC Status is encoded as `'ACTV'/'INAC'` per the splitter convention.

# COMMAND ----------

import sys

dbutils.widgets.dropdown("scale_factor", "10",
                         ["10", "100", "1000", "5000", "10000", "20000"])
dbutils.widgets.text("tpcdi_directory",
                     "/Volumes/main/tpcdi_raw_data/tpcdi_volume/")
dbutils.widgets.text("catalog", "main")

scale_factor    = dbutils.widgets.get("scale_factor").strip()
tpcdi_directory = dbutils.widgets.get("tpcdi_directory").strip()
catalog         = dbutils.widgets.get("catalog").strip()

target_dir = f"{tpcdi_directory.rstrip('/')}/sf={scale_factor}"  # tpcdi_directory base_param already ends with augmented_incremental/_staging/

_nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
_tools_dir = f"/Workspace{_nb_path.split('/src')[0]}/src/tools"
if _tools_dir not in sys.path:
    sys.path.insert(0, _tools_dir)
from augmented_staging._stage_ingestion import stage_to_files

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW _stage_account AS
SELECT
  CASE WHEN ActionType IN ('NEW', 'ADDACCT') THEN 'I' ELSE 'U' END AS cdc_flag,
  cdc_dsn,
  accountid,
  brokerid,
  customerid,
  accountdesc,
  taxstatus,
  CASE WHEN ActionType IN ('CLOSEACCT', 'INACT') THEN 'INAC' ELSE 'ACTV' END AS status,
  to_date(update_ts) AS update_dt,
  to_date(update_ts) AS _pdate  -- duplicate partition col so update_dt stays in the data file
FROM {catalog}.tpcdi_raw_data.customermgmt{scale_factor}
WHERE stg_target = 'files'
  AND ActionType NOT IN ('UPDCUST', 'INACT')
""")

# COMMAND ----------

stage_to_files(
    spark, dbutils,
    source_view="_stage_account",
    date_col="_pdate",
    filename="Account.txt",
    target_dir=target_dir,
)
