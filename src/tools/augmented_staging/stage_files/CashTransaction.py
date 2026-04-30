# Databricks notebook source
# MAGIC %md
# MAGIC # Stage Files — CashTransaction
# MAGIC
# MAGIC Reads `tpcdi_raw_data.cashtransaction{sf}` filtered to rows with
# MAGIC `stg_target='files'` (i.e. ct_dts >= 2015-07-06). DIGen splitter
# MAGIC adds `cdc_flag='I'` + sequential `cdc_dsn` + per-row `event_dt`.

# COMMAND ----------

import sys
from pyspark.sql import functions as F

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
CREATE OR REPLACE TEMP VIEW _stage_cashtransaction AS
SELECT
  'I' AS cdc_flag,
  cdc_dsn,
  accountid,
  ct_dts,
  ct_amt,
  ct_name,
  to_date(ct_dts) AS event_dt,
  to_date(ct_dts) AS _pdate  -- duplicate partition col so event_dt stays in the data file
FROM {catalog}.tpcdi_raw_data.cashtransaction{scale_factor}
WHERE stg_target = 'files'
""")

# COMMAND ----------

stage_to_files(
    spark, dbutils,
    source_view="_stage_cashtransaction",
    date_col="_pdate",
    filename="CashTransaction.txt",
    target_dir=target_dir,
)
