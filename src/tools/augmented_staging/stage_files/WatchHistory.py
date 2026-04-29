# Databricks notebook source
# MAGIC %md
# MAGIC # Stage Files — WatchHistory
# MAGIC
# MAGIC Reads `tpcdi_raw_data.watchhistory{sf}` filtered to `stg_target='files'`
# MAGIC (w_dts >= 2015-07-06). DIGen splitter adds `cdc_flag='I'`, sequential
# MAGIC `cdc_dsn`, and `event_dt` derived from `w_dts`.

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

target_dir = f"{tpcdi_directory.rstrip('/')}/augmented_incremental/_staging/sf={scale_factor}"

_nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
_tools_dir = f"/Workspace{_nb_path.split('/src')[0]}/src/tools"
if _tools_dir not in sys.path:
    sys.path.insert(0, _tools_dir)
from augmented_staging._stage_ingestion import stage_to_files

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW _stage_watchhistory AS
SELECT
  'I' AS cdc_flag,
  row_number() OVER (ORDER BY w_dts, w_c_id, w_s_symb) - 1 AS cdc_dsn,
  w_c_id,
  w_s_symb,
  w_dts,
  w_action,
  to_date(w_dts) AS event_dt
FROM {catalog}.tpcdi_raw_data.watchhistory{scale_factor}
WHERE stg_target = 'files'
""")

# COMMAND ----------

stage_to_files(
    spark, dbutils,
    source_view="_stage_watchhistory",
    date_col="event_dt",
    filename="WatchHistory.txt",
    target_dir=target_dir,
)
