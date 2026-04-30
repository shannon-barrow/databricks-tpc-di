# Databricks notebook source
# MAGIC %md
# MAGIC # Stage Files — HoldingHistory
# MAGIC
# MAGIC Reads `tpcdi_raw_data.holdinghistory{sf}` filtered to `stg_target='files'`.
# MAGIC `event_dt` is already on the table (date of the trade's `_complete_ts`),
# MAGIC so no join back to trade is needed (DIGen splitter does
# MAGIC `JOIN max(event_dt) per tradeid` to derive it; we computed it at
# MAGIC stage 0 instead).

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
CREATE OR REPLACE TEMP VIEW _stage_holdinghistory AS
SELECT
  'I' AS cdc_flag,
  cdc_dsn,
  hh_h_t_id,
  hh_t_id,
  hh_before_qty,
  hh_after_qty,
  event_dt,
  event_dt AS _pdate  -- duplicate partition col so event_dt stays in the data file
FROM {catalog}.tpcdi_raw_data.holdinghistory{scale_factor}
WHERE stg_target = 'files'
""")

# COMMAND ----------

stage_to_files(
    spark, dbutils,
    source_view="_stage_holdinghistory",
    date_col="_pdate",
    dataset="HoldingHistory",
    target_dir=target_dir,
)
