# Databricks notebook source
# MAGIC %md
# MAGIC # Stage Files — Trade
# MAGIC
# MAGIC Joins `tpcdi_raw_data.tradehistory{sf}` (status transitions, filtered to
# MAGIC `stg_target='files'`) to `tpcdi_raw_data.trade{sf}` (final-state fields).
# MAGIC Mirrors the DIGen `incremental_file_splitting/Trade.sql` join:
# MAGIC
# MAGIC - cdc_flag = 'I' for the first TH per tradeid (oldest th_dts), 'U' for
# MAGIC   subsequent transitions
# MAGIC - tradeprice / fee / commission / tax are emitted only when status='CMPT'
# MAGIC - event_dt = date(th_dts)

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
CREATE OR REPLACE TEMP VIEW _stage_trade AS
SELECT
  CASE
    WHEN row_number() OVER (PARTITION BY th.tradeid ORDER BY th.th_dts) = 1
    THEN 'I' ELSE 'U'
  END AS cdc_flag,
  row_number() OVER (ORDER BY th.th_dts, th.tradeid) - 1 AS cdc_dsn,
  th.tradeid,
  th.th_dts AS t_dts,
  th.status,
  t.t_tt_id,
  t.t_is_cash AS cashflag,
  t.t_s_symb,
  t.quantity,
  t.bidprice,
  t.t_ca_id,
  t.executedby,
  CASE WHEN th.status = 'CMPT' THEN t.tradeprice END AS tradeprice,
  CASE WHEN th.status = 'CMPT' THEN t.fee END AS fee,
  CASE WHEN th.status = 'CMPT' THEN t.commission END AS commission,
  CASE WHEN th.status = 'CMPT' THEN t.tax END AS tax,
  to_date(th.th_dts) AS event_dt
FROM {catalog}.tpcdi_raw_data.tradehistory{scale_factor} th
JOIN {catalog}.tpcdi_raw_data.trade{scale_factor} t
  ON t.t_id = th.tradeid
WHERE th.stg_target = 'files'
""")

# COMMAND ----------

stage_to_files(
    spark, dbutils,
    source_view="_stage_trade",
    date_col="event_dt",
    filename="Trade.txt",
    target_dir=target_dir,
)
