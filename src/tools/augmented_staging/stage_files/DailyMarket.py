# Databricks notebook source
# MAGIC %md
# MAGIC # Stage Files — DailyMarket
# MAGIC
# MAGIC Reads `tpcdi_raw_data.dailymarket{sf}` (no `stg_target` filter — every
# MAGIC DailyMarket row is post-2015-07-06, so it all becomes per-day files).
# MAGIC Synthesizes the CDC columns the DIGen splitter would have produced
# MAGIC (`cdc_flag='I'`, sequential `cdc_dsn`) and lets `stage_to_files`
# MAGIC fan the rows out to one `DailyMarket.txt` per `dm_date` under
# MAGIC `{tpcdi_directory}augmented_incremental/_staging/sf={sf}/{date}/`.

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

# DIGen splitter shape: cdc_flag = 'I' for every DailyMarket row, cdc_dsn = monotonically increasing. Output column order matches the existing `incremental_file_splitting/DailyMarket.sql` rawdailymarket{sf} schema so the per-day Customer-side ingest doesn't need to know it came from a different generator.
spark.sql(f"""
CREATE OR REPLACE TEMP VIEW _stage_dailymarket AS
SELECT
  'I' AS cdc_flag,
  cdc_dsn,
  dm_date,
  dm_s_symb,
  dm_close,
  dm_high,
  dm_low,
  dm_vol,
  dm_date AS _pdate  -- duplicate partition col so dm_date stays in the data file (the rawdailymarket schema has dm_date as a regular data column too)
FROM {catalog}.tpcdi_raw_data.dailymarket{scale_factor}
WHERE stg_target = 'files'
""")

# COMMAND ----------

stage_to_files(
    spark, dbutils,
    source_view="_stage_dailymarket",
    date_col="_pdate",
    dataset="DailyMarket",
    target_dir=target_dir,
)
