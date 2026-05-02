# Databricks notebook source
# MAGIC %md
# MAGIC # data_gen task: gen_daily_market
# MAGIC
# MAGIC Generates DailyMarket — in augmented mode writes
# MAGIC `tpcdi_raw_data.dailymarket{sf}` Delta with `stg_target` partitioning;
# MAGIC in standard mode writes DailyMarket.txt files.
# MAGIC
# MAGIC Depends on `gen_finwire` (reads `_gen_symbols` and re-registers it as
# MAGIC `_symbols` so existing market_data.py code keeps working).

# COMMAND ----------

import sys

dbutils.widgets.dropdown("scale_factor", "10", ["10", "100", "1000", "5000", "10000", "20000"])
dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("wh_db", "")
dbutils.widgets.text("tpcdi_directory", "/Volumes/main/tpcdi_raw_data/tpcdi_volume/")
dbutils.widgets.dropdown("regenerate_data", "NO", ["NO", "YES"])
dbutils.widgets.dropdown("log_level", "INFO", ["DEBUG", "INFO", "WARN", "ERROR"])
dbutils.widgets.dropdown("augmented_incremental", "true", ["true", "false"])

scale_factor          = dbutils.widgets.get("scale_factor").strip()
catalog               = dbutils.widgets.get("catalog").strip()
wh_db                 = dbutils.widgets.get("wh_db").strip()
tpcdi_directory       = dbutils.widgets.get("tpcdi_directory").strip()
regenerate_data       = dbutils.widgets.get("regenerate_data").strip()
log_level             = dbutils.widgets.get("log_level").strip()
augmented_incremental = dbutils.widgets.get("augmented_incremental").strip().lower() == "true"

# COMMAND ----------

_nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
workspace_src_path = f"/Workspace{_nb_path.split('/src')[0]}/src"
if f"{workspace_src_path}/tools" not in sys.path:
    sys.path.insert(0, f"{workspace_src_path}/tools")

from data_gen_tasks._shared import (
    bootstrap, read_intermediate_view, is_already_generated,
)

ctx = bootstrap(spark=spark, dbutils=dbutils, scale_factor=scale_factor,
                catalog=catalog, wh_db=wh_db, tpcdi_directory=tpcdi_directory,
                log_level=log_level, augmented_incremental=augmented_incremental,
                workspace_src_path=workspace_src_path, load_dicts=False)
cfg = ctx["cfg"]

# COMMAND ----------

output_fq = f"{catalog}.tpcdi_raw_data.dailymarket{scale_factor}"
if (augmented_incremental and regenerate_data != "YES"
        and is_already_generated(spark, output_fq)):
    print(f"[gen_daily_market] {output_fq} already populated — skipping")
    dbutils.notebook.exit("skipped")

# COMMAND ----------

read_intermediate_view(spark, catalog=catalog, wh_db=wh_db,
                       scale_factor=scale_factor,
                       name="_gen_symbols", view_name="_symbols")
print(f"[gen_daily_market] re-registered _symbols from staging schema")

from tpcdi_gen import market_data
result = market_data.generate(spark, cfg, dbutils)

import json as _json
counts = result if isinstance(result, dict) else {}
dbutils.jobs.taskValues.set(key="record_counts",
                            value=_json.dumps({f"{k[0]}::{k[1]}": v for k, v in counts.items()}))
print(f"[gen_daily_market] complete — {len(counts)} record_counts entries set")
