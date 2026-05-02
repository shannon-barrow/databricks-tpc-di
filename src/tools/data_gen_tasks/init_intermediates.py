# Databricks notebook source
# MAGIC %md
# MAGIC # data_gen task: init_intermediates
# MAGIC
# MAGIC Creates the intermediate `_stage` schema (`{catalog}.{wh_db}_{sf}_stage`)
# MAGIC that holds cross-task Delta tables (`_gen_brokers`, `_gen_symbols`,
# MAGIC `_gen_customer_dates`) plus any disk_cache replacements (CustomerMgmt
# MAGIC schedules, trade_df, etc.).
# MAGIC
# MAGIC When `regenerate_data=YES`, drops every `_gen_*` table that may exist
# MAGIC from a prior run so per-task self-skips don't false-positive.

# COMMAND ----------

import sys

dbutils.widgets.dropdown("scale_factor", "10", ["10", "100", "1000", "5000", "10000", "20000"])
dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("wh_db", "")
dbutils.widgets.dropdown("regenerate_data", "NO", ["NO", "YES"])
dbutils.widgets.dropdown("log_level", "INFO", ["DEBUG", "INFO", "WARN", "ERROR"])
dbutils.widgets.text("tpcdi_directory", "/Volumes/main/tpcdi_raw_data/tpcdi_volume/")

scale_factor    = dbutils.widgets.get("scale_factor").strip()
catalog         = dbutils.widgets.get("catalog").strip()
wh_db           = dbutils.widgets.get("wh_db").strip()
regenerate_data = dbutils.widgets.get("regenerate_data").strip()
log_level       = dbutils.widgets.get("log_level").strip()
tpcdi_directory = dbutils.widgets.get("tpcdi_directory").strip()

# COMMAND ----------

_nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
workspace_src_path = f"/Workspace{_nb_path.split('/src')[0]}/src"
if f"{workspace_src_path}/tools" not in sys.path:
    sys.path.insert(0, f"{workspace_src_path}/tools")

from data_gen_tasks._shared import stage_schema_fq

stage_schema = stage_schema_fq(catalog, wh_db, scale_factor)
print(f"[init_intermediates] ensuring {stage_schema} exists")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {stage_schema} "
          f"COMMENT 'data_gen + benchmark intermediate temp tables'")

# COMMAND ----------

if regenerate_data == "YES":
    # Drop every transient table the gen tasks (and the disk_cache replacements)
    # may have written from a prior run. Leaving them in place would let a
    # gen task short-circuit its self-skip check when the rest of the world
    # has been wiped (e.g. tpcdi_raw_data was dropped manually).
    for _t in ("_gen_brokers", "_gen_symbols", "_gen_customer_dates",
               "_gen_finwire_symbols", "_gen_customermgmt_schedules",
               "_gen_customermgmt_actions", "_gen_trade_df"):
        spark.sql(f"DROP TABLE IF EXISTS {stage_schema}.{_t}")
    print(f"[init_intermediates] regenerate_data=YES → dropped any prior _gen_* "
          f"temp tables in {stage_schema}")
else:
    print(f"[init_intermediates] regenerate_data=NO → leaving prior _gen_* "
          f"tables in place for downstream self-skip / repair")
