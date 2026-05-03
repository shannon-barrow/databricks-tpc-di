# Databricks notebook source
# MAGIC %md
# MAGIC # data_gen task: copy_hr
# MAGIC
# MAGIC Copies HR.csv staging → final. Depends on `gen_hr` and runs in
# MAGIC parallel with downstream gen tasks (gen_customer, gen_daily_market,
# MAGIC gen_trade, gen_watch_history) since the staging files are persisted
# MAGIC on the volume by gen_hr but the actual copy work is decoupled.
# MAGIC
# MAGIC Synchronous: waits for `wait_for_background_copies()` before exiting
# MAGIC so any large-part daemon threads complete cleanly.

# COMMAND ----------

import sys

dbutils.widgets.dropdown("scale_factor", "10", ["10", "100", "1000", "5000", "10000", "20000"])
dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("wh_db", "tpcdi_incremental_staging")
dbutils.widgets.text("tpcdi_directory", "/Volumes/main/tpcdi_raw_data/tpcdi_volume/")
dbutils.widgets.dropdown("log_level", "INFO", ["DEBUG", "INFO", "WARN", "ERROR"])
dbutils.widgets.dropdown("augmented_incremental", "true", ["true", "false"])

scale_factor          = dbutils.widgets.get("scale_factor").strip()
catalog               = dbutils.widgets.get("catalog").strip()
wh_db                 = dbutils.widgets.get("wh_db").strip()
tpcdi_directory       = dbutils.widgets.get("tpcdi_directory").strip()
log_level             = dbutils.widgets.get("log_level").strip()
augmented_incremental = dbutils.widgets.get("augmented_incremental").strip().lower() == "true"

# COMMAND ----------

_nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
workspace_src_path = f"/Workspace{_nb_path.split('/src')[0]}/src"
if f"{workspace_src_path}/tools" not in sys.path:
    sys.path.insert(0, f"{workspace_src_path}/tools")

from data_gen_tasks._shared import bootstrap

ctx = bootstrap(spark=spark, dbutils=dbutils, scale_factor=scale_factor,
                catalog=catalog, wh_db=wh_db, tpcdi_directory=tpcdi_directory,
                log_level=log_level, augmented_incremental=augmented_incremental,
                workspace_src_path=workspace_src_path, load_dicts=False)
cfg = ctx["cfg"]

# COMMAND ----------

from data_gen_tasks._copy_helper import copy_dataset

n = copy_dataset(cfg=ctx["cfg"], dbutils=dbutils, filenames=["HR.csv"], num_batches=1)
print(f"[copy_hr] {n} files renamed")
