# Databricks notebook source
# MAGIC %md
# MAGIC # data_gen task: cleanup_intermediates
# MAGIC
# MAGIC Drops the `_gen_*` temp tables in `{catalog}.{wh_db}_{sf}_stage` once
# MAGIC every gen task has succeeded. Wired with `run_if=ALL_SUCCESS` in the
# MAGIC workflow so a partial-failure run leaves the intermediates in place
# MAGIC for repair-runs to read.
# MAGIC
# MAGIC The `_stage` schema itself is left intact — the benchmark workflow
# MAGIC may still create its own interim tables there later.

# COMMAND ----------

import sys

dbutils.widgets.dropdown("scale_factor", "10", ["10", "100", "1000", "5000", "10000", "20000"])
dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("wh_db", "tpcdi_incremental_staging")

scale_factor = dbutils.widgets.get("scale_factor").strip()
catalog      = dbutils.widgets.get("catalog").strip()
wh_db        = dbutils.widgets.get("wh_db").strip()

_nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
workspace_src_path = f"/Workspace{_nb_path.split('/src')[0]}/src"
if f"{workspace_src_path}/tools" not in sys.path:
    sys.path.insert(0, f"{workspace_src_path}/tools")

from data_gen_tasks._shared import stage_schema_fq

stage_schema = stage_schema_fq(catalog, wh_db, scale_factor)

# COMMAND ----------

# All `_gen_*` tables the data_gen tasks may have produced. DROP IF EXISTS
# is a no-op when a particular task didn't write its intermediate (e.g.
# `_gen_customer_dates` is only used in the dynamic-audit path).
for _t in ("_gen_brokers", "_gen_symbols", "_gen_customer_dates",
           "_gen_finwire_symbols", "_gen_customermgmt_schedules",
           "_gen_customermgmt_actions", "_gen_trade_df"):
    spark.sql(f"DROP TABLE IF EXISTS {stage_schema}.{_t}")

print(f"[cleanup_intermediates] dropped data_gen temp tables in {stage_schema}")
