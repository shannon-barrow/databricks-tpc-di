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

# Drop everything in {wh_db}_{sf}_stage that this data_gen run created:
# - `_gen_*` cross-task intermediates (gen_hr → _gen_brokers, etc.)
# - `_dc_*` per-call disk_cache temps (FINWIRE symbols, CustomerMgmt
#   schedules / actions, trade_df, …) — the prefix is set in
#   tpcdi_gen.utils.INTERMEDIATE_DC_PREFIX and is unique to data_gen.
# Other tables in this schema (e.g. dw_init's CustomerMgmt /
# WatchIncremental / etc.) belong to the benchmark phase — leave alone.
rows = spark.sql(f"SHOW TABLES IN {stage_schema}").collect()
dropped = []
for r in rows:
    name = r["tableName"]
    if name.startswith("_gen_") or name.startswith("_dc_"):
        spark.sql(f"DROP TABLE IF EXISTS {stage_schema}.{name}")
        dropped.append(name)

print(f"[cleanup_intermediates] dropped {len(dropped)} data_gen temp tables in "
      f"{stage_schema}: {dropped}")
