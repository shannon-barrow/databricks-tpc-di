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
# Default "true" = the SAFE direction: the whole-schema DROP below only fires
# when a caller EXPLICITLY declares augmented_incremental=false (the standard
# datagen path, which uses a per-user throwaway schema). Any run that forgets
# to pass this flag is treated as augmented and the schema is preserved.
dbutils.widgets.dropdown("augmented_incremental", "true", ["true", "false"])

scale_factor = dbutils.widgets.get("scale_factor").strip()
catalog      = dbutils.widgets.get("catalog").strip()
wh_db        = dbutils.widgets.get("wh_db").strip()
augmented_incremental = dbutils.widgets.get("augmented_incremental").strip().lower() == "true"

# The augmented path's staging schema is deliberately SHARED (granted to
# `account users`) and consumed by the benchmark — it must NEVER be dropped
# here. Belt-and-suspenders guard: even if augmented_incremental were passed
# wrong, we refuse to drop any schema derived from this reserved wh_db.
_SHARED_STAGING_WH_DB = "tpcdi_incremental_staging"

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

# Standard-path only: the whole `_stage` schema is a per-user throwaway
# (`{catalog}.{user}_datagen_{sf}_stage`) that exists solely to hand data
# between the datagen tasks. Nothing downstream reads it (standard output is
# volume Batch files), so drop it entirely to avoid leaving per-user schema
# shells behind. Two guards keep the SHARED augmented staging schema safe:
#   1. augmented_incremental must be explicitly false, AND
#   2. wh_db must not be the reserved shared name.
if not augmented_incremental and wh_db != _SHARED_STAGING_WH_DB:
    spark.sql(f"DROP SCHEMA IF EXISTS {stage_schema} CASCADE")
    print(f"[cleanup_intermediates] dropped throwaway schema {stage_schema}")
else:
    print(f"[cleanup_intermediates] left schema {stage_schema} intact "
          f"(augmented_incremental={augmented_incremental}, wh_db={wh_db})")
