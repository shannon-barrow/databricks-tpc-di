# Databricks notebook source
# MAGIC %md
# MAGIC # data_gen task: init_intermediates
# MAGIC
# MAGIC One-time setup before the per-dataset gen tasks run:
# MAGIC
# MAGIC 1. Creates the intermediate `_stage` schema
# MAGIC    (`{catalog}.{wh_db}_{sf}_stage`) where cross-task `_gen_*` Delta
# MAGIC    tables live (same convention as `dw_init.sql` benchmark interim
# MAGIC    tables).
# MAGIC 2. Creates the `tpcdi_raw_data` schema + `tpcdi_volume`.
# MAGIC 3. Ensures Batch1/2/3 output directories exist on the volume so
# MAGIC    parallel writers don't race on dir creation.
# MAGIC 4. When `regenerate_data=YES`: wipes the volume's per-SF tree, drops
# MAGIC    every `_gen_*` temp from any prior run, and drops the 7 dataset
# MAGIC    tables from `tpcdi_raw_data.{dataset}{sf}`. Per-task self-skip
# MAGIC    relies on those outputs being absent for `regenerate_data=YES` to
# MAGIC    actually rebuild.

# COMMAND ----------

import sys

dbutils.widgets.dropdown("scale_factor", "10", ["10", "100", "1000", "5000", "10000", "20000"])
dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("wh_db", "tpcdi_incremental_staging")
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

from data_gen_tasks._shared import bootstrap, stage_schema_fq

ctx = bootstrap(spark=spark, dbutils=dbutils, scale_factor=scale_factor,
                catalog=catalog, wh_db=wh_db, tpcdi_directory=tpcdi_directory,
                log_level=log_level, augmented_incremental=augmented_incremental,
                workspace_src_path=workspace_src_path, load_dicts=False)
cfg = ctx["cfg"]

# COMMAND ----------

# Schemas first.
spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog}.tpcdi_raw_data")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.tpcdi_raw_data.tpcdi_volume")

stage_schema = stage_schema_fq(catalog, wh_db, scale_factor)
print(f"[init_intermediates] ensuring {stage_schema} exists")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {stage_schema} "
          f"COMMENT 'data_gen + benchmark interim temp tables'")

if augmented_incremental:
    # The augmented benchmark also reads from these schemas. spark_runner
    # used to create them inline — keep parity.
    spark.sql(f"CREATE DATABASE IF NOT EXISTS "
              f"{catalog}.tpcdi_incremental_staging_{scale_factor} "
              f"COMMENT 'Shared TPC-DI augmented_incremental staging schema'")

# COMMAND ----------

if regenerate_data == "YES":
    # Wipe the volume's per-SF tree, drop intermediates and dataset Deltas.
    print(f"[init_intermediates] regenerate_data=YES → wiping {cfg.volume_path}")
    try:
        dbutils.fs.rm(cfg.volume_path, recurse=True)
    except Exception as e:
        print(f"  rm {cfg.volume_path} skipped: {type(e).__name__}: {e}")

    # Cross-task and disk_cache temps.
    for _t in ("_gen_brokers", "_gen_symbols", "_gen_customer_dates",
               "_gen_finwire_symbols", "_gen_customermgmt_schedules",
               "_gen_customermgmt_actions", "_gen_trade_df"):
        spark.sql(f"DROP TABLE IF EXISTS {stage_schema}.{_t}")
    print(f"[init_intermediates] dropped any prior _gen_* temps in {stage_schema}")

    # Per-dataset Delta deliverables (augmented mode writes these).
    if augmented_incremental:
        for _t in ("customermgmt", "trade", "tradehistory", "cashtransaction",
                   "holdinghistory", "watchhistory", "dailymarket"):
            spark.sql(f"DROP TABLE IF EXISTS "
                      f"{catalog}.tpcdi_raw_data.{_t}{scale_factor}")
        print(f"[init_intermediates] dropped 7 augmented dataset Deltas in "
              f"{catalog}.tpcdi_raw_data")
else:
    print(f"[init_intermediates] regenerate_data=NO → keeping prior state for "
          f"per-task self-skip")

# COMMAND ----------

# Always (re)create the Batch1/2/3 dirs so parallel writers don't race on
# dir creation. Idempotent.
from tpcdi_gen.utils import make_output_dirs
from tpcdi_gen.config import NUM_INCREMENTAL_BATCHES
make_output_dirs(cfg.volume_path, NUM_INCREMENTAL_BATCHES + 1, dbutils)
print(f"[init_intermediates] Batch1/2/3 dirs ready under {cfg.volume_path}")
