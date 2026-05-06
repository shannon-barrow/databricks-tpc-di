# Databricks notebook source
# MAGIC %md
# MAGIC # data_gen (unified entry point)
# MAGIC
# MAGIC Single dispatch task for every data-generation workflow:
# MAGIC
# MAGIC - **`data_gen_type=spark` / `augmented_incremental`** — initializes
# MAGIC   the cross-task `_stage` schema, ensures `tpcdi_raw_data` + volume
# MAGIC   exist, and (when `regenerate_data=YES`) wipes prior outputs. The
# MAGIC   downstream per-dataset gen tasks do the actual generation. Each
# MAGIC   gen task self-skips when its output Delta is intact and
# MAGIC   `regenerate_data=NO`, so a re-trigger only redoes missing datasets.
# MAGIC - **`data_gen_type=native`** — runs the legacy DIGen.jar generator
# MAGIC   inline in this task's process via ``digen_runner.run()``. The
# MAGIC   DIGen workflow has no downstream tasks; this single task is the
# MAGIC   whole job. Forced non-serverless DBR 15.4 + Photon (Java
# MAGIC   subprocess can't run on serverless).
# MAGIC
# MAGIC The task is invoked with the same set of widgets in all three modes
# MAGIC so a single notebook serves every datagen workflow. Mode-specific
# MAGIC behavior branches on ``data_gen_type``.

# COMMAND ----------

import sys

dbutils.widgets.dropdown("data_gen_type", "spark",
                         ["spark", "native", "augmented_incremental"])
dbutils.widgets.dropdown("scale_factor", "10", ["10", "100", "1000", "5000", "10000", "20000"])
dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("wh_db", "tpcdi_incremental_staging")
dbutils.widgets.text("tpcdi_directory", "/Volumes/main/tpcdi_raw_data/tpcdi_volume/")
dbutils.widgets.dropdown("regenerate_data", "NO", ["NO", "YES"])
dbutils.widgets.dropdown("log_level", "INFO", ["DEBUG", "INFO", "WARN", "ERROR"])

data_gen_type = dbutils.widgets.get("data_gen_type").strip().lower()
scale_factor  = dbutils.widgets.get("scale_factor").strip()
catalog       = dbutils.widgets.get("catalog").strip()
wh_db         = dbutils.widgets.get("wh_db").strip()
tpcdi_directory = dbutils.widgets.get("tpcdi_directory").strip()
regenerate_data = dbutils.widgets.get("regenerate_data").strip().upper()
log_level     = dbutils.widgets.get("log_level").strip().upper()

if data_gen_type not in ("spark", "native", "augmented_incremental"):
    raise ValueError(
        f"data_gen_type must be 'spark', 'native', or 'augmented_incremental' "
        f"(got {data_gen_type!r})"
    )
augmented_incremental = data_gen_type == "augmented_incremental"

_nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
workspace_src_path = f"/Workspace{_nb_path.split('/src')[0]}/src"
if f"{workspace_src_path}/tools" not in sys.path:
    sys.path.insert(0, f"{workspace_src_path}/tools")

print(f"data_gen → {data_gen_type!r} (SF={scale_factor}, catalog={catalog})")

# COMMAND ----------

# DIGen branch: single-task workflow. Run the JAR inline and exit.
if data_gen_type == "native":
    from digen_runner import run as runner
    runner(
        scale_factor=int(scale_factor),
        catalog=catalog,
        tpcdi_directory=tpcdi_directory,
        regenerate_data=(regenerate_data == "YES"),
        log_level=log_level,
        workspace_src_path=workspace_src_path,
        dbutils=dbutils,
        spark=spark,
    )
    dbutils.notebook.exit("digen_complete")

# COMMAND ----------

# Spark / augmented_incremental branch: init the cross-task intermediates.
# Downstream gen_* / copy_* / audit_emit / cleanup_intermediates do the rest.
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
print(f"[data_gen] ensuring {stage_schema} exists")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {stage_schema} "
          f"COMMENT 'data_gen + benchmark interim temp tables'")

if augmented_incremental:
    # The augmented benchmark also reads from these schemas.
    spark.sql(f"CREATE DATABASE IF NOT EXISTS "
              f"{catalog}.tpcdi_incremental_staging_{scale_factor} "
              f"COMMENT 'Shared TPC-DI augmented_incremental staging schema'")

# COMMAND ----------

if regenerate_data == "YES":
    # Wipe the volume's per-SF tree, drop intermediates and dataset Deltas.
    print(f"[data_gen] regenerate_data=YES → wiping {cfg.volume_path}")
    try:
        dbutils.fs.rm(cfg.volume_path, recurse=True)
    except Exception as e:
        print(f"  rm {cfg.volume_path} skipped: {type(e).__name__}: {e}")

    # Cross-task `_gen_*` and per-call `_dc_*` temps from any prior run.
    rows = spark.sql(f"SHOW TABLES IN {stage_schema}").collect()
    dropped = []
    for r in rows:
        name = r["tableName"]
        if name.startswith("_gen_") or name.startswith("_dc_"):
            spark.sql(f"DROP TABLE IF EXISTS {stage_schema}.{name}")
            dropped.append(name)
    print(f"[data_gen] dropped {len(dropped)} prior data_gen temps "
          f"in {stage_schema}: {dropped}")

    # Per-dataset Delta deliverables (augmented mode writes these).
    if augmented_incremental:
        for _t in ("customermgmt", "trade", "tradehistory", "cashtransaction",
                   "holdinghistory", "watchhistory", "dailymarket"):
            spark.sql(f"DROP TABLE IF EXISTS "
                      f"{catalog}.tpcdi_raw_data.{_t}{scale_factor}")
        print(f"[data_gen] dropped 7 augmented dataset Deltas in "
              f"{catalog}.tpcdi_raw_data")
else:
    print(f"[data_gen] regenerate_data=NO → keeping prior state for "
          f"per-task self-skip")

# COMMAND ----------

# Always (re)create the Batch1/2/3 dirs so parallel writers don't race on
# dir creation. Idempotent.
from tpcdi_gen.utils import make_output_dirs
from tpcdi_gen.config import NUM_INCREMENTAL_BATCHES
make_output_dirs(cfg.volume_path, NUM_INCREMENTAL_BATCHES + 1, dbutils)
print(f"[data_gen] Batch1/2/3 dirs ready under {cfg.volume_path}")
