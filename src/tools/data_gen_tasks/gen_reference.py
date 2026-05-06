# Databricks notebook source
# MAGIC %md
# MAGIC # data_gen task: gen_reference
# MAGIC
# MAGIC Generates the static reference/dimension tables that don't vary with SF
# MAGIC (StatusType, TaxRate, Date, Time, Industry, TradeType) plus the per-batch
# MAGIC BatchDate.txt. Has no upstream dependencies — runs first in the DAG
# MAGIC alongside `gen_hr` and `gen_finwire`.
# MAGIC
# MAGIC In augmented_incremental mode: emits files at `{volume}/Batch1/` only.
# MAGIC No Delta intermediates are needed for reference tables.

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

from data_gen_tasks._shared import bootstrap

ctx = bootstrap(spark=spark, dbutils=dbutils, scale_factor=scale_factor,
                catalog=catalog, wh_db=wh_db, tpcdi_directory=tpcdi_directory,
                log_level=log_level, augmented_incremental=augmented_incremental,
                workspace_src_path=workspace_src_path, load_dicts=True)
cfg = ctx["cfg"]

# COMMAND ----------

# Reference tables go directly to the volume — they're cheap to regenerate
# (a few thousand rows total) and there's no per-task self-skip benefit.
# We always regenerate them; this also guarantees `BatchDate.txt` reflects
# the current run's batch dates.
from tpcdi_gen.utils import make_output_dirs
from tpcdi_gen.config import NUM_INCREMENTAL_BATCHES
from tpcdi_gen import reference_tables

make_output_dirs(cfg.volume_path, NUM_INCREMENTAL_BATCHES + 1, dbutils)
counts = reference_tables.generate_all(spark, cfg, ctx["dicts"], dbutils)

# Surface counts as a task value so audit_emit (standard mode) can read them.
import json as _json
dbutils.jobs.taskValues.set(key="record_counts",
                            value=_json.dumps({f"{k[0]}::{k[1]}": v for k, v in counts.items()}))
print(f"[gen_reference] complete — {len(counts)} record_counts entries set")
