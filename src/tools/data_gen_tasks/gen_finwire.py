# Databricks notebook source
# MAGIC %md
# MAGIC # data_gen task: gen_finwire
# MAGIC
# MAGIC Generates FINWIRE quarterly fixed-width files (CMP/SEC/FIN) at
# MAGIC `{volume}/Batch1/FINWIRE_*.txt` and persists the `_symbols` view as a
# MAGIC `_gen_symbols` Delta in `{wh_db}_{sf}_stage` so downstream gen tasks
# MAGIC (`gen_daily_market`, `gen_watch_history`, `gen_trade`) can read it.
# MAGIC
# MAGIC No upstream dependencies. In the single-task model FINWIRE used a
# MAGIC `symbols_ready_event` to unblock downstream generators mid-run; in
# MAGIC the per-task model that overlap is gone (downstream tasks start
# MAGIC strictly after this one finishes).

# COMMAND ----------

import sys
import threading

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
    bootstrap, intermediate_table_fq, write_intermediate_delta,
    is_already_generated,
)

ctx = bootstrap(spark=spark, dbutils=dbutils, scale_factor=scale_factor,
                catalog=catalog, wh_db=wh_db, tpcdi_directory=tpcdi_directory,
                log_level=log_level, augmented_incremental=augmented_incremental,
                workspace_src_path=workspace_src_path, load_dicts=True)
cfg = ctx["cfg"]

# COMMAND ----------

symbols_fq = intermediate_table_fq(catalog, wh_db, scale_factor, "_gen_symbols")
if regenerate_data != "YES" and is_already_generated(spark, symbols_fq):
    print(f"[gen_finwire] {symbols_fq} already populated — skipping")
    dbutils.notebook.exit("skipped")

# COMMAND ----------

from tpcdi_gen import finwire
# Pass a dummy event since the in-task signal-and-continue overlap doesn't
# apply in the per-task workflow model.
result = finwire.generate(spark, cfg, ctx["dicts"], dbutils,
                          symbols_ready_event=threading.Event())

# Persist `_symbols` for downstream tasks. Schema:
#   Symbol STRING, creation_quarter INT, deactivation_quarter INT, _idx LONG
write_intermediate_delta(spark.table("_symbols"),
                         catalog=catalog, wh_db=wh_db,
                         scale_factor=scale_factor, name="_gen_symbols")
print(f"[gen_finwire] persisted _symbols → {symbols_fq}")

import json as _json
counts = result.get("counts", {})
dbutils.jobs.taskValues.set(key="record_counts",
                            value=_json.dumps({f"{k[0]}::{k[1]}": v for k, v in counts.items()}))
print(f"[gen_finwire] complete — {len(counts)} record_counts entries set")
