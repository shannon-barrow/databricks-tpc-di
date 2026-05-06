# Databricks notebook source
# MAGIC %md
# MAGIC # data_gen task: gen_holdinghistory (V6 leaf)
# MAGIC
# MAGIC Reads ``_gen_trade_df`` Delta (produced by ``gen_trade_base``) and
# MAGIC writes ``HoldingHistory.txt`` for B1 (+ B2/B3 incrementals in
# MAGIC standard mode, which include the new-market-CMPT HH rows for the
# MAGIC same-day-completing TMB/TMS new trades). In augmented mode writes
# MAGIC a Delta table at ``tpcdi_raw_data.holdinghistory{sf}`` instead.

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

from data_gen_tasks._shared import (
    bootstrap, read_intermediate_view, is_already_generated,
)

ctx = bootstrap(spark=spark, dbutils=dbutils, scale_factor=scale_factor,
                catalog=catalog, wh_db=wh_db, tpcdi_directory=tpcdi_directory,
                log_level=log_level, augmented_incremental=augmented_incremental,
                workspace_src_path=workspace_src_path, load_dicts=True)
cfg = ctx["cfg"]

# COMMAND ----------

if augmented_incremental and regenerate_data != "YES":
    fq = f"{catalog}.tpcdi_raw_data.holdinghistory{scale_factor}"
    if is_already_generated(spark, fq):
        print(f"[gen_holdinghistory] {fq} already populated — skipping")
        dbutils.notebook.exit("skipped")

# COMMAND ----------

# In standard mode the HH leaf reads `_symbols` for n_brokers resolution
# (only used in dynamic-audit path).
read_intermediate_view(spark, catalog=catalog, wh_db=wh_db,
                       scale_factor=scale_factor,
                       name="_gen_symbols", view_name="_symbols")
try:
    read_intermediate_view(spark, catalog=catalog, wh_db=wh_db,
                           scale_factor=scale_factor,
                           name="_gen_brokers", view_name="_brokers")
except Exception:
    pass

from tpcdi_gen import trade_split
import tpcdi_gen.utils as _u
_u._DEFER_COPIES["enabled"] = True
try:
    base_df = trade_split.read_base(spark, cfg)
    counts = trade_split.gen_holdinghistory(spark, cfg, dbutils, ctx["dicts"], base_df)
finally:
    _u._DEFER_COPIES["enabled"] = False

import json as _json
dbutils.jobs.taskValues.set(key="record_counts",
                            value=_json.dumps({f"{k[0]}::{k[1]}": v for k, v in counts.items()}))
print(f"[gen_holdinghistory] complete — {len(counts)} record_counts entries set")
