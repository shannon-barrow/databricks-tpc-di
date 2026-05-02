# Databricks notebook source
# MAGIC %md
# MAGIC # data_gen task: gen_trade
# MAGIC
# MAGIC Generates Trade + TradeHistory + CashTransaction + HoldingHistory.
# MAGIC In augmented mode writes 4 Delta tables under
# MAGIC `tpcdi_raw_data.{trade,tradehistory,cashtransaction,holdinghistory}{sf}`;
# MAGIC in standard mode writes the corresponding .txt files.
# MAGIC
# MAGIC Depends on `gen_finwire` (`_gen_symbols`). In augmented mode does NOT
# MAGIC need `_brokers` (cfg.n_brokers_estimate is analytical when
# MAGIC `static_audits_available=True`, which is unconditionally true in
# MAGIC augmented mode).

# COMMAND ----------

import sys

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
    bootstrap, read_intermediate_view, is_already_generated,
)

ctx = bootstrap(spark=spark, dbutils=dbutils, scale_factor=scale_factor,
                catalog=catalog, wh_db=wh_db, tpcdi_directory=tpcdi_directory,
                log_level=log_level, augmented_incremental=augmented_incremental,
                workspace_src_path=workspace_src_path, load_dicts=True)
cfg = ctx["cfg"]

# COMMAND ----------

# Self-skip only when ALL 4 Trade-family Delta outputs are intact — partial
# failure of any one warrants a full Trade rerun (they share the in-memory
# trade_df and timestamp logic).
if augmented_incremental and regenerate_data != "YES":
    fqs = [
        f"{catalog}.tpcdi_raw_data.trade{scale_factor}",
        f"{catalog}.tpcdi_raw_data.tradehistory{scale_factor}",
        f"{catalog}.tpcdi_raw_data.cashtransaction{scale_factor}",
        f"{catalog}.tpcdi_raw_data.holdinghistory{scale_factor}",
    ]
    if all(is_already_generated(spark, fq) for fq in fqs):
        print(f"[gen_trade] all 4 Trade outputs already populated — skipping")
        dbutils.notebook.exit("skipped")

# COMMAND ----------

read_intermediate_view(spark, catalog=catalog, wh_db=wh_db,
                       scale_factor=scale_factor,
                       name="_gen_symbols", view_name="_symbols")
print(f"[gen_trade] re-registered _symbols from staging schema")

# In standard mode without static audits, trade.generate also reads
# `_brokers` for an exact count. Re-register that too if it exists in
# the staging schema (gen_hr persists it unconditionally).
try:
    read_intermediate_view(spark, catalog=catalog, wh_db=wh_db,
                           scale_factor=scale_factor,
                           name="_gen_brokers", view_name="_brokers")
    print(f"[gen_trade] re-registered _brokers (only used in dynamic-audit path)")
except Exception:
    print(f"[gen_trade] _brokers not available — using analytical broker count")

from tpcdi_gen import trade
result = trade.generate(spark, cfg, ctx["dicts"], dbutils)

import json as _json
counts = result if isinstance(result, dict) else {}
dbutils.jobs.taskValues.set(key="record_counts",
                            value=_json.dumps({f"{k[0]}::{k[1]}": v for k, v in counts.items()}))
print(f"[gen_trade] complete — {len(counts)} record_counts entries set")
