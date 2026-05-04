# Databricks notebook source
# MAGIC %md
# MAGIC # data_gen task: gen_trade_base (V6)
# MAGIC
# MAGIC Materializes ``_gen_trade_df`` Delta in
# MAGIC ``{catalog}.{wh_db}_{sf}_stage`` with the *minimum* cross-task
# MAGIC column set used by the 4 trade-family leaf tasks
# MAGIC (gen_trade / gen_tradehistory / gen_cashtransaction /
# MAGIC gen_holdinghistory). Each leaf re-derives single-consumer
# MAGIC columns from t_id locally — saves ~300 GB at SF=20k vs staging
# MAGIC ``_ct_name_raw`` and friends.
# MAGIC
# MAGIC Depends on ``gen_finwire`` (``_gen_symbols``). Reads ``_brokers``
# MAGIC only when ``static_audits_available`` returns False (dynamic-
# MAGIC audit path); augmented mode unconditionally returns True so the
# MAGIC Delta hand-off doesn't depend on gen_hr.

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
    bootstrap, intermediate_table_fq, is_already_generated,
    read_intermediate_view,
)

ctx = bootstrap(spark=spark, dbutils=dbutils, scale_factor=scale_factor,
                catalog=catalog, wh_db=wh_db, tpcdi_directory=tpcdi_directory,
                log_level=log_level, augmented_incremental=augmented_incremental,
                workspace_src_path=workspace_src_path, load_dicts=False)
cfg = ctx["cfg"]

# COMMAND ----------

# Self-skip when the staged Delta is intact and regenerate_data=NO.
_trade_base_fq = intermediate_table_fq(catalog, wh_db, scale_factor, "_gen_trade_df")
if regenerate_data != "YES" and is_already_generated(spark, _trade_base_fq):
    print(f"[gen_trade_base] {_trade_base_fq} already populated — skipping")
    dbutils.notebook.exit("skipped")

# COMMAND ----------

read_intermediate_view(spark, catalog=catalog, wh_db=wh_db,
                       scale_factor=scale_factor,
                       name="_gen_symbols", view_name="_symbols")
print(f"[gen_trade_base] re-registered _symbols")

try:
    read_intermediate_view(spark, catalog=catalog, wh_db=wh_db,
                           scale_factor=scale_factor,
                           name="_gen_brokers", view_name="_brokers")
    print(f"[gen_trade_base] re-registered _brokers (dynamic-audit path)")
except Exception:
    print(f"[gen_trade_base] _brokers not available — using analytical broker count")

# COMMAND ----------

from tpcdi_gen import trade_split

result = trade_split.materialize_base(spark, cfg, dbutils)
print(f"[gen_trade_base] complete — {result['fq']} (n_brokers={result['n_brokers']:,})")
