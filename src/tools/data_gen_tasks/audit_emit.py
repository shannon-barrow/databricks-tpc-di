# Databricks notebook source
# MAGIC %md
# MAGIC # data_gen task: audit_emit (standard mode)
# MAGIC
# MAGIC Aggregates `record_counts` task values from each gen_* task and
# MAGIC calls `audit.generate(...)` to produce the per-batch `*_audit.csv`
# MAGIC files plus `Generator_audit.csv`.
# MAGIC
# MAGIC Augmented mode short-circuits — `audit.generate` is a no-op when
# MAGIC `cfg.augmented_incremental=True` (and `static_audits_available()`
# MAGIC unconditionally returns True there). We still run this task because
# MAGIC it's harmless and avoids two diverging code paths.

# COMMAND ----------

import sys

dbutils.widgets.dropdown("scale_factor", "10", ["10", "100", "1000", "5000", "10000", "20000"])
dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("wh_db", "tpcdi_incremental_staging")
dbutils.widgets.text("tpcdi_directory", "/Volumes/main/tpcdi_raw_data/tpcdi_volume/")
dbutils.widgets.dropdown("log_level", "INFO", ["DEBUG", "INFO", "WARN", "ERROR"])
dbutils.widgets.dropdown("augmented_incremental", "false", ["true", "false"])

scale_factor          = dbutils.widgets.get("scale_factor").strip()
catalog               = dbutils.widgets.get("catalog").strip()
wh_db                 = dbutils.widgets.get("wh_db").strip()
tpcdi_directory       = dbutils.widgets.get("tpcdi_directory").strip()
log_level             = dbutils.widgets.get("log_level").strip()
augmented_incremental = dbutils.widgets.get("augmented_incremental").strip().lower() == "true"

_nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
workspace_src_path = f"/Workspace{_nb_path.split('/src')[0]}/src"
if f"{workspace_src_path}/tools" not in sys.path:
    sys.path.insert(0, f"{workspace_src_path}/tools")

from data_gen_tasks._shared import bootstrap

ctx = bootstrap(spark=spark, dbutils=dbutils, scale_factor=scale_factor,
                catalog=catalog, wh_db=wh_db, tpcdi_directory=tpcdi_directory,
                log_level=log_level, augmented_incremental=augmented_incremental,
                workspace_src_path=workspace_src_path, load_dicts=False)
cfg = ctx["cfg"]

# COMMAND ----------

# Pull each gen_* task's `record_counts` task value and merge.
import json as _json
all_counts = {}
gen_tasks = ["gen_reference", "gen_hr", "gen_finwire", "gen_customer",
             "gen_daily_market",
             "gen_trade", "gen_tradehistory",
             "gen_cashtransaction", "gen_holdinghistory",
             "gen_watch_history", "gen_prospect"]
for tk in gen_tasks:
    try:
        raw = dbutils.jobs.taskValues.get(taskKey=tk, key="record_counts",
                                          default="{}", debugValue="{}")
        for k, v in _json.loads(raw).items():
            # Keys were serialized as "{table}::{batch_id}" — split back.
            tbl, batch = k.rsplit("::", 1)
            all_counts[(tbl, int(batch))] = v
    except Exception as e:
        print(f"[audit_emit] {tk}: no record_counts ({type(e).__name__})")

print(f"[audit_emit] aggregated {len(all_counts)} record_counts entries from "
      f"{len(gen_tasks)} gen tasks")

# COMMAND ----------

from datetime import datetime, timezone
from tpcdi_gen import audit
audit.generate(cfg, all_counts, datetime.now(tz=timezone.utc), dbutils)
print(f"[audit_emit] audit.generate complete")
