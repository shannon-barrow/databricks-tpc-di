# Databricks notebook source
# MAGIC %md
# MAGIC # data_gen task: copy_finwire
# MAGIC
# MAGIC Copies FINWIRE_*.txt staging → final. Depends on `gen_finwire` and
# MAGIC runs in parallel with gen_daily_market / gen_trade / gen_watch_history.
# MAGIC
# MAGIC FINWIRE writes 3 separate Spark staging dirs (CMP/SEC/FIN subsets).
# MAGIC Output files are numbered FINWIRE_1.txt, FINWIRE_2.txt, … with a
# MAGIC counter shared across the 3 subsets — preserves the same numbering
# MAGIC the original single-task `finwire.generate` produced (CMP files
# MAGIC first, then SEC, then FIN).
# MAGIC
# MAGIC Synchronous: waits for `wait_for_background_copies()` before
# MAGIC exiting so large-part daemon threads complete cleanly.

# COMMAND ----------

import sys

dbutils.widgets.dropdown("scale_factor", "10", ["10", "100", "1000", "5000", "10000", "20000"])
dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("wh_db", "tpcdi_incremental_staging")
dbutils.widgets.text("tpcdi_directory", "/Volumes/main/tpcdi_raw_data/tpcdi_volume/")
dbutils.widgets.dropdown("log_level", "INFO", ["DEBUG", "INFO", "WARN", "ERROR"])
dbutils.widgets.dropdown("augmented_incremental", "true", ["true", "false"])

scale_factor          = dbutils.widgets.get("scale_factor").strip()
catalog               = dbutils.widgets.get("catalog").strip()
wh_db                 = dbutils.widgets.get("wh_db").strip()
tpcdi_directory       = dbutils.widgets.get("tpcdi_directory").strip()
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
                workspace_src_path=workspace_src_path, load_dicts=False)
cfg = ctx["cfg"]

# COMMAND ----------

from tpcdi_gen.utils import (
    register_copies_from_staging, wait_for_background_copies, _cleanup,
)

# Mirror the CMP/SEC/FIN counter-sharing flow from finwire.generate.
next_idx = 1
final_path = f"{cfg.batch_path(1)}/FINWIRE.txt"
staging_dirs = []
for subset in ("cmp", "sec", "fin"):
    staging = f"{cfg.batch_path(1)}/FINWIRE_{subset}.txt__staging"
    staging_dirs.append(staging)
    print(f"[copy_finwire] {staging} → FINWIRE_{next_idx}+.txt")
    _, next_idx = register_copies_from_staging(
        staging, final_path, dbutils, start_idx=next_idx)

wait_for_background_copies()
for s in staging_dirs:
    _cleanup(s, dbutils)
print(f"[copy_finwire] done — wrote FINWIRE_1..{next_idx - 1}.txt")
