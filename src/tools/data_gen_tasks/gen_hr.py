# Databricks notebook source
# MAGIC %md
# MAGIC # data_gen task: gen_hr
# MAGIC
# MAGIC Generates HR.csv at `{volume}/Batch1/HR.csv` and persists the
# MAGIC `_brokers` view as a `_gen_brokers` Delta in `{wh_db}_{sf}_stage` so
# MAGIC `gen_customer` (the only downstream consumer) can read it.
# MAGIC
# MAGIC No upstream dependencies — runs in the first wave of the DAG
# MAGIC alongside `gen_reference` and `gen_finwire`.

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
    bootstrap, intermediate_table_fq, write_intermediate_delta,
    is_already_generated,
)

ctx = bootstrap(spark=spark, dbutils=dbutils, scale_factor=scale_factor,
                catalog=catalog, wh_db=wh_db, tpcdi_directory=tpcdi_directory,
                log_level=log_level, augmented_incremental=augmented_incremental,
                workspace_src_path=workspace_src_path, load_dicts=True)
cfg = ctx["cfg"]

# COMMAND ----------

# Self-skip when the brokers intermediate is already complete and the user
# didn't ask for a fresh regen. Repair-runs after a downstream failure can
# re-trigger the workflow without redoing HR.
brokers_fq = intermediate_table_fq(catalog, wh_db, scale_factor, "_gen_brokers")
if regenerate_data != "YES" and is_already_generated(spark, brokers_fq):
    print(f"[gen_hr] {brokers_fq} already populated — skipping")
    dbutils.notebook.exit("skipped")

# COMMAND ----------

from tpcdi_gen import hr
import tpcdi_gen.utils as _u
# Skip the inline copy of HR.csv staging → final. copy_hr task does it
# in parallel with gen_customer (and any other downstream gen tasks).
_u._DEFER_COPIES["enabled"] = True
try:
    result = hr.generate(spark, cfg, ctx["dicts"], dbutils)
finally:
    _u._DEFER_COPIES["enabled"] = False

# Persist the in-memory `_brokers` temp view (created inside hr.generate at
# tpcdi_gen/hr.py:128) so gen_customer can read it from Delta in its own
# session. Schema: broker_id STRING, _idx LONG.
write_intermediate_delta(spark.table("_brokers"),
                         catalog=catalog, wh_db=wh_db,
                         scale_factor=scale_factor, name="_gen_brokers")
print(f"[gen_hr] persisted _brokers → {brokers_fq}")

# Surface counts for downstream audit_emit (standard mode).
import json as _json
counts = result.get("counts", {})
dbutils.jobs.taskValues.set(key="record_counts",
                            value=_json.dumps({f"{k[0]}::{k[1]}": v for k, v in counts.items()}))
print(f"[gen_hr] complete — {len(counts)} record_counts entries set")
