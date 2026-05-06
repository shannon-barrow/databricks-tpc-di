# Databricks notebook source
# MAGIC %md
# MAGIC # data_gen task: gen_customer
# MAGIC
# MAGIC Generates CustomerMgmt — in augmented mode this writes
# MAGIC `tpcdi_raw_data.customermgmt{sf}` Delta directly (no XML / Customer.txt /
# MAGIC Account.txt files); in standard mode it writes those files plus the
# MAGIC B2/B3 incrementals. Persists `_customer_dates` as a Delta intermediate
# MAGIC for `gen_watch_history`.
# MAGIC
# MAGIC Depends on `gen_hr` (reads `_gen_brokers` and re-registers it as
# MAGIC `_brokers` so the existing customer.py code that does
# MAGIC `spark.table("_brokers")` keeps working).

# COMMAND ----------

import sys
import threading

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
    read_intermediate_view, is_already_generated,
)

ctx = bootstrap(spark=spark, dbutils=dbutils, scale_factor=scale_factor,
                catalog=catalog, wh_db=wh_db, tpcdi_directory=tpcdi_directory,
                log_level=log_level, augmented_incremental=augmented_incremental,
                workspace_src_path=workspace_src_path, load_dicts=True)
cfg = ctx["cfg"]

# COMMAND ----------

# In augmented mode the deliverable is the customermgmt Delta. In standard
# mode it's the CustomerMgmt.xml + Customer.txt files; self-skip on the
# Delta presence is augmented-specific so we only check it in that mode.
output_fq = f"{catalog}.tpcdi_raw_data.customermgmt{scale_factor}"
if (augmented_incremental and regenerate_data != "YES"
        and is_already_generated(spark, output_fq)):
    print(f"[gen_customer] {output_fq} already populated — skipping")
    dbutils.notebook.exit("skipped")

# COMMAND ----------

# Re-register the upstream `_brokers` view so customer.generate's existing
# `spark.table("_brokers")` reads keep working unchanged.
read_intermediate_view(spark, catalog=catalog, wh_db=wh_db,
                       scale_factor=scale_factor,
                       name="_gen_brokers", view_name="_brokers")
print(f"[gen_customer] re-registered _brokers from staging schema")

from tpcdi_gen import customer
import tpcdi_gen.utils as _u
# Standard-mode write_file calls inside customer.generate get deferred
# to copy_customer / copy_account. No-op in augmented mode (writes go
# through write_delta, not write_file).
_u._DEFER_COPIES["enabled"] = True
try:
    result = customer.generate(spark, cfg, ctx["dicts"], dbutils,
                               views_ready_event=threading.Event())
finally:
    _u._DEFER_COPIES["enabled"] = False

# Persist `_customer_dates` for gen_watch_history. customer.py creates this
# view at line 831 with columns (cust_id, cust_create_ts, cust_create_update,
# cust_inact_ts).
try:
    write_intermediate_delta(spark.table("_customer_dates"),
                             catalog=catalog, wh_db=wh_db,
                             scale_factor=scale_factor,
                             name="_gen_customer_dates")
    print(f"[gen_customer] persisted _customer_dates → "
          f"{intermediate_table_fq(catalog, wh_db, scale_factor, '_gen_customer_dates')}")
except Exception as e:
    # _customer_dates is only used by watch_history's dynamic-audit path;
    # in augmented mode where static_audits_available=True it's not needed.
    print(f"[gen_customer] _customer_dates not persisted ({type(e).__name__}); "
          f"OK if gen_watch_history doesn't need it")

# COMMAND ----------

import json as _json
counts = result.get("counts", {})
dbutils.jobs.taskValues.set(key="record_counts",
                            value=_json.dumps({f"{k[0]}::{k[1]}": v for k, v in counts.items()}))
print(f"[gen_customer] complete — {len(counts)} record_counts entries set")
