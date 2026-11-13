# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# One-time EMR Delta staging per SF (the same step setup_emr runs first).
#
# DEEP CLONEs the 20 tables of {catalog}.tpcdi_incremental_staging_{sf} to
# {s3}_emr/staging_delta/sf={sf}/{table}, with the table features EMR's
# delta-spark 3.x can't handle removed (see _emr_staging.py). Each EMR run's
# setup then SHALLOW CLONEs from there. Tables already there are skipped
# unless force_materialize=true.
#
# Don't force a re-clone while an EMR run is using that SF: its shallow
# clones read the staging's files.
#
# scale_factor takes one SF or a comma list (done one after another).
# s3_volume_prefix blank = the volume's storage location (DESCRIBE VOLUME).
# Compute: serverless or any UC cluster that can read the staging schema and
# write the volume.

import os
import sys

_nb = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
_here = os.path.dirname(_nb if _nb.startswith("/Workspace") else "/Workspace" + _nb)
for _p in (_here, os.path.join(_here, "jobs")):
    if _p not in sys.path:
        sys.path.insert(0, _p)
import _emr_conn as emr_c
import _emr_staging
import tpcdi_sql

# COMMAND ----------

dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("scale_factor", "10", "SF, or a comma list e.g. 10,100,1000,20000")
dbutils.widgets.text("tpcdi_directory", "/Volumes/main/tpcdi_raw_data/tpcdi_volume/")
dbutils.widgets.text("s3_volume_prefix", "", "s3:// uri behind tpcdi_directory (blank = look it up)")
dbutils.widgets.dropdown("force_materialize", "false", ["true", "false"])

catalog = dbutils.widgets.get("catalog").strip()
sfs = [x.strip() for x in dbutils.widgets.get("scale_factor").split(",") if x.strip()]
vol_root = dbutils.widgets.get("tpcdi_directory").strip()
s3_root = dbutils.widgets.get("s3_volume_prefix").strip()
force = dbutils.widgets.get("force_materialize").strip().lower() == "true"

if not s3_root:
    _c, _s, _v = vol_root.strip("/").split("/")[1:4]
    _row = spark.sql(f"DESCRIBE VOLUME {_c}.{_s}.{_v}").collect()[0].asDict()
    s3_root = _row["storage_location"]
s3_root = s3_root.rstrip("/") + "/"
print(f"[materialize] volume {vol_root} = {s3_root}; SFs {sfs}; force={force}")

# COMMAND ----------

for sf in sfs:
    # wh_db only names the run database; the staging path doesn't use it.
    p = emr_c.paths(s3_root, "materialize", sf)
    _emr_staging.materialize_sf(
        spark, dbutils, catalog=catalog, sf=sf, staging_delta=p["staging_delta"],
        staging_delta_volume=emr_c.to_volume(p["staging_delta"], s3_root, vol_root),
        tables=tpcdi_sql.STAGING_TABLES, force=force)
