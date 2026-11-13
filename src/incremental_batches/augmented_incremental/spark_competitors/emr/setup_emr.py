# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# dependencies = [
#   "boto3",
# ]
# ///
# Parent `setup_emr` task (runs ON Databricks; the work runs on EMR).
#
#   1. Delta staging: DEEP CLONE main.tpcdi_incremental_staging_{sf} to
#      {s3}_emr/staging_delta/sf={sf}/{table}, once per SF (tables already
#      there are skipped unless force_materialize=true); see _emr_staging.py
#   2. copy jobs/*.py to {s3}_emr/code/ through the volume
#   3. EMR steps: sync the code onto the primary node, then `setup` (reset the
#      Glue database {wh_db}_{sf}: SHALLOW CLONE of the 20 staging tables +
#      6 empty bronze tables). emr_cluster_mode=job (default) runs them on a
#      job cluster that EMR terminates when they end, and each child batch
#      then starts its own (a daily run's cold start, like a classic
#      Databricks job cluster). emr_cluster_mode=shared creates one cluster
#      (or reuses emr_cluster_id) for setup and every batch.
#   4. task values: batch_date_ls, emr_cluster_id (shared only), emr_created,
#      run_id
#
# AWS identity: the UC service credential named in aws_service_credential, or
# the cluster's instance profile when that is blank. It needs EMR
# (RunJobFlow, AddJobFlowSteps, Describe*/List*, TerminateJobFlows) and
# iam:PassRole on the EMR roles. S3 goes through the UC volume.

import os
import sys
from datetime import datetime, timezone

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
dbutils.widgets.dropdown("scale_factor", "10", ["10", "100", "1000", "5000", "10000", "20000"])
dbutils.widgets.text("wh_db", "")
dbutils.widgets.text("tpcdi_directory", "/Volumes/main/tpcdi_raw_data/tpcdi_volume/")
dbutils.widgets.text("s3_volume_prefix", "", "s3:// uri behind tpcdi_directory")
dbutils.widgets.text("aws_region", "us-west-2")
dbutils.widgets.text("aws_service_credential", "", "UC service credential (blank = instance profile)")
dbutils.widgets.dropdown("emr_cluster_mode", "job", ["job", "shared"],
                         "job: a cluster per setup/batch; shared: one cluster for the run")
dbutils.widgets.text("emr_cluster_id", "", "shared mode: existing cluster to reuse (blank = create one)")
dbutils.widgets.text("emr_subnet_id", "")
dbutils.widgets.text("emr_master_sg", "")
dbutils.widgets.text("emr_slave_sg", "")
dbutils.widgets.text("emr_instance_type", emr_c.DEFAULT_INSTANCE_TYPE)
dbutils.widgets.text("emr_core_count", "2")
dbutils.widgets.text("emr_release_label", emr_c.DEFAULT_RELEASE)
dbutils.widgets.text("emr_service_role", "EMR_DefaultRole")
dbutils.widgets.text("emr_instance_profile", "EMR_EC2_DefaultRole")
dbutils.widgets.text("shuffle_partitions", "1000")
dbutils.widgets.text("incremental_batches_to_run", "365")
dbutils.widgets.dropdown("force_materialize", "false", ["true", "false"],
                         "Re-DEEP-CLONE the Delta staging even if it already exists")

w = {k: dbutils.widgets.get(k).strip() for k in (
    "catalog", "scale_factor", "wh_db", "tpcdi_directory", "s3_volume_prefix", "aws_region",
    "aws_service_credential", "emr_cluster_mode", "emr_cluster_id", "emr_subnet_id", "emr_master_sg", "emr_slave_sg",
    "emr_instance_type", "emr_core_count", "emr_release_label", "emr_service_role",
    "emr_instance_profile", "shuffle_partitions", "incremental_batches_to_run",
    "force_materialize")}
_missing = [k for k in ("wh_db", "s3_volume_prefix") if not w[k]]
if w["emr_cluster_mode"] not in ("job", "shared"):
    raise ValueError(f"emr_cluster_mode must be job or shared, not {w['emr_cluster_mode']!r}")
job_mode = w["emr_cluster_mode"] == "job"
if job_mode or not w["emr_cluster_id"]:
    _missing += [k for k in ("emr_subnet_id", "emr_master_sg", "emr_slave_sg") if not w[k]]
if _missing:
    raise ValueError(f"missing required params: {_missing}")

sf = w["scale_factor"]
run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
p = emr_c.paths(w["s3_volume_prefix"], w["wh_db"], sf, run_id)
vol = lambda uri: emr_c.to_volume(uri, w["s3_volume_prefix"], w["tpcdi_directory"])
sess = emr_c.boto3_session(w["aws_region"], dbutils=dbutils, service_credential=w["aws_service_credential"])
emr = sess.client("emr")

# COMMAND ----------

# 1. Delta staging (idempotent: tables whose clone already committed are kept)
_emr_staging.materialize_sf(
    spark, dbutils, catalog=w["catalog"], sf=sf, staging_delta=p["staging_delta"],
    staging_delta_volume=vol(p["staging_delta"]), tables=tpcdi_sql.STAGING_TABLES,
    force=w["force_materialize"].lower() == "true")

# 2. code -> S3 via the volume
for f in emr_c.JOB_FILES:
    dbutils.fs.cp(f"file:{os.path.join(_here, 'jobs', f)}", f"{vol(p['code'])}{f}")
print(f"[code] {emr_c.JOB_FILES} -> {p['code']}")

# COMMAND ----------

# 3. cluster + setup steps
def _spec():
    return emr_c.cluster_spec(
        name=f"tpcdi-emr-{p['db']}", subnet_id=w["emr_subnet_id"], master_sg=w["emr_master_sg"],
        slave_sg=w["emr_slave_sg"], log_uri=p["logs"], core_count=int(w["emr_core_count"]),
        instance_type=w["emr_instance_type"], release_label=w["emr_release_label"],
        service_role=w["emr_service_role"], job_flow_role=w["emr_instance_profile"],
        tags={"RunName": f"tpcdi-emr-{p['db']}", "RunId": run_id})


created = False
if job_mode:
    import json
    with open(os.path.join(_here, "prices_snapshot.json")) as f:
        prices = json.load(f)
    setup_steps = [emr_c.sync_code_step(p["code"]), emr_c.setup_step(p, w["shuffle_partitions"])]
    for attempt in (1, 2):  # a transient S3 filesystem error must not fail the run
        spec = emr_c.job_cluster_spec(_spec(), setup_steps)
        spec["Name"] = f"tpcdi-emr-{p['db']}-setup"
        err = None
        try:
            res = emr_c.run_job_cluster(emr, spec)
        except emr_c.StepFailed as e:
            err, res = e, e.result
        cost = emr_c.cluster_cost(emr, res["cluster_id"], prices)
        dbutils.fs.put(f"{vol(p['metrics'])}/clustercost=setup{attempt}.json",
                       json.dumps(cost, indent=1, default=str), overwrite=True)
        t = emr_c.cluster_timing(res["cluster"])
        print(f"[emr] setup on job cluster {res['cluster_id']}: boot {t['boot_sec']}s, "
              f"cluster {t['cluster_sec']}s, ${cost['total_cost']}"
              f"{' FAILED: ' + str(err)[:300] if err else ''}")
        if not err:
            break
    else:
        raise err
    cluster_id = ""
else:
    created = not w["emr_cluster_id"]
    if created:
        cluster_id = emr_c.create_cluster(emr, _spec())
        print(f"[emr] created {cluster_id}; waiting for it to be ready")
    else:
        cluster_id = w["emr_cluster_id"]
    try:
        emr_c.wait_cluster_ready(emr, cluster_id)
        print(f"[emr] {cluster_id} ready {emr_c.cluster_counts(emr, cluster_id)}")
        emr_c.run_step(emr, cluster_id, emr_c.sync_code_step(p["code"]))
        st = emr_c.run_step(emr, cluster_id, emr_c.setup_step(p, w["shuffle_partitions"]))
        print(f"[emr] setup {p['db']} in {emr_c.step_seconds(st)}s")
    except Exception:
        # Don't leave a cluster we created billing when setup can't finish.
        if created:
            emr_c.terminate(emr, cluster_id)
            print(f"[emr] setup failed; terminate requested for {cluster_id}")
        raise

# COMMAND ----------

# 4. outputs for the loop + teardown
n = max(1, min(365, int(w["incremental_batches_to_run"])))
dbutils.jobs.taskValues.set(key="batch_date_ls", value=emr_c.batch_dates(n))
dbutils.jobs.taskValues.set(key="emr_cluster_id", value=cluster_id)
dbutils.jobs.taskValues.set(key="emr_created", value="true" if created else "false")
dbutils.jobs.taskValues.set(key="run_id", value=run_id)
print(f"[setup] {n} batches on {cluster_id or 'a job cluster each'}, metrics -> {p['metrics']}")
