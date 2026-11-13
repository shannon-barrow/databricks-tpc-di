# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# dependencies = [
#   "boto3",
# ]
# ///
# Parent `teardown_emr` task. Runs after the loop whatever its outcome
# (run_if ALL_DONE), so a cluster the run created never outlives it:
#   1. when delete_tables_when_finished=TRUE: EMR `drop` step (Glue database
#      + its Delta files) and remove the run's file drops
#   2. batches.csv + task_times.csv from the per-batch metrics
#   3. terminate the cluster if setup created it, then write cluster_cost.json
#      from every instance's lifetime (EC2 + EMR uplift + EBS, on demand)
# The parquet staging and the uploaded code are kept for the next run.

import json
import os
import sys

_nb = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
_here = os.path.dirname(_nb if _nb.startswith("/Workspace") else "/Workspace" + _nb)
if _here not in sys.path:
    sys.path.insert(0, _here)
import _emr_conn as emr_c

# COMMAND ----------

for k, d in (("scale_factor", "10"), ("wh_db", ""),
             ("tpcdi_directory", "/Volumes/main/tpcdi_raw_data/tpcdi_volume/"),
             ("s3_volume_prefix", ""), ("aws_region", "us-west-2"),
             ("aws_service_credential", ""), ("emr_cluster_id", ""), ("emr_created", "false"),
             ("run_id", ""), ("delete_tables_when_finished", "TRUE")):
    dbutils.widgets.text(k, d)
w = {k: dbutils.widgets.get(k).strip() for k in (
    "scale_factor", "wh_db", "tpcdi_directory", "s3_volume_prefix", "aws_region",
    "aws_service_credential", "emr_cluster_id", "emr_created", "run_id",
    "delete_tables_when_finished")}
if not w["emr_cluster_id"]:
    dbutils.notebook.exit("no cluster: setup_emr did not get that far")

p = emr_c.paths(w["s3_volume_prefix"], w["wh_db"], w["scale_factor"], w["run_id"])
vol = lambda uri: emr_c.to_volume(uri, w["s3_volume_prefix"], w["tpcdi_directory"])
emr = emr_c.boto3_session(w["aws_region"], dbutils=dbutils,
                          service_credential=w["aws_service_credential"]).client("emr")
cid = w["emr_cluster_id"]

# COMMAND ----------

try:
    if w["delete_tables_when_finished"].upper() == "TRUE":
        emr_c.run_step(emr, cid, emr_c.drop_step(p))
        dbutils.fs.rm(vol(p["dailybatches"]), recurse=True)
        print(f"[cleanup] dropped {p['db']} and {p['dailybatches']}")

    batches, tasks = [], []
    try:
        entries = sorted(dbutils.fs.ls(vol(p["metrics"])), key=lambda e: e.name)
    except Exception:
        entries = []
    for e in entries:
        with open(e.path.replace("dbfs:", "")) as f:
            obj = json.load(f)
        if e.name.startswith("batchrow="):
            batches.append(obj)
        elif e.name.startswith("batch="):
            tasks += emr_c.task_rows(obj)
    dbutils.fs.put(f"{vol(p['metrics'])}/batches.csv", emr_c.to_csv(batches, emr_c.BATCH_FIELDS), overwrite=True)
    dbutils.fs.put(f"{vol(p['metrics'])}/task_times.csv", emr_c.to_csv(tasks, emr_c.TASK_FIELDS), overwrite=True)
    print(f"[report] {len(batches)} batches, {len(tasks)} task rows -> {p['metrics']}")
finally:
    if w["emr_created"] == "true":
        emr_c.terminate(emr, cid)
        emr.get_waiter("cluster_terminated").wait(ClusterId=cid)
        print(f"[emr] {cid} terminated")

with open(os.path.join(_here, "prices_snapshot.json")) as f:
    prices = json.load(f)
rep = emr_c.cluster_cost(emr, cid, prices)
dbutils.fs.put(f"{vol(p['metrics'])}/cluster_cost.json", json.dumps(rep, indent=1, default=str), overwrite=True)
print(f"[report] {cid}: {rep['instance_hours']} instance-hours, ${rep['total_cost']} "
      f"(EC2 {rep['ec2_cost']} + EMR {rep['emr_cost']} + EBS {rep['ebs_cost']})")
