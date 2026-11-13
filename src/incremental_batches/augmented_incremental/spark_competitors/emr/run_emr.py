# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# dependencies = [
#   "boto3",
# ]
# ///
# Child `run_emr` task: run one batch date as an EMR step and block until it
# ends. The preceding simulate_filedrops task has already copied the day's
# files to {s3}augmented_incremental/_dailybatches/{wh_db}_{sf}/{date}/.
# The runner on EMR writes the per-table timings JSON; this task adds the
# EMR step timing + the batch's cost at the cluster's on-demand rate and
# writes both next to it.

import json
import os
import sys

_nb = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
_here = os.path.dirname(_nb if _nb.startswith("/Workspace") else "/Workspace" + _nb)
if _here not in sys.path:
    sys.path.insert(0, _here)
import _emr_conn as emr_c

# COMMAND ----------

for k, d in (("scale_factor", "10"), ("wh_db", ""), ("batch_date", ""),
             ("tpcdi_directory", "/Volumes/main/tpcdi_raw_data/tpcdi_volume/"),
             ("s3_volume_prefix", ""), ("aws_region", "us-west-2"),
             ("aws_service_credential", ""), ("emr_cluster_id", ""), ("run_id", ""),
             ("concurrency", "8"), ("shuffle_partitions", "1000"),
             ("batch_timeout_secs", "14400")):
    dbutils.widgets.text(k, d)
w = {k: dbutils.widgets.get(k).strip() for k in (
    "scale_factor", "wh_db", "batch_date", "tpcdi_directory", "s3_volume_prefix", "aws_region",
    "aws_service_credential", "emr_cluster_id", "run_id", "concurrency", "shuffle_partitions",
    "batch_timeout_secs")}
_missing = [k for k in ("wh_db", "batch_date", "s3_volume_prefix", "emr_cluster_id", "run_id") if not w[k]]
if _missing:
    raise ValueError(f"missing required params: {_missing}")

p = emr_c.paths(w["s3_volume_prefix"], w["wh_db"], w["scale_factor"], w["run_id"])
vol = lambda uri: emr_c.to_volume(uri, w["s3_volume_prefix"], w["tpcdi_directory"])
emr = emr_c.boto3_session(w["aws_region"], dbutils=dbutils,
                          service_credential=w["aws_service_credential"]).client("emr")
cid, d = w["emr_cluster_id"], w["batch_date"]

with open(os.path.join(_here, "prices_snapshot.json")) as f:
    prices = json.load(f)
rate = emr_c.fleet_rate(prices, emr_c.cluster_counts(emr, cid))

# COMMAND ----------

err = None
try:
    step = emr_c.run_step(emr, cid, emr_c.batch_step(
        p, d, concurrency=int(w["concurrency"]), shuffle_partitions=w["shuffle_partitions"]),
        timeout_secs=int(w["batch_timeout_secs"]))
except emr_c.StepFailed as e:
    err, step = e, e.step

try:
    with open(vol(emr_c.metrics_uri(p, d))) as f:
        metrics = json.load(f)
except FileNotFoundError:
    metrics = None  # the runner died before writing its metrics

row = emr_c.batch_row(d, step, metrics, drop_sec=None, rate=rate)
dbutils.fs.put(f"{vol(p['metrics'])}/batchrow={d}.json", json.dumps(row), overwrite=True)
for t in (metrics or {}).get("tasks", []):
    print(f"[{t['status']:7s}] {t['node']:32s} {t['seconds'] or 0:8.1f}s")
print(f"[emr] {d}: step {row['step_sec']}s, runner {row['runner_sec']}s, "
      f"${row['batch_cost_usd']} at ${rate:.4f}/hr, {row['status']}")
if err:
    raise err
