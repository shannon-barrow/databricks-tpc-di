"""Integration smoke test — create + run a few workflow variations end-to-end.

Builds JSON via the renamed Python builders, submits to the real Jobs/Pipelines
APIs (via `databricks` CLI for auth), triggers each, and polls until terminal.

Run: python tests/smoke_run_workflows.py
"""
import json
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src" / "tools"))
sys.path.insert(0, str(ROOT / "src" / "tools" / "workflow_builders"))

from workflow_builders import (
    datagen_spark, workflows_incremental, workflows_single_batch,
    sdp_pipeline, sdp_workflow,
)


PROFILE = "tpc-di"
SF = 10
CATALOG = "main"
NAME_BASE = "Shannon-Barrow-TPCDI-Smoke"
WH_TARGET = "shannon_barrow_smoke"
REPO_SRC = "/Workspace/Users/shannon.barrow@databricks.com/databricks-tpc-di-augmented/src"
TPCDI_DIR = f"/Volumes/{CATALOG}/tpcdi_raw_data/tpcdi_volume/spark_datagen/"
DBR = "16.4.x-photon-scala2.12"
WORKER_TYPE = "Standard_D8pds_v6"
NODE_TYPES = {WORKER_TYPE: {"num_cores": 8}}
CLOUD = "Azure"


def cli(args, json_in=None, capture=True):
    """Run a `databricks` CLI command. Returns parsed JSON stdout."""
    if json_in is not None:
        args = args + ["--json", json_in]
    cmd = ["databricks"] + args + ["--profile", PROFILE]
    r = subprocess.run(cmd, capture_output=capture, text=True)
    if r.returncode != 0:
        print(f"  CMD failed: databricks {' '.join(args[:3])} ...")
        print(f"  stderr: {r.stderr}")
        raise RuntimeError(r.stderr)
    return json.loads(r.stdout) if r.stdout else {}


def post_job(payload):
    return cli(["api", "post", "/api/2.1/jobs/create"],
               json_in=json.dumps(payload))["job_id"]


def post_pipeline(payload):
    return cli(["api", "post", "/api/2.0/pipelines"],
               json_in=json.dumps(payload))["pipeline_id"]


def run_now(job_id, params=None):
    payload = {"job_id": job_id}
    if params:
        payload["job_parameters"] = params
    return cli(["api", "post", "/api/2.1/jobs/run-now"],
               json_in=json.dumps(payload))["run_id"]


def poll(run_id, label, timeout_s=1800):
    start = time.time()
    last_state = None
    while time.time() - start < timeout_s:
        d = cli(["api", "get", f"/api/2.1/jobs/runs/get?run_id={run_id}"])
        state = d.get("state", {})
        life = state.get("life_cycle_state")
        if life != last_state:
            print(f"  [{label}] {life} ({int(time.time() - start)}s)")
            last_state = life
        if life in ("TERMINATED", "INTERNAL_ERROR", "SKIPPED"):
            return state.get("result_state", "UNKNOWN"), d
        time.sleep(20)
    return "TIMEOUT", None


def main():
    print(f"== Smoke test @ SF={SF}, base={NAME_BASE} ==\n")

    # ---- Build all four workflow specs ----
    datagen = datagen_spark.build(
        job_name=f"{NAME_BASE}-SF{SF}-SparkGen",
        scale_factor=SF, catalog=CATALOG,
        regenerate_data="NO", log_level="INFO",
        repo_src_path=REPO_SRC,
        default_dbr_version=DBR, default_worker_type=WORKER_TYPE,
        serverless="YES", node_types=NODE_TYPES, cloud_provider=CLOUD,
    )

    bench_common = dict(
        catalog=CATALOG, wh_target=WH_TARGET,
        tpcdi_directory=TPCDI_DIR, scale_factor=SF,
        repo_src_path=REPO_SRC, cloud_provider=CLOUD,
        serverless="YES", pred_opt="DISABLE",
        null_constraint="", perf_opt_flg=True,
        opt_write="'delta.autoOptimize.optimizeWrite'=False",
        index_cols=", 'delta.dataSkippingNumIndexedCols' = 0",
        data_generator="spark", datagen_label="spark_data_gen",
        dbr=DBR,
        worker_node_type=WORKER_TYPE, driver_node_type=WORKER_TYPE,
        worker_node_count=0,
    )

    cluster_inc_name = f"{NAME_BASE}-SF{SF}-Incremental-Cluster-SparkGen"
    cluster_inc = workflows_incremental.build(
        job_name=cluster_inc_name, exec_type="CLUSTER",
        exec_folder="CLUSTER", batched_label="incremental",
        **bench_common,
    )

    # DBSQL needs a warehouse — look up an existing 2X-Small one
    wh_lookup = cli(["api", "get", "/api/2.0/sql/warehouses"])
    wh_id = next((w["id"] for w in wh_lookup["warehouses"]
                  if w["name"] == "TPCDI_2X-Small"), None)
    if not wh_id:
        print("  ⚠ No TPCDI_2X-Small warehouse found — skipping DBSQL variation")
        dbsql_single = None
    else:
        dbsql_single_name = f"{NAME_BASE}-SF{SF}-SingleBatch-DBSQL-SparkGen"
        dbsql_single = workflows_single_batch.build(
            job_name=dbsql_single_name, exec_type="DBSQL",
            exec_folder="SQL", batched_label="single_batch",
            wh_id=wh_id, wh_name="TPCDI_2X-Small", wh_size="2X-Small",
            **bench_common,
        )

    # SDP-CORE — build pipeline first, then workflow
    sdp_name = f"{NAME_BASE}-SF{SF}-SDP-CORE-SparkGen"
    sdp_pipe_payload = sdp_pipeline.build(
        job_name=sdp_name, edition="CORE",
        wh_target=WH_TARGET, datagen_label="spark_data_gen",
        catalog=CATALOG, scale_factor=SF, repo_src_path=REPO_SRC,
        tpcdi_directory=TPCDI_DIR, serverless="YES",
    )

    # ---- Submit ----
    print("Creating jobs:")
    datagen_id = post_job(datagen)
    print(f"  ✓ datagen        — job {datagen_id}  ({datagen['name']})")

    cluster_inc_id = post_job(cluster_inc)
    print(f"  ✓ Cluster/Inc    — job {cluster_inc_id}  ({cluster_inc['name']})")

    dbsql_id = None
    if dbsql_single:
        dbsql_id = post_job(dbsql_single)
        print(f"  ✓ DBSQL/Single   — job {dbsql_id}  ({dbsql_single['name']})")

    print("  Creating SDP pipeline...")
    pipeline_id = post_pipeline(sdp_pipe_payload)
    sdp_wf = sdp_workflow.build(
        job_name=sdp_name, catalog=CATALOG, wh_target=WH_TARGET,
        edition="CORE", datagen_label="spark_data_gen",
        scale_factor=SF, repo_src_path=REPO_SRC,
        tpcdi_directory=TPCDI_DIR, data_generator="spark",
        pipeline_id=pipeline_id,
    )
    sdp_id = post_job(sdp_wf)
    print(f"  ✓ SDP-CORE       — job {sdp_id}  ({sdp_wf['name']})  pipeline {pipeline_id}")

    # ---- Trigger benchmark workflows in parallel (datagen at SF=10 is no-op) ----
    print("\nTriggering benchmark workflows...")
    runs = []
    rid = run_now(cluster_inc_id)
    runs.append((rid, "Cluster/Inc", cluster_inc_id))
    print(f"  → Cluster/Inc run {rid}")
    if dbsql_id:
        rid = run_now(dbsql_id)
        runs.append((rid, "DBSQL/Single", dbsql_id))
        print(f"  → DBSQL/Single run {rid}")
    rid = run_now(sdp_id)
    runs.append((rid, "SDP-CORE", sdp_id))
    print(f"  → SDP-CORE run {rid}")

    # ---- Poll all in parallel via repeated checks ----
    print("\nPolling for completion (timeout 30 min)...\n")
    results = {}
    pending = {rid: (label, jid) for rid, label, jid in runs}
    start = time.time()
    while pending and time.time() - start < 1800:
        for rid in list(pending):
            label, jid = pending[rid]
            d = cli(["api", "get", f"/api/2.1/jobs/runs/get?run_id={rid}"])
            life = d.get("state", {}).get("life_cycle_state")
            if life in ("TERMINATED", "INTERNAL_ERROR", "SKIPPED"):
                result = d["state"].get("result_state", "UNKNOWN")
                results[label] = (result, rid, jid)
                print(f"  [{label}] {life}/{result}  (run {rid})")
                del pending[rid]
        if pending:
            time.sleep(30)

    for rid, (label, jid) in pending.items():
        results[label] = ("TIMEOUT", rid, jid)
        print(f"  [{label}] TIMEOUT  (run {rid})")

    # ---- Summary ----
    print(f"\n== Summary ({int(time.time() - start)}s) ==")
    for label, (result, rid, jid) in results.items():
        mark = "✓" if result == "SUCCESS" else "✗"
        print(f"  {mark} {label:15s} — {result}  job {jid}, run {rid}")

    fails = [k for k, (r, _, _) in results.items() if r != "SUCCESS"]
    if fails:
        print(f"\n{len(fails)} variation(s) failed: {', '.join(fails)}")
        sys.exit(1)
    print("\nAll smoke runs SUCCEEDED.")


if __name__ == "__main__":
    main()
