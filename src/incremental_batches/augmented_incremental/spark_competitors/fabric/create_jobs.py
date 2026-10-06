"""Create the Databricks parent/child jobs for the Augmented Incremental
TPC-DI benchmark — **Fabric structured-streaming (fabric_ss) variant**.

Direct analog of `tools/workflow_builders/augmented_classic.py` (the Cluster
variant), NOT the Snowflake/dbt one: same parent shape (setup → 365-day
for_each loop → gated cleanup) and the same per-batch child, EXCEPT the child's
16 benchmark tasks collapse into a single `run_fabric` task. `run_fabric`
REST-triggers the Fabric `batch_runner` notebook, which runs the exact 17-node
bronze→silver/gold DAG via `notebookutils.notebook.runMultiple` inside ONE
Fabric Spark session. So compute + orchestration of the transforms live in
Fabric; Databricks only drops the day's files (into OneLake) and blocks on the
Fabric run.

  parent:  setup_fabric → loop_incremental_tpcdi (for_each) → gate → cleanup
  child:   simulate_filedrops_fabric → run_fabric

Naming follows the variant convention (fabric_ss / fabric_nee / synapse_ss);
the job-name token defaults to "FabricSS". All Databricks-side tasks pin the
interactive cluster (Fabric is the benchmark compute; the Databricks cluster
only orchestrates + copies files, so it's off the critical timing path).

Run headless:
    python3 create_jobs.py --wh-db shannon_barrow_augincr --scale-factor 10
(uses the `tpc-di` CLI profile by default). Creates the child first, then the
parent referencing its job_id; re-running resets the existing jobs in place.
"""
from __future__ import annotations

import argparse
from typing import Any


_DEFAULT_NOTIF = {
    "no_alert_for_skipped_runs": False,
    "no_alert_for_canceled_runs": False,
    "alert_on_last_attempt": False,
}
# Retries OFF: a failed Fabric batch (esp. run_fabric) is not transient — retrying just
# re-runs a ~30-min doomed batch_runner 3× more before surfacing the failure. Fail fast
# so we can diagnose. (Set >0 only if a genuinely transient issue warrants it.)
_RETRY_POLICY = {
    "max_retries": 0,
    "min_retry_interval_millis": 15000,
    "retry_on_timeout": False,
}
_AUG_PATH = "incremental_batches/augmented_incremental"

# Every Fabric-side task reads these; the job declares them at the top level and
# the for_each iteration forwards them to the child. batch_date is per-iteration.
_COMMON_PARAMS = {
    "catalog":             "{{job.parameters.catalog}}",
    "scale_factor":        "{{job.parameters.scale_factor}}",
    "tpcdi_directory":     "{{job.parameters.tpcdi_directory}}",
    "wh_db":               "{{job.parameters.wh_db}}",
    "fabric_workspace_id": "{{job.parameters.fabric_workspace_id}}",
    "fabric_lakehouse_id": "{{job.parameters.fabric_lakehouse_id}}",
    "secret_scope":        "{{job.parameters.secret_scope}}",
    # Fabric NEE toggle; only run_fabric forwards it to batch_runner, the other
    # notebooks ignore it. "true" = NEE (fabric_nee), "false" = plain-Spark batch.
    "enable_nee":          "{{job.parameters.enable_nee}}",
    # Fabric driver notebook name run_fabric triggers. The plain-Spark-batch variant
    # points at a batch_runner deployed on the non-NEE env, so this is overridable.
    "fabric_runner_notebook": "{{job.parameters.fabric_runner_notebook}}",
}
_BATCHED_PARAMS = dict(_COMMON_PARAMS, batch_date="{{job.parameters.batch_date}}")


def _make_task(
    *,
    task_key: str,
    notebook_path: str,
    depends_on: list[str] | None = None,
    base_params: dict | None = None,
    run_if: str = "ALL_SUCCESS",
    existing_cluster_id: str | None = None,
) -> dict:
    nb: dict[str, Any] = {"notebook_path": notebook_path, "source": "WORKSPACE"}
    if base_params is not None:
        nb["base_parameters"] = base_params
    task: dict[str, Any] = {"task_key": task_key}
    if depends_on:
        task["depends_on"] = [{"task_key": d} for d in depends_on]
    task["run_if"] = run_if
    task["notebook_task"] = nb
    if existing_cluster_id:
        task["existing_cluster_id"] = existing_cluster_id
    task["timeout_seconds"] = 0
    task["email_notifications"] = {}
    task["notification_settings"] = dict(_DEFAULT_NOTIF)
    task["webhook_notifications"] = {}
    task.update(_RETRY_POLICY)
    return task


def build_child(
    *,
    job_name: str,
    repo_src_path: str,
    catalog: str,
    scale_factor: int,
    tpcdi_directory: str,
    wh_db: str,
    interactive_cluster_id: str,
    **_unused,
) -> dict:
    """Per-date child: drop the day's files into OneLake, then run the Fabric
    batch_runner DAG and block until it finishes.

    Both tasks pin the interactive cluster: `simulate_filedrops_fabric` writes
    files into OneLake, and `run_fabric` just mints a token + polls the Fabric
    job (no heavy Databricks compute). The benchmark transforms run in Fabric.
    """
    aug = f"{repo_src_path}/{_AUG_PATH}"
    tasks = [
        _make_task(
            task_key="simulate_filedrops_fabric",
            notebook_path=f"{aug}/fabric/simulate_filedrops_fabric",
            base_params=_BATCHED_PARAMS,
            existing_cluster_id=interactive_cluster_id,
        ),
        _make_task(
            task_key="run_fabric",
            notebook_path=f"{aug}/fabric/run_fabric",
            depends_on=["simulate_filedrops_fabric"],
            base_params=_BATCHED_PARAMS,
            existing_cluster_id=interactive_cluster_id,
        ),
    ]
    return {
        "name": job_name,
        "description": (
            f"TPC-DI Augmented Incremental benchmark (Fabric structured streaming, "
            f"**child**) at SF={scale_factor}. Triggered once per simulated business "
            f"day by the parent's for_each. Drops the day's file into OneLake, then "
            f"REST-triggers the Fabric batch_runner (17-node runMultiple DAG) and "
            f"blocks. Transforms materialize in Fabric lakehouse schema "
            f"{wh_db}_{scale_factor}."
        ),
        "tags": {"data_generator": "spark", "engine": "fabric", "variant": "fabric_ss"},
        "email_notifications": {"no_alert_for_skipped_runs": False},
        "webhook_notifications": {},
        "timeout_seconds": 0,
        "max_concurrent_runs": 1000,
        "performance_target": "PERFORMANCE_OPTIMIZED",
        "parameters": [
            {"name": "catalog",             "default": catalog},
            {"name": "scale_factor",        "default": str(scale_factor)},
            {"name": "tpcdi_directory",     "default": tpcdi_directory},
            {"name": "wh_db",               "default": wh_db},
            {"name": "fabric_workspace_id", "default": _unused.get("fabric_workspace_id", "")},
            {"name": "fabric_lakehouse_id", "default": _unused.get("fabric_lakehouse_id", "")},
            {"name": "secret_scope",        "default": _unused.get("secret_scope", "tpcdi_fabric")},
            {"name": "enable_nee",          "default": _unused.get("enable_nee", "true")},
            {"name": "fabric_runner_notebook", "default": _unused.get("fabric_runner_notebook", "batch_runner")},
            {"name": "batch_date",          "default": ""},
        ],
        "tasks": tasks,
        "queue": {"enabled": True},
    }


def build_parent(
    *,
    job_name: str,
    child_job_id: int,
    repo_src_path: str,
    catalog: str,
    scale_factor: int,
    tpcdi_directory: str,
    wh_db: str,
    interactive_cluster_id: str,
    **_unused,
) -> dict:
    """Parent: setup_fabric (materialize + Fabric per-run reset + emit dates) →
    for_each loop over the child per simulated day → gated cleanup.

    The for_each is sequential (no concurrency set) — the 365-day streaming
    pipeline is inherently ordered; each batch builds on the prior checkpoint
    + SCD2 state."""
    aug = f"{repo_src_path}/{_AUG_PATH}"

    setup_task = _make_task(
        task_key="setup_fabric",
        notebook_path=f"{aug}/fabric/setup_fabric",
        base_params={
            **_COMMON_PARAMS,
            "incremental_batches_to_run": "{{job.parameters.incremental_batches_to_run}}",
        },
        existing_cluster_id=interactive_cluster_id,
    )

    loop_task: dict[str, Any] = {
        "task_key": "loop_incremental_tpcdi",
        "depends_on": [{"task_key": "setup_fabric"}],
        "run_if": "ALL_SUCCESS",
        "for_each_task": {
            "inputs": "{{tasks.setup_fabric.values.batch_date_ls}}",
            "task": {
                "task_key": "loop_incremental_tpcdi_iteration",
                "run_if": "ALL_SUCCESS",
                "run_job_task": {
                    "job_id": child_job_id,
                    "job_parameters": {
                        "catalog":             "{{job.parameters.catalog}}",
                        "scale_factor":        "{{job.parameters.scale_factor}}",
                        "tpcdi_directory":     "{{job.parameters.tpcdi_directory}}",
                        "wh_db":               "{{job.parameters.wh_db}}",
                        "fabric_workspace_id": "{{job.parameters.fabric_workspace_id}}",
                        "fabric_lakehouse_id": "{{job.parameters.fabric_lakehouse_id}}",
                        "secret_scope":        "{{job.parameters.secret_scope}}",
                        "enable_nee":          "{{job.parameters.enable_nee}}",
                        "fabric_runner_notebook": "{{job.parameters.fabric_runner_notebook}}",
                        "batch_date":          "{{input}}",
                    },
                },
                "timeout_seconds": 0,
                "email_notifications": {},
                "notification_settings": dict(_DEFAULT_NOTIF),
                "webhook_notifications": {},
            },
        },
        "timeout_seconds": 0,
        "email_notifications": {},
        "notification_settings": dict(_DEFAULT_NOTIF),
        "webhook_notifications": {},
    }

    GATE = "delete_when_finished_TRUE_FALSE"
    gate_task: dict[str, Any] = {
        "task_key": GATE,
        "depends_on": [{"task_key": "loop_incremental_tpcdi"}],
        "run_if": "ALL_DONE",
        "condition_task": {
            "op": "EQUAL_TO",
            "left": "{{job.parameters.delete_tables_when_finished}}",
            "right": "TRUE",
        },
        "timeout_seconds": 0,
        "email_notifications": {},
        "notification_settings": dict(_DEFAULT_NOTIF),
        "webhook_notifications": {},
    }
    cleanup_task = _make_task(
        task_key="cleanup",
        notebook_path=f"{aug}/fabric/teardown_fabric",
        base_params=_COMMON_PARAMS,
        existing_cluster_id=interactive_cluster_id,
    )
    cleanup_task["depends_on"] = [{"task_key": GATE, "outcome": "true"}]

    return {
        "name": job_name,
        "description": (
            f"TPC-DI Augmented Incremental benchmark (Fabric structured streaming, "
            f"**parent**) at SF={scale_factor}. setup_fabric materializes the 20 "
            f"staging tables into OneLake (idempotent DEEP CLONE) + triggers the "
            f"Fabric per-run reset, then loops the child per simulated day "
            f"(2016-07-06 →). Cleanup gated by delete_tables_when_finished "
            f"(default FALSE while the port is under bring-up)."
        ),
        "tags": {"data_generator": "spark", "engine": "fabric", "variant": "fabric_ss"},
        "email_notifications": {"no_alert_for_skipped_runs": False},
        "webhook_notifications": {},
        "timeout_seconds": 0,
        "max_concurrent_runs": 1,
        "performance_target": "PERFORMANCE_OPTIMIZED",
        "parameters": [
            {"name": "catalog",                     "default": catalog},
            {"name": "scale_factor",                "default": str(scale_factor)},
            {"name": "tpcdi_directory",             "default": tpcdi_directory},
            {"name": "wh_db",                       "default": wh_db},
            {"name": "fabric_workspace_id",         "default": _unused.get("fabric_workspace_id", "")},
            {"name": "fabric_lakehouse_id",         "default": _unused.get("fabric_lakehouse_id", "")},
            {"name": "secret_scope",                "default": _unused.get("secret_scope", "tpcdi_fabric")},
            {"name": "enable_nee",                  "default": _unused.get("enable_nee", "true")},
            {"name": "fabric_runner_notebook",      "default": _unused.get("fabric_runner_notebook", "batch_runner")},
            {"name": "delete_tables_when_finished", "default": "FALSE"},
            {"name": "incremental_batches_to_run",  "default": "365"},
        ],
        "tasks": [setup_task, loop_task, gate_task, cleanup_task],
        "queue": {"enabled": True},
    }


# --------------------------------------------------------------------------- #
# Creation (idempotent find-or-reset via the Databricks SDK)
# --------------------------------------------------------------------------- #
def _find_or_upsert(w, spec: dict) -> int:
    """Create the job, or reset it in place if one with the same name exists.

    Uses the raw Jobs REST API (via the SDK's api_client) so the exact JSON spec
    we build is sent verbatim — the typed `w.jobs.create(**spec)` path chokes on
    raw dict sub-objects (email_notifications etc.).
    """
    existing = [j for j in w.jobs.list(name=spec["name"])]
    if existing:
        jid = existing[0].job_id
        w.api_client.do("POST", "/api/2.2/jobs/reset",
                        body={"job_id": jid, "new_settings": spec})
        print(f"[reset]  {spec['name']} → job_id={jid}")
        return jid
    resp = w.api_client.do("POST", "/api/2.2/jobs/create", body=spec)
    jid = resp["job_id"]
    print(f"[create] {spec['name']} → job_id={jid}")
    return jid


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--profile", default="tpc-di")
    ap.add_argument("--wh-db", required=True, help="wh_db prefix; Fabric schema = {wh_db}_{sf}")
    ap.add_argument("--scale-factor", type=int, default=10)
    ap.add_argument("--catalog", default="main", help="Databricks catalog holding tpcdi_incremental_staging_{sf}")
    ap.add_argument("--tpcdi-directory", default="/Volumes/main/tpcdi_raw_data/tpcdi_volume/")
    ap.add_argument("--repo-src-path",
                    default="/Workspace/Users/shannon.barrow@databricks.com/databricks-tpc-di-augmented/src")
    ap.add_argument("--interactive-cluster-id", default="0905-212234-wq3cjc8j")
    ap.add_argument("--fabric-workspace-id", default="4f119fd7-d1f7-48bc-be77-6c41c782b541")
    ap.add_argument("--fabric-lakehouse-id", default="3f5b1c43-a52c-4a2e-90d7-4de7504e6122")
    ap.add_argument("--secret-scope", default="tpcdi_fabric")
    ap.add_argument("--base-name", default="TPC-DI")
    ap.add_argument("--variant", default="FabricSS", help="job-name token (FabricSS / FabricNEE / FabricSparkBatch / SynapseSS)")
    ap.add_argument("--enable-nee", default="true",
                    help="'true' = NEE (fabric_nee); 'false' = plain-Spark batch on the same code")
    ap.add_argument("--fabric-runner-notebook", default="batch_runner",
                    help="Fabric driver notebook name run_fabric triggers (e.g. batch_runner_spark for the no-NEE variant)")
    args = ap.parse_args()

    from databricks.sdk import WorkspaceClient
    w = WorkspaceClient(profile=args.profile)

    common = dict(
        repo_src_path=args.repo_src_path,
        catalog=args.catalog,
        scale_factor=args.scale_factor,
        tpcdi_directory=args.tpcdi_directory,
        wh_db=args.wh_db,
        interactive_cluster_id=args.interactive_cluster_id,
        fabric_workspace_id=args.fabric_workspace_id,
        fabric_lakehouse_id=args.fabric_lakehouse_id,
        secret_scope=args.secret_scope,
        enable_nee=args.enable_nee,
        fabric_runner_notebook=args.fabric_runner_notebook,
    )
    stem = f"{args.base_name}-SF{args.scale_factor}-AugmentedIncremental-{args.variant}"

    child_id = _find_or_upsert(w, build_child(job_name=f"{stem}-Child", **common))
    parent_id = _find_or_upsert(w, build_parent(job_name=f"{stem}-Parent",
                                                 child_job_id=child_id, **common))
    print(f"\n[done] parent job_id={parent_id}, child job_id={child_id}")


if __name__ == "__main__":
    main()
