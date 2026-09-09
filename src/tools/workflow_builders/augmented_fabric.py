"""Builder for the Augmented Incremental TPC-DI benchmark — Fabric DW variant.

Mirrors `augmented_redshift.py` in shape. The Fabric DW variant runs the same
dbt project with `--target fabric` (dbt-fabric / T-SQL). Compute that runs the
models is the Fabric Data Warehouse; Databricks orchestrates, materializes the
OneLake staging (one-time), and drops per-batch CSVs into OneLake.

KEY STRUCTURAL DIFFERENCE FROM REDSHIFT: every task runs on a SHARED CLASSIC
job cluster (not serverless), because:
  - dbt-fabric needs the msodbcsql18 ODBC driver, installed by the cluster
    init script `init_msodbcsql18.sh` (serverless can't apt-install it).
  - setup/teardown need OneLake OAuth (SP) + UC single-user to read `main` for
    the one-time DEEP CLONE materialize.
So there are no serverless `environments`; instead a `job_clusters` entry with
the init script, shared by all tasks via `job_cluster_key`.

Pre-requisites (one-time, out-of-band):
  - Fabric Warehouse item (host/name are plain params).
  - Entra service principal (tpcdi-fabric-sp) with a login/user IN the WH, its
    client_id/client_secret in the `tpcdi_fabric` UC secret scope.
  - The `tpcdi_fabric` Lakehouse in the same workspace (staging_sf{sf} is DEEP
    CLONE'd there by setup if missing).

Two builders:
  - build_child(...)  — 2-task per-date job: simulate_filedrops_fabric -> run_dbt
  - build_parent(...) — setup_fabric -> for_each loop over the child -> gated cleanup
"""
from __future__ import annotations

from typing import Any

_DEFAULT_NOTIF = {
    "no_alert_for_skipped_runs": False,
    "no_alert_for_canceled_runs": False,
    "alert_on_last_attempt": False,
}
_RETRY_POLICY = {"max_retries": 0, "min_retry_interval_millis": 0, "retry_on_timeout": False}
_AUG_PATH = "incremental_batches/augmented_incremental"
_FAB_DIR = f"{_AUG_PATH}/dbt/competitors/fabric"
_CLUSTER_KEY = "fabric_dbt_cluster"

# Plain (non-secret) params every task needs. SP client_id/secret are NOT here —
# the notebooks read them from the `tpcdi_fabric` UC secret scope directly.
_COMMON_PARAMS = {
    "catalog":              "{{job.parameters.catalog}}",
    "scale_factor":         "{{job.parameters.scale_factor}}",
    "tpcdi_directory":      "{{job.parameters.tpcdi_directory}}",
    "wh_db":                "{{job.parameters.wh_db}}",
    "fabric_wh_host":       "{{job.parameters.fabric_wh_host}}",
    "fabric_wh_name":       "{{job.parameters.fabric_wh_name}}",
    "fabric_workspace_id":  "{{job.parameters.fabric_workspace_id}}",
    "fabric_lakehouse_id":  "{{job.parameters.fabric_lakehouse_id}}",
    "tenant_id":            "{{job.parameters.tenant_id}}",
    "file_ext":             "{{job.parameters.file_ext}}",
}
_BATCHED_PARAMS = dict(_COMMON_PARAMS, batch_date="{{job.parameters.batch_date}}")


def _job_cluster(*, repo_src_path: str, spark_version: str, node_type_id: str,
                 single_user_name: str | None) -> dict:
    """Shared classic single-node cluster: UC single-user (reads `main`) +
    the msodbcsql18 init script (ODBC for dbt-fabric/pyodbc)."""
    nc: dict[str, Any] = {
        "spark_version": spark_version,
        "node_type_id": node_type_id,
        "num_workers": 0,
        "spark_conf": {
            "spark.master": "local[*]",
            "spark.databricks.cluster.profile": "singleNode",
        },
        "custom_tags": {"ResourceClass": "SingleNode"},
        "data_security_mode": "SINGLE_USER",
        "init_scripts": [{
            "workspace": {"destination": f"{repo_src_path}/{_FAB_DIR}/init_msodbcsql18.sh"}
        }],
    }
    if single_user_name:
        nc["single_user_name"] = single_user_name
    return {"job_cluster_key": _CLUSTER_KEY, "new_cluster": nc}


def _make_task(*, task_key: str, notebook_path: str, base_params: dict,
               depends_on: list[str] | None = None, run_if: str = "ALL_SUCCESS") -> dict:
    task: dict[str, Any] = {"task_key": task_key, "job_cluster_key": _CLUSTER_KEY}
    if depends_on:
        task["depends_on"] = [{"task_key": d} for d in depends_on]
    task["run_if"] = run_if
    task["notebook_task"] = {"notebook_path": notebook_path, "source": "WORKSPACE",
                             "base_parameters": base_params}
    task["timeout_seconds"] = 0
    task["email_notifications"] = {}
    task["notification_settings"] = dict(_DEFAULT_NOTIF)
    task["webhook_notifications"] = {}
    task.update(_RETRY_POLICY)
    return task


def build_child(*, job_name: str, repo_src_path: str, catalog: str, scale_factor: int,
                tpcdi_directory: str, wh_db: str,
                fabric_wh_host: str, fabric_wh_name: str = "tpcdi_fabric_dw",
                fabric_workspace_id: str, fabric_lakehouse_id: str,
                tenant_id: str, file_ext: str = "txt",
                spark_version: str = "15.4.x-scala2.12",
                node_type_id: str = "Standard_D8ds_v5",
                single_user_name: str | None = None, **_unused) -> dict:
    aug = f"{repo_src_path}/{_AUG_PATH}"
    tasks = [
        _make_task(task_key="simulate_filedrops_fabric",
                   notebook_path=f"{aug}/dbt/competitors/fabric/simulate_filedrops_fabric",
                   base_params=_BATCHED_PARAMS),
        _make_task(task_key="dbt_run",
                   notebook_path=f"{aug}/dbt/competitors/fabric/run_dbt",
                   depends_on=["simulate_filedrops_fabric"],
                   base_params=dict(_BATCHED_PARAMS, dbt_project_dir=f"{aug}/dbt")),
    ]
    return {
        "name": job_name,
        "description": (f"TPC-DI Augmented Incremental (Fabric DW, child) SF={scale_factor}. "
                        f"Per simulated day: simulate_filedrops_fabric drops the day's CSVs "
                        f"into OneLake, then dbt_run executes `dbt run --target fabric` "
                        f"(bronze via OPENROWSET) into the {fabric_wh_name} warehouse."),
        "tags": {"data_generator": "spark", "engine": "fabric_dw"},
        "timeout_seconds": 0,
        "max_concurrent_runs": 1000,
        "job_clusters": [_job_cluster(repo_src_path=repo_src_path, spark_version=spark_version,
                                      node_type_id=node_type_id, single_user_name=single_user_name)],
        "parameters": [
            {"name": "catalog",             "default": catalog},
            {"name": "scale_factor",        "default": str(scale_factor)},
            {"name": "tpcdi_directory",     "default": tpcdi_directory},
            {"name": "wh_db",               "default": wh_db},
            {"name": "fabric_wh_host",      "default": fabric_wh_host},
            {"name": "fabric_wh_name",      "default": fabric_wh_name},
            {"name": "fabric_workspace_id", "default": fabric_workspace_id},
            {"name": "fabric_lakehouse_id", "default": fabric_lakehouse_id},
            {"name": "tenant_id",           "default": tenant_id},
            {"name": "file_ext",            "default": file_ext},
            {"name": "batch_date",          "default": ""},
        ],
        "tasks": tasks,
        "queue": {"enabled": True},
    }


def build_parent(*, job_name: str, child_job_id: int, repo_src_path: str, catalog: str,
                 scale_factor: int, tpcdi_directory: str, wh_db: str,
                 fabric_wh_host: str, fabric_wh_name: str = "tpcdi_fabric_dw",
                 fabric_workspace_id: str, fabric_lakehouse_id: str,
                 tenant_id: str, file_ext: str = "txt",
                 spark_version: str = "15.4.x-scala2.12",
                 node_type_id: str = "Standard_D8ds_v5",
                 single_user_name: str | None = None, **_unused) -> dict:
    aug = f"{repo_src_path}/{_AUG_PATH}"

    setup_task = _make_task(
        task_key="setup_fabric",
        notebook_path=f"{aug}/dbt/competitors/fabric/setup_fabric",
        base_params={**_COMMON_PARAMS,
                     "incremental_batches_to_run": "{{job.parameters.incremental_batches_to_run}}"})

    child_params = {k: v for k, v in _COMMON_PARAMS.items()}
    child_params["batch_date"] = "{{input}}"
    loop_task: dict[str, Any] = {
        "task_key": "loop_incremental_tpcdi",
        "depends_on": [{"task_key": "setup_fabric"}],
        "run_if": "ALL_SUCCESS",
        "for_each_task": {
            "inputs": "{{tasks.setup_fabric.values.batch_date_ls}}",
            "task": {
                "task_key": "loop_incremental_tpcdi_iteration",
                "run_if": "ALL_SUCCESS",
                "run_job_task": {"job_id": child_job_id, "job_parameters": child_params},
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
        "condition_task": {"op": "EQUAL_TO",
                           "left": "{{job.parameters.delete_tables_when_finished}}",
                           "right": "TRUE"},
        "timeout_seconds": 0,
        "email_notifications": {},
        "notification_settings": dict(_DEFAULT_NOTIF),
        "webhook_notifications": {},
    }
    cleanup_task = _make_task(task_key="cleanup",
                              notebook_path=f"{aug}/dbt/competitors/fabric/teardown_fabric",
                              base_params=_COMMON_PARAMS)
    cleanup_task["depends_on"] = [{"task_key": GATE, "outcome": "true"}]

    return {
        "name": job_name,
        "description": (f"TPC-DI Augmented Incremental (Fabric DW, parent) SF={scale_factor}. "
                        f"setup_fabric (ensure OneLake staging, cross-DB CTAS the clustered "
                        f"baseline into [{wh_db}_{scale_factor}], pre-create bronze) -> "
                        f"for_each day -> gated cleanup. All tasks on a classic cluster with "
                        f"the msodbcsql18 init script."),
        "tags": {"data_generator": "spark", "engine": "fabric_dw"},
        "timeout_seconds": 0,
        "max_concurrent_runs": 1,
        "job_clusters": [_job_cluster(repo_src_path=repo_src_path, spark_version=spark_version,
                                      node_type_id=node_type_id, single_user_name=single_user_name)],
        "parameters": [
            {"name": "catalog",                     "default": catalog},
            {"name": "scale_factor",                "default": str(scale_factor)},
            {"name": "tpcdi_directory",             "default": tpcdi_directory},
            {"name": "wh_db",                       "default": wh_db},
            {"name": "fabric_wh_host",              "default": fabric_wh_host},
            {"name": "fabric_wh_name",              "default": fabric_wh_name},
            {"name": "fabric_workspace_id",         "default": fabric_workspace_id},
            {"name": "fabric_lakehouse_id",         "default": fabric_lakehouse_id},
            {"name": "tenant_id",                   "default": tenant_id},
            {"name": "file_ext",                    "default": file_ext},
            {"name": "delete_tables_when_finished", "default": "TRUE"},
            {"name": "incremental_batches_to_run",  "default": "365"},
        ],
        "tasks": [setup_task, loop_task, gate_task, cleanup_task],
        "queue": {"enabled": True},
    }
