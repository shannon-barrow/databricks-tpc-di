"""Builder for the Augmented Incremental TPC-DI benchmark — DBT variant.

Same shape as `augmented_classic.py`/`augmented_sdp.py`: a parent job loops
a child benchmark job per simulated business day. The child runs two
tasks per iteration:

  1. ``simulate_filedrops``  — drops the day's pre-staged CSV files into
     the Auto-Loader-style watch directory.
  2. ``dbt_run``             — Databricks-native dbt task that executes
     the dbt project at ``src/dbt_augmented_incremental/``, passing
     ``batch_date`` and the run-level vars. The dbt project's model DAG
     (bronze → silver → gold) handles the rest.

A dedicated DBSQL warehouse must be passed in (``wh_id``) — that's where
the dbt commands run. The dbt task type auto-generates a profiles.yml
using OAuth on behalf of the run-as user, so no profile credentials are
needed in the project.

Two builders here mirror the SDP/Classic pattern:
  - ``build_child(...)``  — the inner per-day benchmark job (2 tasks)
  - ``build_parent(...)`` — the wrapper that loops it via for_each_task
"""
from __future__ import annotations

from typing import Any


_DEFAULT_NOTIF = {
    "no_alert_for_skipped_runs": False,
    "no_alert_for_canceled_runs": False,
    "alert_on_last_attempt": False,
}

_RETRY_POLICY = {
    "max_retries": 3,
    "min_retry_interval_millis": 15000,
    "retry_on_timeout": True,
}

_AUG_PATH = "incremental_batches/augmented_incremental"
_DBT_PROJECT_RELPATH = "dbt_augmented_incremental"

_COMMON_PARAMS = {
    "catalog":         "{{job.parameters.catalog}}",
    "scale_factor":    "{{job.parameters.scale_factor}}",
    "tpcdi_directory": "{{job.parameters.tpcdi_directory}}",
    "wh_db":           "{{job.parameters.wh_db}}",
}
_BATCHED_PARAMS = dict(_COMMON_PARAMS, batch_date="{{job.parameters.batch_date}}")


def _make_notebook_task(
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


def _dbt_run_command() -> str:
    """Per-batch dbt invocation. The five vars match the project's
    ``dbt_project.yml`` defaults; Databricks substitutes the
    ``{{job.parameters.X}}`` placeholders before the shell sees them.
    Single-quoted JSON payload keeps the shell from interpreting the
    embedded double-quotes / colons."""
    return (
        'dbt run --vars \'{'
        '"batch_date": "{{job.parameters.batch_date}}", '
        '"scale_factor": "{{job.parameters.scale_factor}}", '
        '"wh_db": "{{job.parameters.wh_db}}", '
        '"tpcdi_directory": "{{job.parameters.tpcdi_directory}}", '
        '"catalog": "{{job.parameters.catalog}}"'
        '}\''
    )


def build_child(
    *,
    job_name: str,
    catalog: str,
    scale_factor: int,
    tpcdi_directory: str,
    wh_db: str,
    repo_src_path: str,
    wh_id: str,
    simulate_filedrops_cluster_id: str | None = None,
    **_unused,
) -> dict:
    """Inner per-batch_date job: simulate_filedrops → dbt_run.

    Args:
        repo_src_path: Workspace absolute path up to (but not including)
            ``dbt_augmented_incremental/`` — same convention as the other
            augmented builders.
        wh_id: DBSQL warehouse id for the dbt task. Must already exist.
        simulate_filedrops_cluster_id: Optional warm interactive cluster
            for the file-copy task; None → serverless.
    """
    aug = f"{repo_src_path}/{_AUG_PATH}"
    dbt_project_dir = f"{repo_src_path}/{_DBT_PROJECT_RELPATH}"

    simulate_task = _make_notebook_task(
        task_key="simulate_filedrops",
        notebook_path=f"{aug}/simulate_filedrops",
        base_params=_BATCHED_PARAMS,
        existing_cluster_id=simulate_filedrops_cluster_id,
    )

    dbt_task: dict[str, Any] = {
        "task_key": "dbt_run",
        "depends_on": [{"task_key": "simulate_filedrops"}],
        "run_if": "ALL_SUCCESS",
        "dbt_task": {
            "project_directory": dbt_project_dir,
            "commands": [_dbt_run_command()],
            "schema": f"{wh_db}_{scale_factor}",
            "warehouse_id": wh_id,
            "catalog": catalog,
            "source": "WORKSPACE",
        },
        "timeout_seconds": 0,
        "email_notifications": {},
        "notification_settings": dict(_DEFAULT_NOTIF),
        "webhook_notifications": {},
        **_RETRY_POLICY,
    }

    desc = (
        f"TPC-DI Augmented Incremental benchmark (DBT, child) at "
        f"SF={scale_factor}. Triggered once per simulated business day in "
        f"the 730-day window 2015-07-06 → 2017-07-05 by the parent's "
        f"for_each_task. Each run: drops the day's pre-staged files into "
        f"`{tpcdi_directory}augmented_incremental/_dailybatches/{wh_db}_{scale_factor}/`, "
        f"then a dbt task runs the 15 incremental models (7 bronze + 4 "
        f"silver + 4 gold) against warehouse {wh_id}. Materializes "
        f"`{catalog}.{wh_db}_{scale_factor}`."
    )

    return {
        "name": job_name,
        "description": desc,
        "tags": {"data_generator": "spark", "engine": "dbt"},
        "email_notifications": {"no_alert_for_skipped_runs": False},
        "webhook_notifications": {},
        "timeout_seconds": 0,
        "max_concurrent_runs": 1000,
        "performance_target": "PERFORMANCE_OPTIMIZED",
        "parameters": [
            {"name": "catalog", "default": catalog},
            {"name": "scale_factor", "default": str(scale_factor)},
            {"name": "tpcdi_directory", "default": tpcdi_directory},
            {"name": "wh_db", "default": wh_db},
            {"name": "batch_date", "default": ""},
        ],
        "tasks": [simulate_task, dbt_task],
        "queue": {"enabled": True},
    }


def build_parent(
    *,
    job_name: str,
    child_job_id: int,
    catalog: str,
    scale_factor: int,
    tpcdi_directory: str,
    wh_db: str,
    repo_src_path: str,
    wh_id: str,
    setup_cluster_id: str | None = None,
    **_unused,
) -> dict:
    """Wrapper job: setup_dbt → for_each(child) → gate → cleanup."""
    aug = f"{repo_src_path}/{_AUG_PATH}"

    setup_task = _make_notebook_task(
        task_key="setup",
        notebook_path=f"{aug}/setup_dbt",
        base_params={
            **_COMMON_PARAMS,
            "incremental_batches_to_run":
                "{{job.parameters.incremental_batches_to_run}}",
        },
        existing_cluster_id=setup_cluster_id,
    )

    loop_task: dict[str, Any] = {
        "task_key": "loop_incremental_tpcdi",
        "depends_on": [{"task_key": "setup"}],
        "run_if": "ALL_SUCCESS",
        "for_each_task": {
            "inputs": "{{tasks.setup.values.batch_date_ls}}",
            "task": {
                "task_key": "loop_incremental_tpcdi_iteration",
                "run_if": "ALL_SUCCESS",
                "run_job_task": {
                    "job_id": child_job_id,
                    "job_parameters": {
                        "catalog":         "{{job.parameters.catalog}}",
                        "scale_factor":    "{{job.parameters.scale_factor}}",
                        "tpcdi_directory": "{{job.parameters.tpcdi_directory}}",
                        "wh_db":           "{{job.parameters.wh_db}}",
                        "batch_date":      "{{input}}",
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
    cleanup_task = _make_notebook_task(
        task_key="cleanup",
        notebook_path=f"{aug}/teardown",
        base_params=_COMMON_PARAMS,
        existing_cluster_id=setup_cluster_id,
    )
    cleanup_task["depends_on"] = [{"task_key": GATE, "outcome": "true"}]
    cleanup_task["run_if"] = "ALL_SUCCESS"

    desc = (
        f"TPC-DI Augmented Incremental benchmark (DBT, **parent**) at "
        f"SF={scale_factor}. Wraps the child benchmark job in a 730-day "
        f"for_each_task loop. setup_dbt clones the staging schema "
        f"(12 shallow + 6 deep) and emits the date list; the child runs "
        f"per simulated day on warehouse {wh_id}. Cleanup gated by "
        f"`delete_tables_when_finished` (default TRUE — drops the run's "
        f"schema and Autoloader directories on completion; set FALSE if "
        f"you want to inspect tables after the run)."
    )

    return {
        "name": job_name,
        "description": desc,
        "tags": {"data_generator": "spark", "engine": "dbt"},
        "email_notifications": {"no_alert_for_skipped_runs": False},
        "webhook_notifications": {},
        "timeout_seconds": 0,
        "max_concurrent_runs": 1,
        "performance_target": "PERFORMANCE_OPTIMIZED",
        "parameters": [
            {"name": "catalog", "default": catalog},
            {"name": "scale_factor", "default": str(scale_factor)},
            {"name": "tpcdi_directory", "default": tpcdi_directory},
            {"name": "wh_db", "default": wh_db},
            {"name": "delete_tables_when_finished", "default": "TRUE"},
            {"name": "incremental_batches_to_run", "default": "730"},
        ],
        "tasks": [setup_task, loop_task, gate_task, cleanup_task],
        "queue": {"enabled": True},
    }
