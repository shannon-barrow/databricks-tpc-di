"""Builder for the Augmented Incremental TPC-DI benchmark — SDP variant.

The SDP path of the augmented benchmark has compromises vs the Classic
"ideal path" — SDP can't easily switch between bulk-loading historical
data and streaming incrementals in the same pipeline run, so the parent
job swaps the pipeline's library notebook between two versions:

  - `DLT/dlt_historical` — INSERT INTO ONCE flows that read from the
    pre-staged historical schema and bulk-load into the SDP target tables.
  - `DLT/dlt_incremental` — Auto CDC streaming flows that consume the
    Autoloader-fed bronze tables.

The swap is implemented by `DLT/update_pipeline_notebook.py`: it loads
the pipeline spec via the SDK, finds the library notebook entry whose
path ends with the current suffix, rewrites it to the desired suffix,
and PUTs the spec back.

Three builders here:
- ``build_pipeline(...)`` — the SDP pipeline definition
- ``build_child(*, pipeline_id, ...)`` — 2-task per-date job: file drop
  + pipeline run with `full_refresh=False`
- ``build_parent(*, child_job_id, pipeline_id, ...)`` — wrapper that does
  the historical bulk-load, swaps to incremental, then for_each loops
  the child job per date with the cleanup gate at the end

Caller orchestrates: pipeline → child (referencing pipeline_id) → parent
(referencing both ids).
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

_COMMON_PARAMS = {
    "catalog":         "{{job.parameters.catalog}}",
    "scale_factor":    "{{job.parameters.scale_factor}}",
    "tpcdi_directory": "{{job.parameters.tpcdi_directory}}",
    "wh_db":           "{{job.parameters.wh_db}}",
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
    nb: dict[str, Any] = {
        "notebook_path": notebook_path,
        "source": "WORKSPACE",
    }
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


def build_pipeline(
    *,
    name: str,
    catalog: str,
    scale_factor: int,
    tpcdi_directory: str,
    wh_db: str,
    repo_src_path: str,
    **_unused,
) -> dict:
    """Builds the SDP pipeline spec. Library list defaults to the
    incremental notebook — the parent job's `set_pipeline_historical`
    task swaps it to `dlt_historical` for the bulk-load phase, then
    `set_pipeline_incremental` swaps it back before the per-date loop.
    """
    dlt_path = f"{repo_src_path}/{_AUG_PATH}/DLT"
    return {
        "name": name,
        "pipeline_type": "WORKSPACE",
        "allow_duplicate_names": "true",
        "development": False,
        "continuous": False,
        "channel": "PREVIEW",
        "photon": True,
        "serverless": True,
        "catalog": catalog,
        "target": f"{wh_db}_{scale_factor}",
        "data_sampling": False,
        "libraries": [
            {"notebook": {"path": f"{dlt_path}/dlt_ingest_bronze"}},
            {"notebook": {"path": f"{dlt_path}/dlt_incremental"}},
        ],
        "configuration": {
            "scale_factor":    str(scale_factor),
            "tpcdi_directory": tpcdi_directory,
            "wh_db":           wh_db,
            "catalog":         catalog,
        },
    }


def _description_child(*, scale_factor: int, catalog: str, wh_db: str,
                        tpcdi_directory: str) -> str:
    return (
        f"TPC-DI Augmented Incremental benchmark (SDP, child) at "
        f"SF={scale_factor}. Triggered once per simulated business day in "
        f"the 730-day window 2015-07-06 → 2017-07-05 by the parent job's "
        f"for_each_task. Each run drops the day's pre-staged files into "
        f"`{tpcdi_directory}augmented_incremental/_dailybatches/{wh_db}_{scale_factor}/` "
        f"then triggers the SDP pipeline (full_refresh=False) for an "
        f"incremental Auto-CDC update of the dimensional model in "
        f"`{catalog}.{wh_db}_{scale_factor}`."
    )


def _description_parent(*, scale_factor: int, catalog: str, wh_db: str,
                         tpcdi_directory: str) -> str:
    return (
        f"TPC-DI Augmented Incremental benchmark (SDP, **parent**) at "
        f"SF={scale_factor}. Sequence: (1) `setup` shallow-clones the 12 "
        f"static dim tables from "
        f"`{catalog}.tpcdi_incremental_staging_{scale_factor}` into "
        f"`{catalog}.{wh_db}_{scale_factor}`; (2) `set_pipeline_historical` "
        f"swaps the SDP pipeline's library to `dlt_historical`; (3) "
        f"`run_historical_pipeline` runs the pipeline with full_refresh=True "
        f"to bulk-load history; (4) `set_pipeline_incremental` swaps "
        f"back to `dlt_incremental`; (5) `loop_incremental_tpcdi` "
        f"for_each-loops the child job per simulated day. Cleanup gated "
        f"by `delete_tables_when_finished` (default TRUE — set to FALSE if "
        f"you want to inspect tables after the run). Shared `_staging/sf={scale_factor}/` "
        f"is preserved across runs."
    )


def build_child(
    *,
    job_name: str,
    pipeline_id: str,
    repo_src_path: str,
    catalog: str,
    scale_factor: int,
    tpcdi_directory: str,
    wh_db: str,
    simulate_filedrops_cluster_id: str | None = None,
    **_unused,
) -> dict:
    """Builds the inner per-date SDP benchmark job spec.

    Two tasks: `simulate_filedrops` then a pipeline_task that runs the
    SDP pipeline with `full_refresh=False` (incremental update).
    """
    aug = f"{repo_src_path}/{_AUG_PATH}"
    tasks = [
        _make_task(
            task_key="simulate_filedrops",
            notebook_path=f"{aug}/simulate_filedrops",
            base_params=_BATCHED_PARAMS,
            existing_cluster_id=simulate_filedrops_cluster_id,
        ),
        {
            "task_key": "DLT_Pipeline_Incremental_TPCDI",
            "depends_on": [{"task_key": "simulate_filedrops"}],
            "run_if": "ALL_SUCCESS",
            "pipeline_task": {
                "pipeline_id": str(pipeline_id),
                "full_refresh": False,
            },
            "timeout_seconds": 0,
            "email_notifications": {"no_alert_for_skipped_runs": False},
        },
    ]

    return {
        "name": job_name,
        "description": _description_child(
            scale_factor=scale_factor, catalog=catalog,
            wh_db=wh_db, tpcdi_directory=tpcdi_directory),
        "tags": {"data_generator": "spark"},
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
        "tasks": tasks,
        "queue": {"enabled": True},
    }


def build_parent(
    *,
    job_name: str,
    child_job_id: int,
    pipeline_id: str,
    catalog: str,
    scale_factor: int,
    tpcdi_directory: str,
    wh_db: str,
    repo_src_path: str,
    setup_cluster_id: str | None = None,
    **_unused,
) -> dict:
    """Builds the parent (orchestration + loop wrapper) job spec.

    Six tasks plus the cleanup pair: setup → set_historical →
    run_historical_pipeline → set_incremental → for_each loop → gate →
    cleanup.
    """
    aug = f"{repo_src_path}/{_AUG_PATH}"
    update_nb = f"{aug}/DLT/update_pipeline_notebook"

    setup_task = _make_task(
        task_key="setup",
        notebook_path=f"{aug}/DLT/pipelines_setup",
        base_params={
            **_COMMON_PARAMS,
            "incremental_batches_to_run":
                "{{job.parameters.incremental_batches_to_run}}",
        },
        existing_cluster_id=setup_cluster_id,
    )

    set_historical = _make_task(
        task_key="set_pipeline_historical",
        notebook_path=update_nb,
        depends_on=["setup"],
        base_params={
            "pipeline_id": str(pipeline_id),
            "historical_flag": "true",
        },
        existing_cluster_id=setup_cluster_id,
    )

    run_historical = {
        "task_key": "run_historical_pipeline",
        "depends_on": [{"task_key": "set_pipeline_historical"}],
        "run_if": "ALL_SUCCESS",
        "pipeline_task": {
            "pipeline_id": str(pipeline_id),
            "full_refresh": True,
        },
        "timeout_seconds": 0,
        "email_notifications": {"no_alert_for_skipped_runs": False},
    }

    set_incremental = _make_task(
        task_key="set_pipeline_incremental",
        notebook_path=update_nb,
        depends_on=["run_historical_pipeline"],
        base_params={
            "pipeline_id": str(pipeline_id),
            "historical_flag": "false",
        },
        existing_cluster_id=setup_cluster_id,
    )

    loop_task: dict[str, Any] = {
        "task_key": "loop_incremental_tpcdi",
        "depends_on": [{"task_key": "set_pipeline_incremental"}],
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
    cleanup_task = _make_task(
        task_key="cleanup",
        notebook_path=f"{aug}/teardown",
        base_params=_COMMON_PARAMS,
        existing_cluster_id=setup_cluster_id,
    )
    cleanup_task["depends_on"] = [{"task_key": GATE, "outcome": "true"}]

    return {
        "name": job_name,
        "description": _description_parent(
            scale_factor=scale_factor, catalog=catalog,
            wh_db=wh_db, tpcdi_directory=tpcdi_directory),
        "tags": {"data_generator": "spark"},
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
        "tasks": [setup_task, set_historical, run_historical, set_incremental,
                  loop_task, gate_task, cleanup_task],
        "queue": {"enabled": True},
    }
