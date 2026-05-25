"""Builder for the Augmented Incremental TPC-DI benchmark — Snowflake
Dynamic Tables variant with PARALLEL per-leaf branch tasks.

This is a refactor of `augmented_snowflake_dt.py` where the per-batch
child job runs 4 parallel branch tasks (one per gold leaf) instead of one
serialized `seed_raw` + `dt_wait_refresh` pair. Each branch:
  - Loads its own bronze table(s) via COPY INTO on its own Snowflake
    session (thread A independent + thread B chain inside the notebook).
  - Issues `ALTER DYNAMIC TABLE <leaf> REFRESH` after those COPYs converge.

Net effect: per-batch wall clock = max(branch wall), not sum. The DT
graph is the same as the non-parallel variant — only the orchestrator is
restructured.

Setup notebook (`setup_sf_dt`) is unchanged from the non-parallel variant.

Parallel structure:
  parent:
    setup_sf_dt → for_each(batch_date) → child[
      simulate_filedrops_sf
        ├── branch_factmarkethistory
        ├── branch_factwatches
        ├── branch_factcashbalances
        └── branch_factholdings
    ]
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
    "catalog":             "{{job.parameters.catalog}}",
    "scale_factor":        "{{job.parameters.scale_factor}}",
    "wh_db":               "{{job.parameters.wh_db}}",
    "snowflake_stage":     "{{job.parameters.snowflake_stage}}",
    "secret_scope":        "{{job.parameters.secret_scope}}",
    "snowflake_warehouse": "{{job.parameters.snowflake_warehouse}}",
}
_BATCHED_PARAMS = dict(
    _COMMON_PARAMS,
    batch_date="{{job.parameters.batch_date}}",
    tpcdi_directory="{{job.parameters.tpcdi_directory}}",
)

_BRANCH_TASK_KEYS = [
    "branch_factmarkethistory",
    "branch_factwatches",
    "branch_factcashbalances",
    "branch_factholdings",
]


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


def _description_child(*, scale_factor: int, catalog: str, wh_db: str) -> str:
    return (
        f"Snowflake Dynamic Tables variant — per-batch child job (parallel branches) "
        f"at SF={scale_factor}. Triggered once per simulated day by the parent. "
        f"Each run: simulate_filedrops_sf copies the day's .txt files into the "
        f"UC external volume; then 4 branch tasks run in parallel — each one "
        f"COPY-INTOs its required bronze tables and ALTER REFRESHes its leaf DT. "
        f"Per-batch wall = max(branch), not sum."
    )


def _description_parent(*, scale_factor: int, catalog: str, wh_db: str) -> str:
    return (
        f"Snowflake Dynamic Tables variant — orchestration / for_each (parent) "
        f"at SF={scale_factor}, parallel-branch child. Sequence: setup_sf_dt "
        f"clones reference tables + bootstraps federation + creates the 9 DTs "
        f"on the backfill warehouse; then for_each over per-batch dates calls "
        f"the parallel-branch child job."
    )


# Match the warehouse default heuristic of the non-parallel variant.
_SF_WH_BY_SCALE = {
    20000: "BARROW_MED_GEN2",
    10000: "BARROW_SMA_GEN2",
    5000:  "BARROW_XS_GEN2",
    1000:  "BARROW_XS_GEN2",
    100:   "BARROW_XS_GEN2",
    10:    "BARROW_XS_GEN2",
}
def _default_sf_warehouse(scale_factor: int) -> str:
    for sf, name in sorted(_SF_WH_BY_SCALE.items(), reverse=True):
        if scale_factor >= sf:
            return name
    return "BARROW_2XL_GEN2"


def build_child(
    *,
    job_name: str,
    repo_src_path: str,
    catalog: str,
    scale_factor: int,
    tpcdi_directory: str,
    wh_db: str,
    snowflake_stage: str = "TPCDI_STAGE",
    secret_scope: str = "tpcdi_snowflake",
    snowflake_warehouse: str | None = None,
    interactive_cluster_id: str | None = None,
    **_unused,
) -> dict:
    """Per-batch child job — parallel branches.

      simulate_filedrops_sf
        ↓ (each branch depends on simulate_filedrops_sf only)
        branch_factmarkethistory   |
        branch_factwatches         |  ←  4 tasks run in parallel
        branch_factcashbalances    |     at the Databricks Jobs level
        branch_factholdings        |
    """
    if snowflake_warehouse is None:
        snowflake_warehouse = _default_sf_warehouse(scale_factor)
    aug = f"{repo_src_path}/{_AUG_PATH}"

    file_drops = _make_task(
        task_key="simulate_filedrops_sf",
        notebook_path=f"{aug}/snowflake/simulate_filedrops_sf",
        base_params=_BATCHED_PARAMS,
        existing_cluster_id=interactive_cluster_id,
    )

    branches = [
        _make_task(
            task_key=tk,
            notebook_path=f"{aug}/snowflake/dynamic_tables/{tk}",
            depends_on=["simulate_filedrops_sf"],
            base_params=_BATCHED_PARAMS,
            existing_cluster_id=interactive_cluster_id,
        )
        for tk in _BRANCH_TASK_KEYS
    ]

    return {
        "name": job_name,
        "description": _description_child(
            scale_factor=scale_factor, catalog=catalog, wh_db=wh_db),
        "tags": {"data_generator": "spark", "engine": "snowflake_dt_parallel"},
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
            {"name": "snowflake_stage",     "default": snowflake_stage},
            {"name": "secret_scope",        "default": secret_scope},
            {"name": "snowflake_warehouse", "default": snowflake_warehouse},
            {"name": "batch_date",          "default": ""},
        ],
        "tasks": [file_drops, *branches],
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
    snowflake_stage: str = "TPCDI_STAGE",
    secret_scope: str = "tpcdi_snowflake",
    snowflake_warehouse: str | None = None,
    interactive_cluster_id: str | None = None,
    **_unused,
) -> dict:
    """Parent (orchestration + for_each wrapper). Setup notebook is shared
    with the non-parallel variant — the only change is that the for_each
    invokes the parallel-branch child."""
    if snowflake_warehouse is None:
        snowflake_warehouse = _default_sf_warehouse(scale_factor)
    aug = f"{repo_src_path}/{_AUG_PATH}"

    setup_task = _make_task(
        task_key="setup_sf_dt",
        notebook_path=f"{aug}/snowflake/dynamic_tables/setup_sf_dt",
        base_params={
            **_COMMON_PARAMS,
            "incremental_batches_to_run":
                "{{job.parameters.incremental_batches_to_run}}",
            "dt_create_sql_path":
                f"{aug}/snowflake/dynamic_tables/dt_create.sql",
            "catalog_integration":
                "{{job.parameters.catalog_integration}}",
            "dbx_pat_secret_key":
                "{{job.parameters.dbx_pat_secret_key}}",
            "backfill_warehouse":
                "{{job.parameters.backfill_warehouse}}",
        },
        existing_cluster_id=interactive_cluster_id,
    )

    loop_task: dict[str, Any] = {
        "task_key": "loop_incremental_tpcdi",
        "depends_on": [{"task_key": "setup_sf_dt"}],
        "run_if": "ALL_SUCCESS",
        "for_each_task": {
            "inputs": "{{tasks.setup_sf_dt.values.batch_date_ls}}",
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
                        "snowflake_stage":     "{{job.parameters.snowflake_stage}}",
                        "secret_scope":        "{{job.parameters.secret_scope}}",
                        "snowflake_warehouse": "{{job.parameters.snowflake_warehouse}}",
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
        notebook_path=f"{aug}/teardown",
        base_params=_COMMON_PARAMS,
        existing_cluster_id=interactive_cluster_id,
    )
    cleanup_task["depends_on"] = [{"task_key": GATE, "outcome": "true"}]

    return {
        "name": job_name,
        "description": _description_parent(
            scale_factor=scale_factor, catalog=catalog, wh_db=wh_db),
        "tags": {"data_generator": "spark", "engine": "snowflake_dt_parallel"},
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
            {"name": "snowflake_stage",             "default": snowflake_stage},
            {"name": "secret_scope",                "default": secret_scope},
            {"name": "snowflake_warehouse",         "default": snowflake_warehouse},
            {"name": "delete_tables_when_finished", "default": "TRUE"},
            {"name": "incremental_batches_to_run",  "default": "365"},
            {"name": "catalog_integration",         "default": "TPCDI_DBX_UC_SF10_INT"},
            {"name": "dbx_pat_secret_key",          "default": "dbx_pat_workspace"},
            {"name": "backfill_warehouse",          "default": ""},
        ],
        "tasks": [setup_task, loop_task, gate_task, cleanup_task],
        "queue": {"enabled": True},
    }
