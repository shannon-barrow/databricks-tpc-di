"""Builder for the Augmented Incremental TPC-DI benchmark — Classic variant.

The "ideal path" for the augmented benchmark: a wrapper PARENT job runs
setup once, then loops the CHILD benchmark job per date via a
for_each_task over a 730-day list, with an opt-in cleanup gate at the end.

The augmented benchmark reshapes TPC-DI from the standard 3-batch bulk
load into a 730-day streaming pipeline (2015-07-06 → 2017-07-05). Each
iteration: simulate_filedrops drops one day's pre-staged files into an
Autoloader watch directory, then 7 bronze ingest tasks fan out, then
silver/gold incremental MERGEs follow the dimensional dependency chain.

Two builders here:
- ``build_child(...)`` — the inner benchmark job (17 tasks)
- ``build_parent(*, child_job_id, ...)`` — the wrapper that loops it

Caller orchestrates: create child first to get its job_id, then parent
referencing it.
"""
from __future__ import annotations

from typing import Any


_DEFAULT_NOTIF = {
    "no_alert_for_skipped_runs": False,
    "no_alert_for_canceled_runs": False,
    "alert_on_last_attempt": False,
}

# Match the retry policy the existing production job uses on every task.
_RETRY_POLICY = {
    "max_retries": 3,
    "min_retry_interval_millis": 15000,
    "retry_on_timeout": True,
}

# Bronze ingestion is parameterized by `Table` — same notebook
# (`bronze/ingest_bronze`) handles all 7. Names match the bronze table
# names (e.g. "account" → bronzeaccount table) and the staged file names
# under `_dailybatches/{wh_db}_{sf}/{date}/Account.txt` etc.
_BRONZE_TABLES = ["account", "cashtransaction", "customer", "dailymarket",
                  "holdings", "trade", "watches"]

# Workspace path prefix shared by all augmented_incremental notebooks.
_AUG_PATH = "incremental_batches/augmented_incremental"

# Standard set of params every augmented notebook reads. The job declares
# these at the top level; for tasks driven by the parent's for_each loop,
# `batch_date` is overridden per iteration via {{input}}.
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
    """Standard notebook-task envelope. `existing_cluster_id` (when
    provided) pins the task to a warm interactive cluster — used for
    `simulate_filedrops` and the lightweight setup/teardown tasks so the
    per-iteration cluster startup tax doesn't dominate over 730 loops.
    Leaving it None falls back to serverless (perf-optimized at the job
    level)."""
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


def _description_child(*, scale_factor: int, catalog: str, wh_db: str,
                        tpcdi_directory: str) -> str:
    return (
        f"TPC-DI Augmented Incremental benchmark (Cluster, child) at "
        f"SF={scale_factor}. Triggered once per simulated business day in "
        f"the 730-day window 2015-07-06 → 2017-07-05 by the parent job's "
        f"for_each_task. Each run: drops the day's pre-staged files into "
        f"`{tpcdi_directory}augmented_incremental/_dailybatches/{wh_db}_{scale_factor}/` "
        f"for Autoloader, fans out 7 bronze ingestion tasks, then runs "
        f"silver/gold MERGE incrementals along the dimensional dependency "
        f"chain. Materializes `{catalog}.{wh_db}_{scale_factor}`."
    )


def _description_parent(*, scale_factor: int, catalog: str, wh_db: str,
                         tpcdi_directory: str) -> str:
    return (
        f"TPC-DI Augmented Incremental benchmark (Cluster, **parent**) at "
        f"SF={scale_factor}. Wraps the child benchmark job in a 730-day "
        f"for_each_task loop. Setup deep/shallow-clones from the shared "
        f"staging schema `{catalog}.tpcdi_incremental_staging_{scale_factor}` "
        f"(populated once-per-SF by Phase B file-splitting tools), then "
        f"the child runs per simulated day. Final cleanup is gated by the "
        f"`delete_tables_when_finished` job parameter — set to `TRUE` to "
        f"drop the run's schema and Autoloader directories on completion. "
        f"Default is `FALSE` because a full 730-day run takes ~a week and "
        f"users typically want to inspect the result tables. The shared "
        f"`_staging/sf={scale_factor}/` data is preserved across runs."
    )


def build_child(
    *,
    job_name: str,
    catalog: str,
    scale_factor: int,
    tpcdi_directory: str,
    wh_db: str,
    repo_src_path: str,
    simulate_filedrops_cluster_id: str | None = None,
    **_unused,
) -> dict:
    """Builds the inner per-date benchmark job spec.

    Args:
        job_name: Final job name (Driver builds it as
            ``{base}-SF{sf}-AugmentedIncremental-Cluster``).
        simulate_filedrops_cluster_id: Optional warm interactive cluster id
            to pin `simulate_filedrops` to. None ⇒ runs on serverless. The
            production pattern keeps this on a dedicated warm cluster so
            (a) per-iteration startup tax is amortized over 730 loops and
            (b) the non-benchmark file-copy cost is isolated from
            benchmark compute for clean cost attribution.
    """
    aug = f"{repo_src_path}/{_AUG_PATH}"
    bronze_path = f"{aug}/bronze/ingest_bronze"
    incr = f"{aug}/incremental"

    tasks: list[dict] = []

    # 1. simulate_filedrops — drops the day's files into Autoloader watch
    tasks.append(_make_task(
        task_key="simulate_filedrops",
        notebook_path=f"{aug}/simulate_filedrops",
        base_params=_BATCHED_PARAMS,
        existing_cluster_id=simulate_filedrops_cluster_id,
    ))

    # 2. Bronze ingestion — 7 tasks fan out in parallel after simulate_filedrops
    for tbl in _BRONZE_TABLES:
        tasks.append(_make_task(
            task_key=f"bronze{tbl}",
            notebook_path=bronze_path,
            depends_on=["simulate_filedrops"],
            base_params=dict(_COMMON_PARAMS, Table=tbl),
        ))

    # 3. account_updates_from_customer — derives account-side updates from
    #    bronzecustomer (cross-table edge); append-writes to bronzeaccount.
    tasks.append(_make_task(
        task_key="account_updates_from_customer",
        notebook_path=f"{aug}/bronze/account_updates_from_customer",
        depends_on=["bronzecustomer"],
        base_params=_COMMON_PARAMS,
    ))

    # 4. Dim chain (sequential): DimCustomer → DimAccount → DimTrade
    tasks.append(_make_task(
        task_key="DimCustomer_Incremental",
        notebook_path=f"{incr}/DimCustomer Incremental",
        depends_on=["bronzecustomer"],
        base_params=_COMMON_PARAMS,
    ))
    tasks.append(_make_task(
        task_key="DimAccount_Incremental",
        notebook_path=f"{incr}/DimAccount Incremental",
        depends_on=["bronzeaccount", "account_updates_from_customer",
                    "DimCustomer_Incremental"],
        base_params=_COMMON_PARAMS,
    ))
    tasks.append(_make_task(
        task_key="DimTrade_Incremental",
        notebook_path=f"{incr}/DimTrade Incremental",
        depends_on=["DimAccount_Incremental", "bronzetrade"],
        base_params=_COMMON_PARAMS,
    ))

    # 5. currentaccountbalances — running-balance staging intermediate
    tasks.append(_make_task(
        task_key="currentaccountbalances_Incremental",
        notebook_path=f"{incr}/currentaccountbalances Incremental",
        depends_on=["bronzecashtransaction"],
        base_params=_COMMON_PARAMS,
    ))

    # 6. Fact tables (parallel after their respective dim deps)
    tasks.append(_make_task(
        task_key="FactCashBalances_Incremental",
        notebook_path=f"{incr}/FactCashBalances Incremental",
        depends_on=["currentaccountbalances_Incremental",
                    "DimAccount_Incremental"],
        base_params=_COMMON_PARAMS,
    ))
    tasks.append(_make_task(
        task_key="FactMarketHistory_Incremental",
        notebook_path=f"{incr}/FactMarketHistory Incremental",
        depends_on=["bronzedailymarket"],
        base_params=_BATCHED_PARAMS,  # 52-week lookback needs the date
    ))
    tasks.append(_make_task(
        task_key="FactHoldings_Incremental",
        notebook_path=f"{incr}/FactHoldings Incremental",
        depends_on=["bronzeholdings", "DimTrade_Incremental"],
        base_params=_BATCHED_PARAMS,  # event_dt derived from trade close date
    ))
    tasks.append(_make_task(
        task_key="FactWatches_Incremental",
        notebook_path=f"{incr}/FactWatches Incremental",
        depends_on=["bronzewatches", "DimCustomer_Incremental"],
        base_params=_COMMON_PARAMS,
    ))

    workflow: dict[str, Any] = {
        "name": job_name,
        "description": _description_child(
            scale_factor=scale_factor, catalog=catalog,
            wh_db=wh_db, tpcdi_directory=tpcdi_directory),
        "tags": {"data_generator": "spark"},
        "email_notifications": {"no_alert_for_skipped_runs": False},
        "webhook_notifications": {},
        "timeout_seconds": 0,
        # Parent's for_each fans out per-date child runs; high cap allows
        # manual debug runs to coexist with an in-flight loop.
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
    return workflow


def build_parent(
    *,
    job_name: str,
    child_job_id: int,
    catalog: str,
    scale_factor: int,
    tpcdi_directory: str,
    wh_db: str,
    repo_src_path: str,
    setup_cluster_id: str | None = None,
    **_unused,
) -> dict:
    """Builds the parent (loop wrapper) job spec.

    Args:
        child_job_id: id of the child benchmark job — caller creates the
            child first so this is available.
        setup_cluster_id: Optional warm cluster id for the setup +
            cleanup tasks (lightweight; production uses an interactive
            cluster). None ⇒ serverless.
    """
    aug = f"{repo_src_path}/{_AUG_PATH}"

    setup_task = _make_task(
        task_key="setup",
        notebook_path=f"{aug}/setup",
        base_params=_COMMON_PARAMS,
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

    # Cleanup gate + cleanup pair. ALL_DONE on the gate so partial-failure
    # runs still reach it; outcome=true on cleanup so it skips entirely
    # (no compute spin-up) when the user keeps `delete_tables_when_finished`
    # at its default of FALSE.
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
    cleanup_task["run_if"] = "ALL_SUCCESS"

    workflow: dict[str, Any] = {
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
            {"name": "delete_tables_when_finished", "default": "FALSE"},
        ],
        "tasks": [setup_task, loop_task, gate_task, cleanup_task],
        "queue": {"enabled": True},
    }
    return workflow
