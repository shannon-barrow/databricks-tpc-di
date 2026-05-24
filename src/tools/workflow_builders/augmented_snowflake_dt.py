"""Builder for the Augmented Incremental TPC-DI benchmark — Snowflake
Dynamic Tables variant. NO dbt.

This variant tests Snowflake's declarative-pipeline product (Dynamic Tables)
head-to-head against Databricks SDP (Spark Declarative Pipelines). The 16
transformations from the dbt variant are expressed as CREATE OR REPLACE
DYNAMIC TABLE statements that Snowflake auto-refreshes on TARGET_LAG.

See `src/incremental_batches/augmented_incremental/snowflake/DYNAMIC_TABLES_DESIGN.md`
for the architecture rationale and per-model translation notes.

Pre-requisites (one-time, manual, out-of-band — same as the dbt variant):
- Snowflake `TPCDI_SVC` service user with KEY_PAIR auth
- Snowflake `STORAGE INTEGRATION` + `STAGE TPCDI_TEST.{wh_db}_{sf}.TPCDI_STAGE`
- Databricks secret scope `tpcdi_snowflake` (account, user, role, warehouse,
  private_key PEM)
- `STAGING_SF{sf}` schema self-bootstrapped by `setup_sf_dt` via `sf_staging_bootstrap`
- UC external volume `main.tpcdi_raw_data.tpcdi_benchmarking`

What's different from `augmented_snowflake.py`:
- `setup_sf` → `setup_sf_dt` (clones reference tables + creates bronze_raw
  + creates all 16 dynamic tables in one shot)
- Per-batch child job tasks: `simulate_filedrops_sf` → `seed_raw` →
  `dt_wait_refresh` (no `run_dbt`)
- No `dbt_project_dir` parameter; no profiles.yml dance
- New parameter `target_lag` for the leaf gold DTs (intermediates use
  DOWNSTREAM and don't need a setting)

Two builders here:
- ``build_child(...)``  — 2-task per-date job: simulate_filedrops_sf, seed_raw, dt_wait_refresh
- ``build_parent(...)`` — wrapper: setup_sf_dt + for_each over child per day
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

# Shared per-task parameters. snowflake_warehouse is the DT refresh warehouse
# (every DT runs on this) — overridable per run-now to test XS/S/M/L sizing.
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
        f"TPC-DI Augmented Incremental benchmark — Snowflake **Dynamic Tables** "
        f"(child) at SF={scale_factor}. Triggered once per simulated business "
        f"day by the parent. Each run: (1) simulate_filedrops_sf copies the "
        f"day's .txt files into the UC external volume; (2) seed_raw COPY-INTOs "
        f"those files into the `bronze*_raw` regular tables on Snowflake; "
        f"(3) dt_wait_refresh triggers `ALTER DYNAMIC TABLE … REFRESH` on the "
        f"leaf gold DTs and polls until the refresh cascade settles. "
        f"Models land in `{catalog}.{wh_db}_{scale_factor}` — same target as "
        f"the dbt variant for parity comparison."
    )


def _description_parent(*, scale_factor: int, catalog: str, wh_db: str) -> str:
    return (
        f"TPC-DI Augmented Incremental benchmark — Snowflake **Dynamic Tables** "
        f"(parent) at SF={scale_factor}. Sequence: (1) `setup_sf_dt` clones "
        f"reference tables, pre-creates bronze_raw, and runs `dt_create.sql` "
        f"to declare all 16 dynamic tables; (2) `loop_incremental_tpcdi` "
        f"for_each-loops the child job per simulated day. No dbt anywhere — "
        f"the refresh is owned by Snowflake's DT engine."
    )


_SF_WH_BY_SCALE_CEILING: tuple[tuple[int, str], ...] = (
    # (max scale factor inclusive, warehouse name)
    (5_000,  "BARROW_XS_GEN2"),     # SF <=  5k → X-Small
    (10_000, "BARROW_SMALL_GEN2"),  # SF == 10k → Small
    (20_000, "BARROW_MED_GEN2"),    # SF == 20k → Medium
    (40_000, "BARROW_LARGE_GEN2"),  # SF == 40k → Large (one size per doubling)
    (80_000, "BARROW_XL_GEN2"),
)


def _default_sf_warehouse(scale_factor: int) -> str:
    """Default Snowflake warehouse sized to scale factor.

    Anchored at "Medium for SF=20k" (the tuned anchor), each size covers a
    doubling of the SF range above it. SF<=5k all run on X-Small since
    the per-batch workload is tiny.
    """
    for ceiling, name in _SF_WH_BY_SCALE_CEILING:
        if scale_factor <= ceiling:
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
    """Builds the per-date child job spec.

    Three tasks, all pinned to the same interactive cluster:
      1. `simulate_filedrops_sf` — copies day's .txt files into the UC
         external volume (Snowflake reads via storage integration).
      2. `seed_raw` — COPY INTO each bronze*_raw table for the day's files.
      3. `dt_wait_refresh` — trigger REFRESH on the 4 leaf gold DTs and
         poll INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY until done.
    """
    if snowflake_warehouse is None:
        snowflake_warehouse = _default_sf_warehouse(scale_factor)
    aug = f"{repo_src_path}/{_AUG_PATH}"
    tasks = [
        _make_task(
            task_key="simulate_filedrops_sf",
            notebook_path=f"{aug}/snowflake/simulate_filedrops_sf",
            base_params=_BATCHED_PARAMS,
            existing_cluster_id=interactive_cluster_id,
        ),
        _make_task(
            task_key="seed_raw",
            notebook_path=f"{aug}/snowflake/dynamic_tables/seed_raw",
            depends_on=["simulate_filedrops_sf"],
            base_params=_BATCHED_PARAMS,
            existing_cluster_id=interactive_cluster_id,
        ),
        _make_task(
            task_key="dt_wait_refresh",
            notebook_path=f"{aug}/snowflake/dynamic_tables/dt_wait_refresh",
            depends_on=["seed_raw"],
            base_params=_BATCHED_PARAMS,
            existing_cluster_id=interactive_cluster_id,
        ),
    ]

    return {
        "name": job_name,
        "description": _description_child(
            scale_factor=scale_factor, catalog=catalog, wh_db=wh_db),
        "tags": {"data_generator": "spark", "engine": "snowflake_dt"},
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
    snowflake_stage: str = "TPCDI_STAGE",
    secret_scope: str = "tpcdi_snowflake",
    snowflake_warehouse: str | None = None,
    target_lag: str = "1 minute",
    interactive_cluster_id: str | None = None,
    **_unused,
) -> dict:
    """Builds the parent (orchestration + loop wrapper) job spec.

    Three real tasks plus the cleanup pair:
      setup_sf_dt → loop_incremental_tpcdi (for_each) → cleanup (gated)
    """
    if snowflake_warehouse is None:
        snowflake_warehouse = _default_sf_warehouse(scale_factor)
    aug = f"{repo_src_path}/{_AUG_PATH}"

    setup_task = _make_task(
        task_key="setup_sf_dt",
        notebook_path=f"{aug}/snowflake/dynamic_tables/setup_sf_dt",
        base_params={
            **_COMMON_PARAMS,
            "target_lag":                  "{{job.parameters.target_lag}}",
            "incremental_batches_to_run":
                "{{job.parameters.incremental_batches_to_run}}",
            # sibling of setup_sf_dt notebook. `aug` is already passed in
            # with a /Workspace prefix (matches the dbt-snowflake variant's
            # convention) so it's directly usable by Python open() from
            # inside the notebook.
            "dt_create_sql_path":
                f"{aug}/snowflake/dynamic_tables/dt_create.sql",
            # Self-bootstrapping params — setup_sf_dt's first run for a new
            # SF refreshes the catalog integration token + creates the
            # federation. Subsequent runs no-op. Leave empty to skip token
            # refresh (federation must already be live).
            "catalog_integration":
                "{{job.parameters.catalog_integration}}",
            "dbx_pat_secret_key":
                "{{job.parameters.dbx_pat_secret_key}}",
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
        "tags": {"data_generator": "spark", "engine": "snowflake_dt"},
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
            {"name": "target_lag",                  "default": target_lag},
            {"name": "delete_tables_when_finished", "default": "TRUE"},
            {"name": "incremental_batches_to_run",  "default": "365"},
            {"name": "catalog_integration",         "default": "TPCDI_DBX_UC_SF10_INT"},
            {"name": "dbx_pat_secret_key",          "default": "dbx_pat_workspace"},
        ],
        "tasks": [setup_task, loop_task, gate_task, cleanup_task],
        "queue": {"enabled": True},
    }
