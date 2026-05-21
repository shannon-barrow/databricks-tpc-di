"""Builder for the Augmented Incremental TPC-DI benchmark — Snowflake variant.

The Snowflake variant of the augmented benchmark runs the same dbt project
the Databricks dbt variant uses, but with `--target snowflake`. The compute
that runs the models is a Snowflake warehouse; Databricks just orchestrates
and writes the per-batch files into a UC external volume whose underlying
S3 prefix Snowflake reads from via a `STORAGE INTEGRATION` + `STAGE`.

See `src/incremental_batches/augmented_incremental/snowflake/PLAN.md` for
the full architecture.

Pre-requisites (one-time, manual, out-of-band):
- Snowflake `TPCDI_SVC` service user with KEY_PAIR auth policy (see
  ALTER USER + NIKHIL_SURI.PUBLIC.KEY_PAIR policy)
- Snowflake `STORAGE INTEGRATION TPCDI_BENCHMARKING_S3` (or `S3_INT` if
  reusing an existing one) pointing at the target bucket
- `STAGE TPCDI_TEST.{wh_db}_{sf}.TPCDI_STAGE` (or a shared stage name)
  backed by that integration
- One-time `seed_staging.py` run to copy
  `main.tpcdi_incremental_staging_{sf}` → `TPCDI_TEST.STAGING_SF{sf}`
- Databricks secret scope `tpcdi_snowflake` with keys: account, user, role,
  warehouse, private_key (PEM)
- UC external volume `main.tpcdi_raw_data.tpcdi_benchmarking`
  rooted at the bucket Snowflake's stage reads from
- Interactive cluster with `dbt-snowflake==1.9.*` + `dbt-core==1.9.*`
  pre-installed as libraries (defensive pip-install in `dbt_run.py`
  as backup)

Two builders here:
- ``build_child(...)`` — 2-task per-date job: simulate_filedrops_sf then
  dbt_run, both on the same interactive cluster
- ``build_parent(...)`` — wrapper: setup_sf then for_each loop over
  the child job per simulated day, gated cleanup at the end

Same parent_run_id / batch_date plumbing as `augmented_sdp.py` so the
existing dashboard variants CTE can pick up the runs by `child_job_id` +
`parent_run_min_start` with no schema changes.
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

# Job parameters that the setup task emits + every per-batch task needs.
# snowflake_warehouse is a job parameter (not a hardcoded secret) so the
# same job can target XS / S / M / L by overriding it at run-now time
# without changing code or secrets. Default is XS — fine for SF=10; SF=20k
# benchmark runs want Small or larger.
_COMMON_PARAMS = {
    "catalog":                   "{{job.parameters.catalog}}",
    "scale_factor":              "{{job.parameters.scale_factor}}",
    "tpcdi_directory":           "{{job.parameters.tpcdi_directory}}",
    "wh_db":                     "{{job.parameters.wh_db}}",
    "snowflake_stage":           "{{job.parameters.snowflake_stage}}",
    "secret_scope":              "{{job.parameters.secret_scope}}",
    "snowflake_warehouse":       "{{job.parameters.snowflake_warehouse}}",
    "snowflake_warehouse_setup": "{{job.parameters.snowflake_warehouse_setup}}",
    "table_format":              "{{job.parameters.table_format}}",
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


def _description_child(*, scale_factor: int, catalog: str, wh_db: str,
                        tpcdi_directory: str) -> str:
    return (
        f"TPC-DI Augmented Incremental benchmark (Snowflake, **child**) "
        f"at SF={scale_factor}. Triggered once per simulated business day "
        f"by the parent job's for_each_task. Each run drops the day's "
        f"pre-staged files into "
        f"`{tpcdi_directory}augmented_incremental/_dailybatches/{wh_db}_{scale_factor}/` "
        f"(UC external volume, same prefix Snowflake's STAGE reads from) "
        f"and then runs dbt against `--target snowflake` for that batch "
        f"date. Models land in `TPCDI_TEST.{wh_db}_{scale_factor}` on the "
        f"Snowflake side."
    )


def _description_parent(*, scale_factor: int, catalog: str, wh_db: str,
                         tpcdi_directory: str) -> str:
    return (
        f"TPC-DI Augmented Incremental benchmark (Snowflake, **parent**) "
        f"at SF={scale_factor}. Sequence: (1) `setup_sf` runs on an "
        f"interactive cluster, dispatching SQL to Snowflake: "
        f"CREATE SCHEMA `TPCDI_TEST.{wh_db}_{scale_factor}`, CLONE the 12 "
        f"reference + dimension tables from `TPCDI_TEST.STAGING_SF{scale_factor}` "
        f"(seeded once via `seed_staging.py`), pre-create the 16 "
        f"bronze/silver/gold target tables; (2) `loop_incremental_tpcdi` "
        f"for_each-loops the child job per simulated day from the emitted "
        f"`batch_date_ls`. Each child runs simulate_filedrops_sf + dbt_run "
        f"on the same interactive cluster. Cleanup gated by "
        f"`delete_tables_when_finished` (default TRUE)."
    )


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
    snowflake_warehouse: str = "BARROW_XS_GEN2",
    snowflake_warehouse_setup: str = "",
    table_format: str = "snowflake_native",
    interactive_cluster_id: str | None = None,
    **_unused,
) -> dict:
    """Builds the per-date child job spec.

    Two tasks, both pinned to the same interactive cluster:
      1. `simulate_filedrops_sf` — copies day's .txt files into the UC
         external volume (writes via UC; Snowflake reads the same bytes
         via storage integration)
      2. `dbt_run` — pip-checks dbt-snowflake, writes profiles.yml from
         the secret scope, runs `dbt run --target snowflake --vars {...}`
    """
    aug = f"{repo_src_path}/{_AUG_PATH}"
    tasks = [
        _make_task(
            task_key="simulate_filedrops_sf",
            notebook_path=f"{aug}/snowflake/simulate_filedrops_sf",
            base_params=_BATCHED_PARAMS,
            existing_cluster_id=interactive_cluster_id,
        ),
        _make_task(
            task_key="dbt_run",
            notebook_path=f"{aug}/snowflake/run_dbt",
            depends_on=["simulate_filedrops_sf"],
            base_params=dict(
                _BATCHED_PARAMS,
                dbt_project_dir=f"{aug}/dbt",
            ),
            existing_cluster_id=interactive_cluster_id,
        ),
    ]

    return {
        "name": job_name,
        "description": _description_child(
            scale_factor=scale_factor, catalog=catalog,
            wh_db=wh_db, tpcdi_directory=tpcdi_directory),
        "tags": {"data_generator": "spark", "engine": "snowflake"},
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
            {"name": "snowflake_warehouse",       "default": snowflake_warehouse},
            {"name": "snowflake_warehouse_setup", "default": snowflake_warehouse_setup},
            {"name": "table_format",              "default": table_format},
            {"name": "batch_date",                "default": ""},
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
    snowflake_warehouse: str = "BARROW_XS_GEN2",
    snowflake_warehouse_setup: str = "",
    table_format: str = "snowflake_native",
    interactive_cluster_id: str | None = None,
    **_unused,
) -> dict:
    """Builds the parent (orchestration + loop wrapper) job spec.

    Three real tasks plus the cleanup pair:
      setup_sf → loop_incremental_tpcdi (for_each) → cleanup (gated)
    """
    aug = f"{repo_src_path}/{_AUG_PATH}"

    setup_task = _make_task(
        task_key="setup_sf",
        notebook_path=f"{aug}/snowflake/setup_sf",
        base_params={
            **_COMMON_PARAMS,
            "incremental_batches_to_run":
                "{{job.parameters.incremental_batches_to_run}}",
        },
        existing_cluster_id=interactive_cluster_id,
    )

    loop_task: dict[str, Any] = {
        "task_key": "loop_incremental_tpcdi",
        "depends_on": [{"task_key": "setup_sf"}],
        "run_if": "ALL_SUCCESS",
        "for_each_task": {
            "inputs": "{{tasks.setup_sf.values.batch_date_ls}}",
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
                        "snowflake_warehouse":       "{{job.parameters.snowflake_warehouse}}",
                        "snowflake_warehouse_setup": "{{job.parameters.snowflake_warehouse_setup}}",
                        "table_format":              "{{job.parameters.table_format}}",
                        "batch_date":                "{{input}}",
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
            scale_factor=scale_factor, catalog=catalog,
            wh_db=wh_db, tpcdi_directory=tpcdi_directory),
        "tags": {"data_generator": "spark", "engine": "snowflake"},
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
            {"name": "snowflake_warehouse_setup",   "default": snowflake_warehouse_setup},
            {"name": "table_format",                "default": table_format},
            {"name": "delete_tables_when_finished", "default": "TRUE"},
            {"name": "incremental_batches_to_run",  "default": "365"},
        ],
        "tasks": [setup_task, loop_task, gate_task, cleanup_task],
        "queue": {"enabled": True},
    }
