"""Builder for the Augmented Incremental TPC-DI benchmark — BigQuery variant.

The BQ variant of the augmented benchmark runs the same dbt project the
Databricks + Snowflake variants use, but with `--target bigquery`. The
compute that runs the models is BigQuery (serverless slot-based); Databricks
just orchestrates and writes the per-batch files into a UC external volume
backed by GCS — BigQuery reads the same bytes via external tables CREATE OR
REPLACEd per batch.

Pre-requisites (one-time, manual, out-of-band):
- GCS bucket `gs://shannon-tpcdi/tpcdi/` in `us-central1`
- UC external volume `main.tpcdi_raw_data.tpcdi_volume` backed by that bucket
- BQ project `databricks-sandbox-perfeng` (or override via the `catalog`
  job parameter)
- Databricks secret scope `tpcdi_bigquery` with key `sa_json` containing a
  service-account key JSON with BigQuery Data Editor + Job User on the
  target project
- Phase B already run for this SF (seeds `tpcdi_staging_sf{N}`), OR
  set up to self-bootstrap from setup_bq.py (it falls back to
  seed_staging_py if the staging dataset is missing/incomplete)
- Interactive cluster with `dbt-bigquery==1.9.*` + `dbt-core==1.9.*`
  pre-installed as libraries (defensive pip-install in run_dbt.py as backup)

Two builders here:
- ``build_child(...)`` — 2-task per-date job: simulate_filedrops_bq then
  dbt_run, both on the same interactive cluster
- ``build_parent(...)`` — wrapper: setup_bq then for_each loop over the
  child job per simulated day, gated cleanup at the end

Mirrors `augmented_snowflake.py` 1:1 in shape — the workflow_type dispatch
in `generate_benchmark_workflow.py` only differs in which `_AUG_PATH/bigquery`
notebooks are referenced and which job params get plumbed.
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

# Job parameters every per-batch task needs. BQ has no warehouse-sizing
# knob (serverless slots); the only optional override is the GCS volume
# prefix in case a different bucket gets used later.
_COMMON_PARAMS = {
    "catalog":           "{{job.parameters.catalog}}",
    "scale_factor":      "{{job.parameters.scale_factor}}",
    "tpcdi_directory":   "{{job.parameters.tpcdi_directory}}",
    "wh_db":             "{{job.parameters.wh_db}}",
    "secret_scope":      "{{job.parameters.secret_scope}}",
    "bq_location":       "{{job.parameters.bq_location}}",
    "gcs_volume_prefix": "{{job.parameters.gcs_volume_prefix}}",
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
        f"TPC-DI Augmented Incremental benchmark (BigQuery, **child**) "
        f"at SF={scale_factor}. Triggered once per simulated business day "
        f"by the parent job's for_each_task. Each run drops the day's "
        f"pre-staged files into "
        f"`{tpcdi_directory}augmented_incremental/_dailybatches/{wh_db}_{scale_factor}/` "
        f"(UC external volume backed by GCS), refreshes 7 BQ external tables "
        f"pointing at that directory, then runs dbt against "
        f"`--target bigquery` for that batch date. Models land in "
        f"`{catalog}.{wh_db}_{scale_factor}` on the BigQuery side."
    )


def _description_parent(*, scale_factor: int, catalog: str, wh_db: str,
                         tpcdi_directory: str) -> str:
    return (
        f"TPC-DI Augmented Incremental benchmark (BigQuery, **parent**) "
        f"at SF={scale_factor}. Sequence: (1) `setup_bq` runs on an "
        f"interactive cluster, dispatching SQL to BigQuery: CREATE the "
        f"per-run BQ dataset `{catalog}.{wh_db}_{scale_factor}`, CLONE "
        f"the 22 reference + dimension tables from "
        f"`{catalog}.tpcdi_staging_sf{scale_factor}` (seeded once via "
        f"seed_staging_py — auto-bootstrapped on first run), pre-create "
        f"the 7 bronze target tables; (2) `loop_incremental_tpcdi` "
        f"for_each-loops the child job per simulated day from the emitted "
        f"`batch_date_ls`. Each child runs simulate_filedrops_bq + "
        f"dbt_run on the same interactive cluster. Cleanup gated by "
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
    secret_scope: str = "tpcdi_bigquery",
    bq_location: str = "us-central1",
    gcs_volume_prefix: str = "gs://shannon-tpcdi/tpcdi/",
    interactive_cluster_id: str | None = None,
    **_unused,
) -> dict:
    """Builds the per-date child job spec.

    Two tasks, both pinned to the same interactive cluster:
      1. `simulate_filedrops_bq` — copies day's .txt files into the UC
         external volume (writes via UC; BigQuery reads the same bytes
         via external tables CREATE OR REPLACEd at the end of the same
         notebook)
      2. `dbt_run` — pip-checks dbt-bigquery, writes profiles.yml from
         the secret scope, runs `dbt run --target bigquery --vars {...}`
    """
    aug = f"{repo_src_path}/{_AUG_PATH}"
    tasks = [
        _make_task(
            task_key="simulate_filedrops_bq",
            notebook_path=f"{aug}/bigquery/simulate_filedrops_bq",
            base_params=_BATCHED_PARAMS,
            existing_cluster_id=interactive_cluster_id,
        ),
        _make_task(
            task_key="dbt_run",
            notebook_path=f"{aug}/bigquery/run_dbt",
            depends_on=["simulate_filedrops_bq"],
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
        "tags": {"data_generator": "spark", "engine": "bigquery"},
        "email_notifications": {"no_alert_for_skipped_runs": False},
        "webhook_notifications": {},
        "timeout_seconds": 0,
        "max_concurrent_runs": 1000,
        "performance_target": "PERFORMANCE_OPTIMIZED",
        "parameters": [
            {"name": "catalog",           "default": catalog},
            {"name": "scale_factor",      "default": str(scale_factor)},
            {"name": "tpcdi_directory",   "default": tpcdi_directory},
            {"name": "wh_db",             "default": wh_db},
            {"name": "secret_scope",      "default": secret_scope},
            {"name": "bq_location",       "default": bq_location},
            {"name": "gcs_volume_prefix", "default": gcs_volume_prefix},
            {"name": "batch_date",        "default": ""},
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
    secret_scope: str = "tpcdi_bigquery",
    bq_location: str = "us-central1",
    gcs_volume_prefix: str = "gs://shannon-tpcdi/tpcdi/",
    databricks_catalog: str = "main",
    interactive_cluster_id: str | None = None,
    **_unused,
) -> dict:
    """Builds the parent (orchestration + loop wrapper) job spec.

    Three real tasks plus the cleanup pair:
      setup_bq → loop_incremental_tpcdi (for_each) → cleanup (gated)
    """
    aug = f"{repo_src_path}/{_AUG_PATH}"

    setup_task = _make_task(
        task_key="setup_bq",
        notebook_path=f"{aug}/bigquery/setup_bq",
        base_params={
            **_COMMON_PARAMS,
            "databricks_catalog":
                "{{job.parameters.databricks_catalog}}",
            "incremental_batches_to_run":
                "{{job.parameters.incremental_batches_to_run}}",
        },
        existing_cluster_id=interactive_cluster_id,
    )

    loop_task: dict[str, Any] = {
        "task_key": "loop_incremental_tpcdi",
        "depends_on": [{"task_key": "setup_bq"}],
        "run_if": "ALL_SUCCESS",
        "for_each_task": {
            "inputs": "{{tasks.setup_bq.values.batch_date_ls}}",
            "task": {
                "task_key": "loop_incremental_tpcdi_iteration",
                "run_if": "ALL_SUCCESS",
                "run_job_task": {
                    "job_id": child_job_id,
                    "job_parameters": {
                        "catalog":           "{{job.parameters.catalog}}",
                        "scale_factor":      "{{job.parameters.scale_factor}}",
                        "tpcdi_directory":   "{{job.parameters.tpcdi_directory}}",
                        "wh_db":             "{{job.parameters.wh_db}}",
                        "secret_scope":      "{{job.parameters.secret_scope}}",
                        "bq_location":       "{{job.parameters.bq_location}}",
                        "gcs_volume_prefix": "{{job.parameters.gcs_volume_prefix}}",
                        "batch_date":        "{{input}}",
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
        "tags": {"data_generator": "spark", "engine": "bigquery"},
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
            {"name": "secret_scope",                "default": secret_scope},
            {"name": "bq_location",                 "default": bq_location},
            {"name": "gcs_volume_prefix",           "default": gcs_volume_prefix},
            {"name": "databricks_catalog",          "default": databricks_catalog},
            {"name": "delete_tables_when_finished", "default": "TRUE"},
            {"name": "incremental_batches_to_run",  "default": "365"},
        ],
        "tasks": [setup_task, loop_task, gate_task, cleanup_task],
        "queue": {"enabled": True},
    }
