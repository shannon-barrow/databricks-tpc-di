"""Builder for the Augmented Incremental TPC-DI benchmark — Redshift variant.

Mirrors `augmented_bigquery.py` and `augmented_snowflake.py` 1:1 in shape. The
Redshift variant runs the same dbt project the Databricks/Snowflake/BQ
variants use, but with `--target redshift`. Compute that runs the models is
a Redshift Serverless workgroup; Databricks just orchestrates and writes
per-batch files into a UC external volume backed by S3 (same bucket the
Redshift workgroup is configured to read).

Pre-requisites (one-time, manual, out-of-band):
- S3 bucket `s3://tpcds-datasets/shannon_tpcdi/` in `us-west-2`
- UC external volume `main.tpcdi_raw_data.tpcdi_volume` backed by that bucket
- Redshift Serverless workgroup `xsmall-8rpu-workgroup` (us-west-2)
- IAM role `arn:aws:iam::384416317380:role/tpcds-redshift` attached to the
  workgroup; trust policy allows the workgroup to assume the role; bucket
  policy grants the role s3:Get* on the prefix
- Databricks secret scope `tpcdi_redshift` with keys:
  host, port, database, user, password, iam_role
- `tpcdi_staging_sf{N}` schema seeded in Redshift (one-time, via
  `onetime_stg_rs_tables.py` — paid once per scale_factor)
- Interactive cluster with `dbt-redshift==1.10.0` + `psycopg2-binary`
  pre-installed as libraries (defensive pip-install in tools as backup)

Two builders here:
- ``build_child(...)`` — 2-task per-date job: simulate_filedrops_rs →
  dbt_run, both on the same interactive cluster (bronze COPY runs as a
  dbt pre_hook inside the dbt_run task)
- ``build_parent(...)`` — wrapper: setup_rs then for_each loop over the
  child job per simulated day, gated cleanup at the end

The Redshift child has the SAME 2-task shape as BQ/SF — simulate_filedrops
then dbt_run. Bronze ingestion (COPY from S3) happens INSIDE the dbt run via
`pre_hook`s on the 7 CSV-driven `rs_bronze/*.sql` models — keeps the bronze
read on the same Redshift compute that runs silver+gold, so per-batch cost
attribution is apples-to-apples with the other engines (decision documented
in `incremental_batches/augmented_incremental/redshift/PORT_NOTES.md`).
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

# Job parameters every per-batch task needs. Redshift Serverless has no
# warehouse-sizing knob at the per-task level — workgroup MaxRPU is set at
# the workgroup level out-of-band. The IAM role for COPY comes from the
# secret scope so it doesn't appear here.
_COMMON_PARAMS = {
    "catalog":           "{{job.parameters.catalog}}",
    "database":          "{{job.parameters.database}}",
    "scale_factor":      "{{job.parameters.scale_factor}}",
    "tpcdi_directory":   "{{job.parameters.tpcdi_directory}}",
    "wh_db":             "{{job.parameters.wh_db}}",
    "secret_scope":      "{{job.parameters.secret_scope}}",
    "s3_volume_prefix":  "{{job.parameters.s3_volume_prefix}}",
    "aws_region":        "{{job.parameters.aws_region}}",
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
    environment_key: str | None = None,
) -> dict:
    """Build a notebook task. Pass EXACTLY ONE of existing_cluster_id (pin to
    a classic cluster) or environment_key (run on serverless against a
    job-level `environments` entry). Setup/teardown belong on serverless
    because all the heavy work is dispatched to Redshift — the Spark driver
    only orchestrates. The dbt + simulate_filedrops + load_bronze tasks need
    a classic cluster (dbt-redshift's psycopg2 imports + the
    google.cloud-style pandas issue surface on serverless DBR too)."""
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
    if existing_cluster_id and environment_key:
        raise ValueError(
            f"task {task_key}: pass existing_cluster_id OR environment_key, not both"
        )
    if existing_cluster_id:
        task["existing_cluster_id"] = existing_cluster_id
    elif environment_key:
        task["environment_key"] = environment_key
    task["timeout_seconds"] = 0
    task["email_notifications"] = {}
    task["notification_settings"] = dict(_DEFAULT_NOTIF)
    task["webhook_notifications"] = {}
    task.update(_RETRY_POLICY)
    return task


def _description_child(*, scale_factor: int, database: str, wh_db: str,
                        tpcdi_directory: str) -> str:
    return (
        f"TPC-DI Augmented Incremental benchmark (Redshift, **child**) "
        f"at SF={scale_factor}. Triggered once per simulated business day "
        f"by the parent's for_each_task. Each run: (1) drops the day's "
        f"pre-staged files into "
        f"`{tpcdi_directory}augmented_incremental/_dailybatches/{wh_db}_{scale_factor}/` "
        f"(UC external volume backed by S3), (2) `COPY`s those files into "
        f"the Redshift `_bronze` schema, (3) runs dbt against "
        f"`--target redshift` for that batch_date. Models land in "
        f"`{database}.{wh_db}_{scale_factor}` on the Redshift side."
    )


def _description_parent(*, scale_factor: int, database: str, wh_db: str,
                         tpcdi_directory: str) -> str:
    return (
        f"TPC-DI Augmented Incremental benchmark (Redshift, **parent**) "
        f"at SF={scale_factor}. Sequence: (1) `setup_rs` runs on an "
        f"interactive cluster, dispatching DDL/CTAS to Redshift: "
        f"DROP+CREATE the per-run schema `{wh_db}_{scale_factor}`, "
        f"self-bootstrap `tpcdi_staging_sf{scale_factor}` if missing "
        f"(via rs_staging_bootstrap inline), CTAS the 22 reference + "
        f"dimension tables from staging into the per-run schema; "
        f"(2) `loop_incremental_tpcdi` for_each-loops the child job per "
        f"simulated day from the emitted `batch_date_ls`. Each child runs "
        f"simulate_filedrops_rs + dbt_run (with bronze COPY inside dbt "
        f"pre_hooks). Cleanup gated by `delete_tables_when_finished` "
        f"(default TRUE)."
    )


def build_child(
    *,
    job_name: str,
    repo_src_path: str,
    catalog: str,
    scale_factor: int,
    tpcdi_directory: str,
    wh_db: str,
    database: str = "dev",
    secret_scope: str = "tpcdi_redshift",
    s3_volume_prefix: str = "s3://tpcds-datasets/shannon_tpcdi/",
    aws_region: str = "us-west-2",
    interactive_cluster_id: str | None = None,
    **_unused,
) -> dict:
    """Builds the per-date child job spec.

    Two tasks, both pinned to the same compute target:
      1. `simulate_filedrops_rs` — copies day's .txt files into the UC
         external volume (writes via UC; Redshift reads the same bytes
         via COPY in the bronze pre_hooks)
      2. `dbt_run` — pip-checks dbt-redshift, writes profiles.yml from
         the secret scope, runs `dbt run --target redshift --vars {...}`.
         Each rs_bronze model's pre_hook issues a `COPY ... FROM
         's3://.../{batch_date}/{Dataset}.txt' ... FORMAT AS CSV` into
         a temp table, then the model body appends to the persistent
         bronze table.
    """
    aug = f"{repo_src_path}/{_AUG_PATH}"
    # On tpcdi-fresh the workspace only has serverless compute available
    # (no interactive cluster access). This causes per-batch cold-start
    # variability that pollutes timing — documented in PORT_NOTES.md.
    # `interactive_cluster_id` is accepted for forward compatibility but
    # if unset (the common case), all child tasks pin to the serverless
    # environment declared on the parent job.
    use_classic = bool(interactive_cluster_id)
    env_key = None if use_classic else "serverless_rs"
    # Two child tasks now: simulate_filedrops drops the day's CSVs into S3,
    # then dbt_run does everything else (bronze COPY happens inside dbt via
    # pre_hooks on bronze models — no separate load_bronze task needed).
    tasks = [
        _make_task(
            task_key="simulate_filedrops_rs",
            notebook_path=f"{aug}/redshift/simulate_filedrops_rs",
            base_params=_BATCHED_PARAMS,
            existing_cluster_id=interactive_cluster_id,
            environment_key=env_key,
        ),
        _make_task(
            task_key="dbt_run",
            notebook_path=f"{aug}/redshift/run_dbt",
            depends_on=["simulate_filedrops_rs"],
            base_params=dict(
                _BATCHED_PARAMS,
                dbt_project_dir=f"{aug}/dbt",
            ),
            existing_cluster_id=interactive_cluster_id,
            environment_key=env_key,
        ),
    ]

    return {
        "name": job_name,
        "description": _description_child(
            scale_factor=scale_factor, database=database,
            wh_db=wh_db, tpcdi_directory=tpcdi_directory),
        "tags": {"data_generator": "spark", "engine": "redshift"},
        "email_notifications": {"no_alert_for_skipped_runs": False},
        "webhook_notifications": {},
        "timeout_seconds": 0,
        "max_concurrent_runs": 1000,
        "performance_target": "PERFORMANCE_OPTIMIZED",
        "parameters": [
            {"name": "catalog",           "default": catalog},
            {"name": "database",          "default": database},
            {"name": "scale_factor",      "default": str(scale_factor)},
            {"name": "tpcdi_directory",   "default": tpcdi_directory},
            {"name": "wh_db",             "default": wh_db},
            {"name": "secret_scope",      "default": secret_scope},
            {"name": "s3_volume_prefix",  "default": s3_volume_prefix},
            {"name": "aws_region",        "default": aws_region},
            {"name": "batch_date",        "default": ""},
        ],
        "tasks": tasks,
        # Serverless env for ALL child tasks when no interactive cluster
        # is provided (tpcdi-fresh's situation). dbt-redshift + psycopg2 +
        # any deps the notebooks need go here.
        "environments": (
            [] if use_classic else
            [{
                "environment_key": "serverless_rs",
                "spec": {
                    "client": "3",
                    "dependencies": [
                        "dbt-redshift==1.10.0",
                        "psycopg2-binary",
                    ],
                },
            }]
        ),
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
    database: str = "dev",
    secret_scope: str = "tpcdi_redshift",
    s3_volume_prefix: str = "s3://tpcds-datasets/shannon_tpcdi/",
    aws_region: str = "us-west-2",
    interactive_cluster_id: str | None = None,
    **_unused,
) -> dict:
    """Builds the parent (orchestration + loop wrapper) job spec.

    Three real tasks plus the cleanup pair:
      setup_rs → loop_incremental_tpcdi (for_each) → cleanup (gated)
    """
    aug = f"{repo_src_path}/{_AUG_PATH}"

    setup_task = _make_task(
        task_key="setup_rs",
        notebook_path=f"{aug}/redshift/setup_rs",
        base_params={
            **_COMMON_PARAMS,
            "incremental_batches_to_run":
                "{{job.parameters.incremental_batches_to_run}}",
        },
        environment_key="serverless_rs",
    )

    loop_task: dict[str, Any] = {
        "task_key": "loop_incremental_tpcdi",
        "depends_on": [{"task_key": "setup_rs"}],
        "run_if": "ALL_SUCCESS",
        "for_each_task": {
            "inputs": "{{tasks.setup_rs.values.batch_date_ls}}",
            "task": {
                "task_key": "loop_incremental_tpcdi_iteration",
                "run_if": "ALL_SUCCESS",
                "run_job_task": {
                    "job_id": child_job_id,
                    "job_parameters": {
                        "catalog":          "{{job.parameters.catalog}}",
                        "database":         "{{job.parameters.database}}",
                        "scale_factor":     "{{job.parameters.scale_factor}}",
                        "tpcdi_directory":  "{{job.parameters.tpcdi_directory}}",
                        "wh_db":            "{{job.parameters.wh_db}}",
                        "secret_scope":     "{{job.parameters.secret_scope}}",
                        "s3_volume_prefix": "{{job.parameters.s3_volume_prefix}}",
                        "aws_region":       "{{job.parameters.aws_region}}",
                        "batch_date":       "{{input}}",
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
        notebook_path=f"{aug}/redshift/teardown_rs",
        base_params=_COMMON_PARAMS,
        environment_key="serverless_rs",
    )
    cleanup_task["depends_on"] = [{"task_key": GATE, "outcome": "true"}]

    return {
        "name": job_name,
        "description": _description_parent(
            scale_factor=scale_factor, database=database,
            wh_db=wh_db, tpcdi_directory=tpcdi_directory),
        "tags": {"data_generator": "spark", "engine": "redshift"},
        "email_notifications": {"no_alert_for_skipped_runs": False},
        "webhook_notifications": {},
        "timeout_seconds": 0,
        "max_concurrent_runs": 1,
        "performance_target": "PERFORMANCE_OPTIMIZED",
        "parameters": [
            {"name": "catalog",                     "default": catalog},
            {"name": "database",                    "default": database},
            {"name": "scale_factor",                "default": str(scale_factor)},
            {"name": "tpcdi_directory",             "default": tpcdi_directory},
            {"name": "wh_db",                       "default": wh_db},
            {"name": "secret_scope",                "default": secret_scope},
            {"name": "s3_volume_prefix",            "default": s3_volume_prefix},
            {"name": "aws_region",                  "default": aws_region},
            {"name": "delete_tables_when_finished", "default": "TRUE"},
            {"name": "incremental_batches_to_run",  "default": "365"},
        ],
        "tasks": [setup_task, loop_task, gate_task, cleanup_task],
        # Serverless env for setup_rs + cleanup. psycopg2-binary is the only
        # runtime dep — both notebooks dispatch all heavy work to Redshift;
        # the Spark driver only orchestrates. dbt + simulate_filedrops +
        # load_bronze remain on the interactive cluster via build_child.
        "environments": [{
            "environment_key": "serverless_rs",
            "spec": {"client": "3", "dependencies": ["psycopg2-binary"]},
        }],
        "queue": {"enabled": True},
    }
