"""Builder for the Augmented Incremental TPC-DI benchmark — Amazon EMR (on EC2).

Spark competitor, not dbt: the transforms are the repo's PySpark job
(spark_competitors/emr/jobs/), ported from the Fabric Spark notebooks, run as
one EMR step per batch on a fixed-size on-demand cluster with Delta tables in
the Glue catalog. Databricks orchestrates: it DEEP CLONEs the staging tables
to Delta on S3 (once per SF; each run SHALLOW CLONEs them), creates the
cluster, drops each day's files into S3 through the UC volume and blocks on
that day's step.

  parent:  setup_emr -> for_each day (child) -> teardown_emr (ALL_DONE)
  child:   simulate_filedrops_emr -> run_emr

The file drop reuses the Redshift variant's notebook: both engines read the
same S3-backed volume layout. teardown_emr always runs so a cluster the run
created never outlives it; it also does the cleanup when
delete_tables_when_finished=TRUE.

Compute: the Databricks tasks only call AWS APIs, so they default to
serverless (env v5) and use a UC service credential (aws_service_credential).
Pass interactive_cluster_id to use a classic cluster's instance profile
instead (aws_service_credential blank).

Pre-requisites (one-time, out-of-band):
  - The S3-backed UC volume tpcdi_directory and its s3:// prefix
    (s3_volume_prefix), with Stage 0 run for this SF.
  - EMR_DefaultRole / EMR_EC2_DefaultRole (the instance profile needs S3 on
    the bucket and Glue), a subnet and the two EMR-managed security groups.
  - An AWS identity for the Databricks tasks with EMR + iam:PassRole on those
    roles: a UC service credential, or an instance profile on the cluster.
"""
from __future__ import annotations

from typing import Any

_DEFAULT_NOTIF = {
    "no_alert_for_skipped_runs": False,
    "no_alert_for_canceled_runs": False,
    "alert_on_last_attempt": False,
}
# Retries off: a failed batch isn't transient, and a retry would re-run on
# half-applied SCD2 state.
_RETRY_POLICY = {"max_retries": 0, "min_retry_interval_millis": 0, "retry_on_timeout": False}
_AUG_PATH = "incremental_batches/augmented_incremental"
_EMR_DIR = f"{_AUG_PATH}/spark_competitors/emr"
_DROPS_NB = f"{_AUG_PATH}/dbt/competitors/redshift/simulate_filedrops_rs"
_ENV_KEY = "serverless_emr"

_COMMON_PARAMS = {
    "catalog":                "{{job.parameters.catalog}}",
    "scale_factor":           "{{job.parameters.scale_factor}}",
    "tpcdi_directory":        "{{job.parameters.tpcdi_directory}}",
    "wh_db":                  "{{job.parameters.wh_db}}",
    "s3_volume_prefix":       "{{job.parameters.s3_volume_prefix}}",
    "aws_region":             "{{job.parameters.aws_region}}",
    "aws_service_credential": "{{job.parameters.aws_service_credential}}",
    "file_ext":               "{{job.parameters.file_ext}}",
    "concurrency":            "{{job.parameters.concurrency}}",
    "shuffle_partitions":     "{{job.parameters.shuffle_partitions}}",
}
_CLUSTER_PARAMS = ("emr_cluster_mode", "emr_cluster_id", "emr_subnet_id", "emr_master_sg", "emr_slave_sg",
                   "emr_instance_type", "emr_core_count", "emr_release_label",
                   "emr_service_role", "emr_instance_profile")
_SETUP_PARAMS = dict(_COMMON_PARAMS,
                     **{k: f"{{{{job.parameters.{k}}}}}" for k in _CLUSTER_PARAMS},
                     incremental_batches_to_run="{{job.parameters.incremental_batches_to_run}}",
                     force_materialize="{{job.parameters.force_materialize}}")
_SETUP_OUT = {k: f"{{{{tasks.setup_emr.values.{k}}}}}" for k in ("emr_cluster_id", "run_id")}


def _tags() -> dict:
    return {"data_generator": "spark", "engine": "emr", "variant": "emr_spark"}


def _environments(interactive_cluster_id):
    if interactive_cluster_id:
        return {}
    # UC secret / service-credential reads need serverless env v5+.
    return {"environments": [{"environment_key": _ENV_KEY,
                              "spec": {"client": "5", "dependencies": ["boto3"]}}]}


def _make_task(*, task_key: str, notebook_path: str, base_params: dict,
               depends_on: list[str] | None = None, run_if: str = "ALL_SUCCESS",
               interactive_cluster_id: str | None = None) -> dict:
    task: dict[str, Any] = {"task_key": task_key}
    if depends_on:
        task["depends_on"] = [{"task_key": d} for d in depends_on]
    task["run_if"] = run_if
    task["notebook_task"] = {"notebook_path": notebook_path, "source": "WORKSPACE",
                             "base_parameters": base_params}
    if interactive_cluster_id:
        task["existing_cluster_id"] = interactive_cluster_id
    else:
        task["environment_key"] = _ENV_KEY
    task["timeout_seconds"] = 0
    task["email_notifications"] = {}
    task["notification_settings"] = dict(_DEFAULT_NOTIF)
    task["webhook_notifications"] = {}
    task.update(_RETRY_POLICY)
    return task


def _common_job_params(*, catalog, scale_factor, tpcdi_directory, wh_db, s3_volume_prefix,
                       aws_region, aws_service_credential, file_ext, concurrency,
                       shuffle_partitions) -> list:
    return [
        {"name": "catalog",                "default": catalog},
        {"name": "scale_factor",           "default": str(scale_factor)},
        {"name": "tpcdi_directory",        "default": tpcdi_directory},
        {"name": "wh_db",                  "default": wh_db},
        {"name": "s3_volume_prefix",       "default": s3_volume_prefix},
        {"name": "aws_region",             "default": aws_region},
        {"name": "aws_service_credential", "default": aws_service_credential},
        {"name": "file_ext",               "default": file_ext},
        {"name": "concurrency",            "default": str(concurrency)},
        {"name": "shuffle_partitions",     "default": str(shuffle_partitions)},
    ]


def build_child(*, job_name: str, repo_src_path: str, catalog: str, scale_factor: int,
                tpcdi_directory: str, wh_db: str, s3_volume_prefix: str,
                aws_region: str = "us-west-2", aws_service_credential: str = "",
                file_ext: str = "txt", concurrency: int = 8, shuffle_partitions: int = 1000,
                interactive_cluster_id: str | None = None, **_unused) -> dict:
    run_params = dict(_COMMON_PARAMS, batch_date="{{job.parameters.batch_date}}",
                      run_id="{{job.parameters.run_id}}",
                      **{k: f"{{{{job.parameters.{k}}}}}" for k in _CLUSTER_PARAMS})
    tasks = [
        _make_task(task_key="simulate_filedrops_emr", notebook_path=f"{repo_src_path}/{_DROPS_NB}",
                   base_params=dict(_COMMON_PARAMS, batch_date="{{job.parameters.batch_date}}"),
                   interactive_cluster_id=interactive_cluster_id),
        _make_task(task_key="run_emr", notebook_path=f"{repo_src_path}/{_EMR_DIR}/run_emr",
                   depends_on=["simulate_filedrops_emr"], base_params=run_params,
                   interactive_cluster_id=interactive_cluster_id),
    ]
    return {
        "name": job_name,
        "description": (f"TPC-DI Augmented Incremental (Amazon EMR, child) SF={scale_factor}. "
                        f"Per simulated day: drop the day's files into S3 through the UC volume, "
                        f"then run that date's EMR step (bronze -> silver/gold DAG, Delta in "
                        f"Glue database {wh_db}_{scale_factor}) and block."),
        "tags": _tags(),
        "timeout_seconds": 0,
        "max_concurrent_runs": 1000,
        **_environments(interactive_cluster_id),
        "parameters": _common_job_params(
            catalog=catalog, scale_factor=scale_factor, tpcdi_directory=tpcdi_directory,
            wh_db=wh_db, s3_volume_prefix=s3_volume_prefix, aws_region=aws_region,
            aws_service_credential=aws_service_credential, file_ext=file_ext,
            concurrency=concurrency, shuffle_partitions=shuffle_partitions) + [
            {"name": "batch_date", "default": ""},
            {"name": "run_id", "default": ""},
        ] + [{"name": k, "default": "job" if k == "emr_cluster_mode" else ""}
             for k in _CLUSTER_PARAMS],
        "tasks": tasks,
        "queue": {"enabled": True},
    }


def build_parent(*, job_name: str, child_job_id: int, repo_src_path: str, catalog: str,
                 scale_factor: int, tpcdi_directory: str, wh_db: str, s3_volume_prefix: str,
                 emr_subnet_id: str = "", emr_master_sg: str = "", emr_slave_sg: str = "",
                 emr_cluster_id: str = "", emr_cluster_mode: str = "job",
                 emr_instance_type: str = "m8gd.2xlarge", emr_core_count: int = 16,
                 emr_release_label: str = "emr-7.14.0",
                 emr_service_role: str = "EMR_DefaultRole",
                 emr_instance_profile: str = "EMR_EC2_DefaultRole",
                 aws_region: str = "us-west-2", aws_service_credential: str = "",
                 file_ext: str = "txt", concurrency: int = 8, shuffle_partitions: int = 1000,
                 interactive_cluster_id: str | None = None, **_unused) -> dict:
    d = f"{repo_src_path}/{_EMR_DIR}"
    setup_task = _make_task(task_key="setup_emr", notebook_path=f"{d}/setup_emr",
                            base_params=_SETUP_PARAMS,
                            interactive_cluster_id=interactive_cluster_id)
    child_params = dict({k: v for k, v in _COMMON_PARAMS.items()},
                        batch_date="{{input}}",
                        **{k: f"{{{{job.parameters.{k}}}}}" for k in _CLUSTER_PARAMS
                           if k not in _SETUP_OUT},
                        **_SETUP_OUT)
    # Sequential for_each: each batch builds on the prior batch's SCD2 state.
    loop_task: dict[str, Any] = {
        "task_key": "loop_incremental_tpcdi",
        "depends_on": [{"task_key": "setup_emr"}],
        "run_if": "ALL_SUCCESS",
        "for_each_task": {
            "inputs": "{{tasks.setup_emr.values.batch_date_ls}}",
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
    teardown_task = _make_task(
        task_key="teardown_emr", notebook_path=f"{d}/teardown_emr",
        depends_on=["loop_incremental_tpcdi"], run_if="ALL_DONE",
        base_params=dict(_COMMON_PARAMS, **_SETUP_OUT,
                         emr_created="{{tasks.setup_emr.values.emr_created}}",
                         delete_tables_when_finished="{{job.parameters.delete_tables_when_finished}}"),
        interactive_cluster_id=interactive_cluster_id)

    return {
        "name": job_name,
        "description": (f"TPC-DI Augmented Incremental (Amazon EMR, parent) SF={scale_factor}. "
                        f"setup_emr DEEP CLONEs the Delta staging (once per SF) and resets the "
                        f"Glue database; the loop runs the child per simulated day "
                        f"(2016-07-06 ->). Fixed-size on-demand EMR ({emr_core_count} x "
                        f"{emr_instance_type} core + 1 primary, {emr_release_label}): with "
                        f"emr_cluster_mode=job (default) setup and every batch each start their "
                        f"own job cluster that EMR terminates when its steps end, like a daily "
                        f"classic job cluster; emr_cluster_mode=shared uses one cluster for the "
                        f"run. teardown_emr always runs (report, cleanup when "
                        f"delete_tables_when_finished, terminate a shared cluster)."),
        "tags": _tags(),
        "timeout_seconds": 0,
        "max_concurrent_runs": 1,
        **_environments(interactive_cluster_id),
        "parameters": _common_job_params(
            catalog=catalog, scale_factor=scale_factor, tpcdi_directory=tpcdi_directory,
            wh_db=wh_db, s3_volume_prefix=s3_volume_prefix, aws_region=aws_region,
            aws_service_credential=aws_service_credential, file_ext=file_ext,
            concurrency=concurrency, shuffle_partitions=shuffle_partitions) + [
            {"name": "emr_cluster_mode",            "default": emr_cluster_mode},
            {"name": "emr_cluster_id",              "default": emr_cluster_id},
            {"name": "emr_subnet_id",               "default": emr_subnet_id},
            {"name": "emr_master_sg",               "default": emr_master_sg},
            {"name": "emr_slave_sg",                "default": emr_slave_sg},
            {"name": "emr_instance_type",           "default": emr_instance_type},
            {"name": "emr_core_count",              "default": str(emr_core_count)},
            {"name": "emr_release_label",           "default": emr_release_label},
            {"name": "emr_service_role",            "default": emr_service_role},
            {"name": "emr_instance_profile",        "default": emr_instance_profile},
            {"name": "delete_tables_when_finished", "default": "TRUE"},
            {"name": "incremental_batches_to_run",  "default": "365"},
            {"name": "force_materialize",           "default": "false"},
        ],
        "tasks": [setup_task, loop_task, teardown_task],
        "queue": {"enabled": True},
    }
