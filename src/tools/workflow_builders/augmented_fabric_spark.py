"""Builder for the Augmented Incremental TPC-DI benchmark — Fabric Spark and
Fabric NEE (Native Execution Engine) variants.

Spark competitor, not dbt: the transforms are the repo's PySpark notebooks
(spark_competitors/fabric/notebooks/) deployed into Fabric and run there by the
Fabric `batch_runner` notebook. Databricks orchestrates: it provisions the
Fabric side, materializes OneLake staging (once per SF), drops each day's files
into OneLake and blocks on the Fabric batch run.

The two variants share every notebook and job shape; `enable_nee` (forwarded to
batch_runner, which sets spark.native.enabled for the session) is the only
difference, so the comparison is apples-to-apples.

  parent:  setup_fabric -> for_each day (child) -> gated cleanup
  child:   simulate_filedrops_fabric -> run_fabric

Compute: the Databricks-side tasks write OneLake through fs.azure.* confs,
which serverless doesn't allow, so they run on classic compute — the
interactive cluster when one is given, otherwise a single-node job cluster.
A job cluster starts per child run (several minutes per batch, outside the
Fabric-measured time), so an interactive cluster is the practical choice for
long runs.

Pre-requisites (one-time, out-of-band):
  - A Fabric workspace on a running capacity, with a schema-enabled Lakehouse.
  - An Entra service principal that is Contributor (or higher) on the
    workspace; Admin if setup has to create the custom Spark pool.
    tenant_id / client_id are plain params; the client secret is a UC secret
    whose full path is passed as client_secret_secret.
"""
from __future__ import annotations

from typing import Any

_DEFAULT_NOTIF = {
    "no_alert_for_skipped_runs": False,
    "no_alert_for_canceled_runs": False,
    "alert_on_last_attempt": False,
}
# Retries off: a failed Fabric batch isn't transient, and a retry re-runs a
# long batch before the failure surfaces.
_RETRY_POLICY = {"max_retries": 0, "min_retry_interval_millis": 0, "retry_on_timeout": False}
_AUG_PATH = "incremental_batches/augmented_incremental"
_FAB_DIR = f"{_AUG_PATH}/spark_competitors/fabric"
_CLUSTER_KEY = "fabric_spark_cluster"
# Classic DBR for the job cluster. 17.3 LTS is the floor for reading UC secrets
# on classic compute.
_SPARK_VERSION = "18.x-scala2.13"
_NODE_TYPE = "Standard_D8ds_v5"

# Params every task needs. All plain values except client_secret_secret, which
# is the full UC path of the SP client secret (the notebooks resolve it).
_COMMON_PARAMS = {
    "catalog":              "{{job.parameters.catalog}}",
    "scale_factor":         "{{job.parameters.scale_factor}}",
    "tpcdi_directory":      "{{job.parameters.tpcdi_directory}}",
    "wh_db":                "{{job.parameters.wh_db}}",
    "fabric_workspace_id":  "{{job.parameters.fabric_workspace_id}}",
    "fabric_lakehouse_id":  "{{job.parameters.fabric_lakehouse_id}}",
    "tenant_id":            "{{job.parameters.tenant_id}}",
    "client_id":            "{{job.parameters.client_id}}",
    "client_secret_secret": "{{job.parameters.client_secret_secret}}",
    "file_ext":             "{{job.parameters.file_ext}}",
}
_BATCHED_PARAMS = dict(_COMMON_PARAMS,
                       batch_date="{{job.parameters.batch_date}}",
                       enable_nee="{{job.parameters.enable_nee}}")
# Fabric-side compute, read only by setup_fabric.
_SETUP_PARAMS = dict(
    _COMMON_PARAMS,
    fabric_pool_name="{{job.parameters.fabric_pool_name}}",
    fabric_node_size="{{job.parameters.fabric_node_size}}",
    fabric_node_count="{{job.parameters.fabric_node_count}}",
    fabric_runtime="{{job.parameters.fabric_runtime}}",
    fabric_environment_name="{{job.parameters.fabric_environment_name}}",
    incremental_batches_to_run="{{job.parameters.incremental_batches_to_run}}",
)


def variant_label(enable_nee: bool) -> str:
    return "Fabric NEE" if enable_nee else "Fabric Spark"


def _tags(enable_nee: bool) -> dict:
    return {"data_generator": "spark", "engine": "fabric",
            "variant": "fabric_nee" if enable_nee else "fabric_spark"}


def _job_cluster(*, spark_version: str, node_type_id: str,
                 single_user_name: str | None) -> dict:
    """Single-node classic cluster with dedicated access: reads UC `main` and
    UC secrets, and may set the fs.azure.* confs for OneLake."""
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
    }
    if single_user_name:
        nc["single_user_name"] = single_user_name
    return {"job_cluster_key": _CLUSTER_KEY, "new_cluster": nc}


def _make_task(*, task_key: str, notebook_path: str, base_params: dict,
               depends_on: list[str] | None = None, run_if: str = "ALL_SUCCESS",
               interactive_cluster_id: str | None = None) -> dict:
    task: dict[str, Any] = {"task_key": task_key}
    if interactive_cluster_id:
        task["existing_cluster_id"] = interactive_cluster_id
    else:
        task["job_cluster_key"] = _CLUSTER_KEY
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


def _common_job_params(*, catalog, scale_factor, tpcdi_directory, wh_db,
                       fabric_workspace_id, fabric_lakehouse_id, tenant_id,
                       client_id, client_secret_secret, file_ext, enable_nee) -> list:
    return [
        {"name": "catalog",              "default": catalog},
        {"name": "scale_factor",         "default": str(scale_factor)},
        {"name": "tpcdi_directory",      "default": tpcdi_directory},
        {"name": "wh_db",                "default": wh_db},
        {"name": "fabric_workspace_id",  "default": fabric_workspace_id},
        {"name": "fabric_lakehouse_id",  "default": fabric_lakehouse_id},
        {"name": "tenant_id",            "default": tenant_id},
        {"name": "client_id",            "default": client_id},
        {"name": "client_secret_secret", "default": client_secret_secret},
        {"name": "file_ext",             "default": file_ext},
        {"name": "enable_nee",           "default": "true" if enable_nee else "false"},
    ]


def build_child(*, job_name: str, repo_src_path: str, catalog: str, scale_factor: int,
                tpcdi_directory: str, wh_db: str, enable_nee: bool,
                fabric_workspace_id: str, fabric_lakehouse_id: str,
                tenant_id: str, client_id: str, client_secret_secret: str,
                file_ext: str = "txt",
                spark_version: str = _SPARK_VERSION, node_type_id: str = _NODE_TYPE,
                interactive_cluster_id: str | None = None,
                single_user_name: str | None = None, **_unused) -> dict:
    fab = f"{repo_src_path}/{_FAB_DIR}"
    tasks = [
        _make_task(task_key="simulate_filedrops_fabric",
                   notebook_path=f"{fab}/simulate_filedrops_fabric",
                   base_params=_BATCHED_PARAMS, interactive_cluster_id=interactive_cluster_id),
        _make_task(task_key="run_fabric",
                   notebook_path=f"{fab}/run_fabric",
                   depends_on=["simulate_filedrops_fabric"],
                   base_params=_BATCHED_PARAMS, interactive_cluster_id=interactive_cluster_id),
    ]
    return {
        "name": job_name,
        "description": (f"TPC-DI Augmented Incremental ({variant_label(enable_nee)}, child) "
                        f"SF={scale_factor}. Per simulated day: drop the day's files into "
                        f"OneLake, then run the Fabric batch_runner DAG "
                        f"(spark.native.enabled={'true' if enable_nee else 'false'}) and block. "
                        f"Transforms materialize in lakehouse schema {wh_db}_{scale_factor}."),
        "tags": _tags(enable_nee),
        "timeout_seconds": 0,
        "max_concurrent_runs": 1000,
        **({} if interactive_cluster_id else
           {"job_clusters": [_job_cluster(spark_version=spark_version, node_type_id=node_type_id,
                                          single_user_name=single_user_name)]}),
        "parameters": _common_job_params(
            catalog=catalog, scale_factor=scale_factor, tpcdi_directory=tpcdi_directory,
            wh_db=wh_db, fabric_workspace_id=fabric_workspace_id,
            fabric_lakehouse_id=fabric_lakehouse_id, tenant_id=tenant_id, client_id=client_id,
            client_secret_secret=client_secret_secret, file_ext=file_ext,
            enable_nee=enable_nee) + [{"name": "batch_date", "default": ""}],
        "tasks": tasks,
        "queue": {"enabled": True},
    }


def build_parent(*, job_name: str, child_job_id: int, repo_src_path: str, catalog: str,
                 scale_factor: int, tpcdi_directory: str, wh_db: str, enable_nee: bool,
                 fabric_workspace_id: str, fabric_lakehouse_id: str,
                 tenant_id: str, client_id: str, client_secret_secret: str,
                 file_ext: str = "txt",
                 fabric_pool_name: str = "tpcdi_spark_pool",
                 fabric_node_size: str = "Medium", fabric_node_count: int = 16,
                 fabric_runtime: str = "2.0",
                 fabric_environment_name: str = "tpcdi_spark_env",
                 spark_version: str = _SPARK_VERSION, node_type_id: str = _NODE_TYPE,
                 interactive_cluster_id: str | None = None,
                 single_user_name: str | None = None, **_unused) -> dict:
    fab = f"{repo_src_path}/{_FAB_DIR}"

    setup_task = _make_task(task_key="setup_fabric",
                            notebook_path=f"{fab}/setup_fabric",
                            base_params=_SETUP_PARAMS,
                            interactive_cluster_id=interactive_cluster_id)

    child_params = dict(_COMMON_PARAMS,
                        enable_nee="{{job.parameters.enable_nee}}",
                        batch_date="{{input}}")
    # Sequential for_each (no concurrency): each batch builds on the prior
    # batch's SCD2 state.
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
                              notebook_path=f"{fab}/teardown_fabric",
                              base_params=_COMMON_PARAMS,
                              interactive_cluster_id=interactive_cluster_id)
    cleanup_task["depends_on"] = [{"task_key": GATE, "outcome": "true"}]

    return {
        "name": job_name,
        "description": (f"TPC-DI Augmented Incremental ({variant_label(enable_nee)}, parent) "
                        f"SF={scale_factor}. setup_fabric provisions the Fabric side (Spark "
                        f"pool {fabric_pool_name}: {fabric_node_count} x {fabric_node_size}, "
                        f"runtime {fabric_runtime}, notebooks), materializes OneLake staging and "
                        f"resets the run schema, then loops the child per simulated day "
                        f"(2016-07-06 ->). Cleanup gated by delete_tables_when_finished."),
        "tags": _tags(enable_nee),
        "timeout_seconds": 0,
        "max_concurrent_runs": 1,
        **({} if interactive_cluster_id else
           {"job_clusters": [_job_cluster(spark_version=spark_version, node_type_id=node_type_id,
                                          single_user_name=single_user_name)]}),
        "parameters": _common_job_params(
            catalog=catalog, scale_factor=scale_factor, tpcdi_directory=tpcdi_directory,
            wh_db=wh_db, fabric_workspace_id=fabric_workspace_id,
            fabric_lakehouse_id=fabric_lakehouse_id, tenant_id=tenant_id, client_id=client_id,
            client_secret_secret=client_secret_secret, file_ext=file_ext,
            enable_nee=enable_nee) + [
            {"name": "fabric_pool_name",            "default": fabric_pool_name},
            {"name": "fabric_node_size",            "default": fabric_node_size},
            {"name": "fabric_node_count",           "default": str(fabric_node_count)},
            {"name": "fabric_runtime",              "default": fabric_runtime},
            {"name": "fabric_environment_name",     "default": fabric_environment_name},
            {"name": "delete_tables_when_finished", "default": "TRUE"},
            {"name": "incremental_batches_to_run",  "default": "365"},
        ],
        "tasks": [setup_task, loop_task, gate_task, cleanup_task],
        "queue": {"enabled": True},
    }
