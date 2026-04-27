"""Shared helpers for the CLUSTER/DBSQL benchmark workflow builders.

Both `workflows_single_batch.py` and `workflows_incremental.py` build the
same family of multi-task workflow JSON; this module factors out the
repeated task and cluster shapes.
"""
from __future__ import annotations

from typing import Any


_DEFAULT_NOTIF = {
    "no_alert_for_skipped_runs": False,
    "no_alert_for_canceled_runs": False,
    "alert_on_last_attempt": False,
}


def base_parameters(
    *, catalog: str, scale_factor: int, tpcdi_directory: str,
    wh_db_default: str, pred_opt: str,
) -> list[dict]:
    return [
        {"name": "catalog", "default": catalog},
        {"name": "scale_factor", "default": str(scale_factor)},
        {"name": "tpcdi_directory", "default": tpcdi_directory},
        {"name": "wh_db", "default": wh_db_default},
        {"name": "predictive_optimization", "default": pred_opt},
    ]


def serverless_singlenode_cluster(*, job_name: str, dbr: str,
                                  driver_node_type: str,
                                  worker_node_type: str) -> dict:
    """SingleNode cluster used by mavenlib customermgmt task on serverless."""
    return {
        "job_cluster_key": f"{job_name}_compute",
        "new_cluster": {
            "cluster_name": "",
            "spark_version": dbr,
            "spark_conf": {
                "spark.master": "local[*, 4]",
                "spark.databricks.cluster.profile": "singleNode",
                "spark.sql.shuffle.partitions": "auto",
                "spark.databricks.adaptive.localShuffleReader.enabled": "true",
            },
            "driver_node_type_id": driver_node_type,
            "node_type_id": worker_node_type,
            "custom_tags": {"ResourceClass": "SingleNode"},
            "spark_env_vars": {"PYSPARK_PYTHON": "/databricks/python3/bin/python3"},
            "enable_elastic_disk": True,
            "data_security_mode": "SINGLE_USER",
            "runtime_engine": "STANDARD",
            "num_workers": 0,
        },
    }


def classic_photon_cluster(*, job_name: str, dbr: str,
                           driver_node_type: str, worker_node_type: str,
                           worker_node_count: int,
                           cloud_provider: str) -> dict:
    """Classic-cluster spec used by all tasks in non-serverless mode."""
    spark_conf: dict[str, str] = {}
    if worker_node_count == 0:
        spark_conf["spark.master"] = "local[*, 4]"
        spark_conf["spark.databricks.cluster.profile"] = "singleNode"
        spark_conf["spark.databricks.adaptive.localShuffleReader.enabled"] = "true"
    spark_conf["spark.sql.shuffle.partitions"] = "auto"

    new_cluster: dict[str, Any] = {
        "spark_version": dbr,
        "spark_conf": spark_conf,
    }
    if cloud_provider == "Azure":
        new_cluster["azure_attributes"] = {
            "availability": "SPOT_WITH_FALLBACK_AZURE",
            "first_on_demand": 1,
            "spot_bid_price_percent": -1,
        }
    elif cloud_provider == "AWS":
        new_cluster["aws_attributes"] = {
            "availability": "SPOT_WITH_FALLBACK",
            "first_on_demand": 1,
            "zone_id": "auto",
            "spot_bid_price_percent": 100,
        }
    new_cluster["spark_env_vars"] = {"PYSPARK_PYTHON": "/databricks/python3/bin/python3"}
    if worker_node_count == 0:
        new_cluster["custom_tags"] = {"ResourceClass": "SingleNode"}
    new_cluster["driver_node_type_id"] = driver_node_type
    new_cluster["node_type_id"] = worker_node_type
    new_cluster["num_workers"] = worker_node_count
    new_cluster["enable_elastic_disk"] = True
    new_cluster["data_security_mode"] = "SINGLE_USER"
    new_cluster["runtime_engine"] = "PHOTON"

    return {
        "job_cluster_key": f"{job_name}_compute",
        "new_cluster": new_cluster,
    }


def make_task(
    *,
    task_key: str,
    notebook_path: str,
    depends_on: list[str] | None = None,
    base_params: dict | None = None,
    run_if: str = "ALL_SUCCESS",
    exec_type: str,
    serverless: str,
    job_name: str,
    wh_id: str | None = None,
    libraries: list[dict] | None = None,
    job_cluster_key_override: str | None = None,
) -> dict:
    """Common shape for an individual task in the workflow.

    `job_cluster_key_override` is for the customermgmt mavenlib task which
    always pins to `{job_name}_compute` even when serverless.
    """
    notebook_task: dict[str, Any] = {
        "notebook_path": notebook_path,
        "source": "WORKSPACE",
    }
    if base_params is not None:
        notebook_task["base_parameters"] = base_params
    # Tasks pinned to a job cluster (e.g. the mavenlib customermgmt path) run
    # there, not on the SQL warehouse — skip warehouse_id even on DBSQL.
    if exec_type == "DBSQL" and job_cluster_key_override is None:
        notebook_task["warehouse_id"] = wh_id

    task: dict[str, Any] = {
        "task_key": task_key,
    }
    if depends_on:
        task["depends_on"] = [{"task_key": d} for d in depends_on]
    task["run_if"] = run_if
    task["notebook_task"] = notebook_task

    if job_cluster_key_override is not None:
        task["job_cluster_key"] = job_cluster_key_override
    elif serverless != "YES":
        task["job_cluster_key"] = f"{job_name}_compute"

    if libraries is not None:
        task["libraries"] = libraries

    task["timeout_seconds"] = 0
    task["email_notifications"] = {}
    task["notification_settings"] = dict(_DEFAULT_NOTIF)

    # Reorder notebook_task keys to match Jinja's natural ordering: notebook_path,
    # base_parameters, warehouse_id (if any), source.
    ordered_notebook: dict[str, Any] = {"notebook_path": notebook_task["notebook_path"]}
    if "base_parameters" in notebook_task:
        ordered_notebook["base_parameters"] = notebook_task["base_parameters"]
    if "warehouse_id" in notebook_task:
        ordered_notebook["warehouse_id"] = notebook_task["warehouse_id"]
    ordered_notebook["source"] = notebook_task["source"]
    task["notebook_task"] = ordered_notebook
    return task


def make_customermgmt_task(
    *,
    repo_src_path: str,
    job_name: str,
    data_generator: str,
    scale_factor: int,
    serverless: str,
    exec_type: str,
    wh_id: str | None,
    depends_on: list[str],
    modern_notebook_path: str,
) -> dict:
    """Shared customermgmt task for both single_batch and incremental workflows.

    For digen + SF>100 this routes to the mavenlib notebook on a SingleNode
    classic cluster with the legacy spark-xml maven library; otherwise it
    uses `modern_notebook_path` (the read_files()-based notebook, which lives
    in different repo dirs for single_batch vs incremental flows).
    """
    if data_generator == "digen" and scale_factor > 100:
        return make_task(
            task_key="ingest_customermgmt",
            notebook_path=f"{repo_src_path}/incremental_batches/bronze/CustomerMgmtRaw_mavenlib",
            depends_on=depends_on,
            base_params={"xml_lib": "com.databricks.spark.xml"},
            exec_type=exec_type,
            serverless=serverless,
            job_name=job_name,
            wh_id=wh_id,
            libraries=[{"maven": {"coordinates": "com.databricks:spark-xml_2.12:0.18.0"}}],
            job_cluster_key_override=f"{job_name}_compute",
        )
    return make_task(
        task_key="ingest_customermgmt",
        notebook_path=modern_notebook_path,
        depends_on=depends_on,
        base_params={"xml_lib": "xml"},
        exec_type=exec_type,
        serverless=serverless,
        job_name=job_name,
        wh_id=wh_id,
    )


def make_cleanup_task(
    *,
    repo_src_path: str,
    job_name: str,
    exec_type: str,
    serverless: str,
    wh_id: str | None,
    depends_on: list[str],
) -> dict:
    """Final task: drops the run's schemas when `delete_tables_when_finished`
    is TRUE (default). Always runs — `run_if=ALL_DONE` — so a partial-failure
    upstream still has its debris cleaned up.

    Reads catalog / wh_db / scale_factor / delete_tables_when_finished from
    job parameters, all of which the workflow already exposes.

    Always pins to non-warehouse compute by forcing ``exec_type='CLUSTER'`` —
    `cleanup_after_benchmark` is a Python notebook (dbutils.widgets +
    spark.sql), and SQL warehouses only execute SQL cells. For non-serverless
    CLUSTER runs the task lands on the existing job cluster; for serverless
    (CLUSTER serverless or DBSQL — DBSQL is always serverless) it runs on the
    workspace's serverless compute.
    """
    return make_task(
        task_key="cleanup",
        notebook_path=f"{repo_src_path}/tools/cleanup_after_benchmark",
        depends_on=depends_on,
        run_if="ALL_DONE",
        base_params={
            "catalog": "{{job.parameters.catalog}}",
            "wh_db": "{{job.parameters.wh_db}}",
            "scale_factor": "{{job.parameters.scale_factor}}",
            "delete_tables_when_finished": "{{job.parameters.delete_tables_when_finished}}",
        },
        exec_type="CLUSTER",
        serverless="YES" if exec_type == "DBSQL" else serverless,
        job_name=job_name,
        wh_id=None,
    )


def cleanup_param() -> dict:
    """Job parameter the Driver bakes into every benchmark workflow.
    Users override per-run if they want to keep the schemas around."""
    return {"name": "delete_tables_when_finished", "default": "TRUE"}


def constraint_clause(constraint_def: str, perf_opt_flg: bool) -> str:
    """Render the inline constraint string used in dim/fact tasks."""
    return constraint_def if not perf_opt_flg else ""


def tbl_props(opt_write: str, index_cols: str, finwire: bool = False) -> str:
    """Render the inline tbl_props string."""
    if finwire:
        return f"'delta.dataSkippingNumIndexedCols' = 0, 'delta.autoOptimize.autoCompact'=False, {opt_write}"
    return f"'delta.autoOptimize.autoCompact'=False, {opt_write} {index_cols}"
