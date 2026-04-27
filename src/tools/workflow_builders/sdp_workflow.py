"""Builder for the SDP-wrapping multi-task workflow.

This is the Jobs-API workflow that runs the SDP (Spark Declarative
Pipelines) pipeline (created separately by `sdp_pipeline.build()`) plus,
for the DIGen path at SF>100, an upfront SingleNode-cluster task that
ingests CustomerMgmt.xml using the legacy spark-xml Maven library.
"""
from __future__ import annotations


def _description(*, data_generator: str, edition: str, scale_factor: int,
                 catalog: str, wh_target: str, datagen_label: str,
                 tpcdi_directory: str) -> str:
    target = (
        f"{catalog}.{wh_target}_SDP_{edition}_{datagen_label}_{scale_factor}"
    )
    if data_generator == "spark":
        return (
            f"TPC-DI Spark Declarative Pipelines {edition} pipeline (**Spark** "
            f"data generator, SF={scale_factor}). Reads distributed PySpark "
            f"output from `{tpcdi_directory}sf={scale_factor}/` (split files "
            f"like `Customer_1.txt`, `Customer_2.txt`, ..., "
            f"`CustomerMgmt_1.xml`, ...). Materializes target schema "
            f"`{target}`. All 3 TPC-DI batches are processed in a single SDP "
            f"run. The CustomerMgmt XML re-ingestion gate is not used here — "
            f"split XML files re-parse cheaply, so the pipeline always "
            f"re-ingests."
        )
    desc = (
        f"TPC-DI Spark Declarative Pipelines {edition} pipeline "
        f"(**DIGen.jar** native, legacy data generator, SF={scale_factor}). "
        f"Reads single-threaded DIGen output from "
        f"`{tpcdi_directory}sf={scale_factor}/` (one file per table — "
        f"`Customer.txt`, `Trade.txt`, `CustomerMgmt.xml`). Materializes "
        f"target schema `{target}`. All 3 TPC-DI batches are processed in a "
        f"single SDP run."
    )
    if scale_factor > 100:
        desc += (
            " The ***run_customermgmt*** parameter accepts ***YES*** or "
            "***NO*** — DIGen produces a single large CustomerMgmt.xml that "
            "is expensive to re-parse at high SF, so this gate exists to "
            "skip re-ingestion if already loaded; defaults to ***YES***."
        )
    return desc


def _job_clusters(*, job_name: str, dbr: str,
                  driver_node_type: str, worker_node_type: str) -> list[dict]:
    return [{
        "job_cluster_key": f"{job_name}_compute",
        "new_cluster": {
            "spark_version": dbr,
            "spark_conf": {
                "spark.master": "local[*, 4]",
                "spark.databricks.adaptive.localShuffleReader.enabled": "true",
                "spark.databricks.delta.schema.autoMerge.enabled": "true",
                "spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite": "true",
                "spark.databricks.streaming.forEachBatch.optimized.enabled": "true",
                "spark.databricks.preemption.enabled": "false",
                "spark.databricks.streaming.forEachBatch.optimized.fastPath.enabled": "true",
                "spark.sql.shuffle.partitions": "auto",
            },
            "spark_env_vars": {"PYSPARK_PYTHON": "/databricks/python3/bin/python3"},
            "custom_tags": {"ResourceClass": "SingleNode"},
            "driver_node_type_id": driver_node_type,
            "node_type_id": worker_node_type,
            "num_workers": 0,
            "enable_elastic_disk": True,
            "data_security_mode": "SINGLE_USER",
            "runtime_engine": "STANDARD",
        },
    }]


def _digen_high_sf_tasks(*, job_name: str, repo_src_path: str,
                         pipeline_id: int) -> list[dict]:
    """Three-task DAG used only for digen + SF>100: condition → mavenlib → SDP."""
    return [
        {
            "task_key": "run_customermgmt_YES_NO",
            "run_if": "ALL_SUCCESS",
            "condition_task": {
                "op": "EQUAL_TO",
                "left": "{{job.parameters.run_customermgmt}}",
                "right": "YES",
            },
            "timeout_seconds": 0,
            "email_notifications": {},
            "notification_settings": {
                "no_alert_for_skipped_runs": False,
                "no_alert_for_canceled_runs": False,
                "alert_on_last_attempt": False,
            },
            "webhook_notifications": {},
        },
        {
            "task_key": "ingest_customermgmt_cluster",
            "depends_on": [{"task_key": "run_customermgmt_YES_NO", "outcome": "true"}],
            "run_if": "ALL_SUCCESS",
            "notebook_task": {
                "notebook_path": f"{repo_src_path}/incremental_batches/bronze/CustomerMgmtRaw_mavenlib",
                "source": "WORKSPACE",
            },
            "job_cluster_key": f"{job_name}_compute",
            "libraries": [{"maven": {"coordinates": "com.databricks:spark-xml_2.12:0.18.0"}}],
            "timeout_seconds": 0,
            "email_notifications": {},
            "notification_settings": {
                "no_alert_for_skipped_runs": False,
                "no_alert_for_canceled_runs": False,
                "alert_on_last_attempt": False,
            },
            "webhook_notifications": {},
        },
        {
            "task_key": "TPC-DI-SDP-PIPELINE",
            "depends_on": [
                {"task_key": "run_customermgmt_YES_NO", "outcome": "false"},
                {"task_key": "ingest_customermgmt_cluster"},
            ],
            "run_if": "AT_LEAST_ONE_SUCCESS",
            "pipeline_task": {"pipeline_id": str(pipeline_id), "full_refresh": True},
            "timeout_seconds": 0,
            "email_notifications": {"no_alert_for_skipped_runs": False},
        },
    ]


def _simple_pipeline_task(pipeline_id: int) -> list[dict]:
    return [{
        "task_key": "TPC-DI-SDP-PIPELINE",
        "run_if": "ALL_SUCCESS",
        "pipeline_task": {"pipeline_id": str(pipeline_id), "full_refresh": True},
        "timeout_seconds": 0,
        "email_notifications": {"no_alert_for_skipped_runs": False},
    }]


def _cleanup_tasks(*, repo_src_path: str) -> list[dict]:
    """Condition gate + cleanup notebook task. Gate watches the SDP pipeline
    with ALL_DONE so partial-failure runs still reach cleanup; the SQL only
    fires when delete_tables_when_finished=TRUE."""
    GATE = "delete_when_finished_TRUE_FALSE"
    NOTIF = {
        "no_alert_for_skipped_runs": False,
        "no_alert_for_canceled_runs": False,
        "alert_on_last_attempt": False,
    }
    gate = {
        "task_key": GATE,
        "depends_on": [{"task_key": "TPC-DI-SDP-PIPELINE"}],
        "run_if": "ALL_DONE",
        "condition_task": {
            "op": "EQUAL_TO",
            "left": "{{job.parameters.delete_tables_when_finished}}",
            "right": "TRUE",
        },
        "timeout_seconds": 0,
        "email_notifications": {},
        "notification_settings": dict(NOTIF),
        "webhook_notifications": {},
    }
    cleanup = {
        "task_key": "cleanup",
        "depends_on": [{"task_key": GATE, "outcome": "true"}],
        "run_if": "ALL_SUCCESS",
        "notebook_task": {
            "notebook_path": f"{repo_src_path}/tools/cleanup_after_benchmark",
            "base_parameters": {
                "catalog": "{{job.parameters.catalog}}",
                "wh_db": "{{job.parameters.wh_db}}",
                "scale_factor": "{{job.parameters.scale_factor}}",
            },
            "source": "WORKSPACE",
        },
        "timeout_seconds": 0,
        "email_notifications": {},
        "notification_settings": dict(NOTIF),
    }
    return [gate, cleanup]


def build(*, job_name: str, catalog: str, wh_target: str, edition: str,
          datagen_label: str, scale_factor: int, repo_src_path: str,
          tpcdi_directory: str, data_generator: str, pipeline_id: int,
          dbr: str | None = None, driver_node_type: str | None = None,
          worker_node_type: str | None = None,
          **_unused) -> dict:
    digen_high_sf = data_generator == "digen" and scale_factor > 100

    parameters: list[dict] = []
    if digen_high_sf:
        parameters.append({"name": "run_customermgmt", "default": "YES"})
    parameters += [
        {"name": "catalog", "default": catalog},
        {"name": "scale_factor", "default": str(scale_factor)},
        {"name": "wh_db", "default": f"{wh_target}_SDP_{edition}_{datagen_label}"},
        {"name": "tpcdi_directory", "default": tpcdi_directory},
        {"name": "delete_tables_when_finished", "default": "TRUE"},
    ]

    workflow: dict = {
        "name": job_name,
        "description": _description(
            data_generator=data_generator, edition=edition,
            scale_factor=scale_factor, catalog=catalog, wh_target=wh_target,
            datagen_label=datagen_label, tpcdi_directory=tpcdi_directory,
        ),
        "tags": {"data_generator": "spark" if data_generator == "spark" else "native_jar"},
        "email_notifications": {},
        "webhook_notifications": {},
        "timeout_seconds": 0,
        "max_concurrent_runs": 1,
        "format": "MULTI_TASK",
        "parameters": parameters,
    }
    if digen_high_sf:
        workflow["job_clusters"] = _job_clusters(
            job_name=job_name, dbr=dbr or "",
            driver_node_type=driver_node_type or "",
            worker_node_type=worker_node_type or "",
        )
        workflow["tasks"] = _digen_high_sf_tasks(
            job_name=job_name, repo_src_path=repo_src_path,
            pipeline_id=pipeline_id,
        )
    else:
        workflow["tasks"] = _simple_pipeline_task(pipeline_id)
    workflow["tasks"].extend(_cleanup_tasks(repo_src_path=repo_src_path))
    return workflow
