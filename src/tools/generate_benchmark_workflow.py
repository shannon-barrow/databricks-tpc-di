"""Create a TPC-DI benchmark workflow (the ingest → dim/fact pipeline).

Dispatches across workflow types — CLUSTER / DBSQL / SDP / AUGMENTED — by
picking the right Python builder, optionally creating a warehouse or SDP
pipeline first, then submitting the workflow(s) to the Jobs API. Returns
the primary ``job_id`` (parent job for AUGMENTED variants).
"""
import json
from typing import Callable, Optional

from _workflow_utils import submit_dag
from workflow_builders import sdp_pipeline as _sdp_pipeline_builder
from workflow_builders import sdp_workflow as _sdp_workflow_builder
from workflow_builders import warehouse as _warehouse_builder
from workflow_builders import workflows_single_batch as _single_batch_builder
from workflow_builders import workflows_incremental as _incremental_builder
from workflow_builders import augmented_classic as _augmented_classic_builder
from workflow_builders import augmented_sdp as _augmented_sdp_builder


_JOBS_API_ENDPOINT = "/api/2.1/jobs/create"
_PIPELINES_API_ENDPOINT = "/api/2.0/pipelines"
_WH_API_ENDPOINT = "/api/2.0/sql/warehouses"
_WH_SCALE_FACTOR_MAP = {
    "10": "2X-Small",
    "100": "2X-Small",
    "1000": "Small",
    "5000": "Large",
    "10000": "X-Large",
}


def _generate_augmented(*, variant: str, parent_job_name: str,
                         catalog: str, wh_target: str, scale_factor: int,
                         tpcdi_directory: str, repo_src_path: str,
                         api_call: Callable) -> int:
    """Submit the augmented-incremental benchmark resources and return the
    parent job_id.

    Creates (in order):
      - CLUSTER variant: child job → parent job
      - SDP variant: pipeline → child job → parent job

    `wh_db` is built as ``{wh_target}_AugmentedIncremental_{Cluster|SDP}``
    so two users running concurrently don't share Autoloader state under
    ``_dailybatches/{wh_db}_{sf}/``.
    """
    label = "Cluster" if variant == "CLUSTER" else "SDP"
    wh_db = f"{wh_target}_AugmentedIncremental_{label}"

    # parent_job_name comes from the Driver as ``{base}-SF{sf}-AugmentedIncremental-{Cluster|SDP}-Parent`` — derive child + pipeline names by stripping/replacing the suffix.
    child_job_name = parent_job_name[:-len("-Parent")] if parent_job_name.endswith("-Parent") else parent_job_name
    pipeline_name  = f"{child_job_name}-Pipeline"

    common = dict(
        catalog=catalog, scale_factor=scale_factor,
        tpcdi_directory=tpcdi_directory, wh_db=wh_db,
        repo_src_path=repo_src_path,
    )

    print(f"\nAugmented Incremental ({label})")
    print(f"  target schema:  {catalog}.{wh_db}_{scale_factor}")
    print(f"  staging schema: {catalog}.tpcdi_incremental_staging_{scale_factor}")
    print(f"  parent job:     {parent_job_name}")
    print(f"  child job:      {child_job_name}")
    if variant == "SDP":
        print(f"  pipeline:       {pipeline_name}")
    print()

    if variant == "SDP":
        print("Building SDP pipeline JSON via workflow_builders.augmented_sdp.build_pipeline")
        pipeline_dag = _augmented_sdp_builder.build_pipeline(name=pipeline_name, **common)
        print("Submitting SDP pipeline JSON to Databricks Pipelines API")
        pipeline_id = submit_dag(
            pipeline_dag, _PIPELINES_API_ENDPOINT, api_call, dag_type="pipeline")

        print("Building child workflow JSON via workflow_builders.augmented_sdp.build_child")
        child_dag = _augmented_sdp_builder.build_child(
            job_name=child_job_name, pipeline_id=pipeline_id, **common)
        print("Submitting child workflow JSON to Databricks Jobs API")
        child_job_id = submit_dag(child_dag, _JOBS_API_ENDPOINT, api_call)

        print("Building parent workflow JSON via workflow_builders.augmented_sdp.build_parent")
        parent_dag = _augmented_sdp_builder.build_parent(
            job_name=parent_job_name, child_job_id=child_job_id,
            pipeline_id=pipeline_id, **common)
        print("Submitting parent workflow JSON to Databricks Jobs API")
        return submit_dag(parent_dag, _JOBS_API_ENDPOINT, api_call)

    # CLUSTER variant
    print("Building child workflow JSON via workflow_builders.augmented_classic.build_child")
    child_dag = _augmented_classic_builder.build_child(
        job_name=child_job_name, **common)
    print("Submitting child workflow JSON to Databricks Jobs API")
    child_job_id = submit_dag(child_dag, _JOBS_API_ENDPOINT, api_call)

    print("Building parent workflow JSON via workflow_builders.augmented_classic.build_parent")
    parent_dag = _augmented_classic_builder.build_parent(
        job_name=parent_job_name, child_job_id=child_job_id, **common)
    print("Submitting parent workflow JSON to Databricks Jobs API")
    return submit_dag(parent_dag, _JOBS_API_ENDPOINT, api_call)


def _get_or_create_warehouse(wh_name: str, dag_args: dict,
                              workspace_src_path: str, api_call: Callable) -> int:
    """Return existing warehouse id or create one from the warehouse builder."""
    response = api_call(None, "GET", _WH_API_ENDPOINT)
    for wh in json.loads(response.text)["warehouses"]:
        if wh["name"] == wh_name:
            print(f"DB SQL Warehouse {wh_name} exists! Warehouse ID: {wh['id']}")
            return wh["id"]
    print(f"Warehouse {wh_name} does not exist, creating...")
    payload = _warehouse_builder.build(**dag_args)
    response = api_call(payload, "POST", _WH_API_ENDPOINT)
    wh_id = json.loads(response.text)["id"]
    print(f"DB SQL Warehouse {wh_name} created. Warehouse ID: {wh_id}")
    return wh_id


def generate_benchmark_workflow(
    *,
    wf_key: str,
    workflow_type: str,
    job_name: str,
    catalog: str,
    wh_target: str,
    scale_factor: int,
    tpcdi_directory: str,
    repo_src_path: str,
    workspace_src_path: str,
    cloud_provider: str,
    serverless: str,
    pred_opt: str,
    perf_opt_flg: bool,
    incremental: bool,
    data_generator: str,
    api_call: Callable,
    # optional compute — required for non-serverless CLUSTER and SDP paths
    worker_node_type: Optional[str] = None,
    driver_node_type: Optional[str] = None,
    dbr_version_id: Optional[str] = None,
    default_dbr_version: Optional[str] = None,
    default_worker_type: Optional[str] = None,
    cust_mgmt_type: Optional[str] = None,
    worker_cores_mult: Optional[float] = None,
    node_types: Optional[dict] = None,
) -> int:
    """Render the benchmark workflow template and submit it.

    ``wf_key`` is the short key (e.g. ``CLUSTER``, ``SDP-CORE``, ``DBSQL``) and
    ``workflow_type`` is the display label — both are derived by the Driver
    from setup's ``workflows_dict``.
    """
    sku = wf_key.split("-")

    # AUGMENTED variants take an early-exit path. They don't use the CLUSTER/DBSQL/SDP compute selection, always use Spark-staged data, and create multiple resources (parent + child + optional pipeline) rather than a single workflow.
    if sku[0] == "AUGMENTED":
        return _generate_augmented(
            variant=sku[1],
            parent_job_name=job_name,
            catalog=catalog,
            wh_target=wh_target,
            scale_factor=scale_factor,
            tpcdi_directory=tpcdi_directory,
            repo_src_path=repo_src_path,
            api_call=api_call,
        )

    if sku[0] == "SDP":
        print("All 3 TPC-DI batches will be executed in a single SDP pipeline batch.")
    elif incremental:
        print("Each of the 3 TPC-DI batches will be executed incrementally with "
              "batches 2 and 3 performing merges.")
    else:
        print("All 3 TPC-DI batches will be executed in a single job batch.")

    exec_folder = "SQL" if sku[0] == "DBSQL" else sku[0]

    if perf_opt_flg:
        null_constraint = ""
        opt_write = "'delta.autoOptimize.optimizeWrite'=False"
        index_cols = ", 'delta.dataSkippingNumIndexedCols' = 0"
    else:
        null_constraint = "NOT NULL"
        opt_write = "'delta.autoOptimize.optimizeWrite'=True"
        index_cols = ""

    # The Driver builds tpcdi_directory up to (but not including) sf={scale_factor}/ — including the generator-specific subdir (spark_datagen/ for Spark, none for DIGen) — so we pass it through unchanged. Downstream notebooks append sf=${scale_factor}/ themselves. Suffixes appended to wh_db (and embedded in templates' descriptions) so the Spark and DIGen runs at the same SF/exec_type don't share a schema.
    datagen_label = "spark_data_gen" if data_generator == "spark" else "native_data_gen"
    batched_label = "incremental" if incremental else "single_batch"

    dag_args = {
        "catalog": catalog,
        "wh_target": wh_target,
        "tpcdi_directory": tpcdi_directory,
        "scale_factor": scale_factor,
        "job_name": job_name,
        "repo_src_path": repo_src_path,
        "cloud_provider": cloud_provider,
        "exec_type": sku[0],
        "serverless": serverless,
        "pred_opt": pred_opt,
        "exec_folder": exec_folder,
        "null_constraint": null_constraint,
        "perf_opt_flg": perf_opt_flg,
        "opt_write": opt_write,
        "index_cols": index_cols,
        "data_generator": data_generator,
        "datagen_label": datagen_label,
        "batched_label": batched_label,
    }

    # --- Compute selection ---
    if sku[0] == "CLUSTER" and serverless != "YES":
        if not all([worker_node_type, driver_node_type, dbr_version_id,
                    worker_cores_mult, node_types]):
            raise ValueError(
                "Non-serverless CLUSTER workflow requires worker_node_type, "
                "driver_node_type, dbr_version_id, worker_cores_mult, and "
                "node_types")
        worker_node_count = round(
            scale_factor * worker_cores_mult / node_types[worker_node_type]["num_cores"])
        if worker_node_count == 0:
            driver_node_type = worker_node_type
        dag_args["worker_node_type"] = worker_node_type
        dag_args["driver_node_type"] = driver_node_type
        dag_args["worker_node_count"] = worker_node_count
        dag_args["dbr"] = dbr_version_id
        compute_summary = (
            f"Driver Type:              {driver_node_type}\n"
            f"Worker Type:              {worker_node_type}\n"
            f"Worker Count:             {worker_node_count}\n"
            f"DBR Version:              {dbr_version_id}")
    else:
        if default_dbr_version is None or default_worker_type is None:
            raise ValueError(
                "Serverless / non-CLUSTER workflows require default_dbr_version "
                "and default_worker_type")
        dag_args["dbr"] = default_dbr_version
        cm_or_default = (cust_mgmt_type if scale_factor > 1000 and cust_mgmt_type
                         else default_worker_type)
        dag_args["driver_node_type"] = cm_or_default
        dag_args["worker_node_type"] = cm_or_default
        dag_args["worker_node_count"] = 0
        compute_summary = None  # set below

    # Warehouse-backed workflows need a warehouse (create if missing)
    if sku[0] in ("DBT", "STMV", "DBSQL"):
        wh_size = _WH_SCALE_FACTOR_MAP[str(scale_factor)]
        wh_name = f"TPCDI_{wh_size}"
        dag_args["wh_name"] = wh_name
        dag_args["wh_size"] = wh_size
        print(f"Your workflow type requires Databricks SQL.\n"
              f"Serverless Warehouses are created by default.\n"
              f"If you do not have serverless SQL WHs available, please CREATE "
              f"a non-serverless {wh_size} WH with the name '{wh_name}' and try again.")
        compute_summary = (f"Warehouse Name:           {wh_name}\n"
                           f"Warehouse Size:           {wh_size}")
    elif serverless == "YES":
        compute_summary = (
            f"Serverless {sku[0]} selected.\n"
            f"If your workspace does not have access to Serverless the workflow "
            f"creation will potentially fail. If it does, select 'NO' for the "
            f"Serverless widget.")
    elif sku[0] == "SDP":
        if not all([worker_node_type, driver_node_type, worker_cores_mult,
                    node_types]):
            raise ValueError(
                "SDP workflow requires worker_node_type, driver_node_type, "
                "worker_cores_mult, and node_types")
        sdp_count = max(1, round(
            scale_factor * worker_cores_mult / node_types[worker_node_type]["num_cores"]))
        dag_args["sdp_worker_node_type"] = worker_node_type
        dag_args["sdp_driver_node_type"] = driver_node_type
        dag_args["sdp_worker_node_count"] = sdp_count
        compute_summary = (
            f"Driver Type:              {driver_node_type}\n"
            f"Worker Type:              {worker_node_type}\n"
            f"Worker Count:             {sdp_count}")

    print(f"""
Workflow Name:            {job_name}
Workflow Type:            {workflow_type}
{compute_summary}
Target TPCDI Catalog:     {catalog}
Target TPCDI Database:    {wh_target}
TPCDI Staging Database:   {wh_target}_stage
Raw Files Path:           {tpcdi_directory}
Scale Factor:             {scale_factor}
""")

    # --- SDP: create the pipeline first, then the wrapping workflow ---
    if sku[0] == "SDP":
        dag_args["edition"] = sku[1]
        print("Building SDP Pipeline JSON via workflow_builders.sdp_pipeline")
        pipeline_dag = _sdp_pipeline_builder.build(**dag_args)
        print("Submitting built SDP Pipeline JSON to Databricks Pipelines API")
        dag_args["pipeline_id"] = submit_dag(
            pipeline_dag, _PIPELINES_API_ENDPOINT, api_call, dag_type="pipeline")

    # --- Warehouse-backed: ensure a warehouse exists, capture its id ---
    if sku[0] in ("DBT", "STMV", "DBSQL"):
        dag_args["wh_id"] = _get_or_create_warehouse(
            dag_args["wh_name"], dag_args, workspace_src_path, api_call)

    # --- Submit the workflow itself ---
    if sku[0] == "SDP":
        print("Building SDP Workflow JSON via workflow_builders.sdp_workflow")
        workflow_dag = _sdp_workflow_builder.build(**dag_args)
    elif incremental:
        print("Building Workflow JSON via workflow_builders.workflows_incremental")
        workflow_dag = _incremental_builder.build(**dag_args)
    else:
        print("Building Workflow JSON via workflow_builders.workflows_single_batch")
        workflow_dag = _single_batch_builder.build(**dag_args)
    print("Submitting Workflow JSON to Databricks Jobs API")
    return submit_dag(workflow_dag, _JOBS_API_ENDPOINT, api_call)
