"""Create a TPC-DI benchmark workflow (the ingest → dim/fact pipeline).

Dispatches across workflow types — CLUSTER / DBSQL / DLT variants — by picking
the appropriate Jinja template, building its ``dag_args``, optionally creating
a warehouse or DLT pipeline first, then submitting the workflow to the Jobs
API. Returns the created ``job_id``.
"""
import json
from typing import Callable, Optional

from _workflow_utils import render_dag, submit_dag, template_path
from workflow_builders import dlt_pipeline as _dlt_pipeline_builder
from workflow_builders import dlt_workflow as _dlt_workflow_builder


_JOBS_API_ENDPOINT = "/api/2.1/jobs/create"
_PIPELINES_API_ENDPOINT = "/api/2.0/pipelines"
_WH_API_ENDPOINT = "/api/2.0/sql/warehouses"
_WAREHOUSE_TEMPLATE = "warehouse_jinja_template.json"

_WH_SCALE_FACTOR_MAP = {
    "10": "2X-Small",
    "100": "2X-Small",
    "1000": "Small",
    "5000": "Large",
    "10000": "X-Large",
}


def _pick_workflow_template(sku_head: str, incremental: bool) -> str | None:
    """Jinja template for the wrapping workflow, or None if a Python builder
    handles it (currently: DLT)."""
    if sku_head == "DLT":
        return None
    if incremental:
        return "workflows_incremental.json"
    return "workflows_single_batch.json"


def _get_or_create_warehouse(wh_name: str, dag_args: dict,
                              workspace_src_path: str, api_call: Callable) -> int:
    """Return existing warehouse id or create one from the warehouse template."""
    response = api_call(None, "GET", _WH_API_ENDPOINT)
    for wh in json.loads(response.text)["warehouses"]:
        if wh["name"] == wh_name:
            print(f"DB SQL Warehouse {wh_name} exists! Warehouse ID: {wh['id']}")
            return wh["id"]
    print(f"Warehouse {wh_name} does not exist, creating...")
    rendered = render_dag(template_path(workspace_src_path, _WAREHOUSE_TEMPLATE),
                          dag_args)
    response = api_call(rendered, "POST", _WH_API_ENDPOINT)
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
    # optional compute — required for non-serverless CLUSTER and DLT paths
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

    ``wf_key`` is the short key (e.g. ``CLUSTER``, ``DLT-CORE``, ``DBSQL``) and
    ``workflow_type`` is the display label — both are derived by the Driver
    from setup's ``workflows_dict``.
    """
    sku = wf_key.split("-")
    workflow_template = _pick_workflow_template(sku[0], incremental)

    if sku[0] == "DLT":
        print("All 3 TPC-DI batches will be executed in a single DLT pipeline batch.")
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

    # The Driver builds tpcdi_directory up to (but not including) sf={scale_factor}/
    # — including the generator-specific subdir (spark_datagen/ for Spark, none
    # for DIGen) — so we pass it through unchanged. Downstream notebooks append
    # sf=${scale_factor}/ themselves.
    # Suffixes appended to wh_db (and embedded in templates' descriptions) so
    # the Spark and DIGen runs at the same SF/exec_type don't share a schema.
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
    elif sku[0] == "DLT":
        if not all([worker_node_type, driver_node_type, worker_cores_mult,
                    node_types]):
            raise ValueError(
                "DLT workflow requires worker_node_type, driver_node_type, "
                "worker_cores_mult, and node_types")
        dlt_count = max(1, round(
            scale_factor * worker_cores_mult / node_types[worker_node_type]["num_cores"]))
        dag_args["dlt_worker_node_type"] = worker_node_type
        dag_args["dlt_driver_node_type"] = driver_node_type
        dag_args["dlt_worker_node_count"] = dlt_count
        compute_summary = (
            f"Driver Type:              {driver_node_type}\n"
            f"Worker Type:              {worker_node_type}\n"
            f"Worker Count:             {dlt_count}")

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

    # --- DLT: create the pipeline first, then the wrapping workflow ---
    if sku[0] == "DLT":
        dag_args["edition"] = sku[1]
        print("Building DLT Pipeline JSON via workflow_builders.dlt_pipeline")
        pipeline_dag = _dlt_pipeline_builder.build(**dag_args)
        print("Submitting built DLT Pipeline JSON to Databricks Pipelines API")
        dag_args["pipeline_id"] = submit_dag(
            pipeline_dag, _PIPELINES_API_ENDPOINT, api_call, dag_type="pipeline")

    # --- Warehouse-backed: ensure a warehouse exists, capture its id ---
    if sku[0] in ("DBT", "STMV", "DBSQL"):
        dag_args["wh_id"] = _get_or_create_warehouse(
            dag_args["wh_name"], dag_args, workspace_src_path, api_call)

    # --- Submit the workflow itself ---
    if workflow_template is None:
        # DLT path: use the Python builder.
        print("Building DLT Workflow JSON via workflow_builders.dlt_workflow")
        workflow_dag = _dlt_workflow_builder.build(**dag_args)
    else:
        wf_template_path = template_path(workspace_src_path, workflow_template)
        print(f"Rendering New Workflow JSON via {workflow_template}")
        workflow_dag = render_dag(wf_template_path, dag_args)
    print("Submitting Workflow JSON to Databricks Jobs API")
    return submit_dag(workflow_dag, _JOBS_API_ENDPOINT, api_call)
