"""Generate the competitor (non-Databricks) TPC-DI benchmark workflows.

Standalone sibling of ``generate_benchmark_workflow`` — kept separate so the
Databricks dispatcher is never at risk. The **Competitor Driver** notebook
collects the per-engine inputs via widgets and calls ``generate_competitor_workflow``.

Scope: this ONLY creates a competitor's parent+child workflow — dbt for the
warehouses (Snowflake / Redshift / BigQuery / Fabric DW), the repo's PySpark
notebooks for the Spark engines (Fabric Spark / Fabric NEE). It does not
generate data and does not create the Databricks benchmark — the user must have
already run the Databricks augmented-incremental benchmark to generate + stage
the data the competitor reads.

Notebook-native: like the Databricks driver, it builds the job JSON with the
per-engine builders in ``workflow_builders`` and submits via the notebook's own
token (``api_call`` = ``tpcdi_config.api_call``). No CLI profile is used, so it
runs as the notebook user and works OOTB on any workspace.
"""
from __future__ import annotations

from typing import Callable, Optional

from workflow_builders import augmented_snowflake as _sf_builder
from workflow_builders import augmented_redshift as _rs_builder
from workflow_builders import augmented_bigquery as _bq_builder
from workflow_builders import augmented_fabric as _fab_dw_builder
from workflow_builders import augmented_fabric_spark as _fab_spark_builder
from _workflow_utils import submit_dag

_JOBS_API_ENDPOINT = "/api/2.1/jobs/create"

# Competitors this driver can create, and the cloud each is native to. TPC-DI
# data is generated in Databricks and read in the SAME cloud/region (a
# different region incurs egress), so a competitor only makes sense on its own
# cloud. Snowflake runs in every cloud.
COMPETITOR_CLOUD = {
    "snowflake": None,     # all clouds
    "redshift": "AWS",
    "bigquery": "GCP",
    "fabric_dw": "Azure",
    "fabric_spark": "Azure",
    "fabric_nee": "Azure",
}

# Spark competitors run the repo's PySpark notebooks on the competitor's Spark
# engine instead of a dbt project; their builders take an enable_nee-style
# engine toggle rather than warehouse connection params.
SPARK_COMPETITORS = ("fabric_spark", "fabric_nee")


def competitors_for_cloud(cloud: str) -> list[str]:
    """The competitors valid to run from a Databricks workspace in ``cloud``."""
    return [c for c, req in COMPETITOR_CLOUD.items() if req in (None, cloud)]


def _child_parent_names(engine: str, name_prefix: str, scale_factor: int) -> tuple[str, str]:
    """Parent/child job names, matching the driver's competitor suffix scheme:
    {prefix}-SF{sf}-AugmentedIncremental-{Engine}-{Child|Parent}."""
    label = {"snowflake": "Snowflake", "redshift": "Redshift", "bigquery": "BigQuery",
             "fabric_dw": "FabricDW", "fabric_spark": "FabricSpark",
             "fabric_nee": "FabricNEE"}[engine]
    base = f"{name_prefix}-SF{scale_factor}-AugmentedIncremental-{label}"
    return f"{base}-Child", f"{base}-Parent"


def generate_competitor_workflow(
    *,
    engine: str,
    scale_factor: int,
    catalog: str,
    wh_db: str,
    tpcdi_directory: str,
    repo_src_path: str,
    api_call: Callable,
    name_prefix: str,
    interactive_cluster_id: Optional[str] = None,
    # engine-specific inputs (only the selected engine's are required)
    engine_params: Optional[dict] = None,
) -> int:
    """Build + submit the competitor parent+child workflow. Returns parent id.

    ``engine_params`` carries the per-engine inputs the builder needs
    (collected by the notebook widgets):
      - snowflake: account, sf_user, snowflake_warehouse, snowflake_stage,
                   sf_credential_secret, dbx_pat_secret
      - redshift:  rs_host, rs_user, rs_iam_role, rs_password_secret,
                   s3_volume_prefix, aws_region, database
      - bigquery:  gcs_volume_prefix, sa_json_secret, bq_location,
                   databricks_catalog
      - fabric_dw: fabric_wh_host, fabric_wh_name, fabric_workspace_id,
                   fabric_lakehouse_id, fabric_lakehouse_name, tenant_id,
                   client_id, client_secret_secret
      - fabric_spark / fabric_nee: fabric_workspace_id, fabric_lakehouse_id,
                   tenant_id, client_id, client_secret_secret, and optionally
                   fabric_pool_name, fabric_node_size, fabric_node_count,
                   fabric_runtime, fabric_environment_name
    """
    engine = engine.lower()
    ep = dict(engine_params or {})
    child_name, parent_name = _child_parent_names(engine, name_prefix, scale_factor)

    common = dict(
        repo_src_path=repo_src_path,
        catalog=catalog,
        scale_factor=scale_factor,
        tpcdi_directory=tpcdi_directory,
        wh_db=wh_db,
        interactive_cluster_id=interactive_cluster_id,
    )

    print(f"\nCompetitor: {engine}")
    print(f"  target schema:  {catalog}.{wh_db}_{scale_factor}")
    print(f"  parent job:     {parent_name}")
    print(f"  child job:      {child_name}")
    print()

    if engine == "snowflake":
        builder = _sf_builder
        child_kwargs = dict(
            common,
            snowflake_stage=ep["snowflake_stage"],
            account=ep["account"],
            sf_user=ep["sf_user"],
            sf_credential_secret=ep["sf_credential_secret"],
            snowflake_warehouse=ep["snowflake_warehouse"],
        )
        parent_extra = dict(dbx_pat_secret=ep["dbx_pat_secret"])
    elif engine == "redshift":
        builder = _rs_builder
        child_kwargs = dict(
            common,
            database=ep.get("database", "dev"),
            rs_host=ep["rs_host"],
            rs_user=ep["rs_user"],
            rs_iam_role=ep["rs_iam_role"],
            rs_password_secret=ep["rs_password_secret"],
            s3_volume_prefix=ep["s3_volume_prefix"],
            aws_region=ep["aws_region"],
        )
        parent_extra = {}
    elif engine == "bigquery":
        builder = _bq_builder
        child_kwargs = dict(
            common,
            sa_json_secret=ep["sa_json_secret"],
            bq_location=ep["bq_location"],
            gcs_volume_prefix=ep["gcs_volume_prefix"],
        )
        parent_extra = dict(databricks_catalog=ep.get("databricks_catalog", catalog))
    elif engine == "fabric_dw":
        builder = _fab_dw_builder
        child_kwargs = dict(
            common,
            fabric_wh_host=ep["fabric_wh_host"],
            fabric_wh_name=ep["fabric_wh_name"],
            fabric_workspace_id=ep["fabric_workspace_id"],
            fabric_lakehouse_id=ep["fabric_lakehouse_id"],
            fabric_lakehouse_name=ep["fabric_lakehouse_name"],
            tenant_id=ep["tenant_id"],
            client_id=ep["client_id"],
            client_secret_secret=ep["client_secret_secret"],
        )
        parent_extra = {}
    elif engine in SPARK_COMPETITORS:
        builder = _fab_spark_builder
        child_kwargs = dict(
            common,
            enable_nee=(engine == "fabric_nee"),
            fabric_workspace_id=ep["fabric_workspace_id"],
            fabric_lakehouse_id=ep["fabric_lakehouse_id"],
            tenant_id=ep["tenant_id"],
            client_id=ep["client_id"],
            client_secret_secret=ep["client_secret_secret"],
        )
        parent_extra = {k: ep[k] for k in ("fabric_pool_name", "fabric_node_size",
                                           "fabric_node_count", "fabric_runtime",
                                           "fabric_environment_name") if ep.get(k)}
    else:
        raise ValueError(
            f"Unknown competitor engine '{engine}'. "
            f"Valid: {list(COMPETITOR_CLOUD)}")

    print(f"Building child workflow JSON via {builder.__name__}.build_child")
    child_dag = builder.build_child(job_name=child_name, **child_kwargs)
    child_job_id = submit_dag(child_dag, _JOBS_API_ENDPOINT, api_call)

    print(f"Building parent workflow JSON via {builder.__name__}.build_parent")
    parent_dag = builder.build_parent(
        job_name=parent_name, child_job_id=child_job_id,
        **child_kwargs, **parent_extra)
    return submit_dag(parent_dag, _JOBS_API_ENDPOINT, api_call)
