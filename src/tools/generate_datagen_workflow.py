"""Create a TPC-DI data-generation workflow.

Two implementations are supported, selected by the ``data_generator`` arg:

- ``"spark"`` (default): the distributed PySpark generator runs as a
  multi-task DAG (`tools/data_gen_tasks/data_gen` entry → per-dataset
  gen_* / copy_* tasks → audit_emit → cleanup_intermediates) on
  serverless. Outputs are split files like ``Customer_1.txt``,
  ``Customer_2.txt``, etc.
- ``"digen"``: the legacy single-threaded ``DIGen.jar`` utility wrapped by
  `tools/digen_runner`, run inline from a single
  `tools/data_gen_tasks/data_gen` task. Forced non-serverless DBR 15.4 +
  Photon cluster, with worker count scaling by scale_factor. Outputs are
  single files named ``Customer.txt``, etc.

Both workflows share the same entry-point notebook
(`tools/data_gen_tasks/data_gen`) which branches on ``data_gen_type``.
For DIGen mode it imports `digen_runner` and runs the JAR inline; for
spark/augmented_incremental mode it does first-task init work and lets
the downstream per-dataset gen tasks generate the data.
"""
from typing import Callable, Optional

from _workflow_utils import submit_dag
from workflow_builders import datagen_digen, datagen_spark, augmented_staging


_BUILDERS = {
    "spark": datagen_spark.build,
    "digen": datagen_digen.build,
    "augmented_incremental": augmented_staging.build,
}
_JOBS_API_ENDPOINT = "/api/2.1/jobs/create"


def generate_datagen_workflow(
    *,
    job_name: str,
    scale_factor: int,
    catalog: str,
    regenerate_data: str,
    log_level: str,
    repo_src_path: str,
    workspace_src_path: str,
    api_call: Callable,
    data_generator: str = "spark",
    default_dbr_version: Optional[str] = None,
    default_worker_type: Optional[str] = None,
    serverless: str = "YES",
    node_types: Optional[dict] = None,
    cloud_provider: Optional[str] = None,
) -> int:
    """Create the data-generation workflow and return its ``job_id``.

    Args:
        job_name: Final job name as used in the Jobs API (Driver builds it
            as ``{base}-SF{sf}-{SparkGen|NativeGen}``).
        scale_factor: TPC-DI scale factor (int; stringified for the template).
        catalog: UC catalog for the generated volume.
        regenerate_data: ``"YES"`` or ``"NO"`` — whether to delete existing data.
        log_level: Log level for the Spark generator
            (``DEBUG`` / ``INFO`` / ``WARN``). Ignored for DIGen path.
        repo_src_path: Workspace-relative path to the ``src`` directory (without
            ``/Workspace`` prefix). Used inside the rendered notebook_task path.
        workspace_src_path: Absolute workspace path to the ``src`` directory.
            Used to locate the Jinja template file.
        api_call: Callable matching the signature ``api_call(payload, method,
            endpoint) -> Response`` (supplied by the Driver setup).
        data_generator: Which implementation to use — ``"spark"`` (default,
            distributed PySpark on serverless) or ``"digen"`` (legacy DIGen.jar
            on classic single-node).
        default_dbr_version: DBR version id for the DIGen path's classic cluster.
            Required when ``data_generator='digen'``.
        default_worker_type: Node type id for the DIGen path's classic cluster.
            Required when ``data_generator='digen'``.
    """
    if data_generator not in _BUILDERS:
        raise ValueError(
            f"Unknown data_generator={data_generator!r}; "
            f"expected one of {sorted(_BUILDERS.keys())}"
        )
    # augmented_incremental currently shares the single-task spark builder. The multi-task wrapper (stage_files / stage_tables / cleanup) lands in workflow_builders/augmented_staging.py.
    if data_generator == "digen" and not default_worker_type:
        raise ValueError(
            "data_generator='digen' requires default_worker_type — DIGen.jar "
            "runs on a classic, non-serverless cluster (Java subprocess "
            "can't run on serverless), and the default node type comes "
            "from setup_context's cloud-aware default."
        )

    print(f"Building Data Generation Workflow JSON via "
          f"workflow_builders.datagen_{data_generator} (Python builder)")
    dag_dict = _BUILDERS[data_generator](
        job_name=job_name,
        scale_factor=scale_factor,
        catalog=catalog,
        regenerate_data=regenerate_data,
        log_level=log_level,
        repo_src_path=repo_src_path,
        default_dbr_version=default_dbr_version,
        default_worker_type=default_worker_type,
        serverless=serverless,
        node_types=node_types,
        cloud_provider=cloud_provider,
    )
    print(f"Submitting built JSON to Databricks API {_JOBS_API_ENDPOINT}")
    return submit_dag(dag_dict, _JOBS_API_ENDPOINT, api_call)
