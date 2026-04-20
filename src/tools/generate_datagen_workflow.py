"""Create a TPC-DI data-generation workflow (serverless, single notebook task).

Renders ``jinja_templates/datagen_workflow.json`` and POSTs it to the Jobs API.
"""
from typing import Callable

from _workflow_utils import render_and_submit, template_path


_TEMPLATE_NAME = "datagen_workflow.json"
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
) -> int:
    """Create the data-generation workflow and return its ``job_id``.

    Args:
        job_name: Base job name — template appends ``_DataGen``.
        scale_factor: TPC-DI scale factor (int; stringified for the template).
        catalog: UC catalog for the generated volume.
        regenerate_data: ``"YES"`` or ``"NO"`` — whether to delete existing data.
        log_level: Log level for the generator (``DEBUG`` / ``INFO`` / ``WARN``).
        repo_src_path: Workspace-relative path to the ``src`` directory (without
            ``/Workspace`` prefix). Used inside the rendered notebook_task path.
        workspace_src_path: Absolute workspace path to the ``src`` directory.
            Used to locate the Jinja template file.
        api_call: Callable matching the signature ``api_call(payload, method,
            endpoint) -> Response`` (supplied by the Driver setup).
    """
    dag_args = {
        "job_name": job_name,
        "scale_factor": scale_factor,
        "catalog": catalog,
        "regenerate_data": regenerate_data,
        "log_level": log_level,
        "repo_src_path": repo_src_path,
    }
    print(f"Rendering Data Generation Workflow JSON via {_TEMPLATE_NAME}")
    return render_and_submit(
        template_path(workspace_src_path, _TEMPLATE_NAME),
        dag_args,
        _JOBS_API_ENDPOINT,
        api_call,
    )
