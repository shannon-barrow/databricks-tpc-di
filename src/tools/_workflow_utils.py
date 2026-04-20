"""Shared helpers for the two workflow generators."""
import json
import os
from typing import Any, Callable


_TEMPLATES_DIR = "jinja_templates"


def template_path(workspace_src_path: str, template_name: str) -> str:
    return os.path.join(workspace_src_path, "tools", _TEMPLATES_DIR, template_name)


def render_dag(template_path_str: str, dag_args: dict) -> dict:
    """Render a Jinja template and parse as JSON."""
    from jinja2 import Template
    with open(template_path_str, "r") as f:
        dag_str = f.read()
    rendered = Template(dag_str).render(dag_args)
    print(f"Finished rendering JSON from {template_path_str}")
    return json.loads(rendered)


def submit_dag(dag_dict: dict, api_endpoint: str, api_call: Callable,
               dag_type: str = "job") -> int:
    """POST a rendered dag dict to the given API and return the created id."""
    response = api_call(dag_dict, "POST", api_endpoint)
    if response.status_code != 200:
        raise RuntimeError(
            f"API call for {dag_type} submission failed with status "
            f"{response.status_code}: {response.text}")
    response_id = json.loads(response.text)[f"{dag_type}_id"]
    print(f"{dag_type} {response_id} created.")
    return response_id


def render_and_submit(template_path_str: str, dag_args: dict, api_endpoint: str,
                      api_call: Callable, dag_type: str = "job") -> int:
    """Render a template and submit it. Returns the created id."""
    dag = render_dag(template_path_str, dag_args)
    print(f"Submitting rendered JSON to Databricks API {api_endpoint}")
    return submit_dag(dag, api_endpoint, api_call, dag_type)
