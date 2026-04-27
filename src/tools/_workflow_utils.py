"""Shared helper for submitting built workflow dicts to the Databricks API."""
import json
from typing import Callable


def submit_dag(dag_dict: dict, api_endpoint: str, api_call: Callable,
               dag_type: str = "job") -> int:
    """POST a built dag dict to the given API and return the created id."""
    response = api_call(dag_dict, "POST", api_endpoint)
    if response.status_code != 200:
        raise RuntimeError(
            f"API call for {dag_type} submission failed with status "
            f"{response.status_code}: {response.text}")
    response_id = json.loads(response.text)[f"{dag_type}_id"]
    print(f"{dag_type} {response_id} created.")
    return response_id
