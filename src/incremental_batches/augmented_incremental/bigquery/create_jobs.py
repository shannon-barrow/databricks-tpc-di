"""Create the Augmented Incremental BigQuery parent + child jobs on the
GCP Databricks workspace.

Mirrors the Snowflake-variant manual launchers (e.g. the snowflake DT
parallel jobs the user keeps in ~/Downloads or /tmp).

Usage (one-off):

    python3 -c "from create_jobs import create; create(scale_factor=10)"

or directly:

    python3 src/incremental_batches/augmented_incremental/bigquery/create_jobs.py 10

Outputs the parent and child job IDs. Trigger with:

    databricks jobs run-now --profile tpc-di-gcp \
      --json '{"job_id": <parent_id>}'
"""
import json
import os
import subprocess
import sys

sys.path.insert(0, os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "..", "..", "..", "tools"))

from workflow_builders.augmented_bigquery import build_child, build_parent


PROFILE = "tpc-di-gcp"
REPO_SRC_PATH = "/Workspace/Users/shannon.barrow@databricks.com/databricks-tpc-di-augmented/src"

DEFAULTS = dict(
    repo_src_path=REPO_SRC_PATH,
    catalog="databricks-sandbox-perfeng",          # BQ project
    tpcdi_directory="/Volumes/main/tpcdi_raw_data/tpcdi_volume/",
    wh_db="shannon_aug_bq_dbt",                    # target dataset → shannon_aug_bq_dbt_sf{N}
    secret_scope="tpcdi_bigquery",
    bq_location="us-central1",
    gcs_volume_prefix="gs://shannon-tpcdi/tpcdi/",
    databricks_catalog="main",                     # for the bootstrap seed step
)


def _databricks_api(method: str, path: str, body: dict | None = None) -> dict:
    cmd = ["databricks", "api", method, "--profile", PROFILE, path]
    if body is not None:
        cmd += ["--json", json.dumps(body)]
    p = subprocess.run(cmd, capture_output=True, text=True, check=True)
    return json.loads(p.stdout) if p.stdout.strip() else {}


def _create_job(spec: dict) -> int:
    out = _databricks_api("post", "/api/2.1/jobs/create", spec)
    return out["job_id"]


def create(scale_factor: int, *,
           child_name: str | None = None,
           parent_name: str | None = None,
           interactive_cluster_id: str | None = None) -> tuple[int, int]:
    """Build + create the BQ parent + child jobs for one scale factor.

    Returns (child_id, parent_id).
    """
    child_name = child_name or (
        f"shannon-barrow-TPCDI-SF{scale_factor}-AugIncr-BQ-DBT-Child")
    parent_name = parent_name or (
        f"shannon-barrow-TPCDI-SF{scale_factor}-AugIncr-BQ-DBT-Parent")

    common = dict(DEFAULTS, scale_factor=scale_factor,
                  interactive_cluster_id=interactive_cluster_id)

    child_spec = build_child(job_name=child_name, **common)
    child_id = _create_job(child_spec)
    print(f"child job:  {child_id}  ({child_name})")

    parent_spec = build_parent(job_name=parent_name, child_job_id=child_id, **common)
    parent_id = _create_job(parent_spec)
    print(f"parent job: {parent_id}  ({parent_name})")
    print()
    print(f"trigger with:")
    print(f'  databricks jobs run-now --profile {PROFILE} \\')
    print(f'    --json \'{{"job_id": {parent_id}}}\'')
    return (child_id, parent_id)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.exit("usage: create_jobs.py <scale_factor>")
    create(int(sys.argv[1]))
