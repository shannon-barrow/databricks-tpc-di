"""Create the Augmented Incremental Redshift parent + child jobs on the
benchmarking workspace.

Mirrors the BigQuery / Snowflake variant launchers. Run once per scale
factor to register the parent + child Jobs; subsequent runs trigger the
parent via `databricks jobs run-now`.

Usage:

    python3 -c "from create_jobs import create; create(scale_factor=10)"

or:

    python3 src/incremental_batches/augmented_incremental/redshift/create_jobs.py 10

Outputs parent + child job IDs. Trigger with:

    databricks jobs run-now --profile tpcdi-fresh \
      --json '{"job_id": <parent_id>}'
"""
import json
import os
import subprocess
import sys

sys.path.insert(0, os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "..", "..", "..", "tools"))

from workflow_builders.augmented_redshift import build_child, build_parent


PROFILE = "tpcdi-fresh"   # Workspace that owns the UC external volume on the s3 bucket
REPO_SRC_PATH = "/Workspace/Users/shannon.barrow@databricks.com/databricks-tpc-di-augmented/src"

DEFAULTS = dict(
    repo_src_path=REPO_SRC_PATH,
    catalog="main",                                # Databricks UC catalog (for the external volume)
    database="dev",                                # Redshift database
    tpcdi_directory="/Volumes/main/tpcdi_raw_data/tpcdi_volume/",
    wh_db="shannon_aug_rs_dbt",                    # target schema → shannon_aug_rs_dbt_<sf>
    secret_scope="tpcdi_redshift",
    s3_volume_prefix="s3://tpcds-datasets/shannon_tpcdi/",
    aws_region="us-west-2",
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
    """Build + create the Redshift parent + child jobs for one scale factor.

    Returns (child_id, parent_id).
    """
    child_name = child_name or (
        f"shannon-barrow-TPCDI-SF{scale_factor}-AugIncr-RS-DBT-Child")
    parent_name = parent_name or (
        f"shannon-barrow-TPCDI-SF{scale_factor}-AugIncr-RS-DBT-Parent")

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
        sys.exit("usage: create_jobs.py <scale_factor> [interactive_cluster_id]")
    cluster_id = sys.argv[2] if len(sys.argv) > 2 else None
    create(int(sys.argv[1]), interactive_cluster_id=cluster_id)
