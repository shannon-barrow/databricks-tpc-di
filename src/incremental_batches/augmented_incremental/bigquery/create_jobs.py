"""Create the Augmented Incremental BigQuery parent + child jobs on the
GCP Databricks workspace.

Mirrors the Redshift / Snowflake variant launchers. Run once per scale
factor to register the parent + child Jobs; subsequent runs trigger the
parent via `databricks jobs run-now`.

No personal identifiers are baked in. The caller supplies the workspace-
specific values; anything omitted is derived at runtime:
  - repo_src_path : if omitted, derived from `databricks current-user me`
                    (-> /Workspace/Users/{you}/databricks-tpc-di-augmented/src)
  - gcs_volume_prefix : REQUIRED — the gs:// prefix backing the UC external
                    volume (no universal default; supply your own bucket)
  - catalog       : REQUIRED — your BigQuery project id
  - name_prefix   : if omitted, derived from your username so job names are
                    unique per user

Usage (standalone CLI):

    python3 src/incremental_batches/augmented_incremental/bigquery/create_jobs.py \
        10 --catalog my-bq-project --gcs-volume-prefix gs://my-bucket/tpcdi/ \
        [--profile my-profile]

Or imported (e.g. from a driver notebook that already knows repo_src_path):

    from create_jobs import create
    create(scale_factor=10,
           catalog="my-bq-project",
           gcs_volume_prefix="gs://my-bucket/tpcdi/",
           repo_src_path=<workspace src path>,
           profile="my-profile")

Trigger the registered parent with:

    databricks jobs run-now --profile <profile> --json '{"job_id": <parent_id>}'
"""
import json
import os
import subprocess
import sys

sys.path.insert(0, os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "..", "..", "..", "tools"))

from workflow_builders.augmented_bigquery import build_child, build_parent


DEFAULT_PROFILE = "tpc-di-gcp"

# Non-personal defaults. User/infra-specific values are parameters of
# create() (catalog / gcs_volume_prefix / repo_src_path), NOT baked in here.
DEFAULTS = dict(
    tpcdi_directory="/Volumes/main/tpcdi_raw_data/tpcdi_volume/",
    wh_db="tpcdi_aug_bq_dbt",                      # target dataset prefix -> {wh_db}_sf{N}
    secret_catalog="main",
    secret_schema="tpcdi_bigquery",
    bq_location="us-central1",
    databricks_catalog="main",                     # for the bootstrap seed step
)


def _databricks_api(method: str, path: str, profile: str, body: dict | None = None) -> dict:
    cmd = ["databricks", "api", method, "--profile", profile, path]
    if body is not None:
        cmd += ["--json", json.dumps(body)]
    p = subprocess.run(cmd, capture_output=True, text=True, check=True)
    return json.loads(p.stdout) if p.stdout.strip() else {}


def _current_user(profile: str) -> str:
    """Return the caller's username (email) via the Databricks CLI, so
    standalone runs derive repo_src_path / name_prefix without a hardcoded
    identity. A notebook caller can pass those explicitly to skip this."""
    out = subprocess.run(
        ["databricks", "current-user", "me", "--profile", profile, "--output", "json"],
        capture_output=True, text=True, check=True,
    ).stdout
    return json.loads(out)["userName"]


def _create_job(spec: dict, profile: str) -> int:
    out = _databricks_api("post", "/api/2.1/jobs/create", profile, spec)
    return out["job_id"]


def create(scale_factor: int, *,
           catalog: str,
           gcs_volume_prefix: str,
           repo_src_path: str | None = None,
           profile: str = DEFAULT_PROFILE,
           name_prefix: str | None = None,
           child_name: str | None = None,
           parent_name: str | None = None,
           interactive_cluster_id: str | None = None,
           **overrides) -> tuple[int, int]:
    """Build + create the BQ parent + child jobs for one scale factor.

    Args:
        scale_factor: TPC-DI scale factor.
        catalog: REQUIRED. Your BigQuery project id.
        gcs_volume_prefix: REQUIRED. gs:// prefix backing the UC external
            volume (e.g. "gs://my-bucket/tpcdi/").
        repo_src_path: Workspace path to the repo `src` dir. If None,
            derived from the current user via the CLI.
        profile: Databricks CLI profile.
        name_prefix: Job-name prefix. If None, derived from the username.
        overrides: Any DEFAULTS key can be overridden.

    Returns (child_id, parent_id).
    """
    _user = None
    if repo_src_path is None:
        _user = _current_user(profile)
        repo_src_path = f"/Workspace/Users/{_user}/databricks-tpc-di-augmented/src"
    if name_prefix is None:
        _user = _user or _current_user(profile)
        name_prefix = _user.split("@")[0].replace(".", "-")

    child_name = child_name or f"{name_prefix}-TPCDI-SF{scale_factor}-AugIncr-BQ-DBT-Child"
    parent_name = parent_name or f"{name_prefix}-TPCDI-SF{scale_factor}-AugIncr-BQ-DBT-Parent"

    common = dict(
        DEFAULTS,
        catalog=catalog,
        repo_src_path=repo_src_path,
        gcs_volume_prefix=gcs_volume_prefix,
        scale_factor=scale_factor,
        interactive_cluster_id=interactive_cluster_id,
        **overrides,
    )

    child_spec = build_child(job_name=child_name, **common)
    child_id = _create_job(child_spec, profile)
    print(f"child job:  {child_id}  ({child_name})")

    parent_spec = build_parent(job_name=parent_name, child_job_id=child_id, **common)
    parent_id = _create_job(parent_spec, profile)
    print(f"parent job: {parent_id}  ({parent_name})")
    print()
    print(f"trigger with:")
    print(f'  databricks jobs run-now --profile {profile} \\')
    print(f'    --json \'{{"job_id": {parent_id}}}\'')
    return (child_id, parent_id)


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(description="Register BigQuery augmented-incremental jobs.")
    ap.add_argument("scale_factor", type=int)
    ap.add_argument("--catalog", required=True, help="BigQuery project id")
    ap.add_argument("--gcs-volume-prefix", required=True,
                    help="gs:// prefix backing the UC external volume, e.g. gs://my-bucket/tpcdi/")
    ap.add_argument("--profile", default=DEFAULT_PROFILE)
    ap.add_argument("--repo-src-path", default=None,
                    help="Workspace src path; derived from current user if omitted")
    ap.add_argument("--name-prefix", default=None,
                    help="Job-name prefix; derived from current user if omitted")
    ap.add_argument("--interactive-cluster-id", default=None)
    a = ap.parse_args()
    create(a.scale_factor,
           catalog=a.catalog,
           gcs_volume_prefix=a.gcs_volume_prefix,
           profile=a.profile,
           repo_src_path=a.repo_src_path,
           name_prefix=a.name_prefix,
           interactive_cluster_id=a.interactive_cluster_id)
