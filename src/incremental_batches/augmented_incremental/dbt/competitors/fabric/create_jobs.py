"""Create the Augmented Incremental Fabric-DW parent + child jobs on the
orchestration workspace (Azure `tpc-di`). Mirrors redshift/create_jobs.py.

Run once per scale factor to register the parent + child Jobs; trigger the
parent thereafter via `databricks jobs run-now`.

The SP creds are NOT passed here — the notebooks read client_id/client_secret
from the `tpcdi_fabric` UC secret scope. Only plain values (WH host/name,
workspace/lakehouse ids, tenant) are job parameters.

Usage:
    python3 .../dbt/competitors/fabric/create_jobs.py 10 [--profile tpc-di]

Or imported:
    from create_jobs import create
    create(scale_factor=10, repo_src_path=<workspace src>, profile="tpc-di")
"""
import json
import os
import subprocess
import sys

sys.path.insert(0, os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "..", "..", "..", "..", "..", "tools"))

from workflow_builders.augmented_fabric import build_child, build_parent

DEFAULT_PROFILE = "tpc-di"

DEFAULTS = dict(
    catalog="main",
    tpcdi_directory="/Volumes/main/tpcdi_raw_data/tpcdi_volume/",
    wh_db="shannon_aug_fabric_dbt",
    # Fabric DW (dedicated e0a370b6 workspace) — plain values.
    fabric_wh_host="skrtph5o6caeff4w6gdeuehp7q-wzykhydzalbexexpbi7qahqgrm.datawarehouse.fabric.microsoft.com",
    fabric_wh_name="tpcdi_fabric_dw",
    fabric_workspace_id="e0a370b6-0279-4bc2-92ef-0a3f001e068b",
    # tpcdi_fabric_v2 — created after enabling new-metadata-sync so its SQL endpoint
    # discovers freshly-materialized OneLake staging tables fast (the original
    # tpcdi_fabric endpoint never registered staging_sf10). Cross-DB name set via
    # setup_fabric's fabric_lakehouse_name widget default (also tpcdi_fabric_v2).
    fabric_lakehouse_id="e61ac2d6-f74c-4317-904a-c0417302aae7",
    # Cross-DB CTAS source db name; must match fabric_lakehouse_id. SF=10 uses
    # tpcdi_fabric_v2 (new-metadata-sync); SF=20000's staging lives in tpcdi_fabric
    # (9f303259) whose SQL endpoint already resolves staging_sf20000 — override
    # both id+name per SF via --fabric-lakehouse-id / --fabric-lakehouse-name.
    fabric_lakehouse_name="tpcdi_fabric_v2",
    tenant_id="9f37a392-f0ae-4280-9796-f1864a10effc",
    file_ext="txt",
)


def _api(method: str, path: str, profile: str, body: dict | None = None) -> dict:
    cmd = ["databricks", "api", method, "--profile", profile, path]
    if body is not None:
        cmd += ["--json", json.dumps(body)]
    p = subprocess.run(cmd, capture_output=True, text=True, check=True)
    return json.loads(p.stdout) if p.stdout.strip() else {}


def _current_user(profile: str) -> str:
    out = subprocess.run(
        ["databricks", "current-user", "me", "--profile", profile, "--output", "json"],
        capture_output=True, text=True, check=True).stdout
    return json.loads(out)["userName"]


def _create_job(spec: dict, profile: str) -> int:
    return _api("post", "/api/2.1/jobs/create", profile, spec)["job_id"]


def create(scale_factor: int, *, repo_src_path: str | None = None,
           profile: str = DEFAULT_PROFILE, name_prefix: str | None = None,
           **overrides) -> tuple[int, int]:
    user = None
    if repo_src_path is None:
        user = _current_user(profile)
        repo_src_path = f"/Workspace/Users/{user}/databricks-tpc-di-augmented/src"
    if name_prefix is None:
        user = user or _current_user(profile)
        name_prefix = user.split("@")[0].replace(".", "-")
    single_user_name = user or _current_user(profile)

    child_name = f"{name_prefix}-TPCDI-SF{scale_factor}-AugIncr-FabricDW-Child"
    parent_name = f"{name_prefix}-TPCDI-SF{scale_factor}-AugIncr-FabricDW-Parent"

    common = dict(DEFAULTS, repo_src_path=repo_src_path, scale_factor=scale_factor,
                  single_user_name=single_user_name, **overrides)

    child_id = _create_job(build_child(job_name=child_name, **common), profile)
    print(f"child job:  {child_id}  ({child_name})")
    parent_id = _create_job(build_parent(job_name=parent_name, child_job_id=child_id, **common), profile)
    print(f"parent job: {parent_id}  ({parent_name})")
    print(f"\ntrigger with:\n  databricks jobs run-now --profile {profile} "
          f"--json '{{\"job_id\": {parent_id}}}'")
    return (child_id, parent_id)


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(description="Register Fabric-DW augmented-incremental jobs.")
    ap.add_argument("scale_factor", type=int)
    ap.add_argument("--profile", default=DEFAULT_PROFILE)
    ap.add_argument("--repo-src-path", default=None)
    ap.add_argument("--name-prefix", default=None)
    ap.add_argument("--fabric-lakehouse-id", default=None,
                    help="OneLake lakehouse id holding staging_sf{sf} (overrides DEFAULTS)")
    ap.add_argument("--fabric-lakehouse-name", default=None,
                    help="Lakehouse item name = cross-DB CTAS source db (must match the id)")
    ap.add_argument("--interactive-cluster-id", default=None,
                    help="Existing interactive cluster to pin all tasks to (overrides DEFAULTS)")
    a = ap.parse_args()
    overrides = {}
    if a.fabric_lakehouse_id:   overrides["fabric_lakehouse_id"] = a.fabric_lakehouse_id
    if a.fabric_lakehouse_name: overrides["fabric_lakehouse_name"] = a.fabric_lakehouse_name
    if a.interactive_cluster_id: overrides["interactive_cluster_id"] = a.interactive_cluster_id
    create(a.scale_factor, repo_src_path=a.repo_src_path, profile=a.profile,
           name_prefix=a.name_prefix, **overrides)
