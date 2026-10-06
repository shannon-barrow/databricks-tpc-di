"""Create the Augmented Incremental Fabric Spark or Fabric NEE parent + child
jobs from a laptop. The Competitor Driver notebook is the OOTB path; this is
the headless equivalent (mirrors dbt/competitors/fabric/create_jobs.py).

Only the SP client secret is a secret — pass its full UC path
(catalog.schema.key) as client_secret_secret. Everything else is a plain job
parameter. The parent's setup_fabric task provisions the Fabric side (pool,
environment, notebooks), so nothing has to be deployed by hand first.

Usage:
    python3 .../spark_competitors/fabric/create_jobs.py 10 --variant nee \\
        --profile <azure-profile> --fabric-workspace-id <guid> --fabric-lakehouse-id <guid> \\
        --tenant-id <guid> --client-id <guid> \\
        --client-secret-secret main.tpcdi_raw_data.fabric_<client_id>_sp_secret \\
        [--interactive-cluster-id <id>] [--fabric-node-count 16]
"""
import json
import os
import subprocess
import sys

sys.path.insert(0, os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "..", "..", "..", "..", "tools"))

from workflow_builders.augmented_fabric_spark import build_child, build_parent

DEFAULT_PROFILE = "tpc-di"

# Non-personal defaults. Anything user/infra-specific is an override of
# create() (CLI flags below), NOT baked in here.
DEFAULTS = dict(
    catalog="main",
    tpcdi_directory="/Volumes/main/tpcdi_raw_data/tpcdi_volume/",
    wh_db="tpcdi_aug_fabric_spark",               # lakehouse schema prefix -> {wh_db}_{sf}
    fabric_workspace_id="",
    fabric_lakehouse_id="",                       # schema-enabled; staging_sf{sf} + file drops
    tenant_id="",
    client_id="",
    # The only genuine secret — a full UC secret path (catalog.schema.key).
    client_secret_secret="",
    file_ext="txt",
)


def _api(method: str, path: str, profile: str, body: dict | None = None) -> dict:
    cmd = ["databricks", "api", method, "--profile", profile, path]
    if body is not None:
        cmd += ["--json", json.dumps(body)]
    p = subprocess.run(cmd, capture_output=True, text=True, check=True)
    out = p.stdout[p.stdout.find("{"):] if "{" in p.stdout else ""
    return json.loads(out) if out.strip() else {}


def _current_user(profile: str) -> str:
    out = subprocess.run(
        ["databricks", "current-user", "me", "--profile", profile, "--output", "json"],
        capture_output=True, text=True, check=True).stdout
    return json.loads(out[out.find("{"):])["userName"]


def create(scale_factor: int, *, enable_nee: bool, repo_src_path: str | None = None,
           profile: str = DEFAULT_PROFILE, name_prefix: str | None = None,
           **overrides) -> tuple[int, int]:
    user = _current_user(profile)
    if repo_src_path is None:
        repo_src_path = f"/Workspace/Users/{user}/databricks-tpc-di/src"
    if name_prefix is None:
        name_prefix = user.split("@")[0].replace(".", "-")
    token = "FabricNEE" if enable_nee else "FabricSpark"
    child_name = f"{name_prefix}-SF{scale_factor}-AugmentedIncremental-{token}-Child"
    parent_name = f"{name_prefix}-SF{scale_factor}-AugmentedIncremental-{token}-Parent"

    common = dict(DEFAULTS, repo_src_path=repo_src_path, scale_factor=scale_factor,
                  enable_nee=enable_nee, single_user_name=user, **overrides)
    child_id = _api("post", "/api/2.1/jobs/create", profile,
                    build_child(job_name=child_name, **common))["job_id"]
    print(f"child job:  {child_id}  ({child_name})")
    parent_id = _api("post", "/api/2.1/jobs/create", profile,
                     build_parent(job_name=parent_name, child_job_id=child_id, **common))["job_id"]
    print(f"parent job: {parent_id}  ({parent_name})")
    print(f"\ntrigger with:\n  databricks jobs run-now --profile {profile} "
          f"--json '{{\"job_id\": {parent_id}}}'")
    return child_id, parent_id


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(description="Register Fabric Spark / NEE augmented-incremental jobs.")
    ap.add_argument("scale_factor", type=int)
    ap.add_argument("--variant", choices=["spark", "nee"], required=True)
    ap.add_argument("--profile", default=DEFAULT_PROFILE)
    ap.add_argument("--repo-src-path", default=None)
    ap.add_argument("--name-prefix", default=None)
    for k in ("wh_db", "catalog", "fabric_workspace_id", "fabric_lakehouse_id", "tenant_id",
              "client_id", "client_secret_secret", "interactive_cluster_id",
              "fabric_pool_name", "fabric_node_size", "fabric_runtime", "fabric_environment_name"):
        ap.add_argument("--" + k.replace("_", "-"), default=None)
    ap.add_argument("--fabric-node-count", type=int, default=None)
    a = ap.parse_args()
    skip = ("scale_factor", "variant", "profile", "repo_src_path", "name_prefix")
    overrides = {k: v for k, v in vars(a).items() if k not in skip and v}
    missing = [k for k, v in dict(DEFAULTS, **overrides).items() if v == ""]
    if missing:
        ap.error(f"missing required inputs: {missing}")
    create(a.scale_factor, enable_nee=(a.variant == "nee"), repo_src_path=a.repo_src_path,
           profile=a.profile, name_prefix=a.name_prefix, **overrides)
