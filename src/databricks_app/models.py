"""Data models for the TPC-DI benchmark app.

Encodes, per run type, which inputs are needed to emit a benchmark workflow.
This is the single source of truth the form renders from and the backend
consumes — see COMPETITIVE_APP_CONTEXT.md for the prerequisite rationale.

A native Databricks run derives almost everything (user, repo path, catalog).
A competitor run can't, so each engine declares its required fields, the
Unity Catalog secret location (catalog + schema) its credentials live in —
never the values, which the app doesn't handle — and sensible defaults for
the rest.
"""
from __future__ import annotations

import re
import string
from dataclasses import dataclass, field
from enum import Enum


def wh_db_prefix_from_email(email: str) -> str:
    """The per-user schema prefix, reproducing the driver notebook exactly
    (setup_context._init_api_context / _init_workflow_defaults):

        user_name  = lower(regexp_replace(local_part, '\\W+', ' '))
        wh_prefix  = capwords(user_name).replace(' ', '_')
        wh_db      = f"{wh_prefix}_TPCDI"      ->  e.g. First_Last_TPCDI

    Returns "" for a blank/invalid email. Shared by the Databricks baseline
    and every competitor so schemas are unique per user across all engines.
    """
    local = (email or "").split("@")[0]
    user_name = re.sub(r"\W+", " ", local).lower().strip()
    if not user_name:
        return ""
    return f"{string.capwords(user_name).replace(' ', '_')}_TPCDI"


def name_prefix_from_email(email: str) -> str:
    """The job-name prefix, matching the driver's default_job_name:
    capwords(user_name).replace(' ','-') + '-TPCDI'  ->  First-Last-TPCDI."""
    local = (email or "").split("@")[0]
    user_name = re.sub(r"\W+", " ", local).lower().strip()
    if not user_name:
        return ""
    return f"{string.capwords(user_name).replace(' ', '-')}-TPCDI"


def derived_from_email(email: str) -> dict:
    """Ghost-fill map for DERIVED fields: the per-user schema prefix (wh_db)
    and the job-name prefix (job_name_prefix)."""
    out = {}
    if wh_db_prefix_from_email(email):
        out["wh_db"] = wh_db_prefix_from_email(email)
    if name_prefix_from_email(email):
        out["job_name_prefix"] = name_prefix_from_email(email)
    return out


def dbt_wh_size(scale_factor: int) -> str:
    """DBSQL warehouse size for the dbt variant — mirrors _dbt_wh_size in
    generate_benchmark_workflow.py (Small at SF=20000, one size per SF
    doubling)."""
    sizes = ["2X-Small", "X-Small", "Small", "Medium", "Large",
             "X-Large", "2X-Large", "3X-Large", "4X-Large"]
    ceiling = 5000
    for size in sizes:
        if scale_factor <= ceiling:
            return size
        ceiling *= 2
    return sizes[-1]


WH_SIZES = ["2X-Small", "X-Small", "Small", "Medium", "Large",
            "X-Large", "2X-Large", "3X-Large", "4X-Large"]

# Per-cloud ARM / local-NVMe node families (Graviton, Cobalt, Axion), keyed by
# core count. Mirrors setup_context._init_compute_and_catalog_defaults. The
# app sizes clusters from these; users can tweak the generated job afterward.
#   4-core = driver / smallest single node; 8-core = standard worker.
NODE_TYPES = {
    "AWS":   {4: "m8gd.xlarge", 8: "m8gd.2xlarge", 16: "m8gd.4xlarge",
              32: "m8gd.8xlarge"},
    "GCP":   {4: "c4a-standard-4-lssd", 8: "c4a-standard-8-lssd",
              16: "c4a-standard-16-lssd", 32: "c4a-standard-32-lssd"},
    "Azure": {4: "Standard_D4pds_v6", 8: "Standard_D8pds_v6",
              16: "Standard_D16pds_v6", 32: "Standard_D32pds_v6"},
}


def cluster_plan(cloud: str, scale_factor: int, augmented: bool) -> dict:
    """Pick the compute topology for a Cluster/SDP run, matching the tuning
    we've measured. Returns a dict describing the plan (for display + job spec).

    Worker cores scale linearly with the scale factor:
      - augmented incremental: 32 worker cores at SF=20000
      - single-batch / incremental: 144 worker cores at SF=10000
    Topology:
      - <=32 total cores -> SINGLE NODE (a 64-core box loses enough perf to
        cost more than a driver + 8x 8-core workers), snapped up to the next
        node size (min 4 cores).
      - >32 cores -> a cluster of 8-core workers + a 4-core driver, with
        ceil(cores/8) workers.
    """
    nodes = NODE_TYPES.get(cloud, NODE_TYPES["AWS"])
    cores = (32 * scale_factor / 20000) if augmented else (144 * scale_factor / 10000)
    if cores <= 32:
        # Single node: snap up to 4/8/16/32-core box.
        for size in (4, 8, 16, 32):
            if cores <= size:
                node = nodes[size]
                break
        return {"mode": "single_node", "node_type": node, "node_cores": size,
                "num_workers": 0}
    import math
    num_workers = math.ceil(cores / 8)
    return {"mode": "cluster", "worker_type": nodes[8], "driver_type": nodes[4],
            "num_workers": num_workers, "worker_cores": 8, "driver_cores": 4}


def cluster_plan_summary(plan: dict) -> str:
    """One-line human description of a cluster_plan() result."""
    if plan["mode"] == "single_node":
        return f"single node ({plan['node_type']}, {plan['node_cores']} cores)"
    return (f"{plan['num_workers']}× 8-core workers ({plan['worker_type']}) "
            f"+ 4-core driver ({plan['driver_type']})")


# Fallback region per cloud when it can't be parsed from the workspace host.
REGION_DEFAULT = {"AWS": "us-west-2", "GCP": "us-central1", "Azure": "eastus2"}


class Engine(str, Enum):
    DATABRICKS = "databricks"
    REDSHIFT = "redshift"
    BIGQUERY = "bigquery"
    SNOWFLAKE = "snowflake"


# --- guided decision tree ----------------------------------------------------
# Mirrors _WORKFLOWS in `TPC-DI Driver.py` — the authoritative menu of valid
# (batch type -> engine/SKU) combinations. The wizard reveals questions in
# order and filters each choice by the answers before it.

BATCH_TYPES = ["Single Batch", "Incremental", "Augmented Incremental"]

# Databricks SKUs valid per batch type (from _WORKFLOWS).
#   DBSQL has no Augmented; SDP has no per-day Incremental.
DBX_SKUS_BY_BATCH: dict[str, list[str]] = {
    "Single Batch":          ["Cluster", "DBSQL", "SDP"],
    "Incremental":           ["Cluster", "DBSQL"],
    "Augmented Incremental": ["Cluster", "SDP", "dbt"],
}

# SDP Single-Batch splits into editions; everything else is CORE.
SDP_EDITIONS = ["CORE", "PRO", "ADVANCED"]

# Competitive (non-Databricks, dbt-based) engines — only wired for Augmented.
COMPETITIVE_ENGINES = [Engine.REDSHIFT, Engine.BIGQUERY, Engine.SNOWFLAKE]

# Which batch types support a competitive run at all.
COMPETITIVE_BATCH_TYPES = ["Augmented Incremental"]

# TPC-DI data is generated in Databricks and read cross-service in the SAME
# cloud/region (a different region incurs egress + likely code changes), so the
# app pins the competitor region to where it runs and offers only the
# competitors valid in that cloud. Snowflake runs in every cloud; the
# warehouse-native competitor differs per cloud.
_ENGINE_CLOUD = {
    Engine.REDSHIFT: "AWS",
    Engine.BIGQUERY: "GCP",
    Engine.SNOWFLAKE: None,   # available in all clouds
}


def competitors_for_cloud(cloud: str) -> list[Engine]:
    """Competitors valid in `cloud`: Snowflake everywhere, plus the one
    warehouse native to that cloud. Enforces same-cloud runs (and, implicitly,
    that BigQuery and Redshift are never both offered)."""
    return [e for e in COMPETITIVE_ENGINES
            if _ENGINE_CLOUD[e] in (None, cloud)]


class FieldKind(str, Enum):
    PARAM = "param"      # a plain create()/job parameter (literal value)
    SECRET_PATH = "secret_path"  # a full Unity Catalog secret reference,
                                 # "catalog.schema.key", that the operator
                                 # already created; the app passes only the
                                 # path, and the job reads it via
                                 # dbutils.secrets.get(catalog, schema, key)
    DERIVED = "derived"  # filled from workspace context when left blank
    REGION = "region"    # cloud region, inferred + shown read-only (pinned to
                         # where the app runs; not user-editable)


@dataclass(frozen=True)
class InputField:
    """One input the form collects for a run type."""
    key: str
    label: str
    kind: FieldKind
    required: bool = False
    default: str = ""
    help: str = ""
    placeholder: str = ""   # greyed-in example shown in an empty text box


@dataclass(frozen=True)
class EngineSpec:
    """Everything the app needs to render the form and emit a workflow for one
    engine."""
    engine: Engine
    label: str
    fields: tuple[InputField, ...]

    def secret_path_fields(self) -> tuple[InputField, ...]:
        """Fields that hold a full Unity Catalog secret path
        ("catalog.schema.key"). Only genuine secrets — passwords, keys,
        tokens — are these; the operator already created the UC secret and
        enters its path, which the job resolves via dbutils.secrets.get."""
        return tuple(f for f in self.fields if f.kind is FieldKind.SECRET_PATH)

    def param_fields(self) -> tuple[InputField, ...]:
        return tuple(f for f in self.fields if f.kind is FieldKind.PARAM)


# --- shared fields -----------------------------------------------------------

_SCALE_FACTOR = InputField(
    key="scale_factor", label="Scale factor", kind=FieldKind.PARAM,
    required=True, default="10",
    help="TPC-DI scale factor (10, 100, 1000, 5000, 10000, 20000).",
)

# --- per-engine specs --------------------------------------------------------

# Every competitor gets its own catalog + target-schema field so the operator
# can point each engine at a distinct destination. Only genuine secrets
# (passwords, keys, tokens) are collected as a Unity Catalog secret PATH — one
# text box holding "catalog.schema.key" for a secret the operator already
# created. Non-sensitive config (usernames, hosts, accounts, ARNs, regions,
# warehouses) are plain text fields, not routed through secrets.


def _secret_path(key: str, label: str, example: str, help: str) -> InputField:
    """A single UC secret reference field: the operator pastes the full
    catalog.schema.key path to an existing secret. `example` is shown as the
    input's placeholder so the expected 3-dot syntax is visible without
    pre-filling a value."""
    return InputField(key, label, FieldKind.SECRET_PATH, required=True,
                      placeholder=example,
                      help=f"{help} Full Unity Catalog secret path (catalog.schema.key).")

DATABRICKS_SPEC = EngineSpec(
    engine=Engine.DATABRICKS,
    label="Databricks (native)",
    fields=(
        _SCALE_FACTOR,
        InputField("variant", "Variant", FieldKind.PARAM, required=True,
                   default="dbt",
                   help="Cluster / DBSQL / SDP / dbt."),
        InputField("repo_src_path", "Repo src path", FieldKind.DERIVED,
                   help="Derived from the current user if left blank."),
        InputField("catalog", "UC catalog", FieldKind.PARAM, default="main"),
        InputField("wh_db", "Target schema prefix", FieldKind.DERIVED,
                   help="Blank = derive from your username, as the driver does. "
                        "The run appends the scale factor → {prefix}_{sf}."),
    ),
)

REDSHIFT_SPEC = EngineSpec(
    engine=Engine.REDSHIFT,
    label="Amazon Redshift Serverless",
    fields=(
        _SCALE_FACTOR,
        InputField("profile", "Target workspace profile", FieldKind.PARAM, required=True,
                   help="Databricks CLI profile for the workspace that owns the UC external volume backing the S3 bucket — this is where the benchmark jobs get created."),
        InputField("s3_volume_prefix", "S3 volume prefix", FieldKind.PARAM, required=True,
                   help="The s3:// prefix backing the UC external volume where Databricks stages the data Redshift reads, e.g. s3://your-bucket/tpcdi/."),
        InputField("aws_region", "AWS region", FieldKind.REGION,
                   help="Pinned to the region this app runs in — the competitor must run in the same region as the Databricks data (a different region incurs egress). To run elsewhere, launch this app from Databricks in that cloud/region."),
        InputField("rs_host", "Workgroup endpoint", FieldKind.PARAM, required=True,
                   help="The Redshift Serverless workgroup's host the run connects to, e.g. <workgroup>.<account>.<region>.redshift-serverless.amazonaws.com"),
        InputField("rs_user", "Redshift user", FieldKind.PARAM, required=True,
                   help="Redshift database user the run authenticates as."),
        InputField("rs_iam_role", "IAM role ARN (for COPY)", FieldKind.PARAM, required=True,
                   help="IAM role attached to the workgroup that Redshift's COPY assumes to read the staged S3 files, e.g. arn:aws:iam::<account>:role/<role>."),
        _secret_path("rs_password_secret", "Redshift password (secret)",
                     "main.tpcdi_redshift.password",
                     "The Redshift user's password."),
    ),
)

BIGQUERY_SPEC = EngineSpec(
    engine=Engine.BIGQUERY,
    label="Google BigQuery",
    fields=(
        _SCALE_FACTOR,
        InputField("profile", "Target workspace profile", FieldKind.PARAM, required=True,
                   help="Databricks CLI profile for the GCP workspace that owns the UC external volume — where the benchmark jobs get created."),
        InputField("catalog", "BigQuery project", FieldKind.PARAM, required=True,
                   help="The GCP/BigQuery project id the dbt models run in."),
        InputField("gcs_volume_prefix", "GCS volume prefix", FieldKind.PARAM, required=True,
                   help="The gs:// prefix backing the UC external volume where Databricks stages the data BigQuery reads, e.g. gs://your-bucket/tpcdi/."),
        InputField("bq_location", "BQ location", FieldKind.REGION,
                   help="Pinned to the region this app runs in — the competitor must run in the same region as the Databricks data (a different region incurs egress). To run elsewhere, launch this app from Databricks in that cloud/region."),
        _secret_path("sa_json_secret", "Service-account JSON (secret)",
                     "main.tpcdi_bigquery.sa_json",
                     "SA JSON key with BigQuery Data Editor + Job User."),
    ),
)

# How the Snowflake run executes — shown when Snowflake is selected so the
# reason for its extra fields (stage, catalog integration, PAT) is clear.
SNOWFLAKE_SUMMARY = (
    "**How it runs:** the raw TPC-DI data is generated in **Databricks** and written to a UC external storage location (S3). "
    "**Snowflake reads that staged data through catalog federation** — a Snowflake *catalog integration* points at Databricks' Unity Catalog Iceberg-REST endpoint (authenticated with a Databricks PAT), and an *external stage* exposes the per-batch files. "
    "That's why the fields below ask for a stage, a catalog integration, and a Databricks PAT in addition to the usual account/user/warehouse — they wire up that Databricks→Snowflake bridge.")

SNOWFLAKE_SPEC = EngineSpec(
    engine=Engine.SNOWFLAKE,
    label="Snowflake",
    fields=(
        _SCALE_FACTOR,
        InputField("account", "Snowflake account", FieldKind.PARAM, required=True,
                   help="The Snowflake account (<org>-<account>) that runs the dbt models over the federated Databricks data."),
        InputField("sf_user", "Snowflake user", FieldKind.PARAM, required=True,
                   help="Snowflake login the run connects as."),
        InputField("snowflake_warehouse", "Warehouse", FieldKind.PARAM, required=True,
                   help="Snowflake virtual warehouse that runs the dbt models."),
        InputField("catalog", "Target database", FieldKind.PARAM, default="TPCDI_TEST",
                   help="Snowflake database the models land in (not the UC catalog — that's shared above)."),
        InputField("snowflake_stage", "External stage", FieldKind.PARAM, required=True,
                   help="The Snowflake external stage (<db>.<schema>.<stage>) on the S3 location where Databricks drops each day's files, so Snowflake can read the staged raw data."),
        InputField("catalog_integration", "Catalog integration name", FieldKind.PARAM, required=True,
                   help="The Snowflake CATALOG INTEGRATION that federates to Databricks' Unity Catalog Iceberg-REST endpoint — how Snowflake reads the Databricks-generated tables. Requires UniForm enabled on the source tables."),
        _secret_path("sf_credential_secret", "Snowflake password / private key (secret)",
                     "main.tpcdi_snowflake.password",
                     "The Snowflake user's password, or a PEM private key for keypair auth — how the run authenticates to Snowflake."),
        _secret_path("dbx_pat_secret", "Databricks PAT for federation (secret)",
                     "main.tpcdi_snowflake.dbx_pat",
                     "The Databricks personal access token the catalog integration uses (its bearer token) to authenticate to Databricks' Iceberg-REST endpoint — this is what lets Snowflake read the federated data."),
    ),
)

SPECS: dict[Engine, EngineSpec] = {
    Engine.DATABRICKS: DATABRICKS_SPEC,
    Engine.REDSHIFT: REDSHIFT_SPEC,
    Engine.BIGQUERY: BIGQUERY_SPEC,
    Engine.SNOWFLAKE: SNOWFLAKE_SPEC,
}
