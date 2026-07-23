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


def derived_from_email(email: str) -> dict:
    """Ghost-fill map for DERIVED fields. Currently just the wh_db prefix."""
    prefix = wh_db_prefix_from_email(email)
    return {"wh_db": prefix} if prefix else {}


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
                      help=f"{help} Full Unity Catalog secret path "
                           f"(catalog.schema.key).")

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
        InputField("profile", "Target workspace profile", FieldKind.PARAM,
                   required=True,
                   help="Databricks CLI profile / workspace that owns the UC "
                        "external volume backing the S3 bucket."),
        InputField("s3_volume_prefix", "S3 volume prefix", FieldKind.PARAM,
                   required=True,
                   help="e.g. s3://your-bucket/tpcdi/ — the prefix backing the "
                        "UC external volume."),
        InputField("aws_region", "AWS region", FieldKind.REGION,
                   help="Pinned to the region this app runs in — the competitor "
                        "must run in the same region as the Databricks data "
                        "(a different region incurs egress). To run elsewhere, "
                        "launch this app from Databricks in that cloud/region."),
        InputField("catalog", "UC catalog", FieldKind.PARAM, default="main",
                   help="Databricks UC catalog holding the external volume."),
        InputField("wh_db", "Target schema prefix", FieldKind.DERIVED,
                   help="Blank = derive from your username → {prefix}_{sf}."),
        InputField("rs_host", "Workgroup endpoint", FieldKind.PARAM, required=True,
                   help="<workgroup>.<account>.<region>.redshift-serverless.amazonaws.com"),
        InputField("rs_user", "Redshift user", FieldKind.PARAM, required=True),
        InputField("rs_iam_role", "IAM role ARN (for COPY)", FieldKind.PARAM,
                   required=True,
                   help="arn:aws:iam::<account>:role/<role> attached to the workgroup."),
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
        InputField("profile", "Target workspace profile", FieldKind.PARAM,
                   required=True, help="GCP Databricks workspace CLI profile."),
        InputField("catalog", "BigQuery project", FieldKind.PARAM, required=True,
                   help="Your GCP/BigQuery project id."),
        InputField("gcs_volume_prefix", "GCS volume prefix", FieldKind.PARAM,
                   required=True, help="e.g. gs://your-bucket/tpcdi/"),
        InputField("bq_location", "BQ location", FieldKind.REGION,
                   help="Pinned to the region this app runs in — the competitor "
                        "must run in the same region as the Databricks data "
                        "(a different region incurs egress). To run elsewhere, "
                        "launch this app from Databricks in that cloud/region."),
        InputField("wh_db", "Target dataset prefix", FieldKind.DERIVED,
                   help="Blank = derive from your username → {prefix}_{sf}."),
        _secret_path("sa_json_secret", "Service-account JSON (secret)",
                     "main.tpcdi_bigquery.sa_json",
                     "SA JSON key with BigQuery Data Editor + Job User."),
    ),
)

SNOWFLAKE_SPEC = EngineSpec(
    engine=Engine.SNOWFLAKE,
    label="Snowflake",
    fields=(
        _SCALE_FACTOR,
        InputField("account", "Snowflake account", FieldKind.PARAM,
                   required=True, help="<org>-<account>."),
        InputField("sf_user", "Snowflake user", FieldKind.PARAM, required=True),
        InputField("snowflake_warehouse", "Warehouse", FieldKind.PARAM,
                   required=True),
        InputField("catalog", "Target database", FieldKind.PARAM, default="TPCDI_TEST",
                   help="Snowflake database the models land in."),
        InputField("wh_db", "Target schema prefix", FieldKind.DERIVED,
                   help="Blank = derive from your username → {prefix}_{sf}."),
        InputField("snowflake_stage", "Stage", FieldKind.PARAM, required=True,
                   help="<db>.<schema>.<stage> for the per-batch file drops."),
        InputField("catalog_integration", "Catalog integration name",
                   FieldKind.PARAM, required=True,
                   help="Snowflake CATALOG INTEGRATION pointing at the UC "
                        "Iceberg-REST endpoint. Requires UniForm enabled on "
                        "the Databricks source tables."),
        _secret_path("sf_credential_secret", "Snowflake password / private key (secret)",
                     "main.tpcdi_snowflake.password",
                     "The Snowflake user's password, or a PEM private key for "
                     "keypair auth."),
        _secret_path("dbx_pat_secret", "Databricks PAT for federation (secret)",
                     "main.tpcdi_snowflake.dbx_pat",
                     "PAT Snowflake uses to auth to the UC Iceberg-REST "
                     "endpoint (the catalog integration's bearer token)."),
    ),
)

SPECS: dict[Engine, EngineSpec] = {
    Engine.DATABRICKS: DATABRICKS_SPEC,
    Engine.REDSHIFT: REDSHIFT_SPEC,
    Engine.BIGQUERY: BIGQUERY_SPEC,
    Engine.SNOWFLAKE: SNOWFLAKE_SPEC,
}
