"""Data models for the TPC-DI benchmark app.

Encodes, per run type, which inputs are needed to emit a benchmark workflow.
This is the single source of truth the form renders from and the backend
consumes — see COMPETITIVE_APP_CONTEXT.md for the prerequisite rationale.

A native Databricks run derives almost everything (user, repo path, catalog).
A competitor run can't, so each engine declares its required fields, which of
them are secrets (written to a Databricks secret scope, never stored by the
app), and sensible defaults for the rest.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


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


class FieldKind(str, Enum):
    PARAM = "param"      # a create()/job parameter
    SECRET = "secret"    # written to a Databricks secret scope, then referenced
    DERIVED = "derived"  # filled from workspace context when left blank


@dataclass(frozen=True)
class InputField:
    """One input the form collects for a run type."""
    key: str
    label: str
    kind: FieldKind
    required: bool = False
    default: str = ""
    help: str = ""
    secret_scope: str = ""   # for SECRET fields: the scope the value writes to
    secret_key: str = ""     # for SECRET fields: the key within that scope


@dataclass(frozen=True)
class EngineSpec:
    """Everything the app needs to render the form and emit a workflow for one
    engine."""
    engine: Engine
    label: str
    fields: tuple[InputField, ...]
    secret_scope: str = ""   # default scope competitor secrets land in

    def secret_fields(self) -> tuple[InputField, ...]:
        return tuple(f for f in self.fields if f.kind is FieldKind.SECRET)

    def param_fields(self) -> tuple[InputField, ...]:
        return tuple(f for f in self.fields if f.kind is FieldKind.PARAM)


# --- shared fields -----------------------------------------------------------

_SCALE_FACTOR = InputField(
    key="scale_factor", label="Scale factor", kind=FieldKind.PARAM,
    required=True, default="10",
    help="TPC-DI scale factor (10, 100, 1000, 5000, 10000, 20000).",
)
_BATCHES = InputField(
    key="incremental_batches_to_run", label="Batches to run",
    kind=FieldKind.PARAM, default="365",
    help="Daily batches the for_each loop runs. Lower it for a quick smoke run.",
)

# --- per-engine specs --------------------------------------------------------

DATABRICKS_SPEC = EngineSpec(
    engine=Engine.DATABRICKS,
    label="Databricks (native)",
    fields=(
        _SCALE_FACTOR,
        _BATCHES,
        InputField("variant", "Variant", FieldKind.PARAM, required=True,
                   default="dbt",
                   help="Cluster / DBSQL / SDP / dbt."),
        InputField("repo_src_path", "Repo src path", FieldKind.DERIVED,
                   help="Derived from the current user if left blank."),
        InputField("catalog", "UC catalog", FieldKind.PARAM, default="main"),
    ),
)

REDSHIFT_SPEC = EngineSpec(
    engine=Engine.REDSHIFT,
    label="Amazon Redshift Serverless",
    secret_scope="tpcdi_redshift",
    fields=(
        _SCALE_FACTOR,
        _BATCHES,
        InputField("profile", "Target workspace profile", FieldKind.PARAM,
                   required=True,
                   help="Databricks CLI profile / workspace that owns the UC "
                        "external volume backing the S3 bucket."),
        InputField("s3_volume_prefix", "S3 volume prefix", FieldKind.PARAM,
                   required=True,
                   help="e.g. s3://your-bucket/tpcdi/ — the prefix backing the "
                        "UC external volume."),
        InputField("aws_region", "AWS region", FieldKind.PARAM,
                   default="us-west-2"),
        InputField("wh_db", "Target schema prefix", FieldKind.PARAM,
                   default="tpcdi_aug_rs_dbt"),
        InputField("rs_host", "Workgroup endpoint", FieldKind.SECRET,
                   required=True, secret_scope="tpcdi_redshift", secret_key="host",
                   help="<workgroup>.<account>.<region>.redshift-serverless.amazonaws.com"),
        InputField("rs_user", "Redshift user", FieldKind.SECRET, required=True,
                   secret_scope="tpcdi_redshift", secret_key="user"),
        InputField("rs_password", "Redshift password", FieldKind.SECRET,
                   required=True, secret_scope="tpcdi_redshift", secret_key="password"),
        InputField("rs_iam_role", "IAM role ARN (for COPY)", FieldKind.SECRET,
                   required=True, secret_scope="tpcdi_redshift", secret_key="iam_role",
                   help="arn:aws:iam::<account>:role/<role> attached to the workgroup."),
    ),
)

BIGQUERY_SPEC = EngineSpec(
    engine=Engine.BIGQUERY,
    label="Google BigQuery",
    secret_scope="tpcdi_bigquery",
    fields=(
        _SCALE_FACTOR,
        _BATCHES,
        InputField("profile", "Target workspace profile", FieldKind.PARAM,
                   required=True, help="GCP Databricks workspace CLI profile."),
        InputField("catalog", "BigQuery project", FieldKind.PARAM, required=True,
                   help="Your GCP/BigQuery project id."),
        InputField("gcs_volume_prefix", "GCS volume prefix", FieldKind.PARAM,
                   required=True, help="e.g. gs://your-bucket/tpcdi/"),
        InputField("bq_location", "BQ location", FieldKind.PARAM,
                   default="us-central1"),
        InputField("wh_db", "Target dataset prefix", FieldKind.PARAM,
                   default="tpcdi_aug_bq_dbt"),
        InputField("sa_json", "Service-account JSON key", FieldKind.SECRET,
                   required=True, secret_scope="tpcdi_bigquery", secret_key="sa_json",
                   help="SA with BigQuery Data Editor + Job User."),
    ),
)

SNOWFLAKE_SPEC = EngineSpec(
    engine=Engine.SNOWFLAKE,
    label="Snowflake",
    secret_scope="tpcdi_snowflake",
    fields=(
        _SCALE_FACTOR,
        _BATCHES,
        InputField("account", "Snowflake account", FieldKind.PARAM,
                   required=True, help="<org>-<account>."),
        InputField("snowflake_warehouse", "Warehouse", FieldKind.PARAM,
                   required=True),
        InputField("snowflake_stage", "Stage", FieldKind.PARAM, required=True,
                   help="<db>.<schema>.<stage> for the per-batch file drops."),
        InputField("catalog_integration", "Catalog integration name",
                   FieldKind.PARAM, required=True,
                   help="Snowflake CATALOG INTEGRATION pointing at the UC "
                        "Iceberg-REST endpoint. Requires UniForm enabled on "
                        "the Databricks source tables."),
        InputField("sf_user", "Snowflake user", FieldKind.SECRET, required=True,
                   secret_scope="tpcdi_snowflake", secret_key="user"),
        InputField("sf_password", "Snowflake password", FieldKind.SECRET,
                   required=True, secret_scope="tpcdi_snowflake", secret_key="password"),
        InputField("dbx_pat", "Databricks PAT (for federation)", FieldKind.SECRET,
                   required=True, secret_scope="tpcdi_snowflake", secret_key="dbx_pat",
                   help="PAT Snowflake uses to auth to the UC Iceberg-REST "
                        "endpoint (the catalog integration's bearer token)."),
    ),
)

SPECS: dict[Engine, EngineSpec] = {
    Engine.DATABRICKS: DATABRICKS_SPEC,
    Engine.REDSHIFT: REDSHIFT_SPEC,
    Engine.BIGQUERY: BIGQUERY_SPEC,
    Engine.SNOWFLAKE: SNOWFLAKE_SPEC,
}
