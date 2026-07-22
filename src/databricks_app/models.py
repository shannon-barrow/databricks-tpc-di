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
    SECRET_REF = "secret_ref"  # points at a user-managed Unity Catalog secret
                               # location (catalog + schema); no values pass
                               # through the app
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


@dataclass(frozen=True)
class EngineSpec:
    """Everything the app needs to render the form and emit a workflow for one
    engine."""
    engine: Engine
    label: str
    fields: tuple[InputField, ...]

    def secret_ref_fields(self) -> tuple[InputField, ...]:
        """The Unity Catalog secret-location fields (secret_catalog +
        secret_schema) for this engine, if any. The operator creates the UC
        secrets themselves and enters the catalog/schema here; the app passes
        those identifiers to the job, which reads each credential via
        dbutils.secrets.get(catalog=..., schema=..., key=...)."""
        return tuple(f for f in self.fields if f.kind is FieldKind.SECRET_REF)

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
# can point each engine at a distinct destination. Credentials are never
# entered here: the operator pre-creates Unity Catalog secrets under a
# {catalog}.{schema} they own, and the app collects only that catalog + schema
# (SECRET_REF), which the port reads via
# dbutils.secrets.get(catalog=..., schema=..., key=...).


def _secret_ref_fields(default_schema: str, keys: str) -> tuple[InputField, ...]:
    """The two UC secret-location fields shared by every competitor: the
    catalog + schema holding the credentials. `keys` documents the secret
    names the port expects to find there."""
    return (
        InputField("secret_catalog", "Secret catalog", FieldKind.SECRET_REF,
                   required=True, default="main",
                   help="Unity Catalog catalog holding your credential secrets."),
        InputField("secret_schema", "Secret schema", FieldKind.SECRET_REF,
                   required=True, default=default_schema,
                   help=f"UC schema (in that catalog) holding keys: {keys}. "
                        "You create these UC secrets; the app only references "
                        "their location."),
    )

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
        InputField("wh_db", "Target schema", FieldKind.DERIVED,
                   help="Blank = derive from your username, as the driver does."),
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
        InputField("aws_region", "AWS region", FieldKind.PARAM,
                   default="us-west-2"),
        InputField("catalog", "UC catalog", FieldKind.PARAM, default="main",
                   help="Databricks UC catalog holding the external volume."),
        InputField("wh_db", "Target schema prefix", FieldKind.PARAM,
                   default="tpcdi_aug_rs_dbt",
                   help="Redshift target schema prefix → {wh_db}_{sf}."),
        *_secret_ref_fields("tpcdi_redshift", "host, user, password, iam_role"),
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
        InputField("bq_location", "BQ location", FieldKind.PARAM,
                   default="us-central1"),
        InputField("wh_db", "Target dataset prefix", FieldKind.PARAM,
                   default="tpcdi_aug_bq_dbt",
                   help="BigQuery target dataset prefix → {wh_db}_sf{N}."),
        *_secret_ref_fields("tpcdi_bigquery",
                            "sa_json (SA with BigQuery Data Editor + Job User)"),
    ),
)

SNOWFLAKE_SPEC = EngineSpec(
    engine=Engine.SNOWFLAKE,
    label="Snowflake",
    fields=(
        _SCALE_FACTOR,
        InputField("account", "Snowflake account", FieldKind.PARAM,
                   required=True, help="<org>-<account>."),
        InputField("snowflake_warehouse", "Warehouse", FieldKind.PARAM,
                   required=True),
        InputField("catalog", "Target database", FieldKind.PARAM, default="TPCDI_TEST",
                   help="Snowflake database the models land in."),
        InputField("wh_db", "Target schema prefix", FieldKind.PARAM,
                   default="tpcdi_aug_sf_dbt",
                   help="Snowflake target schema prefix → {wh_db}_{sf}."),
        InputField("snowflake_stage", "Stage", FieldKind.PARAM, required=True,
                   help="<db>.<schema>.<stage> for the per-batch file drops."),
        InputField("catalog_integration", "Catalog integration name",
                   FieldKind.PARAM, required=True,
                   help="Snowflake CATALOG INTEGRATION pointing at the UC "
                        "Iceberg-REST endpoint. Requires UniForm enabled on "
                        "the Databricks source tables."),
        *_secret_ref_fields("tpcdi_snowflake",
                            "user, password (or private_key), dbx_pat"),
    ),
)

SPECS: dict[Engine, EngineSpec] = {
    Engine.DATABRICKS: DATABRICKS_SPEC,
    Engine.REDSHIFT: REDSHIFT_SPEC,
    Engine.BIGQUERY: BIGQUERY_SPEC,
    Engine.SNOWFLAKE: SNOWFLAKE_SPEC,
}
