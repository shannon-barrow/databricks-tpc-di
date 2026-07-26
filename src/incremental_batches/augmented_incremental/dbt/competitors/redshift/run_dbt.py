# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# dependencies = [
#   "dbt-core==1.11.8",
#   "dbt-redshift==1.10.1",
#   "psycopg2-binary",
# ]
# ///
# Per-batch dbt task. Runs `dbt run --target redshift` for one batch_date.
# Cluster-libs SHOULD already pin dbt-redshift; defensive pip install below.
#
# Contract:
#   - dbt project lives at {dbt_project_dir} (workspace-repo path)
#   - host/user/iam_role/database/port are plain params; only the password is
#     a UC secret, referenced by full path in rs_password_secret
#   - profiles.yml written to a fresh /tmp dir per invocation
#
# Vars passed to dbt match what the redshift_models dbt models expect.

import os, subprocess, sys, json, tempfile

# COMMAND ----------

dbutils.widgets.text("database",         "dev",  "Redshift database")
dbutils.widgets.text("wh_db",            "")
dbutils.widgets.dropdown("scale_factor", "10", ["10","100","1000","5000","10000","20000"])
dbutils.widgets.text("batch_date",       "")
dbutils.widgets.text("tpcdi_directory",  "/Volumes/main/tpcdi_raw_data/tpcdi_volume/")
dbutils.widgets.text("rs_host",          "", "Redshift Serverless workgroup endpoint (plain value)")
dbutils.widgets.text("rs_user",          "", "Redshift user (plain value)")
dbutils.widgets.text("rs_iam_role",      "", "IAM role ARN for COPY (plain value)")
dbutils.widgets.text("rs_password_secret", "main.tpcdi_redshift.password",
                     "Full UC secret path for the Redshift password (catalog.schema.key)")
dbutils.widgets.text("dbt_project_dir",  "", "Workspace-repo path to the dbt project")
dbutils.widgets.text("s3_volume_prefix", "s3://REPLACE-ME/tpcdi/",
                     "S3 prefix matching the UC volume — bronze pre_hook reads from here")
dbutils.widgets.text("aws_region",       "us-west-2", "REGION clause for COPY")
dbutils.widgets.text("file_ext",         "txt", "File extension of per-batch CSV drops")

database         = dbutils.widgets.get("database")
wh_db            = dbutils.widgets.get("wh_db")
scale_factor     = dbutils.widgets.get("scale_factor")
batch_date       = dbutils.widgets.get("batch_date")
tpcdi_directory  = dbutils.widgets.get("tpcdi_directory")
rs_host          = dbutils.widgets.get("rs_host")
rs_user          = dbutils.widgets.get("rs_user")
rs_iam_role_arn  = dbutils.widgets.get("rs_iam_role")
rs_password_secret = dbutils.widgets.get("rs_password_secret")
dbt_project_dir  = dbutils.widgets.get("dbt_project_dir")
s3_volume_prefix = dbutils.widgets.get("s3_volume_prefix")
aws_region       = dbutils.widgets.get("aws_region")
file_ext         = dbutils.widgets.get("file_ext").strip()

if not (wh_db and batch_date and dbt_project_dir):
    raise ValueError("wh_db, batch_date, and dbt_project_dir are required")

target_schema = f"{wh_db}_{scale_factor}".lower()

# COMMAND ----------

# Defensive install — no-op if cluster library is already there.
try:
    import dbt.version  # noqa: F401
    import dbt.adapters.redshift  # noqa: F401
    print("[ok] dbt-core + dbt-redshift already installed")
except ImportError:
    print("[install] dbt-redshift not found, pip-installing...")
    subprocess.check_call(
        [sys.executable, "-m", "pip", "install", "--quiet",
         "dbt-redshift==1.10.1"]
    )

# COMMAND ----------

# host/user/port/database are plain params; only the password is a UC secret,
# referenced by its full path (catalog.schema.key).
def _secret_from_path(path):
    catalog, schema, key = path.split(".", 2)
    return dbutils.secrets.get(catalog=catalog, schema=schema, key=key)

rs_port     = "5439"
rs_database = database
rs_password = _secret_from_path(rs_password_secret) if rs_password_secret else None
if not all([rs_host, rs_user, rs_password]):
    raise RuntimeError(
        "Redshift connection missing host/user/password "
        "(host/user are plain params; password comes from rs_password_secret)"
    )

# COMMAND ----------

# Write profiles.yml directly with credential values (no env_var indirection).
# We're already running in a single-tenant Databricks task; the file lives in
# a fresh tempdir with 0600 perms and gets garbage-collected with the cluster.
profiles_dir  = tempfile.mkdtemp(prefix="dbt_profiles_")
profile_path  = os.path.join(profiles_dir, "profiles.yml")

# dbt-redshift's `query_comment` config will set query_group via session-level
# SET on connect; we set the JSON tag in dbt_project.yml's query-comment block.
lines = [
    "dbt_augmented_incremental:",
    "  target: redshift",
    "  outputs:",
    "    redshift:",
    "      type: redshift",
    "      method: database",
    f"      host: {rs_host}",
    f"      port: {int(rs_port)}",
    f"      dbname: {rs_database}",
    f"      schema: {target_schema}",
    f"      user: {rs_user}",
    f"      password: {rs_password}",
    "      threads: 8",
    # dbt-redshift passes connect_timeout as the socket timeout, so it also
    # caps individual query duration, not just connect.
    # Large-SF queries can legitimately run 10-20 min, so use 1 hr.
    "      connect_timeout: 3600",
    "      sslmode: require",
]

with open(profile_path, "w") as f:
    f.write("\n".join(lines) + "\n")
os.chmod(profile_path, 0o600)
print(f"wrote profiles.yml to {profile_path}")

# COMMAND ----------

rs_iam_role = rs_iam_role_arn
if not rs_iam_role:
    raise RuntimeError(
        "rs_iam_role is required (plain param, for bronze pre_hook COPY)"
    )

vars_payload = {
    "catalog":          database,                # dbt uses var('catalog') as the DB name
    "wh_db":            wh_db,
    "scale_factor":     str(scale_factor),
    "batch_date":       batch_date,
    "tpcdi_directory":  tpcdi_directory,
    # Redshift-specific — consumed by the rs_bronze_copy_prehook macro
    "s3_volume_prefix": s3_volume_prefix,
    "rs_iam_role":      rs_iam_role,
    "aws_region":       aws_region,
    "file_ext":         file_ext,
}
# Invoke dbt in-process via dbtRunner (the documented Python API).
# A subprocess `python -m dbt.cli.main` doesn't inherit the serverless
# env_version=5 venv, so it can't find dbt even when it's importable here.
from dbt.cli.main import dbtRunner

dbt_args = [
    "run",
    "--target", "redshift",
    "--profiles-dir", profiles_dir,
    "--project-dir", dbt_project_dir,
    "--vars", json.dumps(vars_payload),
    "--no-version-check",
]
print("dbt args:", dbt_args)
result = dbtRunner().invoke(dbt_args)

# Persist a summary log for inspection.
log_dir = f"{tpcdi_directory}_dbt_run_logs/{wh_db}_{scale_factor}_rs"
log_path = f"{log_dir}/{batch_date}.log"
try:
    dbutils.fs.mkdirs(log_dir)
    summary_lines = [
        f"# dbt run target=redshift batch_date={batch_date} success={result.success}",
    ]
    if result.exception:
        summary_lines.append(f"# exception: {type(result.exception).__name__}: {result.exception}")
    if result.result:
        # Include each node's message (holds the SQL error text on failure)
        # so the log has actionable detail, not just status + timing.
        for node_result in getattr(result.result, "results", []):
            summary_lines.append(
                f"  {node_result.status:>8s}  {node_result.node.unique_id:50s}  "
                f"exec={getattr(node_result, 'execution_time', 0):.2f}s"
            )
            msg = getattr(node_result, "message", None)
            if msg:
                # Indent the message for readability
                for line in str(msg).splitlines():
                    summary_lines.append(f"      {line}")
            adapter_response = getattr(node_result, "adapter_response", None)
            if adapter_response:
                summary_lines.append(f"      adapter_response: {adapter_response}")
    dbutils.fs.put(log_path, "\n".join(summary_lines) + "\n", overwrite=True)
    print(f"[log] wrote dbt summary to {log_path}")
except Exception as e:
    print(f"[log] failed to persist dbt summary: {e}")

if not result.success:
    err = result.exception or "see dbt results above"
    dbutils.notebook.exit(
        f"FAILED success={result.success}\nlog={log_path}\nerr={type(err).__name__}: {err}"
    )

print(f"[done] dbt run --target redshift batch_date={batch_date} complete.")
