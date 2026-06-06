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
#   - Redshift creds come from `tpcdi_redshift` secret scope:
#       host, port, database, user, password, iam_role
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
dbutils.widgets.text("secret_scope",     "tpcdi_redshift")
dbutils.widgets.text("dbt_project_dir",  "", "Workspace-repo path to the dbt project")
dbutils.widgets.text("s3_volume_prefix", "s3://tpcds-datasets/shannon_tpcdi/",
                     "S3 prefix matching the UC volume — bronze pre_hook reads from here")
dbutils.widgets.text("aws_region",       "us-west-2", "REGION clause for COPY")
dbutils.widgets.text("file_ext",         "txt", "File extension of per-batch CSV drops")

database         = dbutils.widgets.get("database")
wh_db            = dbutils.widgets.get("wh_db")
scale_factor     = dbutils.widgets.get("scale_factor")
batch_date       = dbutils.widgets.get("batch_date")
tpcdi_directory  = dbutils.widgets.get("tpcdi_directory")
secret_scope     = dbutils.widgets.get("secret_scope")
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

# Read connection creds from the secret scope. Export as env vars so the
# profiles.yml `env_var(...)` template references can resolve.
def _get(name, default=None):
    try:
        return dbutils.secrets.get(scope=secret_scope, key=name)
    except Exception:
        return default

rs_host     = _get("host")
rs_port     = _get("port", "5439")
rs_database = _get("database", database)
rs_user     = _get("user")
rs_password = _get("password")
if not all([rs_host, rs_user, rs_password]):
    raise RuntimeError(
        f"Redshift secret scope '{secret_scope}' missing host/user/password"
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
    # connect_timeout in dbt-redshift is passed as the underlying socket
    # timeout for redshift_connector. Default 30s killed SF=10 bronzes
    # at 30-36s (XS workgroup cold-start latency). Larger SFs have
    # individual queries that legitimately take 10-20 min, so 3600 (1 hr)
    # gives headroom without hiding real hangs.
    "      connect_timeout: 3600",
    "      sslmode: require",
]

with open(profile_path, "w") as f:
    f.write("\n".join(lines) + "\n")
os.chmod(profile_path, 0o600)
print(f"wrote profiles.yml to {profile_path}")

# COMMAND ----------

rs_iam_role = _get("iam_role")
if not rs_iam_role:
    raise RuntimeError(
        f"Redshift secret scope '{secret_scope}' missing 'iam_role' "
        f"(required for bronze pre_hook COPY)"
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
# Invoke dbt IN-PROCESS via dbtRunner (the documented Python API).
# Avoids subprocess + serverless env_version=5 venv inheritance issues that
# make `python -m dbt.cli.main` fail with "No module named 'dbt.cli'" even
# though dbt-redshift is importable in the notebook's own Python process.
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
        # result.result is a RunExecutionResult — iterate nodes; include
        # the per-node message (which holds the SQL error text for
        # failed nodes) so the log captures actionable detail rather
        # than just status + timing.
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
