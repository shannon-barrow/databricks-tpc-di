# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# dependencies = [
#   "dbt-redshift==1.10.0",
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
         "dbt-redshift==1.10.0"]
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
    "      connect_timeout: 30",
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
# On serverless DBR, `python -m dbt.cli.main` fails with
# "No module named 'dbt.cli'" even though dbt-redshift is installed —
# the entrypoint packaging is awkward on env_version=5. The `dbt`
# executable from the env's bin/ is the reliable invocation path.
import shutil
dbt_exe = shutil.which("dbt")
if dbt_exe:
    cmd = [
        dbt_exe, "run",
        "--target", "redshift",
        "--profiles-dir", profiles_dir,
        "--project-dir", dbt_project_dir,
        "--vars", json.dumps(vars_payload),
        "--no-version-check",
    ]
else:
    # Last resort — try the python -m form. If dbt.cli is missing this
    # will also fail, but at least the error path is loud.
    cmd = [
        sys.executable, "-m", "dbt", "run",
        "--target", "redshift",
        "--profiles-dir", profiles_dir,
        "--project-dir", dbt_project_dir,
        "--vars", json.dumps(vars_payload),
        "--no-version-check",
    ]
print("dbt cmd:", " ".join(cmd))
res = subprocess.run(cmd, capture_output=True, text=True)
print(res.stdout)
print(res.stderr, file=sys.stderr)

# Persist dbt output to a volume file for inspection.
log_dir = f"{tpcdi_directory}_dbt_run_logs/{wh_db}_{scale_factor}_rs"
log_path = f"{log_dir}/{batch_date}.log"
try:
    dbutils.fs.mkdirs(log_dir)
    dbutils.fs.put(
        log_path,
        f"# dbt run target=redshift batch_date={batch_date} exit_code={res.returncode}\n"
        f"# --- stdout ---\n{res.stdout}\n"
        f"# --- stderr ---\n{res.stderr}\n",
        overwrite=True,
    )
    print(f"[log] wrote dbt output to {log_path}")
except Exception as e:
    print(f"[log] failed to persist dbt output: {e}")

if res.returncode != 0:
    tail = (res.stdout + res.stderr)[-3000:]
    dbutils.notebook.exit(
        f"FAILED exit={res.returncode}\nlog={log_path}\n---tail---\n{tail}"
    )

print(f"[done] dbt run --target redshift batch_date={batch_date} complete.")
