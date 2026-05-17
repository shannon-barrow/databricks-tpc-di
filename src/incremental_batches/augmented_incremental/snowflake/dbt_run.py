# Databricks notebook source
# Drives `dbt run --target snowflake` for one daily batch of the TPC-DI
# augmented incremental benchmark. Designed to be the per-batch task in
# a Databricks job — the parent job's for_each_task spawns one of these
# per simulated business day.
#
# Pattern mirrors augmented_dbt.py's child-job dbt_task, but instead of
# the native Databricks `dbt_task` resource (which connects to a DBSQL
# warehouse), this notebook just shells out to dbt-snowflake from
# whatever interactive cluster it runs on. Same dbt project, same
# vars, just `--target snowflake`.

# COMMAND ----------

import os, subprocess, sys, json

dbutils.widgets.text("catalog",       "TPCDI_TEST", "Snowflake database (var('catalog'))")
dbutils.widgets.text("wh_db",         "",           "wh_db prefix")
dbutils.widgets.dropdown("scale_factor", "10", ["10","100","1000","5000","10000","20000"], "scale_factor")
dbutils.widgets.text("batch_date",    "",           "Per-batch ISO date (YYYY-MM-DD)")
dbutils.widgets.text("tpcdi_directory", "/Volumes/main/tpcdi_raw_data/tpcdi_volume/", "tpcdi_directory var (unused on SF; kept for parity)")
dbutils.widgets.text("snowflake_stage", "tpcdi_stage", "Snowflake stage name (no @)")
dbutils.widgets.text("dbt_project_dir", "/Workspace/Repos/shannon.barrow@databricks.com/databricks-tpc-di-augmented/src/incremental_batches/augmented_incremental/dbt", "Path to the dbt project root in the workspace")
dbutils.widgets.text("secret_scope",  "tpcdi_snowflake", "Databricks secret scope holding the Snowflake creds")

catalog          = dbutils.widgets.get("catalog")
wh_db            = dbutils.widgets.get("wh_db")
scale_factor     = dbutils.widgets.get("scale_factor")
batch_date       = dbutils.widgets.get("batch_date")
tpcdi_directory  = dbutils.widgets.get("tpcdi_directory")
snowflake_stage  = dbutils.widgets.get("snowflake_stage")
dbt_project_dir  = dbutils.widgets.get("dbt_project_dir")
secret_scope     = dbutils.widgets.get("secret_scope")

if not (wh_db and batch_date):
    raise ValueError("wh_db and batch_date are required")

# COMMAND ----------

# Make sure dbt + dbt-snowflake are available. Cheap if already installed.
subprocess.check_call(
    [sys.executable, "-m", "pip", "install", "--quiet",
     "dbt-core==1.9.*", "dbt-snowflake==1.9.*"]
)

# COMMAND ----------

# Write profiles.yml to a temp dir, pulling creds from the Databricks
# secret scope. dbt-snowflake supports a `private_key_path` connector
# arg — we write the PEM out to disk because the connector wants a file
# path. Password fallback uses authenticator=username_password_mfa with
# client_request_mfa_token (cluster-cached for ~4h).
import tempfile

profiles_dir = tempfile.mkdtemp(prefix="dbt_profiles_")
profile_path = os.path.join(profiles_dir, "profiles.yml")
pk_path = None

def _secret(name, default=None):
    try:
        return dbutils.secrets.get(scope=secret_scope, key=name)
    except Exception:
        return default

account   = _secret("account")
user      = _secret("user")
role      = _secret("role")
warehouse = _secret("warehouse")
pk_pem    = _secret("private_key")
password  = _secret("password")

if not (account and user):
    raise RuntimeError(f"Secret scope '{secret_scope}' missing account/user")
if not (pk_pem or password):
    raise RuntimeError(f"Secret scope '{secret_scope}' missing private_key OR password")

if pk_pem:
    pk_path = os.path.join(profiles_dir, "sf_key.pem")
    with open(pk_path, "w") as f:
        f.write(pk_pem)
    os.chmod(pk_path, 0o600)

lines = [
    "dbt_augmented_incremental:",
    "  target: snowflake",
    "  outputs:",
    "    snowflake:",
    "      type: snowflake",
    f"      account: {account}",
    f"      user: {user}",
    f"      role: {role or 'ACCOUNTADMIN'}",
    f"      warehouse: {warehouse or 'COMPUTE_WH'}",
    f"      database: {catalog}",
    f"      schema: {wh_db}_{scale_factor}",
    "      threads: 8",
]
if pk_pem:
    lines.append(f"      private_key_path: {pk_path}")
else:
    lines.append(f"      password: {password}")
    lines.append("      authenticator: username_password_mfa")
    lines.append("      client_session_keep_alive: true")

with open(profile_path, "w") as f:
    f.write("\n".join(lines) + "\n")
os.chmod(profile_path, 0o600)
print(f"wrote profiles.yml to {profile_path}")

# COMMAND ----------

vars_payload = {
    "catalog":         catalog,
    "wh_db":           wh_db,
    "scale_factor":    str(scale_factor),
    "batch_date":      batch_date,
    "tpcdi_directory": tpcdi_directory,
    "snowflake_stage": snowflake_stage,
}
cmd = [
    sys.executable, "-m", "dbt.cli.main", "run",
    "--target", "snowflake",
    "--profiles-dir", profiles_dir,
    "--project-dir", dbt_project_dir,
    "--vars", json.dumps(vars_payload),
    "--no-version-check",
]
print("dbt cmd:", " ".join(cmd))
res = subprocess.run(cmd, capture_output=True, text=True)
print(res.stdout)
print(res.stderr, file=sys.stderr)
if res.returncode != 0:
    raise RuntimeError(f"dbt run failed with exit code {res.returncode}")

print(f"[done] dbt run --target snowflake batch_date={batch_date} complete.")
