# Databricks notebook source
# Per-batch dbt task. Runs `dbt run --target snowflake` for one batch_date.
# Pinned to the same interactive cluster simulate_filedrops_sf runs on.
#
# Contract:
#   - dbt-snowflake should be pre-installed on the cluster as a library
#     (defensive pip-install below in case it's not)
#   - dbt project lives at {dbt_project_dir} (workspace-repo path, set
#     by the workflow builder; same convention as the existing dbt driver)
#   - Snowflake creds come from the {secret_scope} Databricks secret scope
#   - profiles.yml is written to a fresh /tmp dir per invocation
#
# Vars passed through to dbt match what the snowflake_models dbt models
# expect (see dbt_project.yml `vars:` block).

import os, subprocess, sys, json, tempfile

# COMMAND ----------

dbutils.widgets.text("catalog",        "TPCDI_TEST")
dbutils.widgets.text("wh_db",          "")
dbutils.widgets.dropdown("scale_factor", "10", ["10","100","1000","5000","10000","20000"])
dbutils.widgets.text("batch_date",     "")
dbutils.widgets.text("tpcdi_directory","/Volumes/main/tpcdi_raw_data/tpcdi_benchmarking/")
dbutils.widgets.text("snowflake_stage","TPCDI_STAGE")
dbutils.widgets.text("secret_scope",   "tpcdi_snowflake")
dbutils.widgets.text("dbt_project_dir","", "Workspace-repo path to the dbt project")

catalog          = dbutils.widgets.get("catalog")
wh_db            = dbutils.widgets.get("wh_db")
scale_factor     = dbutils.widgets.get("scale_factor")
batch_date       = dbutils.widgets.get("batch_date")
tpcdi_directory  = dbutils.widgets.get("tpcdi_directory")
snowflake_stage  = dbutils.widgets.get("snowflake_stage")
secret_scope     = dbutils.widgets.get("secret_scope")
dbt_project_dir  = dbutils.widgets.get("dbt_project_dir")

if not (wh_db and batch_date and dbt_project_dir):
    raise ValueError("wh_db, batch_date, and dbt_project_dir are required")

# COMMAND ----------

# Defensive install — no-op if the cluster library is already there.
# Cluster libs SHOULD already pin dbt-core==1.9.* + dbt-snowflake==1.9.*;
# this is the belt for the suspenders.
try:
    import dbt.version  # noqa: F401
    import dbt.adapters.snowflake  # noqa: F401
    print("[ok] dbt-core + dbt-snowflake already installed on cluster")
except ImportError:
    print("[install] dbt-core + dbt-snowflake not found, pip-installing...")
    subprocess.check_call(
        [sys.executable, "-m", "pip", "install", "--quiet",
         "dbt-core==1.9.*", "dbt-snowflake==1.9.*"]
    )

# COMMAND ----------

# Write profiles.yml from the secret scope. Keypair auth preferred.
def _secret(name, default=None):
    try:    return dbutils.secrets.get(scope=secret_scope, key=name)
    except Exception: return default

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

profiles_dir = tempfile.mkdtemp(prefix="dbt_profiles_")
profile_path = os.path.join(profiles_dir, "profiles.yml")
pk_path = None
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
