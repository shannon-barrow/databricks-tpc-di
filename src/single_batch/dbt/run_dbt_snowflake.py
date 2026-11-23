# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# dependencies = [
#   "dbt-core==2.0.0",
#   "dbt-snowflake",
# ]
# ///
# Single-batch (traditional) TPC-DI on Snowflake: one `dbt run` of the
# Snowflake_CSV models over the raw Batch1/2/3 files.
#
# Same contract as the augmented incremental run_dbt:
#   - account / user / role / warehouse are plain params; the credential
#     (PEM private key or password) is a full UC secret path
#   - profiles.yml is written to a fresh /tmp dir per run
# Before dbt runs, the notebook creates the run's schema, the stage over the
# raw files and the file formats (all idempotent DDL, no warehouse needed).
# The dbt project is copied to /tmp with model-paths pointed at
# Snowflake_CSV/models; the committed dbt_project.yml stays untouched.
# Each warehouse gets its own schema ({wh_db}_{scale_factor}) so runs on
# different warehouses never overwrite each other.

import os, re, shutil, subprocess, sys, json, tempfile, time

# COMMAND ----------

dbutils.widgets.text("catalog", "TPCDI_TEST", "Snowflake database")
dbutils.widgets.text("wh_db", "", "Schema prefix; empty = shannon_tpcdi_sb_<warehouse>")
dbutils.widgets.dropdown("scale_factor", "10", ["10", "100", "1000", "5000", "10000", "20000"])
dbutils.widgets.text("tpcdi_directory", "/Volumes/main/tpcdi_raw_data/tpcdi_volume/", "UC volume root (dbt logs land here)")
dbutils.widgets.text("raw_files_url", "s3://tpcds-datasets/shannon_tpcdi/spark_datagen/", "Cloud URL of the raw files root (holds sf=<SF>/Batch1..3)")
dbutils.widgets.text("storage_integration", "S3_INT", "Snowflake storage integration that can read raw_files_url")
dbutils.widgets.text("account", "", "Snowflake account identifier (plain value, not a secret)")
dbutils.widgets.text("sf_user", "", "Snowflake user (plain value, not a secret)")
dbutils.widgets.text("role", "", "Snowflake role to assume (plain value; empty = ACCOUNTADMIN)")
dbutils.widgets.text("sf_credential_secret", "main.tpcdi_raw_data.snowflake_cred_secret",
                     "Full UC secret path to the Snowflake credential (password OR PEM private key)")
dbutils.widgets.text("snowflake_warehouse", "", "Snowflake warehouse that runs the models (plain value)")
dbutils.widgets.text("threads", "21", "dbt threads")
dbutils.widgets.dropdown("dbt_command", "run", ["run", "compile", "parse"], "compile/parse check the models without loading data")
dbutils.widgets.text("dbt_project_dir", "", "Workspace-repo path to src/single_batch/dbt")

catalog             = dbutils.widgets.get("catalog")
scale_factor        = dbutils.widgets.get("scale_factor")
tpcdi_directory     = dbutils.widgets.get("tpcdi_directory")
raw_files_url       = dbutils.widgets.get("raw_files_url").rstrip("/") + "/"
storage_integration = dbutils.widgets.get("storage_integration")
account             = dbutils.widgets.get("account")
user                = dbutils.widgets.get("sf_user")
role                = dbutils.widgets.get("role") or "ACCOUNTADMIN"
sf_credential_secret = dbutils.widgets.get("sf_credential_secret")
warehouse           = dbutils.widgets.get("snowflake_warehouse")
threads             = int(dbutils.widgets.get("threads") or 21)
dbt_project_dir     = dbutils.widgets.get("dbt_project_dir")
dbt_command         = dbutils.widgets.get("dbt_command") or "run"
wh_db               = dbutils.widgets.get("wh_db") or f"shannon_tpcdi_sb_{warehouse}"

if not (account and user and warehouse and dbt_project_dir):
    raise ValueError("account, sf_user, snowflake_warehouse and dbt_project_dir are required")
if not sf_credential_secret:
    raise ValueError("sf_credential_secret is required: full UC secret path to the password OR PEM private key")

schema = f"{wh_db}_{scale_factor}".upper()
stage_name = "TPCDI_RAW_FILES"
stage_fqn = f"{catalog}.{schema}.{stage_name}"
print(f"target {catalog}.{schema} on {warehouse}; raw files {raw_files_url}sf={scale_factor}/")

# COMMAND ----------

# Defensive install: a no-op when the job environment already has them.
try:
    import dbt.version  # noqa: F401
    import dbt.adapters.snowflake  # noqa: F401
    import snowflake.connector  # noqa: F401
    print("[ok] dbt-core + dbt-snowflake + snowflake-connector already installed")
except ImportError:
    print("[install] dbt-core / dbt-snowflake / snowflake-connector not found, pip-installing...")
    subprocess.check_call([sys.executable, "-m", "pip", "install", "--quiet",
                           "dbt-core==2.0.0", "dbt-snowflake"])
    import snowflake.connector  # noqa: F401

# COMMAND ----------

def _secret_from_path(path):
    cat, sch, key = path.split(".", 2)
    return dbutils.secrets.get(catalog=cat, schema=sch, key=key)

credential = _secret_from_path(sf_credential_secret)
_is_keypair = "BEGIN" in credential and "PRIVATE KEY" in credential

profiles_dir = tempfile.mkdtemp(prefix="dbt_profiles_")
pk_path = None
if _is_keypair:
    pk_path = os.path.join(profiles_dir, "sf_key.pem")
    with open(pk_path, "w") as f:
        f.write(credential)
    os.chmod(pk_path, 0o600)

# COMMAND ----------

# Setup DDL: schema, stage over the raw files, file formats from helpers/.
# The helper files carry placeholder database.schema prefixes, so the
# CREATE target is rewritten to this run's schema.
def _connect():
    kwargs = dict(account=account, user=user, role=role, warehouse=warehouse)
    if _is_keypair:
        from cryptography.hazmat.primitives import serialization
        key = serialization.load_pem_private_key(credential.encode(), password=None)
        kwargs["private_key"] = key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption())
    else:
        kwargs["password"] = credential
    return snowflake.connector.connect(**kwargs)

helpers_dir = os.path.join(dbt_project_dir, "Snowflake_CSV", "helpers")
ddl = [
    f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}",
    f"CREATE STAGE IF NOT EXISTS {stage_fqn} URL = '{raw_files_url}' STORAGE_INTEGRATION = {storage_integration}",
]
for name in sorted(os.listdir(helpers_dir)):
    if not name.endswith(".sql"):
        continue
    sql = open(os.path.join(helpers_dir, name)).read()
    sql = re.sub(r"--[^\n]*\n", "\n", sql).strip().rstrip(";")
    sql = re.sub(r"CREATE OR REPLACE FILE FORMAT\s+\S+\.(\w+)",
                 rf"CREATE OR REPLACE FILE FORMAT {catalog}.{schema}.\1", sql, count=1)
    ddl.append(sql)

with _connect() as conn:
    cur = conn.cursor()
    for stmt in ddl:
        print("[setup]", stmt.splitlines()[0])
        cur.execute(stmt)

# COMMAND ----------

# Project copy with model-paths -> Snowflake_CSV and no packages.yml (the
# Snowflake models use no dbt packages, so no `dbt deps` round trip).
project_tmp = tempfile.mkdtemp(prefix="dbt_project_")
shutil.copytree(os.path.join(dbt_project_dir, "Snowflake_CSV"), os.path.join(project_tmp, "Snowflake_CSV"))
with open(os.path.join(dbt_project_dir, "dbt_project.yml")) as f:
    proj = f.read()
proj = re.sub(r"model-paths:.*", 'model-paths: ["Snowflake_CSV/models/"]', proj)
with open(os.path.join(project_tmp, "dbt_project.yml"), "w") as f:
    f.write(proj)

# query_tag lets SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY attribute every query
# of this run without scraping dbt logs.
run_tag = time.strftime("%Y%m%dT%H%M%S")
query_tag = json.dumps({
    "benchmark": "tpcdi_single_batch",
    "wh_db": wh_db,
    "scale_factor": scale_factor,
    "warehouse": warehouse,
    "run": run_tag,
    "task": "dbt_run",
}, separators=(",", ":")).replace("'", "\\'")

lines = [
    "sql_endpoints:",
    "  target: snowflake",
    "  outputs:",
    "    snowflake:",
    "      type: snowflake",
    f"      account: {account}",
    f"      user: {user}",
    f"      role: {role}",
    f"      warehouse: {warehouse}",
    f"      database: {catalog}",
    f"      schema: {schema}",
    f"      query_tag: '{query_tag}'",
    f"      threads: {threads}",
]
if pk_path:
    lines.append(f"      private_key_path: {pk_path}")
else:
    lines.append(f"      password: {credential}")
    lines.append("      authenticator: username_password_mfa")
    lines.append("      client_session_keep_alive: true")
with open(os.path.join(profiles_dir, "profiles.yml"), "w") as f:
    f.write("\n".join(lines) + "\n")
os.chmod(os.path.join(profiles_dir, "profiles.yml"), 0o600)

# COMMAND ----------

vars_payload = {"stage": f"{stage_fqn}/sf={scale_factor}"}
cmd = [
    sys.executable, "-m", "dbt.cli.main", dbt_command,
    "--target", "snowflake",
    "--profiles-dir", profiles_dir,
    "--project-dir", project_tmp,
    "--vars", json.dumps(vars_payload),
    "--no-version-check",
]
from importlib.metadata import version as _v
print(f"[versions] dbt-core {_v('dbt-core')}, dbt-snowflake {_v('dbt-snowflake')}")
print("dbt cmd:", " ".join(cmd))
t0 = time.time()
res = subprocess.run(cmd, capture_output=True, text=True)
wall = time.time() - t0
print(res.stdout)
print(res.stderr, file=sys.stderr)
print(f"[timing] dbt {dbt_command} wall {wall:.1f}s on {warehouse}")

log_dir = f"{tpcdi_directory}_dbt_run_logs/single_batch/{schema}"
log_path = f"{log_dir}/{run_tag}.log"
try:
    dbutils.fs.mkdirs(log_dir)
    dbutils.fs.put(
        log_path,
        f"# dbt {dbt_command} target=snowflake dbt-core={_v('dbt-core')} dbt-snowflake={_v('dbt-snowflake')} warehouse={warehouse} schema={schema} exit_code={res.returncode} wall_s={wall:.1f}\n"
        f"# --- stdout ---\n{res.stdout}\n"
        f"# --- stderr ---\n{res.stderr}\n",
        overwrite=True,
    )
    print(f"[log] wrote dbt output to {log_path}")
except Exception as e:
    print(f"[log] failed to persist dbt output: {e}")

if res.returncode != 0:
    tail = (res.stdout + res.stderr)[-3000:]
    raise RuntimeError(f"dbt {dbt_command} failed exit={res.returncode}\nlog={log_path}\n---tail---\n{tail}")

print(f"[done] single-batch dbt {dbt_command} on {warehouse} into {catalog}.{schema} in {wall:.1f}s (query_tag run={run_tag})")
