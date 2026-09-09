# Databricks notebook source
# Per-batch dbt task. Runs `dbt run --target fabric` for one batch_date against
# the Fabric Data Warehouse. Runs on a CLASSIC cluster whose init script
# (init_msodbcsql18.sh) installed the ODBC Driver 18 that pyodbc/dbt-fabric need.
#
# NO PEP-723 serverless env block here on purpose: dbt-fabric needs the system
# ODBC driver, which only the classic-cluster init script can provide. The
# cluster libraries should pin dbt-fabric; a defensive pip install is below.
#
# Auth: Entra service principal (client_id/client_secret from the `tpcdi_fabric`
# UC secret scope; tenant + host + database are plain params). No SQL logins.
#
# Vars passed to dbt match what the fabric_models expect, plus fabric_files_url
# (the OneLake Files base the fabric__read_daily_csv OPENROWSET(BULK) reads).

import os, subprocess, sys, json, tempfile

# COMMAND ----------

dbutils.widgets.text("wh_db",            "")
dbutils.widgets.dropdown("scale_factor", "10", ["10","100","1000","5000","10000","20000"])
dbutils.widgets.text("batch_date",       "")
dbutils.widgets.text("catalog",          "main")
dbutils.widgets.text("tpcdi_directory",  "/Volumes/main/tpcdi_raw_data/tpcdi_volume/")
dbutils.widgets.text("dbt_project_dir",  "", "Workspace-repo path to the dbt project")
dbutils.widgets.text("fabric_wh_host",   "skrtph5o6caeff4w6gdeuehp7q-wzykhydzalbexexpbi7qahqgrm.datawarehouse.fabric.microsoft.com")
dbutils.widgets.text("fabric_wh_name",   "tpcdi_fabric_dw")
dbutils.widgets.text("tenant_id",        "9f37a392-f0ae-4280-9796-f1864a10effc")
dbutils.widgets.text("fabric_workspace_id", "e0a370b6-0279-4bc2-92ef-0a3f001e068b")
dbutils.widgets.text("fabric_lakehouse_id", "54963c1f-bb06-4acd-8db3-b8b5056e81c3")
dbutils.widgets.text("file_ext",         "txt")

wh_db            = dbutils.widgets.get("wh_db")
scale_factor     = dbutils.widgets.get("scale_factor")
batch_date       = dbutils.widgets.get("batch_date")
catalog          = dbutils.widgets.get("catalog")
tpcdi_directory  = dbutils.widgets.get("tpcdi_directory")
dbt_project_dir  = dbutils.widgets.get("dbt_project_dir")
wh_host          = dbutils.widgets.get("fabric_wh_host")
wh_name          = dbutils.widgets.get("fabric_wh_name")
tenant_id        = dbutils.widgets.get("tenant_id")
ws_id            = dbutils.widgets.get("fabric_workspace_id")
lh_id            = dbutils.widgets.get("fabric_lakehouse_id")
file_ext         = dbutils.widgets.get("file_ext").strip()

if not (wh_db and batch_date and dbt_project_dir):
    raise ValueError("wh_db, batch_date, and dbt_project_dir are required")

run_schema = f"{wh_db}_{scale_factor}".lower()

# OneLake Files base the fabric bronze OPENROWSET(BULK) reads from. The macro
# appends /{run_schema}/{batch_date}/{Dataset}.txt; simulate_filedrops_fabric
# writes the day's files to the matching OneLake path.
# ⚠ VERIFY: OneLake OPENROWSET is in preview and wants an absolute URL. This dfs
# GUID form is the candidate; if OPENROWSET rejects it, try the
# https://onelake.blob.fabric.microsoft.com/... or the <ws>/<lh>.Lakehouse form.
fabric_files_url = (f"https://onelake.dfs.fabric.microsoft.com/{ws_id}/{lh_id}"
                    f"/Files/augmented_incremental/_dailybatches")

# COMMAND ----------

# Defensive install — no-op if the cluster library already provides dbt-fabric.
try:
    import dbt.adapters.fabric  # noqa: F401
    print("[ok] dbt-fabric already installed")
except ImportError:
    print("[install] dbt-fabric not found, pip-installing...")
    subprocess.check_call([sys.executable, "-m", "pip", "install", "--quiet",
                           "dbt-fabric==1.10.0", "pyodbc"])

# Confirm the ODBC driver the init script installed is visible.
try:
    import pyodbc
    drivers = [d for d in pyodbc.drivers() if "ODBC Driver 18" in d]
    print(f"[odbc] drivers: {pyodbc.drivers()}")
    if not drivers:
        raise RuntimeError("ODBC Driver 18 for SQL Server not found — the cluster "
                           "init script (init_msodbcsql18.sh) must be attached.")
except ImportError:
    raise RuntimeError("pyodbc import failed after install")

# COMMAND ----------

client_id     = dbutils.secrets.get(scope="tpcdi_fabric", key="client_id")
client_secret = dbutils.secrets.get(scope="tpcdi_fabric", key="client_secret")

profiles_dir = tempfile.mkdtemp(prefix="dbt_profiles_")
profile_path = os.path.join(profiles_dir, "profiles.yml")
lines = [
    "dbt_augmented_incremental:",
    "  target: fabric",
    "  outputs:",
    "    fabric:",
    "      type: fabric",
    "      driver: ODBC Driver 18 for SQL Server",
    f"      host: {wh_host}",
    f"      database: {wh_name}",
    f"      schema: {run_schema}",
    "      authentication: ServicePrincipal",
    f"      tenant_id: {tenant_id}",
    f"      client_id: {client_id}",
    f"      client_secret: {client_secret}",
    "      encrypt: true",
    "      trust_cert: false",
    "      threads: 4",
    "      retries: 1",
]
with open(profile_path, "w") as f:
    f.write("\n".join(lines) + "\n")
os.chmod(profile_path, 0o600)
print(f"wrote profiles.yml to {profile_path} (target=fabric, schema={run_schema})")

# COMMAND ----------

vars_payload = {
    "catalog":          catalog,
    "wh_db":            wh_db,
    "scale_factor":     str(scale_factor),
    "batch_date":       batch_date,
    "tpcdi_directory":  tpcdi_directory,
    "fabric_files_url": fabric_files_url,
    "file_ext":         file_ext,
}
from dbt.cli.main import dbtRunner

# `dbt deps` once (no-op if packages/ present), then the batch run.
runner = dbtRunner()
runner.invoke(["deps", "--profiles-dir", profiles_dir, "--project-dir", dbt_project_dir])

dbt_args = [
    "run", "--target", "fabric",
    "--profiles-dir", profiles_dir,
    "--project-dir", dbt_project_dir,
    "--vars", json.dumps(vars_payload),
    "--no-version-check",
]
print("dbt args:", dbt_args)
result = runner.invoke(dbt_args)

# COMMAND ----------

# Persist a per-batch summary log (mirrors run_dbt.py on the RS side).
log_dir = f"{tpcdi_directory}_dbt_run_logs/{wh_db}_{scale_factor}_fabric"
log_path = f"{log_dir}/{batch_date}.log"
try:
    dbutils.fs.mkdirs(log_dir)
    out = [f"# dbt run target=fabric batch_date={batch_date} success={result.success}"]
    if result.exception:
        out.append(f"# exception: {type(result.exception).__name__}: {result.exception}")
    if result.result:
        for nr in getattr(result.result, "results", []):
            out.append(f"  {nr.status:>8s}  {nr.node.unique_id:50s}  "
                       f"exec={getattr(nr,'execution_time',0):.2f}s")
            msg = getattr(nr, "message", None)
            if msg:
                for ln in str(msg).splitlines():
                    out.append(f"      {ln}")
    dbutils.fs.put(log_path, "\n".join(out) + "\n", overwrite=True)
    print(f"[log] wrote dbt summary to {log_path}")
except Exception as e:
    print(f"[log] failed to persist dbt summary: {e}")

if not result.success:
    err = result.exception or "see dbt results above"
    dbutils.notebook.exit(f"FAILED success={result.success}\nlog={log_path}\n"
                          f"err={type(err).__name__}: {err}")
print(f"[done] dbt run --target fabric batch_date={batch_date} complete.")
