# Databricks notebook source
# Per-batch dbt task. Runs `dbt run --target fabric` for one batch_date against
# the Fabric Data Warehouse. Runs on a CLASSIC cluster whose init script
# (init_msodbcsql18.sh) installed the ODBC Driver 18 that pyodbc/dbt-fabric need.
#
# NO PEP-723 serverless env block here on purpose: dbt-fabric needs the system
# ODBC driver, which only the classic-cluster init script can provide. The
# cluster libraries should pin dbt-fabric; a defensive pip install is below.
#
# Auth: Entra service principal. tenant_id / client_id / host / database are
# plain params; only the SP client secret is a UC secret, passed as its full
# path (client_secret_secret = "catalog.schema.key"). No SQL logins.
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
dbutils.widgets.text("fabric_wh_host",   "", "Warehouse SQL endpoint host")
dbutils.widgets.text("fabric_wh_name",   "", "Warehouse item name")
dbutils.widgets.text("tenant_id",        "", "Entra tenant id")
dbutils.widgets.text("client_id",        "", "Service principal application (client) id")
dbutils.widgets.text("client_secret_secret", "", "UC secret path of the SP client secret (catalog.schema.key)")
dbutils.widgets.text("fabric_workspace_id", "", "Fabric workspace id")
dbutils.widgets.text("fabric_lakehouse_id", "", "Lakehouse id holding the daily file drops")
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
client_id        = dbutils.widgets.get("client_id")
client_secret_secret = dbutils.widgets.get("client_secret_secret")
ws_id            = dbutils.widgets.get("fabric_workspace_id")
lh_id            = dbutils.widgets.get("fabric_lakehouse_id")
file_ext         = dbutils.widgets.get("file_ext").strip()

_required = dict(wh_db=wh_db, batch_date=batch_date, dbt_project_dir=dbt_project_dir,
                 fabric_wh_host=wh_host, fabric_wh_name=wh_name, tenant_id=tenant_id,
                 client_id=client_id, client_secret_secret=client_secret_secret,
                 fabric_workspace_id=ws_id, fabric_lakehouse_id=lh_id)
_missing = [k for k, v in _required.items() if not v]
if _missing:
    raise ValueError(f"missing required params: {_missing}")

run_schema = f"{wh_db}_{scale_factor}".lower()

# OneLake Files base the fabric bronze OPENROWSET(BULK) reads from. The macro
# appends /{run_schema}/{batch_date}/{Dataset}.txt; simulate_filedrops_fabric
# writes the day's files to the matching OneLake path.
# OPENROWSET(BULK) wants an absolute URL; the dfs GUID form below is the one
# Fabric DW accepts.
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

def _secret_from_path(path):
    """Resolve a full UC secret path "catalog.schema.key" to its value."""
    catalog, schema, key = path.split(".", 2)
    return dbutils.secrets.get(catalog=catalog, schema=schema, key=key)  # noqa: F821


client_secret = _secret_from_path(client_secret_secret)

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

# The dbt `catalog` var feeds ONLY sources.yml's run_schema.database on the
# Fabric path (fabric bronze reads via OPENROWSET(BULK), not catalog; ref()/this
# resolve to the profile's database). The CTAS'd historical tables live in the
# Fabric WH database, so the source database must be the WH name, not UC `main`.
vars_payload = {
    "catalog":          wh_name,
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
    # RAISE (don't exit): dbutils.notebook.exit() returns a value and the task
    # counts as SUCCESS, silently greening the workflow while dbt failed. Failing
    # loudly here is what surfaces model errors to the parent for_each loop.
    err = result.exception or "see dbt results above"
    failed = [f"{nr.node.unique_id}: {getattr(nr,'message','')}"
              for nr in getattr(result.result, "results", [])
              if getattr(nr, "status", "") == "error"]
    raise RuntimeError(f"dbt run FAILED (batch {batch_date}); log={log_path}\n"
                       f"err={type(err).__name__}: {err}\n" + "\n".join(failed))
print(f"[done] dbt run --target fabric batch_date={batch_date} complete.")
