# Databricks notebook source
# Per-batch dbt task. Runs `dbt run --target bigquery` for one batch_date.
# Pinned to the same interactive cluster simulate_filedrops_bq runs on.
#
# Contract:
#   - dbt-bigquery should be pre-installed on the cluster as a library
#     (defensive pip-install below in case it's not)
#   - dbt project lives at {dbt_project_dir} (workspace-repo path, set
#     by the workflow builder; same convention as the Databricks + Snowflake
#     dbt drivers)
#   - BQ creds come from the {secret_scope}.sa_json secret (single JSON key)
#   - profiles.yml is written to a fresh /tmp dir per invocation
#
# Vars passed through to dbt match what the bigquery_models dbt models
# expect (see dbt_project.yml `vars:` block).

# COMMAND ----------

# MAGIC %pip install --quiet "dbt-core==1.9.*" "dbt-bigquery==1.9.*"

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import os, subprocess, sys, json, tempfile

# COMMAND ----------

dbutils.widgets.text("catalog",        "databricks-sandbox-perfeng",
                     "Treated as BQ project — same meaning as `catalog` everywhere else")
dbutils.widgets.text("wh_db",          "")
dbutils.widgets.dropdown("scale_factor", "10", ["10","100","1000","5000","10000","20000"])
dbutils.widgets.text("batch_date",     "")
dbutils.widgets.text("tpcdi_directory","/Volumes/main/tpcdi_raw_data/tpcdi_volume/")
dbutils.widgets.text("secret_scope",   "tpcdi_bigquery")
dbutils.widgets.text("bq_location",    "us-central1")
dbutils.widgets.text("dbt_project_dir","", "Workspace-repo path to the dbt project")

bq_project       = dbutils.widgets.get("catalog")
wh_db            = dbutils.widgets.get("wh_db")
scale_factor     = dbutils.widgets.get("scale_factor")
batch_date       = dbutils.widgets.get("batch_date")
tpcdi_directory  = dbutils.widgets.get("tpcdi_directory")
secret_scope     = dbutils.widgets.get("secret_scope")
bq_location      = dbutils.widgets.get("bq_location")
dbt_project_dir  = dbutils.widgets.get("dbt_project_dir")

if not (wh_db and batch_date and dbt_project_dir):
    raise ValueError("wh_db, batch_date, and dbt_project_dir are required")

# COMMAND ----------

# dbt-core + dbt-bigquery were %pip-installed at the top with a
# restartPython() so the kernel starts fresh and pandas/numpy ABI match.
import dbt.version  # noqa: F401
import dbt.adapters.bigquery  # noqa: F401
print(f"[ok] dbt-core {dbt.version.__version__} + dbt-bigquery loaded")

# COMMAND ----------

# MAGIC %run ./_bq_conn

# COMMAND ----------

# Write profiles.yml from the secret scope. dbt-bigquery's keyfile auth
# needs a path, so we materialize the SA JSON to a tempfile.
profiles_dir = tempfile.mkdtemp(prefix="dbt_profiles_")
profile_path = os.path.join(profiles_dir, "profiles.yml")
keyfile_path = bq_credentials_path(secret_scope=secret_scope, out_dir=profiles_dir)

# BigQuery labels — stamped on every dbt-issued job. The task-time extract
# reads these from INFORMATION_SCHEMA.JOBS_BY_PROJECT to attribute jobs
# back to (batch_date, wh_db) without scraping logs. Mirror Snowflake's
# query_tag plumbing. Label values: lowercase alphanumeric + dash/underscore.
def _sanitize_label(v):
    s = str(v).lower()
    return "".join(c if c.isalnum() or c in "-_" else "-" for c in s)[:63]

target_dataset = f"{wh_db}_{scale_factor}"
lines = [
    "dbt_augmented_incremental:",
    "  target: bigquery",
    "  outputs:",
    "    bigquery:",
    "      type: bigquery",
    "      method: service-account",
    f"      keyfile: {keyfile_path}",
    f"      project: {bq_project}",
    f"      dataset: {target_dataset}",
    f"      location: {bq_location}",
    "      threads: 8",
    "      priority: interactive",
    "      labels:",
    f"        batch_date: {_sanitize_label(batch_date)}",
    f"        wh_db: {_sanitize_label(wh_db)}",
    f"        scale_factor: {_sanitize_label(scale_factor)}",
    f"        task: dbt_run",
]

with open(profile_path, "w") as f:
    f.write("\n".join(lines) + "\n")
os.chmod(profile_path, 0o600)
print(f"wrote profiles.yml to {profile_path}")

# COMMAND ----------

vars_payload = {
    "catalog":         bq_project,
    "wh_db":           wh_db,
    "scale_factor":    str(scale_factor),
    "batch_date":      batch_date,
    "tpcdi_directory": tpcdi_directory,
}
cmd = [
    sys.executable, "-m", "dbt.cli.main", "run",
    "--target", "bigquery",
    "--profiles-dir", profiles_dir,
    "--project-dir", dbt_project_dir,
    "--vars", json.dumps(vars_payload),
    "--no-version-check",
]
print("dbt cmd:", " ".join(cmd))
res = subprocess.run(cmd, capture_output=True, text=True)
print(res.stdout)
print(res.stderr, file=sys.stderr)

# Persist dbt output to a volume file so we can inspect failures even when
# the run-output API truncates the notebook stdout.
log_dir = f"{tpcdi_directory}_dbt_run_logs/{wh_db}_{scale_factor}_bq"
log_path = f"{log_dir}/{batch_date}.log"
try:
    dbutils.fs.mkdirs(log_dir)
    dbutils.fs.put(
        log_path,
        f"# dbt run target=bigquery batch_date={batch_date} exit_code={res.returncode}\n"
        f"# --- stdout ---\n{res.stdout}\n"
        f"# --- stderr ---\n{res.stderr}\n",
        overwrite=True,
    )
    print(f"[log] wrote dbt output to {log_path}")
except Exception as e:
    print(f"[log] failed to persist dbt output: {e}")

if res.returncode != 0:
    # Raise so the task FAILS (dbutils.notebook.exit() always reports
    # success — bad UX for catching dbt failures upstream in the workflow).
    tail = (res.stdout + res.stderr)[-3000:]
    raise RuntimeError(
        f"dbt run failed (exit={res.returncode}); log={log_path}\n"
        f"---tail---\n{tail}"
    )

print(f"[done] dbt run --target bigquery batch_date={batch_date} complete.")
