# Databricks notebook source
# MAGIC %md
# MAGIC # DIGen.jar Data Generator (legacy)
# MAGIC
# MAGIC Runs the original single-threaded Java DIGen utility. Writes raw files
# MAGIC named like `Customer.txt`, `Trade.txt`, etc. (NO `_N` suffix).
# MAGIC
# MAGIC Selectable from the Driver via the `data_generator` widget. The Spark
# MAGIC equivalent lives at `tools/spark_data_generator` and is the default.
# MAGIC
# MAGIC Can run two ways:
# MAGIC 1. **As a standalone notebook/job** (used by `generate_datagen_workflow()`
# MAGIC    when `data_generator='digen'`) — creates its own widgets for
# MAGIC    `scale_factor`, `catalog`, `regenerate_data` and derives
# MAGIC    `tpcdi_directory` and `workspace_src_path` from the notebook context.
# MAGIC 2. **Via `%run ./tools/data_generator` from the Driver notebook** — the
# MAGIC    Driver injects `scale_factor`, `catalog`, `tpcdi_directory`,
# MAGIC    `workspace_src_path`, `regenerate_data` into the shared namespace so
# MAGIC    the widget-creation block below is skipped.

# COMMAND ----------

import os
import re
import concurrent.futures
import requests
import shutil
import subprocess
import shlex

# COMMAND ----------

# DBTITLE 1,Pre-flight: confirm cluster can actually run DIGen.jar BEFORE any side effects
# DIGen.jar is a Java subprocess that requires:
#   - A non-serverless classic cluster (serverless can't shell out to Java)
#   - DBR ≤ 15.4 (the audit logic relies on DBR 15.4 behaviors)
#
# If either check fails we hard-stop NOW, before generate_data() does anything
# destructive. Otherwise a wrong cluster + regenerate_data=YES would wipe the
# existing volume contents and then fail to regenerate.

def _abort_digen(reason: str):
    msg = (
        f"DIGen pre-flight check FAILED: {reason}\n\n"
        f"DIGen.jar requires a NON-SERVERLESS cluster with DBR <= 15.4. "
        f"Re-create this job's cluster as a classic Photon DBR 15.4 cluster, "
        f"or pick the Spark generator instead (spark_or_native_datagen='spark') "
        f"if your cluster is serverless. No volume data has been modified."
    )
    raise RuntimeError(msg)

# Check 1: Java is callable. On serverless, `java` is not on PATH and
# subprocess can't reach it.
try:
    _java_check = subprocess.run(["java", "-version"], capture_output=True, timeout=10)
    if _java_check.returncode != 0:
        _abort_digen(
            f"`java -version` exited {_java_check.returncode}: "
            f"{_java_check.stderr.decode(errors='replace')[:200]}"
        )
except (FileNotFoundError, subprocess.TimeoutExpired, PermissionError) as _e:
    _abort_digen(f"cannot invoke `java` ({type(_e).__name__}: {_e})")

# Check 2: /local_disk0 exists and is writable (DIGen writes its temp output here).
if not os.path.isdir("/local_disk0"):
    _abort_digen("/local_disk0 not present (typical of serverless clusters)")

# Check 3: DBR version <= 15.4. Spark/Databricks expose the DBR version in
# either spark.databricks.clusterUsageTags.sparkVersion or env var.
_dbr = (
    spark.conf.get("spark.databricks.clusterUsageTags.sparkVersion", None)
    or os.environ.get("DATABRICKS_RUNTIME_VERSION", "")
)
_m = re.match(r"^(\d+)\.(\d+)", str(_dbr))
if _m:
    _major, _minor = int(_m.group(1)), int(_m.group(2))
    if (_major, _minor) > (15, 4):
        _abort_digen(
            f"DBR {_dbr} > 15.4. The DIGen audit logic depends on DBR 15.4 "
            f"behaviors and the JAR has not been validated on newer runtimes."
        )
else:
    print(f"WARNING: could not parse DBR version from {_dbr!r}; "
          f"proceeding anyway (cluster looks non-serverless).")

print(f"DIGen pre-flight check OK: java available, /local_disk0 present, DBR={_dbr}")

# COMMAND ----------

# Self-initialize when run as a standalone job (no %run parent injected the inputs).
try:
    scale_factor  # injected by Driver's %run → skip widget init
except NameError:
    dbutils.widgets.dropdown("scale_factor", "10",
                             ["10", "100", "1000", "5000", "10000"],
                             "Scale Factor")
    dbutils.widgets.text("catalog", "tpcdi", "Target Catalog")
    dbutils.widgets.dropdown("regenerate_data", "NO", ["YES", "NO"], "Regenerate Data")
    dbutils.widgets.text("tpcdi_directory", "", "TPCDI Output Directory (defaults to /Volumes/{catalog}/tpcdi_raw_data/tpcdi_volume/)")

    scale_factor = int(dbutils.widgets.get("scale_factor"))
    catalog = dbutils.widgets.get("catalog")
    regenerate_data = dbutils.widgets.get("regenerate_data") == "YES"
    tpcdi_directory = dbutils.widgets.get("tpcdi_directory") or f"/Volumes/{catalog}/tpcdi_raw_data/tpcdi_volume/"
    UC_enabled = catalog != "hive_metastore"
    lighthouse = False
    _nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    workspace_src_path = f"/Workspace{_nb_path.split('/src')[0]}/src"

# COMMAND ----------

def move_file(source_location, target_location):
  shutil.copyfile(source_location, target_location)
  return f"Finished moving {source_location} to {target_location}"

def copy_directory(source_dir, target_dir, overwrite):
  if os.path.exists(target_dir) and overwrite:
    print(f"Overwrite set to true. Deleting: {target_dir}.")
    shutil.rmtree(target_dir)
    print(f"Deleted {target_dir}.")
  try:
    dst = shutil.copytree(source_dir, target_dir)
    print(f"Copied {source_dir} to {target_dir} succesfully!")
    return dst
  except FileExistsError:
    print(f"The folder you're trying to write to exists. Please delete it or set overwrite=True.")
  except FileNotFoundError:
    print(f"The folder you're trying to copy doesn't exist: {source_dir}")
    
def generate_data():
  DRIVER_ROOT      = "/local_disk0"
  tpcdi_tmp_path   = "/tmp/tpcdi/"
  driver_tmp_path  = f"{DRIVER_ROOT}{tpcdi_tmp_path}datagen/"
  driver_out_path  = f"{DRIVER_ROOT}{tpcdi_tmp_path}sf={scale_factor}"
  blob_out_path    = f"{tpcdi_directory}sf={scale_factor}"
  if UC_enabled:  # Unity Catalog enabled so use VOLUMES to store raw files
    os_blob_out_path = blob_out_path
  else: # No Unity Catalog enabled so use DBFS to store raw files instead of volumes
    os_blob_out_path = f"/dbfs{blob_out_path}"
    blob_out_path = f"dbfs:{blob_out_path}" 

  # When run as a standalone job, regenerate_data may be a Python bool.
  # When run via Driver %run, the legacy code didn't have this flag — treat
  # missing as False (skip-if-exists, the original behavior).
  try:
    _regen = bool(regenerate_data) if not isinstance(regenerate_data, str) else regenerate_data == "YES"
  except NameError:
    _regen = False

  if os.path.exists(os_blob_out_path) and _regen:
    print(f"regenerate_data=YES; recursive delete of prior output at {blob_out_path}")
    dbutils.fs.rm(blob_out_path, recurse=True)

  if os.path.exists(os_blob_out_path):
    print(f"Data generation skipped since the raw data/directory {blob_out_path} already exists for this scale factor.")
  else:
    print(f"Raw Data Directory {blob_out_path} does not exist yet.  Proceeding to generate data for scale factor={scale_factor} into this directory")
    copy_directory(f"{workspace_src_path}/tools/datagen", driver_tmp_path, overwrite=True)
    print(f"Data generation for scale factor={scale_factor} is starting in directory: {driver_out_path}")
    DIGen(driver_tmp_path, scale_factor, driver_out_path)
    print(f"Data generation for scale factor={scale_factor} has completed in directory: {driver_out_path}")
    print(f"Moving generated files from Driver directory {driver_out_path} to Storage directory {blob_out_path}")
    if catalog != 'hive_metastore':
      catalog_exists = spark.sql(f"SELECT count(*) FROM system.information_schema.tables WHERE table_catalog = '{catalog}'").first()[0] > 0
      if not catalog_exists:
        spark.sql(f"""CREATE CATALOG IF NOT EXISTS {catalog}""")
        spark.sql(f"""GRANT ALL PRIVILEGES ON CATALOG {catalog} TO `account users`""")
      spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog}.tpcdi_raw_data COMMENT 'Schema for TPC-DI Raw Files Volume'")
      spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.tpcdi_raw_data.tpcdi_volume COMMENT 'TPC-DI Raw Files'")
    filenames = [os.path.join(root, name) for root, dirs, files in os.walk(top=driver_out_path , topdown=True) for name in files]
    dbutils.fs.mkdirs(blob_out_path)
    for dir in next(os.walk(driver_out_path))[1]:
      dbutils.fs.mkdirs(f"{blob_out_path}/{dir}")
    if lighthouse: threads = 8
    else: threads = sc.defaultParallelism
    with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
      futures = []
      for filename in filenames:
        futures.append(executor.submit(move_file, source_location=filename, target_location=filename.replace(driver_out_path, os_blob_out_path)))
      for future in concurrent.futures.as_completed(futures):
        try: print(future.result())
        except requests.ConnectTimeout: print("ConnectTimeout.")
        
def DIGen(digen_path, scale_factor, output_path):
  cmd = f"java -jar {digen_path}DIGen.jar -sf {scale_factor} -o {output_path}"
  print(f"Generating data and outputting to {output_path}")
  args = shlex.split(cmd)
  p3 = subprocess.Popen(
    args,
    cwd=digen_path,
    universal_newlines=True,
    stdin=subprocess.PIPE,
    stdout=subprocess.PIPE, 
    stderr=subprocess.PIPE)
  p3.stdin.write("\n")
  p3.stdin.flush()
  p3.stdin.write("YES\n")
  p3.stdin.flush()
  while True:
    output = p3.stdout.readline()
    if p3.poll() is not None and output == '':
      break
    if output:
      print (output.strip())
  p3.wait()

# COMMAND ----------

generate_data()
