"""Pure-Python runner for the legacy DIGen.jar data generator.

Importable from a Databricks notebook (e.g. `tools/data_gen.py`) to run the
full DIGen flow inline — no `dbutils.notebook.run` indirection, no child
notebook context, no risk of a new cluster on serverless.

Usage:
    from digen_runner import run as digen_run
    digen_run(
        scale_factor=10, catalog="main",
        tpcdi_directory="/Volumes/main/tpcdi_raw_data/tpcdi_volume/",
        regenerate_data=False,
        workspace_src_path="/Workspace/Users/x/databricks-tpc-di-augmented/src",
        dbutils=dbutils, spark=spark,
    )

`run()` does the cluster pre-flight (Java available + DBR ≤ 15.4) BEFORE
any side effects, so a wrong cluster + regenerate_data=YES does not wipe
the existing volume only to fail at DIGen.jar invocation.
"""
from __future__ import annotations

import concurrent.futures
import os
import re
import shlex
import shutil
import subprocess
from typing import Any


# ---------- Pre-flight ----------

def _abort_digen(reason: str) -> None:
    raise RuntimeError(
        f"DIGen pre-flight check FAILED: {reason}\n\n"
        f"DIGen.jar requires a NON-SERVERLESS cluster with DBR <= 15.4. "
        f"Re-create this job's cluster as a classic Photon DBR 15.4 cluster, "
        f"or pick the Spark generator (spark_or_native_datagen='spark') if "
        f"your cluster is serverless. No volume data has been modified."
    )


def preflight(spark: Any) -> None:
    """Verify cluster can actually run DIGen. Raises before any side effect."""
    # Java callable.
    try:
        r = subprocess.run(["java", "-version"], capture_output=True, timeout=10)
        if r.returncode != 0:
            _abort_digen(
                f"`java -version` exited {r.returncode}: "
                f"{r.stderr.decode(errors='replace')[:200]}"
            )
    except (FileNotFoundError, subprocess.TimeoutExpired, PermissionError) as e:
        _abort_digen(f"cannot invoke `java` ({type(e).__name__}: {e})")

    # /local_disk0 present (typical of classic clusters; absent on serverless).
    if not os.path.isdir("/local_disk0"):
        _abort_digen("/local_disk0 not present (typical of serverless clusters)")

    # DBR version ≤ 15.4.
    dbr = (
        spark.conf.get("spark.databricks.clusterUsageTags.sparkVersion", None)
        or os.environ.get("DATABRICKS_RUNTIME_VERSION", "")
    )
    m = re.match(r"^(\d+)\.(\d+)", str(dbr))
    if m:
        major, minor = int(m.group(1)), int(m.group(2))
        if (major, minor) > (15, 4):
            _abort_digen(
                f"DBR {dbr} > 15.4. The DIGen audit logic depends on DBR 15.4 "
                f"behaviors and the JAR has not been validated on newer runtimes."
            )
    else:
        print(f"WARNING: could not parse DBR version from {dbr!r}; "
              f"proceeding anyway (cluster looks non-serverless).")

    print(f"DIGen pre-flight check OK: java available, /local_disk0 present, DBR={dbr}")


# ---------- File ops ----------

def _move_file(source_location: str, target_location: str) -> str:
    shutil.copyfile(source_location, target_location)
    return f"Finished moving {source_location} to {target_location}"


def _copy_directory(source_dir: str, target_dir: str, overwrite: bool):
    if os.path.exists(target_dir) and overwrite:
        print(f"Overwrite set to true. Deleting: {target_dir}.")
        shutil.rmtree(target_dir)
        print(f"Deleted {target_dir}.")
    try:
        dst = shutil.copytree(source_dir, target_dir)
        print(f"Copied {source_dir} to {target_dir} successfully!")
        return dst
    except FileExistsError:
        print(f"The folder you're trying to write to exists. Please delete it or set overwrite=True.")
    except FileNotFoundError:
        print(f"The folder you're trying to copy doesn't exist: {source_dir}")


def _path_exists(dbutils: Any, path: str) -> bool:
    """Existence check that goes through dbutils.fs (Volume API) rather than
    os.path.exists. The FUSE mount caches directory listings and can report
    a freshly-deleted path as still existing for several seconds, which made
    `regenerate_data=YES` log a delete and then immediately skip generation."""
    try:
        dbutils.fs.ls(path)
        return True
    except Exception:
        return False


def _digen_subprocess(digen_path: str, scale_factor: int, output_path: str) -> None:
    cmd = f"java -jar {digen_path}DIGen.jar -sf {scale_factor} -o {output_path}"
    print(f"Generating data and outputting to {output_path}")
    args = shlex.split(cmd)
    p = subprocess.Popen(
        args,
        cwd=digen_path,
        universal_newlines=True,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    # DIGen prompts for license acceptance; auto-acknowledge.
    p.stdin.write("\n"); p.stdin.flush()
    p.stdin.write("YES\n"); p.stdin.flush()
    while True:
        output = p.stdout.readline()
        if p.poll() is not None and output == '':
            break
        if output:
            print(output.strip())
    p.wait()


# ---------- Main entry ----------

def run(
    *,
    scale_factor: int,
    catalog: str,
    tpcdi_directory: str,
    regenerate_data: bool,
    workspace_src_path: str,
    dbutils: Any,
    spark: Any,
    threads: int | None = None,
    **_unused,  # accepts (and ignores) shared kwargs like log_level for signature parity with spark_runner
) -> None:
    """Run the legacy DIGen.jar data-generation flow.

    Args:
        scale_factor: TPC-DI scale factor (10, 100, 1000, ...).
        catalog: Unity Catalog catalog name (or "hive_metastore").
        tpcdi_directory: Fully-qualified base output directory ending in `/`.
            Outputs go to `{tpcdi_directory}sf={scale_factor}/Batch*`.
        regenerate_data: If True, wipe existing output before regenerating.
        workspace_src_path: Absolute workspace path to the `src` directory
            (used to find `tools/datagen/DIGen.jar`).
        dbutils, spark: Databricks notebook builtins, passed in.
        threads: Override for the file-move thread pool. Defaults to
            `spark.sparkContext.defaultParallelism`.
    """
    preflight(spark)

    UC_enabled = catalog != "hive_metastore"
    DRIVER_ROOT = "/local_disk0"
    tpcdi_tmp_path = "/tmp/tpcdi/"
    driver_tmp_path = f"{DRIVER_ROOT}{tpcdi_tmp_path}datagen/"
    driver_out_path = f"{DRIVER_ROOT}{tpcdi_tmp_path}sf={scale_factor}"
    blob_out_path = f"{tpcdi_directory}sf={scale_factor}"

    if UC_enabled:
        os_blob_out_path = blob_out_path
    else:
        os_blob_out_path = f"/dbfs{blob_out_path}"
        blob_out_path = f"dbfs:{blob_out_path}"

    if _path_exists(dbutils, blob_out_path) and regenerate_data:
        print(f"regenerate_data=YES; recursive delete of prior output at {blob_out_path}")
        dbutils.fs.rm(blob_out_path, recurse=True)

    if _path_exists(dbutils, blob_out_path):
        print(f"Data generation skipped since the raw data/directory {blob_out_path} "
              f"already exists for this scale factor.")
        return

    print(f"Raw Data Directory {blob_out_path} does not exist yet. Proceeding to "
          f"generate data for scale factor={scale_factor} into this directory")
    _copy_directory(f"{workspace_src_path}/tools/datagen", driver_tmp_path, overwrite=True)
    print(f"Data generation for scale factor={scale_factor} is starting in directory: {driver_out_path}")
    _digen_subprocess(driver_tmp_path, scale_factor, driver_out_path)
    print(f"Data generation for scale factor={scale_factor} has completed in directory: {driver_out_path}")
    print(f"Moving generated files from Driver directory {driver_out_path} to "
          f"Storage directory {blob_out_path}")

    if UC_enabled:
        catalog_exists = spark.sql(
            f"SELECT count(*) FROM system.information_schema.tables "
            f"WHERE table_catalog = '{catalog}'"
        ).first()[0] > 0
        if not catalog_exists:
            spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
            spark.sql(f"GRANT ALL PRIVILEGES ON CATALOG {catalog} TO `account users`")
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog}.tpcdi_raw_data "
                  f"COMMENT 'Schema for TPC-DI Raw Files Volume'")
        spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.tpcdi_raw_data.tpcdi_volume "
                  f"COMMENT 'TPC-DI Raw Files'")

    filenames = [
        os.path.join(root, name)
        for root, _dirs, files in os.walk(top=driver_out_path, topdown=True)
        for name in files
    ]
    dbutils.fs.mkdirs(blob_out_path)
    for d in next(os.walk(driver_out_path))[1]:
        dbutils.fs.mkdirs(f"{blob_out_path}/{d}")

    if threads is None:
        threads = spark.sparkContext.defaultParallelism

    with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
        futures = []
        for fn in filenames:
            futures.append(executor.submit(
                _move_file,
                source_location=fn,
                target_location=fn.replace(driver_out_path, os_blob_out_path),
            ))
        for f in concurrent.futures.as_completed(futures):
            try:
                print(f.result())
            except Exception as e:
                print(f"  move-file error: {type(e).__name__}: {e}")
