# Databricks notebook source
# MAGIC %md
# MAGIC # TPC-DI Spark Data Generator
# MAGIC
# MAGIC Distributed PySpark replacement for the single-threaded DIGen.jar.
# MAGIC Called from the TPC-DI Driver notebook via `%run ./tools/spark_data_generator`.
# MAGIC
# MAGIC Expects these variables to be set by the Driver:
# MAGIC - `scale_factor` (int): TPC-DI scale factor (10, 100, 1000, etc.)
# MAGIC - `catalog` (str): Unity Catalog name
# MAGIC - `tpcdi_directory` (str): Base path for raw data volume
# MAGIC - `workspace_src_path` (str): Workspace path to the src directory

# COMMAND ----------

import sys, os
import concurrent.futures
from datetime import datetime, timezone

# COMMAND ----------

def spark_generate():
  """Generate TPC-DI data using the distributed Spark-native generator.

  Uses variables from the Driver notebook (scale_factor, catalog, tpcdi_directory,
  workspace_src_path) rather than its own widgets.
  """
  blob_out_path = f"{tpcdi_directory}spark_datagen/sf={scale_factor}"

  if catalog != 'hive_metastore':
    catalog_exists = spark.sql(f"SELECT count(*) FROM system.information_schema.tables WHERE table_catalog = '{catalog}'").first()[0] > 0
    if not catalog_exists:
      spark.sql(f"""CREATE CATALOG IF NOT EXISTS {catalog}""")
      spark.sql(f"""GRANT ALL PRIVILEGES ON CATALOG {catalog} TO `account users`""")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog}.tpcdi_raw_data COMMENT 'Schema for TPC-DI Raw Files Volume'")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.tpcdi_raw_data.tpcdi_volume COMMENT 'TPC-DI Raw Files'")

  if _path_exists(blob_out_path):
    if regenerate_data:
      print(f"Regenerate Data enabled. Deleting existing data at {blob_out_path}")
      dbutils.fs.rm(blob_out_path, recurse=True)
    else:
      print(f"Data generation skipped since {blob_out_path} already exists. Set Regenerate Data to YES to overwrite.")
      return

  # Import tpcdi_gen module.
  # Clear any previously cached module imports to pick up code changes.
  _tools_dir = f"{workspace_src_path}/tools"
  _vol_module_dir = f"{tpcdi_directory}spark_datagen/_module"
  _vol_tools_dir = f"{_vol_module_dir}/tools"

  for _m in list(sys.modules.keys()):
    if _m.startswith("tpcdi_gen"):
      del sys.modules[_m]

  # Try workspace path first (works on SINGLE_USER clusters).
  # Fall back to Volume copy for USER_ISOLATION/SHARED clusters where
  # /Workspace paths aren't accessible via Python file I/O.
  _ws_accessible = False
  try:
    _ws_accessible = os.path.isdir(f"{_tools_dir}/tpcdi_gen")
  except OSError:
    pass

  if _ws_accessible:
    sys.path.insert(0, _tools_dir)
    print(f"Module path (workspace): {_tools_dir}/tpcdi_gen")
  else:
    print(f"Workspace not accessible. Copying modules to Volume...")
    _ws_tpcdi_gen = f"file:{workspace_src_path}/tools/tpcdi_gen"
    _vol_tpcdi_gen = f"{_vol_module_dir}/tools/tpcdi_gen"
    try:
      dbutils.fs.rm(_vol_tpcdi_gen, recurse=True)
    except:
      pass
    dbutils.fs.cp(_ws_tpcdi_gen, _vol_tpcdi_gen, recurse=True)
    # Copy static_files individually (directory cp may not recurse for file: paths)
    _ws_static = f"file:{workspace_src_path}/tools/tpcdi_gen/static_files"
    _vol_static = f"{_vol_tpcdi_gen}/static_files"
    for _sf in ["StatusType.txt", "TaxRate.txt", "Date.txt", "Time.txt", "Industry.txt", "TradeType.txt"]:
      try:
        dbutils.fs.cp(f"{_ws_static}/{_sf}", f"{_vol_static}/{_sf}")
      except:
        pass
    sys.path.insert(0, _vol_tools_dir)
    print(f"Module path (volume): {_vol_tools_dir}/tpcdi_gen")

  from tpcdi_gen.config import ScaleConfig, NUM_INCREMENTAL_BATCHES
  from tpcdi_gen.utils import make_output_dirs, register_dict_views, bulk_copy_all, cleanup_staging
  from tpcdi_gen import dictionaries

  cfg = ScaleConfig(int(scale_factor), catalog)
  gen_start_time = datetime.now(tz=timezone.utc)
  all_counts = {}

  print(f"TPC-DI Spark Data Generator")
  print(f"Scale Factor: {scale_factor} ")
  print(f"Output: {cfg.volume_path}")

  # Clean previous output to prevent stale files
  try:
    dbutils.fs.rm(cfg.volume_path, recurse=True)
    print(f"Cleaned previous output at {cfg.volume_path}")
  except:
    pass

  # Initialize
  make_output_dirs(cfg.volume_path, NUM_INCREMENTAL_BATCHES + 1, dbutils)
  dicts = dictionaries.load_all()
  register_dict_views(spark, dicts)

  # Wave 1: Reference Tables + HR + FINWIRE (parallel)
  from tpcdi_gen import reference_tables, hr, finwire
  print("=" * 60)
  print("WAVE 1: Reference Tables + HR + FINWIRE")
  print("=" * 60)
  with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
    f_ref = executor.submit(reference_tables.generate_all, spark, cfg, dicts, dbutils)
    f_hr = executor.submit(hr.generate, spark, cfg, dicts, dbutils)
    f_fw = executor.submit(finwire.generate, spark, cfg, dicts, dbutils)
    all_counts.update(f_ref.result())
    all_counts.update(f_hr.result()["counts"])
    all_counts.update(f_fw.result()["counts"])
  print(f"\nWave 1 complete.")

  # Wave 2: Customer/Account + DailyMarket + copy Wave 1 files
  from tpcdi_gen import customer, market_data
  print("=" * 60)
  print("WAVE 2: Customer/Account + DailyMarket")
  print("=" * 60)
  with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
    f_cust = executor.submit(customer.generate, spark, cfg, dicts, dbutils)
    f_market = executor.submit(market_data.generate, spark, cfg, dbutils)
    f_copy1 = executor.submit(bulk_copy_all, dbutils, 64)
    all_counts.update(f_cust.result()["counts"])
    all_counts.update(f_market.result())
    f_copy1.result()
  print("\nWave 2 complete.")

  # Wave 3: Trades + Prospect + WatchHistory + copy Wave 2 files
  from tpcdi_gen import trade, prospect, watch_history
  print("=" * 60)
  print("WAVE 3: Trades + Prospect + WatchHistory")
  print("=" * 60)
  with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
    f_trade = executor.submit(trade.generate, spark, cfg, dicts, dbutils)
    f_prospect = executor.submit(prospect.generate, spark, cfg, dicts, dbutils)
    f_wh = executor.submit(watch_history.generate, spark, cfg, dicts, dbutils)
    f_copy2 = executor.submit(bulk_copy_all, dbutils, 64)
    all_counts.update(f_trade.result())
    all_counts.update(f_prospect.result())
    all_counts.update(f_wh.result())
    f_copy2.result()
  print("\nWave 3 complete.")

  # Final copy + cleanup
  bulk_copy_all(dbutils, max_workers=64)
  cleanup_staging(cfg.volume_path, dbutils)

  # Audit files
  from tpcdi_gen import audit
  audit.generate(cfg, all_counts, gen_start_time, dbutils)

  gen_end_time = datetime.now(tz=timezone.utc)
  elapsed = (gen_end_time - gen_start_time).total_seconds()
  total_records = sum(v for k, v in all_counts.items() if k[0] != "HR_BROKERS")
  print(f"\n{'=' * 60}")
  print(f"TPC-DI Data Generation Complete!")
  print(f"  Scale Factor: {scale_factor} ")
  print(f"  Total Records: {total_records:,}")
  print(f"  Elapsed: {elapsed:.1f}s ({total_records / max(1,elapsed):,.0f} records/sec)")
  print(f"  Output: {cfg.volume_path}")
  print(f"{'=' * 60}")

  # Clean up Volume module copies if we used the fallback path
  if not _ws_accessible:
    try:
      dbutils.fs.rm(_vol_module_dir, recurse=True)
      print(f"Cleaned up temporary modules from {_vol_module_dir}")
    except:
      pass


def _path_exists(path):
  """Check if a DBFS/Volume path exists."""
  try:
    dbutils.fs.ls(path)
    return True
  except:
    return False

# COMMAND ----------

spark_generate()
