# Databricks notebook source
# MAGIC %md
# MAGIC # TPC-DI Spark Data Generator
# MAGIC
# MAGIC Distributed PySpark replacement for the single-threaded DIGen.jar.
# MAGIC
# MAGIC Can run two ways:
# MAGIC
# MAGIC 1. **As a standalone notebook/job** — creates its own widgets for
# MAGIC    `scale_factor`, `catalog`, `regenerate_data`, `log_level` and derives
# MAGIC    `tpcdi_directory` and `workspace_src_path`. Use this mode when
# MAGIC    running as a scheduled job; job parameters match widget names.
# MAGIC
# MAGIC 2. **Via `%run ./tools/spark_data_generator` from the Driver notebook** —
# MAGIC    the Driver injects `scale_factor`, `catalog`, `tpcdi_directory`,
# MAGIC    `workspace_src_path`, `regenerate_data` into the shared namespace,
# MAGIC    so the widget-creation block below is skipped.

# COMMAND ----------

import sys, os, warnings, threading
import concurrent.futures
from datetime import datetime, timezone
warnings.filterwarnings("ignore", message=".*No Partition Defined for Window operation.*")

# COMMAND ----------

# Self-initialize when run standalone (no %run parent injected the inputs).
try:
    scale_factor  # injected by Driver's %run → skip widget init
except NameError:
    dbutils.widgets.dropdown("scale_factor", "10",
                             ["10", "100", "1000", "5000", "10000", "20000"],
                             "Scale Factor")
    dbutils.widgets.text("catalog", "tpcdi", "Target Catalog")
    dbutils.widgets.dropdown("regenerate_data", "NO", ["YES", "NO"], "Regenerate Data")
    dbutils.widgets.dropdown("log_level", "INFO", ["DEBUG", "INFO", "WARN"], "Log Level")

    scale_factor = int(dbutils.widgets.get("scale_factor"))
    catalog = dbutils.widgets.get("catalog")
    regenerate_data = dbutils.widgets.get("regenerate_data") == "YES"
    tpcdi_directory = f"/Volumes/{catalog}/tpcdi_raw_data/tpcdi_volume/"
    _nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    workspace_src_path = f"/Workspace{_nb_path.split('/src')[0]}/src"

# COMMAND ----------

def spark_generate():
  """Generate TPC-DI data using the distributed Spark-native generator.

  Uses variables from the Driver notebook (scale_factor, catalog, tpcdi_directory,
  workspace_src_path) rather than its own widgets.
  """
  # Inline timestamped logger usable before tpcdi_gen.utils.log is imported.
  # All early-phase timing goes through this so the gap between cell start and
  # first `[Reference]` log is visible. Once the utils log is imported, we
  # prefer that one for level filtering.
  _t0 = datetime.now(timezone.utc)
  def _tlog(msg):
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"  {ts} [Init] {msg}")

  _tlog(f"spark_generate() entered (scale_factor={scale_factor}, catalog={catalog})")

  # Raise broadcast-join thresholds. The CustomerMgmt schedule DF alone is
  # ~180MB at SF=5000 and scales with SF; default (10MB static / 30MB AQE)
  # forces Spark into shuffle joins that materialize the 25M-row all_df
  # across executors. Bumping to 200MB lets Spark broadcast the schedule
  # without the shuffle storm. NB: on Spark Connect / serverless the very
  # first spark.conf.set() is the first remote call of the session, so
  # initial JVM + Unity-Catalog session init time is charged here (not to
  # the catalog-existence check that runs next).
  _tlog("setting spark.sql.autoBroadcastJoinThreshold=200m (first spark-connect call — warmup)")
  spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "200m")
  _tlog("setting spark.databricks.adaptive.autoBroadcastJoinThreshold=200m")
  spark.conf.set("spark.databricks.adaptive.autoBroadcastJoinThreshold", "200m")
  _tlog("broadcast-join thresholds set")

  blob_out_path = f"{tpcdi_directory}spark_datagen/sf={scale_factor}"

  if catalog != 'hive_metastore':
    _tlog(f"checking catalog '{catalog}' existence")
    catalog_exists = spark.sql(f"SELECT count(*) FROM system.information_schema.tables WHERE table_catalog = '{catalog}'").first()[0] > 0
    _tlog(f"catalog check complete: exists={catalog_exists}")
    if not catalog_exists:
      _tlog("creating catalog + granting privileges")
      spark.sql(f"""CREATE CATALOG IF NOT EXISTS {catalog}""")
      spark.sql(f"""GRANT ALL PRIVILEGES ON CATALOG {catalog} TO `account users`""")
      _tlog("catalog created")
    _tlog("CREATE DATABASE IF NOT EXISTS tpcdi_raw_data")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog}.tpcdi_raw_data COMMENT 'Schema for TPC-DI Raw Files Volume'")
    _tlog("CREATE VOLUME IF NOT EXISTS tpcdi_volume")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.tpcdi_raw_data.tpcdi_volume COMMENT 'TPC-DI Raw Files'")
    _tlog("schema + volume ready")

  _tlog(f"dbutils.fs.ls check on {blob_out_path}")
  if _path_exists(blob_out_path):
    if regenerate_data:
      _tlog(f"regenerate_data=YES; recursive delete of prior output begins")
      dbutils.fs.rm(blob_out_path, recurse=True)
      _tlog("prior output deleted")
    else:
      print(f"Data generation skipped since {blob_out_path} already exists. Set Regenerate Data to YES to overwrite.")
      return
  else:
    _tlog("no prior output to delete")

  # Import tpcdi_gen module.
  # Clear any previously cached module imports to pick up code changes.
  _tools_dir = f"{workspace_src_path}/tools"
  _vol_module_dir = f"{tpcdi_directory}spark_datagen/_module"
  _vol_tools_dir = f"{_vol_module_dir}/tools"

  _tlog("clearing stale sys.modules['tpcdi_gen.*'] entries")
  for _m in list(sys.modules.keys()):
    if _m.startswith("tpcdi_gen"):
      del sys.modules[_m]

  # Try workspace path first (works on SINGLE_USER clusters).
  # Fall back to Volume copy for USER_ISOLATION/SHARED clusters where
  # /Workspace paths aren't accessible via Python file I/O.
  _tlog("probing workspace path accessibility")
  _ws_accessible = False
  try:
    _ws_accessible = os.path.isdir(f"{_tools_dir}/tpcdi_gen")
  except OSError:
    pass

  if _ws_accessible:
    sys.path.insert(0, _tools_dir)
    _tlog(f"module source: workspace path {_tools_dir}/tpcdi_gen")
  else:
    _tlog("workspace path NOT accessible; copying module to volume")
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
    _tlog(f"module source: volume copy {_vol_tools_dir}/tpcdi_gen")

  _tlog("importing tpcdi_gen.config / utils / dictionaries")
  from tpcdi_gen.config import ScaleConfig, NUM_INCREMENTAL_BATCHES
  from tpcdi_gen.utils import make_output_dirs, register_dict_views, bulk_copy_all, cleanup_staging, set_log_level, wait_for_background_copies, log as _utlog
  from tpcdi_gen import dictionaries
  _tlog("core imports done")

  # Set log level from widget
  try:
      set_log_level(dbutils.widgets.get("log_level"))
  except:
      set_log_level("DEBUG")

  cfg = ScaleConfig(int(scale_factor), catalog)
  gen_start_time = datetime.now(tz=timezone.utc)
  all_counts = {}

  print(f"TPC-DI Spark Data Generator")
  print(f"Scale Factor: {scale_factor} ")
  print(f"Output: {cfg.volume_path}")

  # Second recursive delete (redundant with the first one above, but harmless
  # since the first already cleaned anything present).
  _utlog(f"[Init] redundant cleanup of {cfg.volume_path}", "DEBUG")
  try:
    dbutils.fs.rm(cfg.volume_path, recurse=True)
  except:
    pass
  _utlog("[Init] output dir cleaned", "DEBUG")

  _utlog("[Init] creating Batch1/2/3 output directories", "DEBUG")
  make_output_dirs(cfg.volume_path, NUM_INCREMENTAL_BATCHES + 1, dbutils)
  _utlog("[Init] loading dictionary CSVs from disk", "DEBUG")
  dicts = dictionaries.load_all()
  _utlog(f"[Init] loaded {len(dicts)} dictionaries; registering as temp views", "DEBUG")
  register_dict_views(spark, dicts)
  _utlog("[Init] dictionary views registered", "DEBUG")

  # ================================================================
  # Dependency-graph scheduling: each dataset starts as soon as its
  # specific dependencies are met, maximizing parallelism.
  #
  # Dependency graph:
  #   Reference Tables: no deps
  #   HR:               no deps              → creates _brokers
  #   FINWIRE:          no deps              → creates _symbols
  #   Prospect:         no deps (cfg formulas only)
  #   DailyMarket:      _symbols (FINWIRE)
  #   WatchHistory:     _symbols (FINWIRE)
  #   CustomerMgmt:     _brokers (HR)        → creates _created/_closed_accounts
  #   Trade:            _symbols + _brokers + _created/_closed_accounts
  #
  # Optimal timeline:
  #   t=0: Reference, HR, FINWIRE, Prospect all start
  #   HR done → CustomerMgmt starts (doesn't wait for FINWIRE)
  #   FINWIRE done → DailyMarket, WatchHistory start (don't wait for CustMgmt)
  #   CustomerMgmt done → Trade starts (DM+WH already running)
  # ================================================================
  from tpcdi_gen import reference_tables, hr, finwire, customer
  from tpcdi_gen import market_data, trade, prospect, watch_history

  print("=" * 60)
  print("DEPENDENCY-GRAPH SCHEDULING")
  print("=" * 60)

  # Event signaled by FINWIRE as soon as _symbols is materialized to parquet.
  # DailyMarket, WatchHistory, Trade wait on THIS instead of f_fw.result(), so
  # they start ~2-3 min into FINWIRE (after SEC/_symbols is done) instead of
  # waiting for the full 10-25 min CMP+FIN union+text write at SF=5000+.
  fw_symbols_ready = threading.Event()

  with concurrent.futures.ThreadPoolExecutor(max_workers=12) as executor:
    # --- Tier 0: No dependencies (start immediately) ---
    f_ref = executor.submit(reference_tables.generate_all, spark, cfg, dicts, dbutils)
    f_hr = executor.submit(hr.generate, spark, cfg, dicts, dbutils)
    f_fw = executor.submit(finwire.generate, spark, cfg, dicts, dbutils,
                           symbols_ready_event=fw_symbols_ready)
    f_prospect = executor.submit(prospect.generate, spark, cfg, dicts, dbutils)

    # --- Tier 1: CustomerMgmt (depends on HR) ---
    # Uses a threading.Event to signal when temp views are ready.
    # Trade waits on the event (not the full CustomerMgmt completion).
    # XML writing + incremental batches continue in parallel with Trade.
    cust_views_ready = threading.Event()
    def run_customer():
      f_hr.result()  # wait for _brokers
      # Per-dataset async copies are kicked off by each write_file; no wave drain needed.
      return customer.generate(spark, cfg, dicts, dbutils, views_ready_event=cust_views_ready)
    f_cust = executor.submit(run_customer)

    # --- Tier 1: Depends on _symbols (wait on event, not full FINWIRE) ---
    def run_watch_history():
      fw_symbols_ready.wait()
      return watch_history.generate(spark, cfg, dicts, dbutils)
    f_wh = executor.submit(run_watch_history)

    def run_daily_market():
      fw_symbols_ready.wait()
      return market_data.generate(spark, cfg, dbutils)
    f_dm = executor.submit(run_daily_market)

    # --- Tier 2: Trade no longer depends on CustomerMgmt. n_valid is
    # computed analytically (cfg.n_available_accounts) and t_ca_id uses the
    # hash-derived _va_idx directly. Trade only waits for HR (_brokers) and
    # FINWIRE (_symbols).
    def run_trade():
      f_hr.result()            # _brokers view must exist for n_brokers count
      fw_symbols_ready.wait()  # _symbols (staged parquet)
      return trade.generate(spark, cfg, dicts, dbutils)
    f_trade = executor.submit(run_trade)

    # --- Collect all results ---
    all_counts.update(f_ref.result())
    all_counts.update(f_hr.result()["counts"])
    all_counts.update(f_fw.result()["counts"])
    all_counts.update(f_prospect.result())
    all_counts.update(f_cust.result()["counts"])
    all_counts.update(f_dm.result())
    all_counts.update(f_wh.result())
    all_counts.update(f_trade.result())

  from tpcdi_gen.utils import log as _log
  _log("[Orchestrator] All data generation complete. Copying files to final locations...")

  # Final copy + cleanup. Wait for any async copy threads (kicked off e.g.
  # after Trade historical) to drain before cleanup_staging — otherwise
  # cleanup can delete staging part files while the background thread is
  # still copying them, producing PathNotFound and missing final files.
  wait_for_background_copies()
  bulk_copy_all(dbutils, max_workers=64, label="final")
  _log("[Orchestrator] File copy complete. Cleaning up staging directories...")
  cleanup_staging(cfg.volume_path, dbutils)

  # Audit files
  _log("[Orchestrator] Generating audit files...")
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
