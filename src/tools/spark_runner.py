"""Pure-Python runner for the distributed PySpark data generator.

Importable from a Databricks notebook (e.g. `tools/data_gen.py`) to run the
full Spark-native generation flow inline — no `dbutils.notebook.run`
indirection, no child notebook context, no risk of a new cluster on
serverless.

Usage:
    from spark_runner import run as spark_run
    spark_run(
        scale_factor=10, catalog="main",
        tpcdi_directory="/Volumes/main/tpcdi_raw_data/tpcdi_volume/spark_datagen/",
        regenerate_data=False, log_level="INFO",
        workspace_src_path="/Workspace/Users/x/databricks-tpc-di-augmented/src",
        dbutils=dbutils, spark=spark,
    )
"""
from __future__ import annotations

import concurrent.futures
import os
import sys
import threading
import warnings
from datetime import datetime, timezone
from typing import Any

warnings.filterwarnings("ignore", message=".*No Partition Defined for Window operation.*")


def _path_exists(dbutils: Any, path: str) -> bool:
    try:
        dbutils.fs.ls(path)
        return True
    except Exception:
        return False


def run(
    *,
    scale_factor: int,
    catalog: str,
    tpcdi_directory: str,
    regenerate_data: bool,
    log_level: str,
    workspace_src_path: str,
    dbutils: Any,
    spark: Any,
    **_unused,  # accepts (and ignores) any extra kwargs for signature parity with digen_runner
) -> None:
    """Generate TPC-DI data using the distributed Spark-native generator."""

    def _tlog(msg):
        ts = datetime.now().strftime("%H:%M:%S")
        print(f"  {ts} [Init] {msg}")

    _tlog(f"spark_runner.run() entered (scale_factor={scale_factor}, catalog={catalog})")

    # Raise broadcast-join thresholds. The CustomerMgmt schedule DF alone is
    # ~180MB at SF=5000 and scales with SF; default (10MB static / 30MB AQE)
    # forces Spark into shuffle joins that materialize the 25M-row all_df
    # across executors. Bumping to 250MB lets Spark broadcast the schedule
    # without the shuffle storm. NB: on Spark Connect / serverless the very
    # first spark.conf.set() is the first remote call of the session, so
    # initial JVM + Unity-Catalog session init time is charged here.
    _tlog("setting spark.sql.autoBroadcastJoinThreshold=250m (first spark-connect call — warmup)")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "250m")
    _tlog("setting spark.databricks.adaptive.autoBroadcastJoinThreshold=250m")
    spark.conf.set("spark.databricks.adaptive.autoBroadcastJoinThreshold", "250m")
    _tlog("broadcast-join thresholds set")

    blob_out_path = f"{tpcdi_directory}sf={scale_factor}"

    if catalog != "hive_metastore":
        _tlog(f"checking catalog '{catalog}' existence")
        catalog_exists = spark.sql(
            f"SELECT count(*) FROM system.information_schema.tables "
            f"WHERE table_catalog = '{catalog}'"
        ).first()[0] > 0
        _tlog(f"catalog check complete: exists={catalog_exists}")
        if not catalog_exists:
            _tlog("creating catalog + granting privileges")
            spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
            spark.sql(f"GRANT ALL PRIVILEGES ON CATALOG {catalog} TO `account users`")
            _tlog("catalog created")
        _tlog("CREATE DATABASE IF NOT EXISTS tpcdi_raw_data")
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog}.tpcdi_raw_data "
                  f"COMMENT 'Schema for TPC-DI Raw Files Volume'")
        _tlog("CREATE VOLUME IF NOT EXISTS tpcdi_volume")
        spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.tpcdi_raw_data.tpcdi_volume "
                  f"COMMENT 'TPC-DI Raw Files'")
        _tlog("schema + volume ready")

    _tlog(f"dbutils.fs.ls check on {blob_out_path}")
    if _path_exists(dbutils, blob_out_path):
        if regenerate_data:
            _tlog(f"regenerate_data=YES; recursive delete of prior output begins")
            dbutils.fs.rm(blob_out_path, recurse=True)
            _tlog("prior output deleted")
        else:
            print(f"Data generation skipped since {blob_out_path} already exists. "
                  f"Set regenerate_data=YES to overwrite.")
            return
    else:
        _tlog("no prior output to delete")

    # Resolve module source. Try workspace path first (works on SINGLE_USER clusters);
    # fall back to a Volume copy for USER_ISOLATION/SHARED clusters where /Workspace
    # paths aren't accessible via Python file I/O.
    _tools_dir = f"{workspace_src_path}/tools"
    _vol_module_dir = f"{tpcdi_directory}_module"
    _vol_tools_dir = f"{_vol_module_dir}/tools"

    _tlog("clearing stale sys.modules['tpcdi_gen.*'] entries")
    for _m in list(sys.modules.keys()):
        if _m.startswith("tpcdi_gen"):
            del sys.modules[_m]

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
        except Exception:
            pass
        dbutils.fs.cp(_ws_tpcdi_gen, _vol_tpcdi_gen, recurse=True)
        # Copy static_files individually (directory cp may not recurse for file: paths)
        _ws_static = f"file:{workspace_src_path}/tools/tpcdi_gen/static_files"
        _vol_static = f"{_vol_tpcdi_gen}/static_files"
        for _sf in ["StatusType.txt", "TaxRate.txt", "Date.txt", "Time.txt",
                    "Industry.txt", "TradeType.txt"]:
            try:
                dbutils.fs.cp(f"{_ws_static}/{_sf}", f"{_vol_static}/{_sf}")
            except Exception:
                pass
        sys.path.insert(0, _vol_tools_dir)
        _tlog(f"module source: volume copy {_vol_tools_dir}/tpcdi_gen")

    _tlog("importing tpcdi_gen.config / utils / dictionaries")
    from tpcdi_gen.config import ScaleConfig, NUM_INCREMENTAL_BATCHES
    from tpcdi_gen.utils import (
        make_output_dirs, register_dict_views, bulk_copy_all,
        cleanup_staging, set_log_level, wait_for_background_copies,
        log as _utlog,
    )
    from tpcdi_gen import dictionaries
    _tlog("core imports done")

    set_log_level(log_level)

    cfg = ScaleConfig(int(scale_factor), catalog, tpcdi_directory=tpcdi_directory)
    gen_start_time = datetime.now(tz=timezone.utc)
    all_counts: dict = {}

    print(f"TPC-DI Spark Data Generator")
    print(f"Scale Factor: {scale_factor} ")
    print(f"Output: {cfg.volume_path}")

    # Second recursive delete (redundant with the first one above, but harmless
    # since the first already cleaned anything present).
    _utlog(f"[Init] redundant cleanup of {cfg.volume_path}", "DEBUG")
    try:
        dbutils.fs.rm(cfg.volume_path, recurse=True)
    except Exception:
        pass
    _utlog("[Init] output dir cleaned", "DEBUG")

    _utlog("[Init] creating Batch1/2/3 output directories", "DEBUG")
    make_output_dirs(cfg.volume_path, NUM_INCREMENTAL_BATCHES + 1, dbutils)
    _utlog("[Init] loading dictionary CSVs from disk", "DEBUG")
    dicts = dictionaries.load_all()
    _utlog(f"[Init] loaded {len(dicts)} dictionaries; registering as temp views", "DEBUG")
    register_dict_views(spark, dicts)
    _utlog("[Init] dictionary views registered", "DEBUG")

    # Dependency-graph scheduling: each dataset starts as soon as its
    # specific dependencies are met, maximizing parallelism.
    from tpcdi_gen import reference_tables, hr, finwire, customer
    from tpcdi_gen import market_data, trade, prospect, watch_history

    print("=" * 60)
    print("DEPENDENCY-GRAPH SCHEDULING")
    print("=" * 60)

    fw_symbols_ready = threading.Event()

    with concurrent.futures.ThreadPoolExecutor(max_workers=12) as executor:
        # Tier 0: No dependencies (start immediately)
        f_ref = executor.submit(reference_tables.generate_all, spark, cfg, dicts, dbutils)
        f_hr = executor.submit(hr.generate, spark, cfg, dicts, dbutils)
        f_fw = executor.submit(finwire.generate, spark, cfg, dicts, dbutils,
                               symbols_ready_event=fw_symbols_ready)
        f_prospect = executor.submit(prospect.generate, spark, cfg, dicts, dbutils)

        # Tier 1: CustomerMgmt (depends on HR)
        cust_views_ready = threading.Event()

        def run_customer():
            f_hr.result()  # wait for _brokers
            return customer.generate(spark, cfg, dicts, dbutils,
                                     views_ready_event=cust_views_ready)
        f_cust = executor.submit(run_customer)

        # Tier 1: Depends on _symbols (wait on event, not full FINWIRE)
        def run_watch_history():
            fw_symbols_ready.wait()
            return watch_history.generate(spark, cfg, dicts, dbutils)
        f_wh = executor.submit(run_watch_history)

        def run_daily_market():
            fw_symbols_ready.wait()
            return market_data.generate(spark, cfg, dbutils)
        f_dm = executor.submit(run_daily_market)

        # Tier 2: Trade no longer depends on CustomerMgmt or HR (when static
        # audits are available). n_valid and n_brokers come from cfg analytically;
        # t_ca_id uses the hash-derived _va_idx directly. Trade only waits for HR
        # in the dynamic audit regen path (where we need exact _brokers count).
        from tpcdi_gen.audit import static_audits_available as _sa_avail

        def run_trade():
            fw_symbols_ready.wait()  # _symbols (staged parquet) — always required
            if not _sa_avail(cfg):
                f_hr.result()          # _brokers view needed for exact count
            return trade.generate(spark, cfg, dicts, dbutils)
        f_trade = executor.submit(run_trade)

        # Collect results
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

    wait_for_background_copies()
    bulk_copy_all(dbutils, max_workers=64, label="final")
    _log("[Orchestrator] File copy complete. Cleaning up staging directories...")
    cleanup_staging(cfg.volume_path, dbutils)

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
    print(f"  Elapsed: {elapsed:.1f}s ({total_records / max(1, elapsed):,.0f} records/sec)")
    print(f"  Output: {cfg.volume_path}")
    print(f"{'=' * 60}")

    if not _ws_accessible:
        try:
            dbutils.fs.rm(_vol_module_dir, recurse=True)
            print(f"Cleaned up temporary modules from {_vol_module_dir}")
        except Exception:
            pass
