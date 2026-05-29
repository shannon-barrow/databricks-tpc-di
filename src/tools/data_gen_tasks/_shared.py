"""Shared bootstrap helpers for the per-dataset data_gen task notebooks.

Each notebook calls ``bootstrap()`` at the top to set up:
- spark.conf broadcast thresholds (best-effort on serverless)
- Module imports for ``tpcdi_gen`` (workspace path or volume copy)
- ``ScaleConfig`` instance built from job parameters
- Loaded dictionaries
- The intermediate ``_stage`` schema

Returns a context dict the per-task notebooks pass to the underlying
generator functions and to the cross-task intermediate-table helpers.
"""
from __future__ import annotations

import os
import sys
from datetime import datetime, timezone
from typing import Any

# TBLPROPERTIES applied to every Delta table written under
# {wh_db}_{sf}_stage by the data_gen tasks. These are temp tables — disable
# stats collection and auto-optimize so writes are as cheap as possible.
INTERMEDIATE_TBLPROPS = (
    "TBLPROPERTIES("
    "'delta.dataSkippingNumIndexedCols'=0, "
    "'delta.autoOptimize.autoCompact'=False, "
    "'delta.autoOptimize.optimizeWrite'=False)"
)


def stage_schema_fq(catalog: str, wh_db: str, scale_factor) -> str:
    """`{catalog}.{wh_db}_{scale_factor}_stage` — the intermediate schema."""
    return f"{catalog}.{wh_db}_{scale_factor}_stage"


def intermediate_table_fq(catalog: str, wh_db: str, scale_factor, name: str) -> str:
    """Fully-qualified name for a cross-task intermediate Delta table."""
    return f"{stage_schema_fq(catalog, wh_db, scale_factor)}.{name}"


def is_already_generated(spark, table_fq: str, expected_min_rows: int = 1) -> bool:
    """True if ``table_fq`` exists and has at least ``expected_min_rows`` rows.

    Used for per-task self-skip when ``regenerate_data=NO``: lets a workflow
    re-run without redoing the datasets that already completed cleanly.
    Any failure (table missing, permission denied, malformed Delta) returns
    False so the task proceeds with regeneration.
    """
    try:
        cnt = spark.sql(f"SELECT COUNT(*) AS n FROM {table_fq}").collect()[0]["n"]
        return cnt >= expected_min_rows
    except Exception:
        return False


# Names of the historical staging tables built by augmented_staging's Stage 1
# SQL tasks. The benchmark workflows (augmented_classic / _sdp / _dbt) DEEP
# CLONE these into the benchmark working schema before each run, so they ARE
# the consumables. If all are present in tpcdi_incremental_staging_{sf}, the
# historical portion of staging is complete.
_AUG_STAGING_TABLES = frozenset({
    # ingest_* outputs (small reference tables)
    "dimdate", "dimtime", "industry", "statustype", "taxrate", "tradetype",
    "batchdate",
    # Silver static dims
    "dimbroker", "dimcompany", "dimsecurity", "financial",
    # SCD2 historical dims/facts
    "dimcustomer", "dimaccount", "dimtrade",
    "factcashbalances", "factholdings", "factwatches",
    "companyyeareps", "currentaccountbalances",
    "dailymarket", "factmarkethistory",
})

# The 7 augmented-mode datasets that stage_files writes as partitioned CSV
# trees under {volume_path}/{Dataset}/_pdate=*/part-*.csv (with a _SUCCESS
# marker at the dataset root). These feed simulate_filedrops in the daily
# Stage 1 loop.
_AUG_PARTITIONED_DATASETS = (
    "Customer", "Account", "Prospect", "Trade", "WatchHistory",
    "CashTransaction", "DailyMarket",
)

# Standard-mode (non-augmented) Batch1 files that the single-batch SQL
# benchmark ingests. Both DIGen-style names (e.g. HR.csv, Customer.txt) and
# Spark-split forms (HR_1.txt, Customer_1.txt, …) are accepted.
_STD_BATCH1_FILES = (
    "HR.csv", "Customer.txt", "Account.txt", "CustomerMgmt.xml",
    "Prospect.csv", "DailyMarket.txt", "WatchHistory.txt", "Trade.txt",
    "TradeHistory.txt", "CashTransaction.txt", "HoldingHistory.txt",
    "BatchDate.txt",
)


def _path_exists(dbutils, path: str) -> bool:
    try:
        dbutils.fs.ls(path)
        return True
    except Exception:
        return False


def staging_complete_check(spark, dbutils, *, catalog: str, scale_factor,
                           augmented_incremental: bool, volume_path: str) -> bool:
    """True if all staging tables/files the benchmark consumes are intact.

    Used by the entry-point data_gen task to decide whether to short-circuit
    the rest of the workflow (via the `staging_check` condition_task gate).

    What's checked depends on the mode:

    - **augmented_incremental=True** — both halves of Stage 0's output:
      1. Every historical staging table in
         ``{catalog}.tpcdi_incremental_staging_{sf}`` is present (the
         dim/fact tables the benchmark setup notebooks DEEP CLONE from).
      2. A ``_SUCCESS`` marker exists under each of the 7 partitioned-CSV
         dataset dirs at ``{volume_path}/{Dataset}/`` (the daily-loop
         file source).

    - **augmented_incremental=False** — every expected raw file is present
      in ``{volume_path}/Batch1`` (either as the DIGen single-file name or
      the Spark-split form ``{stem}_N.{ext}``).

    Intentionally does NOT check the cross-task intermediates in
    ``{wh_db}_{sf}_stage`` — those get dropped by ``cleanup_intermediates``
    at the end of every successful run, so they'd always look "missing" on
    a re-trigger even when nothing needs regeneration.
    """
    if augmented_incremental:
        # 1. Historical staging tables
        staging_schema = f"{catalog}.tpcdi_incremental_staging_{scale_factor}"
        try:
            present = {r["tableName"] for r in
                       spark.sql(f"SHOW TABLES IN {staging_schema}").collect()}
        except Exception as e:
            print(f"[staging_check] {staging_schema} not queryable "
                  f"({type(e).__name__}: {e}) — treating as incomplete")
            return False
        missing = _AUG_STAGING_TABLES - present
        if missing:
            print(f"[staging_check] {staging_schema} missing tables: "
                  f"{sorted(missing)}")
            return False
        # 2. Partitioned CSV trees with _SUCCESS markers
        for dataset in _AUG_PARTITIONED_DATASETS:
            marker = f"{volume_path.rstrip('/')}/{dataset}/_SUCCESS"
            if not _path_exists(dbutils, marker):
                print(f"[staging_check] missing _SUCCESS marker: {marker}")
                return False
        print(f"[staging_check] augmented staging intact: "
              f"{len(_AUG_STAGING_TABLES)} tables in {staging_schema}, "
              f"{len(_AUG_PARTITIONED_DATASETS)} partitioned CSV trees")
        return True
    else:
        # Standard mode: check Batch1 raw files (the single-batch SQL
        # benchmark consumes from here). Accept either DIGen-style single
        # filenames or Spark-split forms (Customer_1.txt, Customer_2.txt, …).
        batch1 = f"{volume_path.rstrip('/')}/Batch1"
        try:
            existing = {f.name.rstrip("/") for f in dbutils.fs.ls(batch1)}
        except Exception as e:
            print(f"[staging_check] {batch1} not listable "
                  f"({type(e).__name__}: {e}) — treating as incomplete")
            return False
        for filename in _STD_BATCH1_FILES:
            stem, _, ext = filename.rpartition(".")
            split_prefix = f"{stem}_"
            if filename in existing or any(
                f.startswith(split_prefix) and f.endswith(f".{ext}")
                for f in existing
            ):
                continue
            print(f"[staging_check] missing from {batch1}: {filename}")
            return False
        print(f"[staging_check] standard-mode Batch1 intact "
              f"({len(_STD_BATCH1_FILES)} expected files)")
        return True


def write_intermediate_delta(df, *, catalog: str, wh_db: str, scale_factor,
                             name: str, partition_cols: list = None) -> str:
    """Write ``df`` as a Delta temp table in ``{wh_db}_{sf}_stage``.

    Always overwrites. Uses the ``INTERMEDIATE_TBLPROPS`` (no stats / no
    auto-optimize) so writes are as fast as possible for transient data.

    Returns the fully-qualified table name.
    """
    fq = intermediate_table_fq(catalog, wh_db, scale_factor, name)
    spark = df.sparkSession
    spark.sql(f"DROP TABLE IF EXISTS {fq}")
    cols_ddl = ", ".join(f"{f.name} {f.dataType.simpleString()}"
                         for f in df.schema.fields)
    partition_clause = (f"PARTITIONED BY ({', '.join(partition_cols)})"
                        if partition_cols else "")
    spark.sql(f"CREATE TABLE {fq} ({cols_ddl}) USING DELTA "
              f"{partition_clause} {INTERMEDIATE_TBLPROPS}")
    df.write.mode("append").saveAsTable(fq)
    return fq


def read_intermediate_view(spark, *, catalog: str, wh_db: str, scale_factor,
                           name: str, view_name: str = None):
    """Read a `_gen_*` Delta from `{wh_db}_{sf}_stage` and register a temp view.

    Each per-task notebook calls this for any cross-task dependency.
    Default ``view_name`` matches what the underlying generator code reads
    via ``spark.table(...)`` — e.g. ``read_intermediate_view(name="_gen_brokers",
    view_name="_brokers")`` lets the existing ``hr.py`` consumer code keep
    using its session-shared temp-view pattern unchanged.
    """
    fq = intermediate_table_fq(catalog, wh_db, scale_factor, name)
    if view_name is None:
        view_name = name
    df = spark.read.table(fq)
    df.createOrReplaceTempView(view_name)
    return df


def bootstrap(*, spark, dbutils, scale_factor, catalog: str, wh_db: str,
              tpcdi_directory: str, log_level: str,
              augmented_incremental: bool, workspace_src_path: str,
              load_dicts: bool = True) -> dict:
    """One-time setup for a data_gen task notebook.

    - Adds the workspace ``tools`` dir (or a volume copy) to ``sys.path`` so
      ``tpcdi_gen`` can be imported.
    - Sets spark broadcast thresholds (best-effort; some are blocked on
      serverless).
    - Builds ``cfg = ScaleConfig(...)``.
    - Loads dictionaries from disk if requested (most gen tasks need them;
      ``data_gen`` and the cleanup task pass ``load_dicts=False``).
    - Sets ``set_log_level(log_level)``.

    Returns a dict with keys: ``cfg``, ``dicts``, ``stage_schema``,
    ``gen_start_time``.
    """
    # sys.path setup — workspace tools dir takes priority; fall back to a
    # volume copy if the workspace isn't FUSE-accessible from this task.
    _tools_dir = f"{workspace_src_path}/tools"
    _vol_module_dir = f"{tpcdi_directory}_module"
    _vol_tools_dir = f"{_vol_module_dir}/tools"
    for _m in list(sys.modules):
        if _m.startswith("tpcdi_gen"):
            del sys.modules[_m]
    _ws_accessible = False
    try:
        _ws_accessible = os.path.isdir(f"{_tools_dir}/tpcdi_gen")
    except OSError:
        pass
    if _ws_accessible:
        if _tools_dir not in sys.path:
            sys.path.insert(0, _tools_dir)
    else:
        # Copy the module to the volume so worker pods can import it.
        _ws_tpcdi_gen = f"file:{workspace_src_path}/tools/tpcdi_gen"
        _vol_tpcdi_gen = f"{_vol_module_dir}/tools/tpcdi_gen"
        try:
            dbutils.fs.rm(_vol_tpcdi_gen, recurse=True)
        except Exception:
            pass
        dbutils.fs.cp(_ws_tpcdi_gen, _vol_tpcdi_gen, recurse=True)
        _ws_static = f"file:{workspace_src_path}/tools/tpcdi_gen/static_files"
        _vol_static = f"{_vol_tpcdi_gen}/static_files"
        for _sf in ["StatusType.txt", "TaxRate.txt", "Date.txt", "Time.txt",
                    "Industry.txt", "TradeType.txt"]:
            try:
                dbutils.fs.cp(f"{_ws_static}/{_sf}", f"{_vol_static}/{_sf}")
            except Exception:
                pass
        if _vol_tools_dir not in sys.path:
            sys.path.insert(0, _vol_tools_dir)

    # Broadcast thresholds. safe_conf_set swallows the CONFIG_NOT_AVAILABLE
    # exception that serverless raises for non-allowlisted confs.
    from tpcdi_gen.utils import safe_conf_set
    safe_conf_set(spark, "spark.sql.autoBroadcastJoinThreshold", "250m")
    safe_conf_set(spark, "spark.databricks.adaptive.autoBroadcastJoinThreshold", "250m")

    from tpcdi_gen.config import ScaleConfig
    from tpcdi_gen.utils import set_log_level
    set_log_level(log_level)
    cfg = ScaleConfig(int(scale_factor), catalog,
                      tpcdi_directory=tpcdi_directory,
                      augmented_incremental=augmented_incremental,
                      wh_db=wh_db)

    dicts = None
    if load_dicts:
        from tpcdi_gen import dictionaries
        from tpcdi_gen.utils import register_dict_views
        dicts = dictionaries.load_all()
        register_dict_views(spark, dicts)

    return {
        "cfg": cfg,
        "dicts": dicts,
        "stage_schema": stage_schema_fq(catalog, wh_db, scale_factor),
        "gen_start_time": datetime.now(tz=timezone.utc),
    }
