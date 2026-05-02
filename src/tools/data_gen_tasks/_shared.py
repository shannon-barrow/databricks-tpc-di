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
      ``init_intermediates`` and the cleanup task pass ``load_dicts=False``).
    - Sets ``set_log_level(log_level)``.

    Returns a dict with keys: ``cfg``, ``dicts``, ``stage_schema``,
    ``gen_start_time``.
    """
    # sys.path setup — same logic as spark_runner.py:194-234.
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
                      augmented_incremental=augmented_incremental)

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
