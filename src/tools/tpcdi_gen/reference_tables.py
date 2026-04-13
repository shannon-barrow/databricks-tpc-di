"""Generate reference/dimension tables by copying exact DIGen static files.

Static File Copy Approach
--------------------------
Reference/dimension tables (StatusType, TaxRate, Date, Time, Industry, TradeType)
are identical across ALL scale factors. Their content is defined by the TPC-DI spec
and does not vary with SF. Rather than regenerating them, we copy pre-built static
files from the repository into the output directory.

The static files are stored in tpcdi_gen/static_files/ and are read using Python's
open() (not dbutils.fs.cp) because:
  - On Databricks serverless, dbutils.fs.cp does not support /Workspace paths.
  - Python open() works on both /Workspace (serverless) and UC Volume (classic DBR).

Location resolution:
  1. Try workspace-relative path: /Workspace{notebook_dir}/tpcdi_gen/static_files/
  2. Fall back to UC Volume: /Volumes/.../spark_datagen/_module/tpcdi_gen/static_files/
The content is then written to the output UC Volume via dbutils.fs.put().

BatchDate.txt is the one "reference" file that varies per batch. It contains a
single date value representing the batch processing date. Batch 1 = 2017-07-07,
Batch 2 = 2017-07-08, Batch 3 = 2017-07-09.

These tables are identical across all scale factors:
StatusType.txt, TaxRate.txt, Date.txt, Time.txt, Industry.txt, TradeType.txt

BatchDate.txt varies per batch but is always a single date value.
"""

import os
from pyspark.sql import SparkSession
from .config import *
from .utils import write_text


def generate_all(spark: SparkSession, cfg, dicts: dict, dbutils) -> dict:
    """Copy static reference files and generate BatchDate. Returns record counts.

    This function is safe to run in parallel with other Wave 1 generators
    (HR, FINWIRE) because it only writes to distinct output files and does
    not create or depend on any temp views.

    Args:
        spark: Active SparkSession (unused, but kept for consistent module interface).
        cfg: ScaleConfig with batch_path() for output directory routing.
        dicts: Dictionary data (unused; reference tables use static files).
        dbutils: Databricks dbutils for file I/O.

    Returns:
        dict mapping (table_name, batch_id) to row counts for audit reporting.
    """
    counts = {}

    # --- Resolve static file source path ---
    # Static files live in tpcdi_gen/static_files/ relative to this module.
    # Use __file__ to resolve the path regardless of how the module was imported.
    _this_dir = os.path.dirname(os.path.abspath(__file__))
    ws_base = os.path.join(_this_dir, "static_files")

    # Static reference files with their known row counts (fixed by TPC-DI spec).
    # These counts are used for audit file generation.
    static_files = {
        "StatusType.txt": 6,
        "TaxRate.txt": 320,
        "Date.txt": 25933,
        "Time.txt": 86400,
        "Industry.txt": 102,
        "TradeType.txt": 5,
    }

    # Copy each static file from source to output batch 1 directory
    for filename, row_count in static_files.items():
        src = f"{ws_base}/{filename}"
        dst = f"{cfg.batch_path(1)}/{filename}"
        try:
            with open(src, "r") as f:
                content = f.read()
            dbutils.fs.put(dst, content, overwrite=True)
            counts[(filename.replace(".txt", ""), 1)] = row_count
            print(f"  [Reference] {filename}: {row_count} rows (copied)")
        except Exception as e:
            print(f"  [Reference] WARNING: failed to copy {filename}: {e}")

    # --- BatchDate: one row per batch with the batch date ---
    # Each batch directory gets a BatchDate.txt containing a single date string.
    # These dates are fixed constants defined by the TPC-DI spec.
    batch_dates = [
        (1, "2017-07-07"),
        (2, "2017-07-08"),
        (3, "2017-07-09"),
    ]
    for batch_id, bd in batch_dates:
        if batch_id <= NUM_INCREMENTAL_BATCHES + 1:
            write_text(bd + "\n", f"{cfg.batch_path(batch_id)}/BatchDate.txt", dbutils)
            counts[("BatchDate", batch_id)] = 1

    print(f"  [Reference] BatchDate: {NUM_INCREMENTAL_BATCHES + 1} batches")
    return counts
