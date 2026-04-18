"""Shared utilities for TPC-DI data generation.

This module provides four key capabilities used across all TPC-DI generators:

1. **Deterministic randomness** via `seed_for` and `hash_key` -- ensures reproducible
   data generation across runs by deriving seeds from table/column names.

2. **Dictionary broadcast joins** via `register_dict_views` and `dict_join` -- small
   lookup dictionaries (e.g., first names, cities) are registered as Spark temp views
   and broadcast-joined into large DataFrames to map hash-based indices to string values.

3. **Deferred file copy pattern** (staging -> bulk_copy) -- generators write DataFrames
   to temporary staging directories, then register (source, target) pairs. At the end of
   the generation run, `bulk_copy_all()` copies all files in parallel using a thread pool.
   This avoids blocking on individual file I/O during generation and enables efficient
   parallel transfers.

4. **File writing with automatic splitting** via `write_file` -- writes a DataFrame as
   CSV to a staging directory, preserving Spark's natural partitioning. Single-partition
   outputs become one file; multi-partition outputs get numbered suffixes (e.g.,
   `file_1.csv`, `file_2.csv`).
"""

import hashlib
import os
import concurrent.futures
from datetime import datetime
from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.types import StringType, StructType, StructField, LongType
from pyspark import StorageLevel
from .config import MAX_FILE_BYTES


def safe_unpersist(df):
    """Unpersist a DataFrame if possible; swallow errors on serverless.

    Serverless raises NOT_SUPPORTED_WITH_SERVERLESS on UNPERSIST TABLE.
    If the df was never persisted (common on serverless via disk_cache skip),
    unpersist is a no-op anyway.
    """
    try:
        df.unpersist()
    except Exception:
        pass


def _detect_serverless(spark) -> bool:
    """Detect if running on Databricks Serverless (Spark Connect) compute.

    Serverless misreports ``clusterUsageTags.clusterType`` as ``"ondemand"``, so
    that check alone is unreliable. The authoritative signal: Spark Connect
    sessions raise PySparkAttributeError on ``spark.sparkContext`` access
    because no JVM SparkContext exists on the client.
    """
    try:
        _ = spark.sparkContext
        return False
    except Exception:
        return True


def disk_cache(df, spark, label: str = ""):
    """Cache a DataFrame to disk (DISK_ONLY). Skips on serverless.

    DISK_ONLY avoids memory pressure while still preventing plan re-evaluation.
    On distributed clusters, each worker caches its partitions to local NVMe SSD.
    On serverless, PERSIST TABLE is not supported (raises AnalysisException), so
    we detect upfront and skip. Also wraps the persist/count in try/except as a
    defensive fallback — any error (AnalysisException, RuntimeError, etc.) just
    returns the uncached DataFrame.

    Args:
        df: DataFrame to cache.
        spark: Active SparkSession.
        label: Description for logging.

    Returns:
        (cached_df, was_cached: bool)
    """
    if _detect_serverless(spark):
        log(f"[Cache] {label}: skipped (serverless)")
        return df, False

    try:
        df = df.persist(StorageLevel.DISK_ONLY)
        df.count()
        log(f"[Cache] {label}: cached to disk")
        return df, True
    except Exception as e:
        log(f"[Cache] {label}: skipped ({type(e).__name__})")
        return df, False


# Module-level log level setting
_LOG_LEVEL = "INFO"  # default
_LOG_LEVELS = {"VERBOSE": 0, "DEBUG": 1, "INFO": 2, "WARN": 3}


def set_log_level(level: str):
    """Set the global log level. Called from the orchestrator."""
    global _LOG_LEVEL
    _LOG_LEVEL = level.upper()


def log(msg: str, level: str = "INFO"):
    """Print a timestamped log message if the level meets the threshold."""
    if _LOG_LEVELS.get(level.upper(), 2) >= _LOG_LEVELS.get(_LOG_LEVEL, 2):
        ts = datetime.now().strftime("%H:%M:%S")
        print(f"  {ts} {msg}")


def seed_for(table_name: str, col_name: str = "", base_seed: int = 1234567890) -> int:
    """Derive a deterministic integer seed from a table name and optional column name.

    Produces a stable hash-based seed so that the same (table, column) pair always
    yields the same random sequence across runs. This is the foundation for
    reproducible data generation -- every column in every table gets its own unique
    seed derived from its identity rather than from execution order.

    Args:
        table_name: Name of the table being generated (e.g., "Customer", "Account").
        col_name: Optional column name for column-level seed isolation. When provided,
            each column in the same table gets a different seed.
        base_seed: Root seed value. Change this to produce an entirely different
            (but still deterministic) data set.

    Returns:
        A positive integer seed derived from the MD5 hash of the combined input string.
        Only the first 8 hex digits (32 bits) of the hash are used.
    """
    s = f"{base_seed}_{table_name}_{col_name}"
    return int(hashlib.md5(s.encode()).hexdigest()[:8], 16)


# ---------------------------------------------------------------------------
# Dictionary views
# ---------------------------------------------------------------------------
# Dictionaries are small lists of string values (e.g., first names, last names,
# street suffixes, country codes) loaded from text files. They are registered as
# Spark temporary views so they can be broadcast-joined into large DataFrames.

_dict_counts = {}


def register_dict_views(spark: SparkSession, dicts: dict):
    """Register string-list dictionaries as Spark temporary views for broadcast joins.

    Each dictionary is converted into a two-column DataFrame (_idx, value) and
    registered as a temp view named ``_dict_<name>``. The count of each dictionary
    is cached in ``_dict_counts`` so that ``hash_key(...) % dict_count(name)``
    can map any row to a valid dictionary index.

    Args:
        spark: Active SparkSession.
        dicts: Mapping of dictionary name -> list of string values.
            Example: {"first_name": ["Alice", "Bob", ...], "city": ["NYC", ...]}
    """
    global _dict_counts
    for name, values in dicts.items():
        if not values:
            continue
        # Build (index, value) pairs so we can join on the index column
        rows = [(i, v) for i, v in enumerate(values)]
        df = spark.createDataFrame(rows, StructType([
            StructField("_idx", LongType()),
            StructField("value", StringType()),
        ]))
        df.createOrReplaceTempView(f"_dict_{name}")
        _dict_counts[name] = len(values)
    print(f"  Registered {len(dicts)} dictionary views")


def dict_count(name: str) -> int:
    """Return the number of entries in a registered dictionary.

    Used to compute modulo offsets: ``hash_key(...) % dict_count("first_name")``
    ensures the index falls within valid bounds for the dictionary.

    Args:
        name: Dictionary name as registered in ``register_dict_views``.

    Returns:
        Number of entries, or 1 if the dictionary is not found (avoids division by zero).
    """
    return _dict_counts.get(name, 1)


def dict_join_batch(df: DataFrame, lookups: list) -> DataFrame:
    """Batch multiple dictionary lookups into fewer joins.

    Groups lookups by dictionary name, computes all join keys in one pass,
    then performs one join per unique dictionary (instead of one per lookup).
    This reduces the number of broadcast joins from N to the number of unique
    dictionaries, significantly improving Catalyst plan efficiency.

    Args:
        df: Input DataFrame to enrich.
        lookups: List of (dict_name, hash_col_expr, alias) tuples.
            Example: [("cities", hash_key(F.col("C_ID"), seed), "C_CITY"), ...]

    Returns:
        DataFrame with all alias columns added.
    """
    spark = df.sparkSession

    # Step 1: compute all join keys in one pass
    key_exprs = {}
    for dict_name, hash_col, alias in lookups:
        n = _dict_counts[dict_name]
        jk = f"__jk_{alias}"
        if isinstance(hash_col, str):
            key_exprs[jk] = F.col(hash_col) % F.lit(n)
        else:
            key_exprs[jk] = hash_col % F.lit(n)
    df = df.select("*", *[v.alias(k) for k, v in key_exprs.items()])

    # Step 2: group lookups by dictionary name
    from collections import defaultdict
    by_dict = defaultdict(list)
    for dict_name, hash_col, alias in lookups:
        jk = f"__jk_{alias}"
        by_dict[dict_name].append((jk, alias))

    # Step 3: one join per unique dictionary
    for dict_name, jk_aliases in by_dict.items():
        dict_df = spark.table(f"_dict_{dict_name}")
        for jk, alias in jk_aliases:
            renamed = (dict_df
                .withColumnRenamed("_idx", jk)
                .withColumnRenamed("value", alias))
            df = df.join(F.broadcast(renamed), on=jk, how="left").drop(jk)

    return df


def dict_join(df: DataFrame, dict_name: str, hash_col, alias: str) -> DataFrame:
    """Broadcast-join a dictionary view onto a DataFrame using a hash-based index.

    This is the core mechanism for mapping deterministic hash values to human-readable
    strings. The join works as follows:

    1. Compute a join key by taking ``hash_col % N`` where N is the dictionary size.
       This maps each row to a valid dictionary index.
    2. Load the dictionary temp view and rename its columns to match the join key
       and the desired output alias.
    3. Perform a broadcast left join -- the dictionary is tiny (fits in memory on
       every executor), so Spark broadcasts it to avoid a shuffle.
    4. Drop the temporary join key column.

    Args:
        df: Input DataFrame to enrich with dictionary values.
        dict_name: Name of the dictionary (as registered via ``register_dict_views``).
        hash_col: Either a column name (str) or a Spark Column expression that
            produces a non-negative integer. This value is taken modulo the
            dictionary size to produce the join key.
        alias: Output column name for the looked-up dictionary value.

    Returns:
        The input DataFrame with an additional column ``alias`` containing the
        dictionary value corresponding to each row's hash index.
    """
    spark = df.sparkSession
    n = _dict_counts[dict_name]

    # Temporary join key column -- prefixed to avoid collisions with real columns
    jk = f"__jk_{alias}"
    if isinstance(hash_col, str):
        df2 = df.withColumn(jk, F.col(hash_col) % F.lit(n))
    else:
        df2 = df.withColumn(jk, hash_col % F.lit(n))

    # Rename dictionary columns to align with the join key and desired output name
    dict_df = (spark.table(f"_dict_{dict_name}")
        .withColumnRenamed("_idx", jk)
        .withColumnRenamed("value", alias))

    # Broadcast join: the dictionary is small, so broadcast avoids shuffle
    return df2.join(F.broadcast(dict_df), on=jk, how="left").drop(jk)


def hash_key(col_expr, seed: int) -> "F.Column":
    """Generate a deterministic non-negative hash value for a column expression.

    Wraps Spark's built-in ``hash()`` function with a seed literal to produce
    reproducible pseudo-random values. The result is cast to long and made
    non-negative via ``abs()``. This is typically used with ``% dict_count(name)``
    to index into a dictionary, or directly as a source of deterministic randomness
    for numeric fields.

    The combination of ``col_expr`` (usually a row ID) and ``seed`` (derived from
    ``seed_for(table, column)``) ensures that:
    - The same row always gets the same value for a given column.
    - Different columns in the same row get different values.
    - Different tables with the same row IDs get different values.

    Args:
        col_expr: A Spark Column or column name to hash. Typically the row's
            unique identifier (e.g., customer ID).
        seed: Integer seed (from ``seed_for()``) to differentiate hash spaces.

    Returns:
        A Spark Column expression producing a non-negative long integer.
    """
    return F.abs(F.hash(col_expr, F.lit(seed)).cast("long"))


# ---------------------------------------------------------------------------
# Deferred file copy registry
# ---------------------------------------------------------------------------
# Generators write DataFrames to temporary staging directories (via Spark's
# DataFrameWriter) and then register (source, target) path pairs here.
# This "deferred copy" pattern exists because:
#   1. Spark writes to a directory of part files, not a single named file.
#   2. Renaming/copying files one-at-a-time during generation would serialize I/O.
#   3. By deferring all copies to the end, we can execute them in parallel with
#      a thread pool, significantly reducing total wall-clock time.
# At the end of the generation run, bulk_copy_all() processes all pending copies.

_pending_copies = []  # list of (source_path, target_path) tuples


def register_copy(source: str, target: str):
    """Register a file copy to be executed later in bulk.

    Args:
        source: Source file path (typically a Spark part file in a staging directory).
        target: Destination file path (the final output location).
    """
    _pending_copies.append((source, target))


def register_copies_from_staging(staging_dir: str, final_path: str, dbutils):
    """Register all part files from a Spark staging directory for deferred bulk copy.

    Spark writes output as a directory of ``part-NNNNN`` files. This function
    discovers those part files and registers them for later copying:

    - **Single part file**: The target is ``final_path`` directly (e.g., ``Customer.txt``).
    - **Multiple part files**: Targets get numbered suffixes derived from the original
      filename (e.g., ``Customer_1.txt``, ``Customer_2.txt``, ...). This matches the
      TPC-DI specification for split output files.

    Args:
        staging_dir: Path to the Spark output directory containing part files.
        final_path: Desired final output path (e.g., ``/Volumes/.../Batch1/Customer.txt``).
        dbutils: Databricks dbutils object for filesystem operations.

    Returns:
        List of final target paths that were registered.
    """
    files = dbutils.fs.ls(staging_dir)
    part_files = sorted([f for f in files if f.name.startswith("part-")], key=lambda f: f.name)

    targets = []
    # Always use numbered suffix (e.g., file_1.txt, file_2.txt) even for single files.
    # This ensures the fileNamePattern regex (_[0-9]+)? consistently matches all outputs.
    base, ext = os.path.splitext(final_path)
    for i, pf in enumerate(part_files):
        target = f"{base}_{i+1}{ext}"
        register_copy(pf.path, target)
        targets.append(target)
    return targets


def bulk_copy_all(dbutils, max_workers: int = 64, label: str = ""):
    """Execute all registered file copies in parallel, then clear the registry.

    Uses a thread pool to copy files concurrently. Called at dependency boundaries
    to copy files from all generators that have completed since the last call.

    Args:
        dbutils: Databricks dbutils object for filesystem operations.
        max_workers: Maximum number of concurrent copy threads. Defaults to 64,
            which provides good throughput for cloud storage backends.
        label: Optional context label for log messages (e.g., "after HR").
    """
    global _pending_copies
    copies = list(_pending_copies)
    _pending_copies = []

    if not copies:
        return

    def copy_file(src_tgt):
        src, tgt = src_tgt
        dbutils.fs.cp(src, tgt)
        return tgt

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(copy_file, pair) for pair in copies]
        for future in concurrent.futures.as_completed(futures):
            future.result()  # raises if failed

    # Log per-dataset completion: group by (dataset, batch directory)
    import re
    from collections import defaultdict
    by_dataset = defaultdict(lambda: {"count": 0, "dir": ""})
    for _, tgt in copies:
        fname = os.path.basename(tgt)
        # Extract dataset name: strip numeric suffix (e.g., "Trade_1.txt" → "Trade")
        # and strip year/quarter from FINWIRE (e.g., "FINWIRE1967Q1_1" → "FINWIRE")
        base = fname.split("_")[0].split(".")[0] if "_" in fname else fname.rsplit(".", 1)[0]
        dataset = re.sub(r'\d{4}Q\d$', '', base)  # FINWIRE1967Q1 → FINWIRE
        batch_dir = os.path.dirname(tgt)
        batch_name = os.path.basename(batch_dir)  # "Batch1", "Batch2", etc.
        key = (dataset, batch_dir)
        by_dataset[key]["count"] += 1
        by_dataset[key]["dir"] = batch_name
    label_suffix = f" ({label})" if label else ""
    for (dataset, _), info in sorted(by_dataset.items()):
        log(f"[Copy] {dataset}: {info['count']} file(s) -> {info['dir']}{label_suffix}", "DEBUG")


# ---------------------------------------------------------------------------
# File writing (stages to temp dir, defers copy)
# ---------------------------------------------------------------------------



def write_file(df: DataFrame, path: str, delimiter: str = "|",
               dbutils=None, scale_factor: int = 0):
    """Write a DataFrame to a staging directory and register deferred copies to the final path.

    Uses maxRecordsPerFile to cap output files at ~128MB based on hardcoded per-dataset
    row sizes. This splits skewed partitions without a shuffle.

    Args:
        df: DataFrame to write.
        path: Final output file path (e.g., ``/Volumes/.../Batch1/Customer.txt``).
        delimiter: Field delimiter for the CSV output. Defaults to ``|`` per TPC-DI spec.
        dbutils: Databricks dbutils object for filesystem operations.
        scale_factor: TPC-DI scale factor.

    Returns:
        List of final target file paths.
    """
    staging_dir = path + "__staging"
    _cleanup(staging_dir, dbutils)

    # --- File sizing ---
    _BYTES_PER_ROW = {
        "Trade": 188, "TradeHistory": 33, "CashTransaction": 87,
        "HoldingHistory": 25, "WatchHistory": 45, "DailyMarket": 51,
        "HR": 80, "Prospect": 196,
    }
    filename = os.path.basename(path)
    dataset = filename.split(".")[0].split("_")[0] if "_" in filename else filename.rsplit(".", 1)[0]
    is_batch1 = "/Batch1/" in path

    if not is_batch1:
        # Incremental batches: coalesce to minimize file count.
        # DailyMarket and Prospect use maxRecordsPerFile instead (too large to coalesce).
        if dataset not in ("DailyMarket", "Prospect"):
            _INC_MB_PER_1K = {
                "Trade": 19, "CashTransaction": 7, "HoldingHistory": 3,
                "WatchHistory": 35, "Customer": 1, "Account": 1,
            }
            inc_mb = _INC_MB_PER_1K.get(dataset, 1) * (scale_factor / 1000)
            n_files = max(1, int(inc_mb / 128) + (1 if inc_mb % 128 > 0 else 0))
            df = df.coalesce(n_files)

    # Cap file size at ~128MB using maxRecordsPerFile (Batch1 + DM/Prospect incremental).
    row_bytes = _BYTES_PER_ROW.get(dataset, 0)
    max_records = int(128 * 1024 * 1024 / row_bytes) if row_bytes > 0 else 0

    writer = (df
        .write
        .mode("overwrite")
        .option("header", "false")
        .option("delimiter", delimiter)
        .option("quote", "")       # No quoting -- TPC-DI spec uses raw delimiters
        .option("escape", "")      # No escape characters
        .option("nullValue", "")   # Nulls written as empty strings
        .option("emptyValue", ""))  # Empty strings stay empty (no quotes)
    if max_records > 0:
        writer = writer.option("maxRecordsPerFile", max_records)
    writer.csv(staging_dir)

    # Register staging part files for deferred bulk copy to final path(s)
    return register_copies_from_staging(staging_dir, path, dbutils)


def write_text(content: str, path: str, dbutils=None):
    """Write a plain text string directly to a file path.

    Used for small metadata files (e.g., ``BatchDate.txt``, ``_audit.csv``)
    that don't need DataFrame-based writing or the staging/copy pattern.

    Args:
        content: Text content to write.
        path: Destination file path.
        dbutils: Databricks dbutils object for filesystem operations.
    """
    if dbutils:
        import io, sys
        _old_stdout = sys.stdout
        sys.stdout = io.StringIO()  # suppress "Wrote N bytes" output
        try:
            dbutils.fs.put(path, content, overwrite=True)
        finally:
            sys.stdout = _old_stdout


def cleanup_staging(volume_path: str, dbutils):
    """Remove all temporary directories (__staging, __tmp) after bulk copy is complete.

    Walks the output volume's batch directories and deletes any leftover staging
    or temp directories. This is a cleanup step run at the very end of generation
    to reclaim temporary storage.

    Args:
        volume_path: Root volume path containing Batch1/, Batch2/, etc.
        dbutils: Databricks dbutils object for filesystem operations.
    """
    try:
        for batch_dir in dbutils.fs.ls(volume_path):
            if not batch_dir.isDir():
                continue
            for f in dbutils.fs.ls(batch_dir.path):
                if f.name.endswith("__staging/") or f.name.endswith("__tmp/"):
                    dbutils.fs.rm(f.path, recurse=True)
    except:
        pass


def _cleanup(path: str, dbutils):
    """Silently remove a path (file or directory) if it exists.

    Used to clear previous staging directories before writing new output.

    Args:
        path: File or directory path to remove.
        dbutils: Databricks dbutils object for filesystem operations.
    """
    try:
        dbutils.fs.rm(path, recurse=True)
    except Exception:
        pass


def make_output_dirs(volume_path: str, num_batches: int, dbutils=None):
    """Create the output directory structure for TPC-DI batch folders.

    Creates ``Batch1/`` through ``BatchN/`` under the volume path. These
    directories hold the final generated data files.

    Args:
        volume_path: Root volume path (e.g., ``/Volumes/catalog/schema/tpcdi``).
        num_batches: Number of batch directories to create (typically 3 for TPC-DI:
            Batch1 = historical load, Batch2 = incremental 1, Batch3 = incremental 2).
        dbutils: Databricks dbutils object for filesystem operations.
    """
    for b in range(1, num_batches + 1):
        dbutils.fs.mkdirs(f"{volume_path}/Batch{b}")
