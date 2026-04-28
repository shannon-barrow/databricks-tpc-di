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
import re
import concurrent.futures
from datetime import datetime
from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.types import StringType, StructType, StructField, LongType
from pyspark import StorageLevel
from .config import MAX_FILE_BYTES


def safe_unpersist(df, cleanup_info=None):
    """Release a cached or staged DataFrame.

    Accepts the ``cleanup_info`` dict returned by ``disk_cache()``:
      - Classic persist: calls ``unpersist()`` and logs "Unpersisted cache"
      - Serverless Parquet staging: deletes the staging directory and logs
        "Dropped staging table"

    If ``cleanup_info`` is None, falls back to a best-effort unpersist on
    classic compute; on serverless, unpersist is a no-op because persist
    was never supported (disk_cache staged to Parquet instead, and the
    staging dir is already cleaned up by ``cleanup_staging``).
    """
    # Serverless has no persist semantics to reverse. UNCACHE TABLE raises NOT_SUPPORTED_WITH_SERVERLESS; skip the call entirely to avoid noisy query-error logs.
    try:
        spark = df.sparkSession
        if _detect_serverless(spark):
            if cleanup_info and cleanup_info.get("kind") == "parquet":
                path = cleanup_info.get("path")
                dbutils = cleanup_info.get("dbutils")
                if path and dbutils is not None:
                    try:
                        dbutils.fs.rm(path, recurse=True)
                    except Exception:
                        pass
                log(f"[Staging] {cleanup_info.get('label','')}: dropped staging table")
            return
    except Exception:
        pass

    if cleanup_info is None:
        try:
            df.unpersist()
        except Exception:
            pass
        return

    kind = cleanup_info.get("kind")
    label = cleanup_info.get("label", "")
    if kind == "persist":
        try:
            df.unpersist()
        except Exception:
            pass
        log(f"[Cache] {label}: unpersisted cache")
    elif kind == "parquet":
        path = cleanup_info.get("path")
        dbutils = cleanup_info.get("dbutils")
        if path and dbutils is not None:
            try:
                dbutils.fs.rm(path, recurse=True)
            except Exception:
                pass
        log(f"[Staging] {label}: dropped staging table")


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


# Counter for disambiguating temp-table/temp-view names across calls.
_STAGING_COUNTER = [0]


def _sanitize_label(label: str) -> str:
    """Turn a label into a safe SQL identifier component (lowercase + underscores)."""
    slug = re.sub(r"[^a-zA-Z0-9]+", "_", label).strip("_").lower()
    return slug[:40] or "stage"


def disk_cache(df, spark, label: str = "", materialize: bool = True,
               volume_path: str = None, dbutils=None,
               max_records_per_file: int = 0):
    """Materialize a DataFrame so subsequent reads skip the upstream DAG.

    Classic: ``persist(DISK_ONLY)`` + ``count()`` — fast, uses local NVMe.
    Serverless: persist is unsupported, so we write a Parquet staging file at
    ``{volume_path}/_staging/{N}_{label}`` and return a DataFrame that reads it
    back. ``cleanup_staging`` removes the ``_staging/`` directory at the end of
    the run.

    ``max_records_per_file`` caps each parquet staging file, which controls
    the partition count on read-back (each file becomes ~1 input split).
    Use this to keep a high partition count for distribution; without it,
    Spark may produce a few huge parquet files and the read-back partition
    count drops far below the upstream ``repartition(N)``.

    Pass ``materialize=False`` for derived views whose parent is already
    materialized — on serverless they stay as logical views (re-reading a
    column-pruned subset of the parent is cheap); on classic they still
    persist since persist is nearly free memory-wise.

    Returns:
        (materialized_df, cleanup_info or None)

        ``cleanup_info`` is a dict consumed by ``safe_unpersist()``:
          - ``{"kind": "persist", "label": ...}`` on classic
          - ``{"kind": "parquet", "path": ..., "dbutils": ..., "label": ...}``
            on serverless when Parquet staging ran
          - ``None`` if nothing was materialized (skip paths / failures)
    """
    if _detect_serverless(spark):
        if not materialize or volume_path is None:
            return df, None
        _STAGING_COUNTER[0] += 1
        slug = _sanitize_label(label)
        staging_path = f"{volume_path}/_staging/{_STAGING_COUNTER[0]:03d}_{slug}"
        log(f"[Staging] {label}: cache is unavailable on serverless, staging to Parquet")
        try:
            if dbutils is not None:
                _cleanup(staging_path, dbutils)
            writer = df.write.mode("overwrite")
            if max_records_per_file > 0:
                writer = writer.option("maxRecordsPerFile", max_records_per_file)
            writer.parquet(staging_path)
            new_df = spark.read.parquet(staging_path)
            return new_df, {"kind": "parquet", "path": staging_path,
                            "dbutils": dbutils, "label": label}
        except Exception as e:
            log(f"[Staging] {label}: failed ({type(e).__name__}: {e})")
            return df, None

    try:
        df = df.persist(StorageLevel.DISK_ONLY)
        df.count()
        log(f"[Cache] {label}: cached to disk")
        return df, {"kind": "persist", "label": label}
    except Exception as e:
        log(f"[Cache] {label}: skipped ({type(e).__name__})")
        return df, None


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
# Dictionaries are small lists of string values (e.g., first names, last names, street suffixes, country codes) loaded from text files. They are registered as Spark temporary views so they can be broadcast-joined into large DataFrames.

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
            df = df.join(renamed, on=jk, how="left").drop(jk)

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

    return df2.join(dict_df, on=jk, how="left").drop(jk)


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
# Generators write DataFrames to temporary staging directories (via Spark's DataFrameWriter) and then register (source, target) path pairs here. This "deferred copy" pattern exists because:
#   1. Spark writes to a directory of part files, not a single named file. 2. Renaming/copying files one-at-a-time during generation would serialize I/O. 3. By deferring all copies to the end, we can execute them in parallel with a thread pool, significantly reducing total wall-clock time.
# At the end of the generation run, bulk_copy_all() processes all pending copies.

_pending_copies = []  # legacy: rarely used; new register_copies_from_staging
                      # kicks off per-dataset copies directly.
_background_threads = []  # Thread objects for per-dataset async copies


def _start_dataset_copy(dbutils, copies, label: str = "", max_workers: int = 32):
    """Kick off a background thread that copies just this dataset's part files.

    Each write_file call uses this to start copying its large parts as soon
    as the write returns, rather than queueing to a global list for a later
    "drain everything" sweep. This avoids:
      - Racing cleanup_staging vs. still-running bulk copies at scale.
      - Head-of-line blocking: a slow dataset's copy no longer delays
        fast datasets from finalizing.

    Threads are tracked in `_background_threads` so the orchestrator can
    wait_for_background_copies() before cleanup_staging.
    """
    import threading
    def worker():
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(dbutils.fs.cp, src, tgt) for src, tgt in copies]
            for f in concurrent.futures.as_completed(futures):
                f.result()
        log(f"[Copy] {label}: {len(copies)} parts copied", "DEBUG")
    t = threading.Thread(target=worker, daemon=True)
    _background_threads.append(t)
    t.start()


def wait_for_background_copies(timeout_per_thread: float = 3600):
    """Join all tracked background-copy threads. Call before cleanup_staging."""
    threads = list(_background_threads)
    _background_threads.clear()
    for t in threads:
        t.join(timeout=timeout_per_thread)


def register_copy(source: str, target: str):
    """Legacy: register a copy into the global bulk queue.

    Preferred path: write_file -> register_copies_from_staging now starts
    per-dataset async copies directly. This function is retained for
    backward compat if any caller still queues manually.
    """
    _pending_copies.append((source, target))


_LARGE_PART_MIN_BYTES = 50 * 1024 * 1024    # parts >= this bypass packing
_PACK_MAX_BYTES = 128 * 1024 * 1024         # packed file size cap


def _pack_small_parts(small_parts, max_bytes: int):
    """Greedy first-fit-decreasing bin-packing.

    Given a list of part files (each an object with ``.path``, ``.name``, ``.size``),
    group them into bins whose total size does not exceed ``max_bytes``.

    Returns a list of bins, each a list of parts sorted by original part name
    (so concat order is deterministic and matches Spark's partition order within
    each bin).
    """
    sorted_parts = sorted(small_parts, key=lambda f: f.size, reverse=True)
    bins = []  # list of {"files": [...], "size": int}
    for f in sorted_parts:
        placed = False
        for b in bins:
            if b["size"] + f.size <= max_bytes:
                b["files"].append(f)
                b["size"] += f.size
                placed = True
                break
        if not placed:
            bins.append({"files": [f], "size": f.size})
    return [sorted(b["files"], key=lambda f: f.name) for b in bins]


def register_copies_from_staging(staging_dir: str, final_path: str, dbutils,
                                  start_idx: int = 1):
    """Hybrid register: parallel cp for large parts, driver-side cat pack for small.

    Spark writes output as a directory of ``part-NNNNN`` files sized up to
    ``maxRecordsPerFile`` (~128MB). Tail partitions and under-partitioned
    datasets often produce many smaller parts, which are wasteful to copy
    individually (RPC overhead per file, extra files for downstream readers).

    This function splits parts into two paths:

    - **Large parts** (``>= 50MB``): registered for deferred parallel
      ``dbutils.fs.cp`` via ``bulk_copy_all`` — server-side copy, no driver
      bandwidth, ~64-thread parallelism.

    - **Small parts** (``< 50MB``): bin-packed into groups summing to
      ``<= 128MB``, then concatenated into single files via driver-side
      ``xargs cat`` on the FUSE mount. Reduces per-file RPC overhead and
      produces fewer downstream files.

    Output files are numbered sequentially starting from ``start_idx``:
    e.g. ``Customer_1.txt``, ``Customer_2.txt``, … matching the TPC-DI
    ``fileNamePattern`` regex. Multiple calls can share a sequence by
    passing the returned `next_idx` as the next call's ``start_idx`` (used
    by FINWIRE where CMP/SEC/FIN subsets each stage separately but land in
    the same ``FINWIRE_N.txt`` namespace).

    Args:
        staging_dir: Path to the Spark output directory containing part files.
        final_path: Desired final output path (e.g., ``/Volumes/.../Batch1/Customer.txt``).
        dbutils: Databricks dbutils object for filesystem operations.
        start_idx: First ``_N`` value to use (default 1).

    Returns:
        (list of final target paths, next_idx to use for subsequent calls).
    """
    files = dbutils.fs.ls(staging_dir)
    part_files = [f for f in files if f.name.startswith("part-")]
    if not part_files:
        return [], start_idx

    large = sorted([f for f in part_files if f.size >= _LARGE_PART_MIN_BYTES],
                   key=lambda f: f.name)
    small = [f for f in part_files if f.size < _LARGE_PART_MIN_BYTES]
    bins = _pack_small_parts(small, _PACK_MAX_BYTES)

    base, ext = os.path.splitext(final_path)
    targets = []
    idx = start_idx

    # Large parts: kick off a per-dataset async copy immediately so this dataset can start copying as soon as write_file returns, without waiting for a global drain. The thread is tracked in `_background_threads` so orchestrator can wait_for_background_copies() before cleanup_staging.
    large_copies = []
    for pf in large:
        target = f"{base}_{idx}{ext}"
        large_copies.append((pf.path, target))
        targets.append(target)
        idx += 1
    if large_copies:
        _start_dataset_copy(dbutils, large_copies, label=os.path.basename(base))

    # Small parts: cat-pack on driver into ~128MB chunks. FUSE mount on UC Volumes returns EAGAIN ("Resource temporarily unavailable") under heavy parallel I/O. At SF=10000+ we have hundreds of concat operations across TradeHistory/CashTransaction/HH/etc. running simultaneously through the dep-graph scheduler and xargs cat's 5-retry loop wasn't enough. Switched to pure-Python concat with per-source retry: open dst once, copy each src via shutil.copyfileobj; if a source read hits OSError (EAGAIN wraps up as such), back off and retry that source only.
    if bins:
        import shutil, time
        for bin_files in bins:
            target = f"{base}_{idx}{ext}"
            dst_local = target.replace("dbfs:", "") if target.startswith("dbfs:") else target
            with open(dst_local, "wb") as out:
                for pf in bin_files:
                    src_local = pf.path.replace("dbfs:", "") if pf.path.startswith("dbfs:") else pf.path
                    last_err = None
                    for attempt in range(10):
                        try:
                            with open(src_local, "rb") as src:
                                shutil.copyfileobj(src, out, length=4 * 1024 * 1024)
                            break
                        except OSError as e:
                            last_err = e
                            if attempt == 0:
                                log(f"[Concat] {os.path.basename(target)}: source EAGAIN on {os.path.basename(src_local)}, retrying", "WARN")
                            time.sleep(min(30, 0.5 * (2 ** attempt)))
                    else:
                        raise last_err
            targets.append(target)
            idx += 1

    return targets, idx


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
        # Extract dataset name: strip numeric suffix (e.g., "Trade_1.txt" → "Trade") and strip year/quarter from FINWIRE (e.g., "FINWIRE1967Q1_1" → "FINWIRE")
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
    """Write a DataFrame to a staging directory and register final outputs.

    Spark writes part files capped at ~128MB via ``maxRecordsPerFile``, then
    ``register_copies_from_staging`` produces final numbered files using the
    hybrid strategy:

    - Parts ≥ 50MB are queued for deferred parallel ``dbutils.fs.cp`` (server-
      side copy, no driver bandwidth).
    - Parts < 50MB are bin-packed into ≤128MB groups and merged with
      ``xargs cat`` on the driver FUSE mount.

    Small datasets (most incrementals) trivially pack into a single bin, producing
    one ``_1`` file. Large datasets (Batch1, DailyMarket) flow mostly through the
    parallel-cp path with only tail partitions packed. CustomerMgmt XML and audit
    files use their own writers and don't go through this path.

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

    _BYTES_PER_ROW = {
        "Trade": 188, "TradeHistory": 33, "CashTransaction": 87,
        "HoldingHistory": 25, "WatchHistory": 45, "DailyMarket": 51,
        "HR": 80, "Prospect": 196,
    }
    filename = os.path.basename(path)
    dataset = filename.split(".")[0].split("_")[0] if "_" in filename else filename.rsplit(".", 1)[0]
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

    targets, _next = register_copies_from_staging(staging_dir, path, dbutils)
    return targets


def write_delta(df: DataFrame, *, cfg, dataset: str,
                partition_cols: list = None) -> str:
    """Write ``df`` as a Delta table at ``{catalog}.tpcdi_raw_data.{dataset}{sf}``.

    Used by the augmented-incremental staging mode in place of file output.
    The downstream ``stage_files`` / ``stage_tables`` tasks read from these
    temp tables; ``cleanup_stage0`` drops them once per-day CSVs and the
    ``tpcdi_incremental_staging_{sf}`` schema are populated.

    Returns the fully-qualified table name written.
    """
    fq = f"{cfg.catalog}.tpcdi_raw_data.{dataset}{cfg.sf}"
    writer = (df.write
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .format("delta"))
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.saveAsTable(fq)
    log(f"[Delta] wrote {fq}")
    return fq


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
    """Remove all temporary directories (__staging, __tmp, _staging) after bulk copy.

    Walks the output volume's batch directories and deletes any leftover staging
    or temp directories. Also removes the serverless ``_staging/`` directory at
    the volume root where ``disk_cache()`` writes intermediate Parquet files.
    Run at the very end of generation
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

    # Remove serverless materialize-staging dir at the volume root.
    _cleanup(f"{volume_path}/_staging", dbutils)


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
