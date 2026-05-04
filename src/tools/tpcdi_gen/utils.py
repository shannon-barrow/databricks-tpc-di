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
            if cleanup_info and cleanup_info.get("kind") == "delta":
                fq = cleanup_info.get("fq")
                if fq:
                    try:
                        spark.sql(f"DROP TABLE IF EXISTS {fq}")
                    except Exception:
                        pass
                log(f"[Staging] {cleanup_info.get('label','')}: dropped Delta {fq}")
            elif cleanup_info and cleanup_info.get("kind") == "parquet":
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

# Prefix on the per-call disk_cache Delta names — cleanup_intermediates
# uses it to find every disk_cache temp at end of data_gen.
INTERMEDIATE_DC_PREFIX = "_dc_"


def _sanitize_label(label: str) -> str:
    """Turn a label into a safe SQL identifier component (lowercase + underscores)."""
    slug = re.sub(r"[^a-zA-Z0-9]+", "_", label).strip("_").lower()
    return slug[:40] or "stage"


def disk_cache(df, spark, label: str = "", materialize: bool = True,
               volume_path: str = None, dbutils=None,
               max_records_per_file: int = 0,
               cfg=None):
    """Materialize a DataFrame so subsequent reads skip the upstream DAG.

    Classic: ``persist(DISK_ONLY)`` + ``count()`` — fast, uses local NVMe.
    Serverless: persist is unsupported, so we materialize the DataFrame to
    a Delta table in ``{cfg.catalog}.{cfg.wh_db}_{cfg.sf}_stage._dc_{slug}``
    (matching the convention dw_init.sql uses for benchmark interim
    tables) with no-stats / no-auto-optimize TBLPROPERTIES — these are
    transient. Returns a DataFrame that reads back from the Delta table.
    ``cleanup_intermediates`` (or per-task ``safe_unpersist``) drops the
    table at end of data_gen.

    Falls back to the legacy parquet path under ``{volume_path}/_staging/``
    when ``cfg`` is None (single-task entry-point compatibility) or when
    ``cfg.wh_db`` isn't set.

    ``max_records_per_file`` caps each output file (Spark writes one
    partition per record-cap chunk). Use this to keep a high partition
    count for distribution.

    Pass ``materialize=False`` for derived views whose parent is already
    materialized.

    Returns:
        (materialized_df, cleanup_info or None)

        ``cleanup_info`` is a dict consumed by ``safe_unpersist()``:
          - ``{"kind": "persist", "label": ...}`` on classic
          - ``{"kind": "delta", "fq": "...", "label": ...}`` on serverless
            with cfg (new path)
          - ``{"kind": "parquet", "path": ..., "dbutils": ..., "label": ...}``
            on serverless without cfg (legacy fallback)
          - ``None`` if nothing was materialized (skip paths / failures)
    """
    if _detect_serverless(spark):
        if not materialize:
            return df, None

        # New Delta path: write to {catalog}.{wh_db}_{sf}_stage._dc_{slug}
        wh_db = getattr(cfg, "wh_db", None) if cfg is not None else None
        if wh_db:
            _STAGING_COUNTER[0] += 1
            slug = _sanitize_label(label)
            stage_schema = f"{cfg.catalog}.{wh_db}_{cfg.sf}_stage"
            fq = f"{stage_schema}.{INTERMEDIATE_DC_PREFIX}{_STAGING_COUNTER[0]:03d}_{slug}"
            log(f"[Staging] {label}: serverless cache → Delta {fq}")
            try:
                spark.sql(f"DROP TABLE IF EXISTS {fq}")
                cols_ddl = ", ".join(f"{f.name} {f.dataType.simpleString()}"
                                     for f in df.schema.fields)
                spark.sql(
                    f"CREATE TABLE {fq} ({cols_ddl}) USING DELTA "
                    f"TBLPROPERTIES("
                    f"'delta.dataSkippingNumIndexedCols'=0, "
                    f"'delta.autoOptimize.autoCompact'=False, "
                    f"'delta.autoOptimize.optimizeWrite'=False)"
                )
                writer = df.write.mode("append")
                if max_records_per_file > 0:
                    writer = writer.option("maxRecordsPerFile", max_records_per_file)
                writer.saveAsTable(fq)
                return spark.read.table(fq), {"kind": "delta", "fq": fq, "label": label}
            except Exception as e:
                log(f"[Staging] {label}: Delta path failed ({type(e).__name__}: {e})")
                return df, None

        # Legacy parquet fallback (no cfg.wh_db plumbed through).
        if volume_path is None:
            return df, None
        _STAGING_COUNTER[0] += 1
        slug = _sanitize_label(label)
        staging_path = f"{volume_path}/_staging/{_STAGING_COUNTER[0]:03d}_{slug}"
        log(f"[Staging] {label}: serverless cache → Parquet (legacy path)")
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


# When set True, register_copies_from_staging() returns immediately
# without performing the staging→final copies. Per-task data_gen workflow
# (data_gen_tasks/gen_*.py) sets this to True before calling generate(),
# so generators write only the Spark `__staging` directories. A separate
# `copy_*` task notebook then runs `register_copies_from_staging` (with
# this flag back to False) in parallel with downstream gen tasks.
_DEFER_COPIES = {"enabled": False}


# V7 rename worker count. Matches the spec's `mv` parallelism — ABFS
# rename RPCs are I/O-bound, not CPU-bound, so far more workers than
# driver cores still yields linear speedup until FUSE's per-pod
# concurrency limit kicks in (empirically ~64 on serverless).
_RENAME_WORKERS = 32

# EAGAIN backoff schedule. UC Volumes FUSE returns
# "Resource temporarily unavailable" sporadically when many parallel
# renames hit the same backend; absorb up to ~10s of bursts.
_RENAME_RETRIES = (0.2, 0.5, 1.0, 2.0, 5.0)


def _mv_with_retry(src: str, dst: str) -> str:
    """Single-file ``mv src dst`` with EAGAIN retry. Workers run this
    inside a ThreadPoolExecutor — subprocess releases the GIL during
    the syscall so true parallelism across N workers.
    """
    import subprocess
    last_err = None
    for delay in (0.0, *_RENAME_RETRIES):
        if delay:
            import time as _time
            _time.sleep(delay)
        try:
            subprocess.run(["mv", src, dst], check=True,
                           capture_output=True, text=True)
            return dst
        except subprocess.CalledProcessError as e:
            last_err = e
            stderr = (e.stderr or "")
            transient = ("temporarily unavailable" in stderr
                         or "Resource" in stderr
                         or "EAGAIN" in stderr
                         or "EBUSY" in stderr
                         or "I/O error" in stderr)
            if not transient:
                raise
    raise RuntimeError(f"mv {src} -> {dst} failed after retries: "
                       f"{(last_err.stderr or '').strip() if last_err else ''}")


def register_copies_from_staging(staging_dir: str, final_path: str, dbutils,
                                  start_idx: int = 1):
    """V7: parallel cross-directory ``mv`` from Spark staging into the
    flat Batch* layout DIGen produces.

    Files move from ``{staging_dir}/part-NNNNN-UUID[-cM].csv`` →
    ``{dirname(final_path)}/{base}_K{ext}``. K is a 1-based sequential
    counter (start_idx for chained callers like FINWIRE's CMP/SEC/FIN).

    Same FUSE rename cost per file as V5's same-dir variant
    (UC Volumes ``mv`` is NOT a metadata-only op — backend issues a
    rename RPC either way), but parallelized across
    ``_RENAME_WORKERS`` threads with EAGAIN retry. At SF=20k this drops
    copy_trade from ~25 m to <1 m and eliminates the EAGAIN-induced
    task failures we saw with the serial bash loop.

    The flat layout matches the existing bronze SQL ``fileNamePattern``
    globs (``{Trade.txt,Trade_[0-9]*.txt}``) and avoids needing
    ``recursiveFileLookup`` anywhere.

    Args:
        staging_dir: Spark output directory containing part-* files.
        final_path: Desired final output path — used to derive
            both the base directory (e.g. Batch1/) and the
            (base, ext) for naming. e.g. ``…/Batch1/Trade.txt`` →
            base=``Trade``, ext=``.txt``, target dir = ``…/Batch1``.
        dbutils: Databricks dbutils (signature compat).
        start_idx: First sequential number to use (default 1).

    Returns:
        (list of final paths, next_idx for chained callers).
    """
    if _DEFER_COPIES["enabled"]:
        return [], start_idx

    staging_local = staging_dir.replace("dbfs:", "") if staging_dir.startswith("dbfs:") else staging_dir
    final_local = final_path.replace("dbfs:", "") if final_path.startswith("dbfs:") else final_path
    final_dir = os.path.dirname(final_local)
    filename = os.path.basename(final_local)
    base, ext = os.path.splitext(filename)

    # Enumerate part files. Sort so successive runs / start_idx chains
    # are deterministic. Spark's filenames are
    # `part-NNNNN-UUID[-cM].csv`; sort-by-name is fine.
    try:
        all_entries = os.listdir(staging_local)
    except FileNotFoundError:
        log(f"[Rename] {base}: staging dir gone — nothing to do", "WARN")
        return [], start_idx
    parts = sorted(f for f in all_entries if f.startswith("part-"))
    markers = [f for f in all_entries
               if f == "_SUCCESS" or f.startswith("_started_") or f.startswith("_committed_")]

    # Retry-resume: if a prior attempt already moved some part files to
    # `{final_dir}/{base}_K{ext}`, start past the highest existing K so
    # we don't collide-and-overwrite. Flat-layout files have no
    # semantic ordering — gaps in K are harmless (bronze SQL globs
    # `{base}_[0-9]*` doesn't care).
    resume_start = start_idx
    try:
        existing_ks = []
        for f in os.listdir(final_dir):
            if f.startswith(f"{base}_") and f.endswith(ext):
                k_str = f[len(base) + 1: -len(ext)] if ext else f[len(base) + 1:]
                if k_str.isdigit():
                    existing_ks.append(int(k_str))
        if existing_ks:
            resume_start = max(resume_start, max(existing_ks) + 1)
            if resume_start != start_idx:
                log(f"[Rename] {base}: resume — {len(existing_ks)} {base}_K{ext} "
                    f"already in {final_dir}, starting at K={resume_start}", "WARN")
    except FileNotFoundError:
        pass

    # Build (src, dst) pairs at flat Batch* layout.
    moves = []
    next_idx = resume_start
    for f in parts:
        src = f"{staging_local}/{f}"
        dst = f"{final_dir}/{base}_{next_idx}{ext}"
        moves.append((src, dst))
        next_idx += 1

    log(f"[Rename] {base}: {len(moves)} files, "
        f"{staging_local} → {final_dir}/{base}_K{ext} "
        f"(start={resume_start}, workers={_RENAME_WORKERS})")

    # Parallel mv with retry. ThreadPoolExecutor saturates FUSE without
    # building a Spark job (each mv is one syscall, ~250 ms latency
    # bound). Failures bubble up so the caller can see them.
    targets = []
    if moves:
        with concurrent.futures.ThreadPoolExecutor(max_workers=_RENAME_WORKERS) as executor:
            futures = [executor.submit(_mv_with_retry, src, dst) for src, dst in moves]
            for f in concurrent.futures.as_completed(futures):
                targets.append(f.result())

    # Drop Spark markers + the (now-empty) staging dir. Cross-directory
    # move means callers no longer need the staging dir to live on
    # post-rename — bronze ingest reads from the flat Batch* root.
    for m in markers:
        try:
            os.remove(f"{staging_local}/{m}")
        except OSError:
            pass
    try:
        # rmdir only succeeds if the dir is empty — perfect safety net
        # against an edge case where some part-* file failed to move.
        os.rmdir(staging_local)
    except OSError:
        # Likely non-empty (some files left) or a chained caller will
        # add more; leave it for cleanup_intermediates / next start_idx.
        pass

    log(f"[Rename] {base}: done, {len(targets)} files (next_idx={next_idx})")
    return targets, next_idx


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
    # perf/v5: staging dir IS the final output dir — Spark writes part files
    # here and register_copies_from_staging renames them in place to
    # {base}_K{ext}. No cross-dir copy. Drop the ".txt__staging" suffix the
    # legacy code used; just use the dataset name (e.g. "Batch1/Trade").
    staging_dir = os.path.splitext(path)[0]
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


def safe_conf_set(spark, key: str, value) -> bool:
    """Set a Spark conf, swallowing CONFIG_NOT_AVAILABLE on serverless.

    Serverless allowlists only a small set of confs for ``spark.conf.set()``
    (see databricks.com/aws/en/spark/conf). Performance-tuning confs like
    ``spark.sql.autoBroadcastJoinThreshold`` aren't on the list and raise
    ``CONFIG_NOT_AVAILABLE`` when set. The serverless runtime picks sane
    defaults for these anyway, so silently dropping the set lets the same
    code run on classic + serverless without branching.

    Returns True if the conf was applied, False if it was dropped.
    """
    try:
        spark.conf.set(key, value)
        return True
    except Exception as e:
        log(f"[Init] spark.conf.set({key}) skipped: {type(e).__name__}", "DEBUG")
        return False


def write_delta(df: DataFrame, *, cfg, dataset: str,
                partition_cols: list = None) -> str:
    """Write ``df`` as a Delta table at ``{catalog}.tpcdi_raw_data.{dataset}{sf}``.

    Used by the augmented-incremental staging mode in place of file output.
    The downstream ``stage_files`` / ``stage_tables`` tasks read from these
    temp tables; ``cleanup_stage0`` drops them once per-day CSVs and the
    ``tpcdi_incremental_staging_{sf}`` schema are populated.

    Every temp table gets a ``cdc_dsn BIGINT GENERATED ALWAYS AS IDENTITY``
    column appended. stage_files notebooks SELECT it directly instead of
    paying for a row_number() OVER (entire dataset) pass.

    Returns the fully-qualified table name written.
    """
    fq = f"{cfg.catalog}.tpcdi_raw_data.{dataset}{cfg.sf}"

    # DDL-first so Delta auto-assigns cdc_dsn during the INSERT. Drop+recreate so the IDENTITY sequence restarts at 0 on every run.
    spark = df.sparkSession
    spark.sql(f"DROP TABLE IF EXISTS {fq}")

    cols_ddl_parts = [f"{f.name} {f.dataType.simpleString()}" for f in df.schema.fields]
    cols_ddl_parts.append(
        "cdc_dsn BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 0 INCREMENT BY 1)"
    )
    cols_ddl = ", ".join(cols_ddl_parts)
    partition_clause = (f"PARTITIONED BY ({', '.join(partition_cols)})"
                        if partition_cols else "")
    spark.sql(f"CREATE TABLE {fq} ({cols_ddl}) USING DELTA {partition_clause}")

    src_view = f"_wd_src_{dataset}"
    df.createOrReplaceTempView(src_view)
    col_list = ", ".join(f.name for f in df.schema.fields)
    spark.sql(f"INSERT INTO {fq} ({col_list}) SELECT {col_list} FROM {src_view}")
    spark.catalog.dropTempView(src_view)

    log(f"[Delta] wrote {fq} (with IDENTITY cdc_dsn)")
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
