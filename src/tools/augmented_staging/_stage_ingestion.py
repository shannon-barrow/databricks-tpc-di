"""Shared helpers for the Augmented-Incremental file-staging step.

Each per-dataset notebook under ``stage_files/`` builds a temp view that
shapes the data into the format the augmented benchmark expects (cdc_flag,
cdc_dsn, payload columns, plus a date column to partition on). It then
calls ``stage_to_files`` here, which:

1. Writes the view as a ``|``-delimited CSV table partitioned by the date
   column to a temp staging directory. Spark fans out the writes so this
   step is parallel.
2. Loops the per-date partitions and concatenates the part files within
   each into a single ``{date}/{filename}`` under the final target dir.
   Concat uses the same FUSE-hardened ``shutil.copyfileobj`` + retry
   pattern the Spark generator uses (UC Volume FUSE returns EAGAIN under
   heavy parallel I/O at high SF).

The per-date target file is what ``simulate_filedrops`` later copies into
the Autoloader watch directory at benchmark run-time.
"""
from __future__ import annotations

import os
import shutil
import time


def stage_to_files(
    spark,
    dbutils,
    *,
    source_view: str,
    date_col: str,
    filename: str,
    target_dir: str,
    delimiter: str = "|",
    max_retries: int = 10,
) -> None:
    """Write ``source_view`` as ``|``-delimited per-date single-file CSVs.

    Args:
        spark:        active SparkSession
        dbutils:      Databricks dbutils
        source_view:  Spark view/table name to read from. Must contain a
                      column named ``date_col`` to partition on plus the
                      payload columns in their final output order.
        date_col:     Column to partition on. Each distinct value becomes
                      a directory ``{target_dir}/{value}/`` with a single
                      ``filename`` inside. The column is NOT included in
                      the output (Spark CSV partition-by drops it from
                      data files).
        filename:     Final filename within each date directory
                      (e.g. ``"Customer.txt"``).
        target_dir:   Final target directory. Per-date file lands at
                      ``{target_dir}/{date}/{filename}``.
        delimiter:    Field delimiter (default ``"|"``).
        max_retries:  Per-source open retry budget for FUSE EAGAIN.
    """
    # Per-dataset tmp dir so 7 stage_files notebooks running in parallel don't collide on the same path. Filename is the dataset's CSV name (e.g. "DailyMarket.txt") which is unique across the 7 producers, so it makes a safe namespace.
    tmp_dir = f"{target_dir.rstrip('/')}/_tmp_{filename}"
    print(f"[stage_to_files] {source_view} → {target_dir}")
    print(f"  partitioned-CSV staging: {tmp_dir}")

    # Repartition by date_col before the write so each distinct date lands in a single Spark partition. partitionBy alone splits OUTPUT files by date but doesn't shuffle, so without this every Spark partition emits its share of each date as a separate part file. AQE auto-tunes the actual shuffle-partition count. Verified faster end-to-end at SF=10 (run 95882812237571: 7×~250s vs the no-repartition baseline that emitted thousands of tiny files per date).
    (spark.table(source_view)
        .repartition(date_col)
        .write
        .mode("overwrite")
        .option("header", "false")
        .option("delimiter", delimiter)
        .partitionBy(date_col)
        .csv(tmp_dir))

    # Spark wrote `{tmp_dir}/{date_col}=YYYY-MM-DD/part-NNNNN-….csv` per
    # date partition. Concat part files into a single per-date file at
    # `{target_dir}/{date}/{filename}`. Per-date work is independent
    # (different output paths) so we fan out across a thread pool. Use
    # os.listdir (FUSE-direct) instead of dbutils.fs.ls — the latter is a
    # Spark Connect roundtrip per call (~200ms × 730 calls dominated wall
    # clock at SF=10). spark_runner pre-creates the per-date parent dirs
    # so the makedirs() inside _concat_with_retry is a fast no-op.
    import concurrent.futures
    tmp_local = _local(tmp_dir)
    date_partition_names = [n for n in os.listdir(tmp_local)
                            if n.startswith(f"{date_col}=")]
    print(f"  Copying {filename} into {len(date_partition_names)} per-date staging directories")

    def _do_one(part_dir_name):
        date = part_dir_name.split("=", 1)[1]
        part_dir_local = f"{tmp_local}/{part_dir_name}"
        parts = sorted(n for n in os.listdir(part_dir_local) if n.startswith("part-"))
        if not parts:
            return 0
        date_dir_local = _local(f"{target_dir.rstrip('/')}/{date}")
        os.makedirs(date_dir_local, exist_ok=True)
        if len(parts) == 1:
            # Single Spark partition → keep the canonical filename (e.g. Customer.txt).
            try:
                os.rename(f"{part_dir_local}/{parts[0]}",
                          f"{date_dir_local}/{filename}")
            except OSError:
                _concat_with_retry(
                    sources=[f"{part_dir_local}/{parts[0]}"],
                    target=f"{date_dir_local}/{filename}",
                    max_retries=max_retries,
                )
        else:
            # Multi-part → number the files (Customer_1.txt, Customer_2.txt, ...) instead of byte-copy concat. Bronze ingest's glob `{Customer.txt,Customer_[0-9]*.txt}` already matches this shape, and downstream simulate_filedrops just lists everything in the date dir.
            base, ext = os.path.splitext(filename)
            for i, p in enumerate(parts, start=1):
                try:
                    os.rename(f"{part_dir_local}/{p}",
                              f"{date_dir_local}/{base}_{i}{ext}")
                except OSError:
                    _concat_with_retry(
                        sources=[f"{part_dir_local}/{p}"],
                        target=f"{date_dir_local}/{base}_{i}{ext}",
                        max_retries=max_retries,
                    )
        return 1

    with concurrent.futures.ThreadPoolExecutor(max_workers=32) as pool:
        written = sum(pool.map(_do_one, date_partition_names))

    dbutils.fs.rm(tmp_dir, recurse=True)
    print(f"[stage_to_files] done — {filename}: {written}/{len(date_partition_names)} per-date directories populated")


def _local(path: str) -> str:
    """Strip a ``dbfs:`` scheme so Python file ops see the FUSE mount."""
    return path[5:] if path.startswith("dbfs:") else path


def _concat_with_retry(*, sources, target: str, max_retries: int) -> None:
    """Concat ``sources`` into ``target`` with FUSE-EAGAIN retry per source.

    UC Volume FUSE returns ``Resource temporarily unavailable`` under
    heavy parallel I/O. Retry each source open with exponential backoff;
    the destination file is opened once and written to sequentially.
    """
    with open(target, "wb") as dst:
        for src in sources:
            last_err = None
            for attempt in range(max_retries):
                try:
                    with open(src, "rb") as s:
                        shutil.copyfileobj(s, dst, length=4 * 1024 * 1024)
                    break
                except OSError as e:
                    last_err = e
                    time.sleep(min(30, 0.5 * (2 ** attempt)))
            else:
                raise last_err
