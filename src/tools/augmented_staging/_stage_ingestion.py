"""Shared helpers for the Augmented-Incremental file-staging step.

Each per-dataset notebook under ``stage_files/`` builds a temp view that
shapes the data into the format the augmented benchmark expects (cdc_flag,
cdc_dsn, payload columns, plus a date column to partition on). It then
calls ``stage_to_files`` here, which:

1. Writes the view as a ``|``-delimited CSV table partitioned by the date
   column to a temp staging directory. Spark fans out the writes so this
   step is parallel.
2. Loops the per-date partitions and ``dbutils.fs.mv``s each part file to
   ``{date}/{base}_{i}{ext}`` under the final target dir. Server-side
   rename on UC Volume — no driver streaming, no FUSE EAGAIN.

The per-date target files are what ``simulate_filedrops`` later copies
into the Autoloader watch directory at benchmark run-time.
"""
from __future__ import annotations

import os


def stage_to_files(
    spark,
    dbutils,
    *,
    source_view: str,
    date_col: str,
    filename: str,
    target_dir: str,
    delimiter: str = "|",
) -> None:
    """Write ``source_view`` as ``|``-delimited per-date numbered CSVs.

    Args:
        spark:        active SparkSession
        dbutils:      Databricks dbutils
        source_view:  Spark view/table name to read from. Must contain a
                      column named ``date_col`` to partition on plus the
                      payload columns in their final output order.
        date_col:     Column to partition on. Each distinct value becomes
                      a directory ``{target_dir}/{value}/`` containing one
                      or more numbered files. The column is NOT included
                      in the output (Spark CSV partition-by drops it from
                      data files).
        filename:     Base filename pattern (e.g. ``"DailyMarket.txt"``).
                      Output files are numbered ``DailyMarket_1.txt``,
                      ``DailyMarket_2.txt``, … per date — bronze read_files
                      globs match both unnumbered and ``_[0-9]*`` forms.
        delimiter:    Field delimiter (default ``"|"``).
    """
    # Per-dataset tmp dir so 7 stage_files notebooks running in parallel don't collide on the same path. Filename is the dataset's CSV name (e.g. "DailyMarket.txt") which is unique across the 7 producers, so it makes a safe namespace.
    tmp_dir = f"{target_dir.rstrip('/')}/_tmp_{filename}"
    print(f"[stage_to_files] {source_view} → {target_dir}")
    print(f"  partitioned-CSV staging: {tmp_dir}")

    # Repartition by date_col before the partitioned-CSV write. partitionBy alone splits the OUTPUT into per-date dirs but doesn't shuffle, so without this every Spark partition emits its share of each date as a separate part file (verified at SF=1000: ~64 part files per date per dataset = ~327K total tiny files, which then chokes the driver during concat). After this shuffle, AQE picks a sensible partition count and most dates land in a single Spark partition, yielding ~1 part file per date.
    (spark.table(source_view)
        .repartition(date_col)
        .write
        .mode("overwrite")
        .option("header", "false")
        .option("delimiter", delimiter)
        .partitionBy(date_col)
        .csv(tmp_dir))

    # Spark wrote `{tmp_dir}/{date_col}=YYYY-MM-DD/part-NNNNN-….csv` per
    # date. Server-side rename each part file to its final numbered name
    # at `{target_dir}/{date}/{base}_{i}{ext}`. dbutils.fs.mv stays on the
    # storage backend (no bytes streamed through the driver Python process),
    # which at SF=20000 with ~1GB/day for DailyMarket is the difference
    # between a 3-min and a 40-min stage_files step. Use os.listdir
    # (FUSE-direct) to enumerate the partitions — dbutils.fs.ls is a
    # Spark Connect roundtrip per call (~200ms × 730 calls). spark_runner
    # pre-creates the per-date parent dirs so mv lands in an existing dir.
    import concurrent.futures
    tmp_local = _local(tmp_dir)
    date_partition_names = [n for n in os.listdir(tmp_local)
                            if n.startswith(f"{date_col}=")]
    print(f"  Moving {filename} into {len(date_partition_names)} per-date staging directories")

    base, ext = os.path.splitext(filename)

    def _do_one(part_dir_name):
        date = part_dir_name.split("=", 1)[1]
        part_dir = f"{tmp_dir.rstrip('/')}/{part_dir_name}"
        part_dir_local = f"{tmp_local}/{part_dir_name}"
        parts = sorted(n for n in os.listdir(part_dir_local) if n.startswith("part-"))
        if not parts:
            return 0
        target_subdir = f"{target_dir.rstrip('/')}/{date}"
        # Always number, even single-part dates. Bronze read_files globs
        # `{base}.txt,base_[0-9]*.txt`, so `_1.txt` is matched. Numbering
        # uniformly lets multi-part dates (~1GB/file at SF=20000 when
        # Spark splits a partition) slot in without renaming logic.
        for i, part in enumerate(parts, start=1):
            src = f"{part_dir}/{part}"
            target = f"{target_subdir}/{base}_{i}{ext}"
            dbutils.fs.mv(src, target)
        return len(parts)

    with concurrent.futures.ThreadPoolExecutor(max_workers=32) as pool:
        moved = sum(pool.map(_do_one, date_partition_names))

    dbutils.fs.rm(tmp_dir, recurse=True)
    print(f"[stage_to_files] done — {filename}: {moved} files moved across {len(date_partition_names)} per-date directories")


def _local(path: str) -> str:
    """Strip a ``dbfs:`` scheme so Python file ops see the FUSE mount."""
    return path[5:] if path.startswith("dbfs:") else path
