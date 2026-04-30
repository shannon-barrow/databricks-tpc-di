"""Shared helpers for the Augmented-Incremental file-staging step.

Each per-dataset notebook under ``stage_files/`` builds a temp view that
shapes the data into the format the augmented benchmark expects (cdc_flag,
cdc_dsn, payload columns, plus a date column to partition on). It then
calls ``stage_to_files`` here, which writes the view as a ``|``-delimited
CSV partitioned by the date column under
``{target_dir}/{Dataset}/_pdate={date}/part-*.csv``.

No post-write rename or concat. ``simulate_filedrops`` at benchmark run
time scans each dataset's ``_pdate={batch_date}`` dir directly and moves
part files to the auto-loader watch dir with renamed targets
(``{Dataset}_{i}.txt``). The part files only need to land once, and they
only feed exactly one batch — copying them at stage time AND copying them
again at simulate-filedrop time was wasteful.
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
    """Write ``source_view`` as ``|``-delimited per-date partitioned CSVs.

    Output layout: ``{target_dir}/{base}/_pdate={date}/part-*.csv`` where
    ``base`` is ``filename`` without extension (e.g. ``"Customer"`` for
    ``"Customer.txt"``).

    Args:
        spark:        active SparkSession
        dbutils:      Databricks dbutils (unused here; kept for API compat)
        source_view:  Spark view/table name to read from. Must contain a
                      column named ``date_col`` plus the payload columns
                      in their final output order.
        date_col:     Column to partition on (Spark strips it from output).
        filename:     Original-style filename for the dataset (e.g.
                      ``"Customer.txt"``). The stem is used as the dataset
                      subdir name; simulate_filedrops reuses the full
                      filename pattern when renaming part files at move
                      time so the bronze ``{Dataset}_[0-9]*.txt`` glob
                      matches.
        target_dir:   Final target directory. Per-dataset output lands at
                      ``{target_dir}/{base}/_pdate={date}/part-*.csv``.
        delimiter:    Field delimiter (default ``"|"``).
    """
    base, _ext = os.path.splitext(filename)
    dataset_dir = f"{target_dir.rstrip('/')}/{base}"
    print(f"[stage_to_files] {source_view} → {dataset_dir}")

    # Repartition by date_col before the partitioned-CSV write. partitionBy
    # alone splits the OUTPUT into per-date dirs but doesn't shuffle, so
    # without this every Spark partition emits its share of each date as a
    # separate part file (verified at SF=1000: ~64 part files per date per
    # dataset). After this shuffle, AQE picks a sensible partition count
    # and most dates land in a single Spark partition, yielding ~1 part
    # file per date.
    (spark.table(source_view)
        .repartition(date_col)
        .write
        .mode("overwrite")
        .option("header", "false")
        .option("delimiter", delimiter)
        .partitionBy(date_col)
        .csv(dataset_dir))

    print(f"[stage_to_files] done — {dataset_dir}")
