"""Shared helpers for the Augmented-Incremental file-staging step.

Each per-dataset notebook under ``stage_files/`` builds a temp view that
shapes the data into the format the augmented benchmark expects (cdc_flag,
cdc_dsn, payload columns, plus a date column to partition on). It then
calls ``stage_to_files`` here, which writes the view as a partitioned
table under ``{target_dir}/{dataset}/_pdate={date}/part-*.{file_ext}``
(default writer is CSV).

No post-write rename or concat. ``simulate_filedrops`` at benchmark run
time scans each dataset's ``_pdate={batch_date}`` dir directly and moves
the single part file to the auto-loader watch dir, renamed to
``{Dataset}.{job_file_ext}`` (job default ``txt``). The part files only
need to land once, and they only feed exactly one batch — copying them
at stage time AND copying them again at simulate-filedrop time was
wasteful.
"""
from __future__ import annotations


def stage_to_files(
    spark,
    dbutils,
    *,
    source_view: str,
    date_col: str,
    dataset: str,
    target_dir: str,
    delimiter: str = "|",
    file_ext: str = "csv",
) -> None:
    """Write ``source_view`` as a partitioned table for one dataset.

    Output layout: ``{target_dir}/{dataset}/_pdate={date}/part-*.{file_ext}``.

    Args:
        spark:        active SparkSession
        dbutils:      Databricks dbutils (unused here; kept for API compat)
        source_view:  Spark view/table name to read from. Must contain
                      ``date_col`` plus the payload columns in their final
                      output order.
        date_col:     Column to partition on (Spark strips it from output).
        dataset:      Dataset name (e.g. ``"Trade"``, ``"DailyMarket"``).
                      Used as the staging subdir here AND as the renamed
                      file stem in simulate_filedrops; the latter appends
                      the job ``file_ext`` to produce e.g. ``Trade.txt``.
        target_dir:   Final target directory.
        delimiter:    CSV field delimiter (default ``"|"``). Ignored for
                      non-CSV writers.
        file_ext:     Underlying writer format (default ``"csv"``).
                      ``"csv"`` and ``"parquet"`` are wired up. The
                      benchmark-side ``file_ext`` job param controls the
                      renamed extension at filedrop time, which may differ
                      (e.g. ``txt`` written via the ``csv`` writer).
    """
    dataset_dir = f"{target_dir.rstrip('/')}/{dataset}"
    print(f"Writing {dataset} data out as {file_ext} table at staging directory: {dataset_dir}")

    # Repartition by date_col before partitionBy. partitionBy alone splits
    # the OUTPUT into per-date dirs but doesn't shuffle, so without this
    # every Spark partition emits its share of each date as a separate
    # part file. After this shuffle most dates land in a single Spark
    # partition, yielding ~1 part file per date — which simulate_filedrops
    # asserts on.
    writer = (spark.table(source_view)
        .repartition(date_col)
        .write
        .mode("overwrite")
        .partitionBy(date_col))

    if file_ext == "csv":
        (writer.option("header", "false")
               .option("delimiter", delimiter)
               .csv(dataset_dir))
    elif file_ext == "parquet":
        writer.parquet(dataset_dir)
    else:
        raise ValueError(f"unsupported file_ext: {file_ext!r}")

    print(f"Done writing {dataset} data out as {file_ext} table at: {dataset_dir}")
