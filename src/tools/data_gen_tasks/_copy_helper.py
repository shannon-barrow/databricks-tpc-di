"""Shared helper for the per-dataset copy_* notebooks.

perf/v7 — Parallel cross-directory ``mv`` to flat Batch* layout. Each
``copy_*`` task iterates Batch1/2/3 looking for ``{batch_path}/{base}``
staging directories (Spark's output) and calls
``register_copies_from_staging`` which mv's part files concurrently
(ThreadPoolExecutor, 32 workers, EAGAIN-retry) into
``{batch_path}/{base}_K{ext}``. The (now empty) staging dir is
removed at the end of a successful pass so retries cleanly skip.

DIGen-compatible flat layout — bronze SQL ``fileNamePattern`` globs
match the renamed files at the Batch* root (no recursive scan needed).

Augmented mode skips Batch2/3 generation and writes Delta directly, so
the copy_* tasks for Customer/Account/Trade/CashTransaction/
HoldingHistory/DailyMarket/WatchHistory have nothing to do — they
self-skip cleanly when no staging dir is present.
"""
from __future__ import annotations


def copy_dataset(*, cfg, dbutils, filenames, num_batches: int = 3) -> int:
    """For each ``filename`` in ``filenames`` × each batch 1..num_batches,
    rename the Spark part-files in place. Returns total file count.

    ``filenames`` may be a single name (``"HR.csv"``) or a list (e.g.
    ``["Trade.txt", "TradeHistory.txt", "CashTransaction.txt", "HoldingHistory.txt"]``
    for the Trade family which all share the in-memory trade_df).
    """
    from tpcdi_gen.utils import register_copies_from_staging

    if isinstance(filenames, str):
        filenames = [filenames]

    import os
    n_total = 0
    for batch in range(1, num_batches + 1):
        bp = cfg.batch_path(batch)
        for fname in filenames:
            base, ext = os.path.splitext(fname)
            # perf/v7: Spark writes to {batch_path}/{base} subdir.
            # register_copies_from_staging mv's the part files in
            # parallel into {batch_path}/{base}_K{ext} at the Batch*
            # root and removes the (now empty) staging dir. A retry
            # of this task with the staging dir gone is benign —
            # treated as "already done" and skipped.
            staging = f"{bp}/{base}"
            final = f"{bp}/{fname}"
            try:
                dbutils.fs.ls(staging)  # probe
            except Exception:
                # Either nothing staged (augmented mode skipping
                # Batch2/3) or a prior attempt fully drained the dir.
                continue
            print(f"  {staging}: parallel mv → {bp}/{base}_K{ext}")
            targets, _ = register_copies_from_staging(staging, final, dbutils)
            n_total += len(targets)
    return n_total
