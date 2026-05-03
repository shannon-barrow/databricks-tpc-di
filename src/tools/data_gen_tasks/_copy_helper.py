"""Shared helper for the per-dataset copy_* notebooks.

perf/v5 — In-place shell rename approach: each ``copy_*`` task iterates
Batch1/2/3 looking for ``{batch_path}/{filename}__staging`` directories
and calls ``register_copies_from_staging`` which renames the Spark
``part-NNNNN`` files in place to ``{base}_K.{ext}``. The staging dir
remains as the output dir (bronze ingest must scan it recursively).

No post-rename ``_cleanup`` is performed — the renamed files now live
INSIDE the ``__staging`` dir, so wiping that dir would destroy the
output. Spark marker files (``_SUCCESS`` / ``_started_*`` /
``_committed_*``) are deleted inside ``register_copies_from_staging``'s
shell command.

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
            # perf/v5: staging dir is the dataset name (e.g. Batch1/Trade)
            # rather than Batch1/Trade.txt__staging. Spark writes part-* in
            # there; register_copies_from_staging renames them in place to
            # {base}_K{ext}.
            staging = f"{bp}/{base}"
            final = f"{bp}/{fname}"
            try:
                dbutils.fs.ls(staging)  # probe
            except Exception:
                continue  # nothing staged for this batch (e.g. augmented mode)
            print(f"  {staging}: in-place rename → {base}_K{ext}")
            targets, _ = register_copies_from_staging(staging, final, dbutils)
            n_total += len(targets)

    # NOTE: Do NOT _cleanup the staging dirs — renamed files now live there.
    # Bronze ingest must scan {batch_path}/ recursively to pick them up.
    return n_total
