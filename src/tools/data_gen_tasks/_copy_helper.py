"""Shared helper for the per-dataset copy_* notebooks.

Each ``copy_*`` task (in standard mode) iterates Batch1/2/3 looking for
``{batch_path}/{filename}__staging`` directories and runs
``register_copies_from_staging`` synchronously to produce the final
``{filename}_N.{ext}`` files. Then it ``wait_for_background_copies()``
so any large-part daemon threads complete cleanly before the task
exits, and ``_cleanup`` to remove the now-empty staging dirs.

Augmented mode skips Batch2/3 generation and writes Delta directly, so
the copy_* tasks for Customer/Account/Trade/CashTransaction/
HoldingHistory/DailyMarket/WatchHistory have nothing to do — they
self-skip cleanly when no staging dir is present.
"""
from __future__ import annotations


def copy_dataset(*, cfg, dbutils, filenames, num_batches: int = 3) -> int:
    """For each ``filename`` in ``filenames`` × each batch 1..num_batches,
    register & wait on the staging→final copy. Returns total file count.

    ``filenames`` may be a single name (``"HR.csv"``) or a list (e.g.
    ``["Trade.txt", "TradeHistory.txt", "CashTransaction.txt", "HoldingHistory.txt"]``
    for the Trade family which all share the in-memory trade_df).
    """
    from tpcdi_gen.utils import (
        register_copies_from_staging, wait_for_background_copies, _cleanup,
    )

    if isinstance(filenames, str):
        filenames = [filenames]

    n_total = 0
    cleaned = []
    for batch in range(1, num_batches + 1):
        bp = cfg.batch_path(batch)
        for fname in filenames:
            staging = f"{bp}/{fname}__staging"
            final = f"{bp}/{fname}"
            try:
                # Probe: does the staging dir exist?
                dbutils.fs.ls(staging)
            except Exception:
                continue  # nothing staged for this batch (e.g. augmented mode)
            print(f"  {staging} → {final}_N")
            targets, _ = register_copies_from_staging(staging, final, dbutils)
            n_total += len(targets)
            cleaned.append(staging)

    wait_for_background_copies()
    for s in cleaned:
        _cleanup(s, dbutils)
    return n_total
