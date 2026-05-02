"""Per-dataset data_gen task notebooks.

Each notebook in this package is a single workflow task that generates one
dataset (or one logical unit). The DAG is wired up in
``workflow_builders/augmented_staging.py`` (and later, ``datagen_spark.py``).

A failed task can be repair-run independently because:
- Cross-task intermediates (`_gen_brokers`, `_gen_symbols`,
  `_gen_customer_dates`) live as Delta tables in
  `{catalog}.{wh_db}_{scale_factor}_stage` — same convention as
  `dw_init.sql` uses for benchmark interim tables.
- Each gen task self-skips when its output Delta is already complete
  and ``regenerate_data=NO``.
- ``cleanup_intermediates`` runs ``ALL_SUCCESS`` so failed runs leave
  the intermediates in place for the next attempt.

Shared bootstrap (cfg, dictionaries, sys.path, helpers) lives in
``_shared.py`` so each task notebook is a thin wrapper around the
existing ``tpcdi_gen`` generator function.
"""
