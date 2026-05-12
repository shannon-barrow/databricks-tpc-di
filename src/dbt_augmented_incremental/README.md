# dbt Augmented Incremental TPC-DI

A dbt port of the [Augmented Incremental TPC-DI](../incremental_batches/augmented_incremental/README.md)
benchmark. Same 365-day daily streaming workload, same source data, same
business logic — expressed as a dbt project so we can compare:

- **dbt vs SDP** on Databricks
- **dbt incremental vs SDP MV/ST** materializations on Databricks
- **Cross-CDW** dbt performance (Databricks DBSQL vs Snowflake / BigQuery / Redshift)

dbt's scope is **per-batch incremental only**. Stage 0 (data generation)
and Stage 1 setup (CLONE staging schema, reset `_dailybatches/`,
populate the historical SCD2 dims/facts) stay in the existing
Databricks notebooks — `setup_dbt.py` (partitioned) or
`setup_dbt_liquid.py` (Liquid) next to this directory. dbt enters when
the daily loop starts.

## Layout

```
dbt_augmented_incremental/
├── dbt_project.yml             # vars + per-folder materialization defaults
├── profiles.yml.template       # databricks + snowflake outputs (template)
├── macros/
│   ├── _helpers.sql            # tgt_db(), since_last_load(), staging_fq()
│   └── read_files_dispatch.sql # adapter dispatch: read_files vs Snowflake stage
└── models/
    ├── sources.yml             # 12 read-only reference tables (cloned from staging)
    ├── bronze/                 # 7 incremental-append models (daily file ingest)
    ├── silver/                 # 4 dim/fact models (3 SCD2 + 1 SCD1)
    └── gold/                   # 4 fact models (append, merge, insert_overwrite)
```

## End-to-end flow

```
augmented_incremental/setup_dbt.py        # partitioned variant
  or  augmented_incremental/setup_dbt_liquid.py   # Liquid variant
     → CLONE staging → run schema, reset _dailybatches, emit batch_date list
   │
   └─ for batch_date in batch_date_ls:
        simulate_filedrops.py --batch_date <date>
        dbt run --vars '{batch_date: <date>, scale_factor, wh_db, ...,
                          use_liquid_clustering: true|false}'
```

The orchestrator is `tools/workflow_builders/augmented_dbt.py` —
parent + child Databricks Jobs that wire setup_dbt(_liquid) +
simulate_filedrops + a Databricks-native dbt task into a
`for_each_task` loop, mirroring the Classic / SDP variants.

## Materialization summary (15 dbt-managed models)

All models use **stock dbt-databricks strategies** — no custom
macros for materialization. The Jinja gate `var('use_liquid_clustering')`
selects partitioned vs Liquid behavior per model where they differ.

| Layer | Strategy | Partitioned default | Liquid (`use_liquid_clustering=true`) |
|---|---|---|---|
| **bronze** (7) | `incremental` `append` | `partition_by` = batch-date column | (no layout config — table pre-created in setup_dbt_liquid with CLUSTER BY + dataSkippingNumIndexedCols=34) |
| **silver** dimcustomer | `incremental` `merge` | (no partition or cluster) | (no partition or cluster) |
| **silver** dimaccount | `incremental` `merge` | (no partition or cluster) | (no partition or cluster) |
| **silver** dimtrade | `incremental` `merge` + `incremental_predicates=['DBT_INTERNAL_DEST.sk_closedateid IS NULL']` | partitioned target | clustered target |
| **silver** factwatches | `incremental` `merge` + `incremental_predicates=['DBT_INTERNAL_DEST.removed = false', 'DBT_INTERNAL_DEST.sk_dateid_dateremoved IS NULL']` | partitioned target | clustered target (second predicate added to leverage Liquid skipping on the cluster column) |
| **gold** factholdings | `incremental` `append` | partitioned target | clustered target |
| **gold** factmarkethistory | `insert_overwrite` + `partition_by='sk_dateid'` (default) → `merge` + `unique_key=['sk_securityid','sk_dateid']` (Liquid) | partitioned: partition replace today's sk_dateid | Liquid: MERGE keyed on (sk_securityid, sk_dateid) |
| **gold** factcashbalances | `insert_overwrite` + `partition_by='sk_dateid'` (default) → `merge` + `unique_key=['sk_accountid','sk_dateid']` (Liquid) | partitioned: partition replace today's sk_dateid | Liquid: MERGE keyed on (sk_accountid, sk_dateid) |
| **gold** currentaccountbalances | `insert_overwrite` (full table replace) | `partition_by='latest_batch'` (boolean) | no partition (Liquid model's CREATE OR REPLACE doesn't keep cluster_by anyway) |

The 12 read-only static + FinWire reference tables are populated by Stage
0 + CLONEd into the run schema by the setup notebook; the dbt project
`source()`s them but doesn't write to them.

## Liquid clustering path

When `use_liquid_clustering=true` is passed to `dbt run --vars`:

1. **Layout is owned by `setup_dbt_liquid.py`, not dbt.** The setup notebook
   pre-creates every Liquid-clustered table with the right `CLUSTER BY` and
   `delta.dataSkippingNumIndexedCols` TBLPROPERTIES. dbt model configs
   in the Liquid branch deliberately omit `liquid_clustered_by` /
   `tblproperties`. This avoids dbt-databricks emitting an
   `ALTER TABLE CLUSTER BY` + `ALTER TABLE SET TBLPROPERTIES` on every
   batch's MERGE/APPEND to "synchronize" model config to table state
   (even when nothing has drifted).
2. **Strategy changes** for `factmarkethistory` and `factcashbalances`:
   the partitioned variant uses `insert_overwrite` + `partition_by`
   to replace today's date partition; the Liquid variant uses `merge` +
   composite `unique_key=(sk_*_id, sk_dateid)` because Liquid tables
   have no partitions to replace.
3. **`currentaccountbalances`** drops `partition_by='latest_batch'`
   entirely (boolean partition isn't useful as a Liquid cluster key,
   and `insert_overwrite` without `partition_by` degrades to
   `CREATE OR REPLACE TABLE AS SELECT` — same logic the model already
   runs).
4. **`factwatches`** adds a second `incremental_predicate` on
   `sk_dateid_dateremoved IS NULL` (redundant with `removed = false`
   but pinned on the Liquid cluster column so Delta data-skipping
   can prune the merge scan).

## Adapter targets

Per-model dispatch hooks handle the differences:

| Concern | Databricks | Snowflake |
|---|---|---|
| Daily file ingest | `read_files('<path>', schema => …)` | `select $1::T from @stage/…/<file>` |
| Materialization | Delta tables w/ partitions or Liquid clustering | Snowflake transient/permanent tables |
| `merge` strategy | native MERGE | native MERGE |
| `insert_overwrite` strategy | partition replace (DBR 17.1+) / TABLE materialization (warehouse) | (no native) |

## Re-run safety

Bronze models filter the source files by `event_dt` / `update_dt` /
`dm_date` matching `{{ var('batch_date') }}`. Each batch processes
exactly that day's data, and the literal `batch_date` gives the
optimizer a constant for downstream join-prune (notably the
`quarter()`/`year()` filter on companyyeareps in factmarkethistory).

Silver/gold models filter the source bronze with `WHERE date_col =
'{{ var('batch_date') }}'`.

## How to run

```bash
pip install dbt-databricks
cd src/dbt_augmented_incremental
cp profiles.yml.template ~/.dbt/profiles.yml   # fill in credentials

# Per batch (orchestrated by tools/workflow_builders/augmented_dbt.py
# in production; for local dev:)
dbt run --vars '{
  batch_date: "2016-07-06",
  scale_factor: "10",
  wh_db: "shannon_barrow_AugmentedIncremental_DBT_Small_Liquid",
  catalog: "main",
  tpcdi_directory: "/Volumes/main/tpcdi_raw_data/tpcdi_volume/",
  use_liquid_clustering: true
}'
```

## What's NOT here

Same as the augmented_incremental README — BatchDate / Prospect / Audit
are intentionally out of scope.
