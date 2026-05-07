# dbt Augmented Incremental TPC-DI

A dbt port of the [Augmented Incremental TPC-DI](../incremental_batches/augmented_incremental/README.md)
benchmark. Same 730-day daily streaming workload, same source data, same
business logic — expressed as a dbt project so we can compare:

- **dbt vs SDP** on Databricks
- **dbt incremental vs SDP MV/ST** materializations on Databricks
- **Cross-CDW** dbt performance (Databricks DBSQL vs Snowflake / BigQuery / Redshift)

dbt's scope is **per-batch incremental only**. Stage 0 (data generation)
and Stage 1 setup (CLONE staging schema, reset `_dailybatches/`,
populate the historical SCD2 dims/facts) stay in the existing
Databricks notebooks — `setup_dbt.py` next to this directory. dbt
enters when the daily loop starts.

## Layout

```
dbt_augmented_incremental/
├── dbt_project.yml             # vars + per-folder materialization defaults
├── profiles.yml.template       # databricks + snowflake outputs (template)
├── macros/
│   ├── _helpers.sql            # tgt_db(), since_last_load(), staging_fq()
│   ├── read_files_dispatch.sql # adapter dispatch: read_files vs Snowflake stage
│   ├── scd2_merge.sql          # custom incremental_strategy='scd2'
│   └── replace_using.sql       # custom incremental_strategy='replace_using'
└── models/
    ├── sources.yml             # 12 read-only reference tables (cloned from staging)
    ├── bronze/                 # 7 incremental-append models (daily file ingest)
    ├── silver/                 # 4 dim/fact models (3 SCD2 + 1 SCD1)
    └── gold/                   # 4 fact models (append, replace_using, insert_overwrite)
```

## End-to-end flow

```
augmented_incremental/setup_dbt.py    # CLONE staging → run schema, reset
                                      # _dailybatches, emit batch_date list
   │
   └─ for batch_date in batch_date_ls:
        simulate_filedrops.py --batch_date <date>
        dbt run --vars '{batch_date: <date>, scale_factor, wh_db, ...}'
```

The orchestrator is `tools/workflow_builders/augmented_dbt.py` —
parent + child Databricks Jobs that wire setup_dbt + simulate_filedrops
+ a Databricks-native dbt task into a `for_each_task` loop, mirroring
the Classic / SDP variants.

## Materialization summary (15 dbt-managed models)

| Layer | Strategy | Models |
|---|---|---|
| **bronze** (7) | `incremental` `append` | 7× daily ingestion: bronzecustomer/account/cashtransaction/dailymarket/holdings/trade/watches |
| **silver** (4) | `incremental` `scd2` (custom) | dimcustomer, dimaccount |
| | `incremental` `merge` + `incremental_predicates` | dimtrade (open-trades partition prune), factwatches (`!removed` prune) |
| **gold** (4) | `incremental` `append` | factholdings |
| | `incremental` `insert_overwrite` partition_by latest_batch | currentaccountbalances |
| | `incremental` `replace_using` (custom) | factmarkethistory (sk_securityid, sk_dateid), factcashbalances (sk_accountid, sk_dateid) |

The 11 read-only static + FinWire reference tables are populated by Stage
0 + CLONEd into the run schema by `setup_dbt.py`; the dbt project
`source()`s them but doesn't write to them.

## Adapter targets

Per-model dispatch hooks handle the differences:

| Concern | Databricks | Snowflake |
|---|---|---|
| Daily file ingest | `read_files('<path>', schema => …)` | `select $1::T from @stage/…/<file>` |
| Materialization | Delta tables w/ partitions | Snowflake transient/permanent tables |
| `merge` strategy | native MERGE | native MERGE |
| `replace_using` strategy | Delta `INSERT REPLACE USING` | n/a (custom macro raises error) |
| `insert_overwrite` strategy | dynamic partition overwrite (cluster) / TABLE materialization (warehouse) | (no native) |

## Re-run safety

Every bronze model uses the `since_last_load(date_col)` macro:

```sql
{{ since_last_load('update_dt') }}
```

Inside `is_incremental()` it expands to `WHERE date_col > coalesce(MAX(date_col), '1900-01-01')` — same idempotency semantics as the Auto Loader checkpoint in the Classic build, expressed in SQL.

Silver/gold models filter the source bronze with `WHERE date_col = '{{ var('batch_date') }}'`. Each batch processes exactly that day's data, and the literal `batch_date` gives the optimizer a constant for downstream join-prune (notably the `quarter()`/`year()` filter on companyyeareps that gives the dbt build a meaningful perf edge over Classic's streaming version, where the batch date is implicit in the streaming checkpoint and not a SQL-visible literal).

## How to run

```bash
pip install dbt-databricks
cd src/dbt_augmented_incremental
cp profiles.yml.template ~/.dbt/profiles.yml   # fill in credentials

# Per batch (orchestrated by tools/workflow_builders/augmented_dbt.py
# in production; for local dev:)
dbt run --vars '{
  batch_date: "2015-07-06",
  scale_factor: "10",
  wh_db: "shannon_barrow_AugmentedIncremental_DBT",
  catalog: "main",
  tpcdi_directory: "/Volumes/main/tpcdi_raw_data/tpcdi_volume/"
}'
```

## What's NOT here

Same as the augmented_incremental README — BatchDate / Prospect / Audit
are intentionally out of scope.
