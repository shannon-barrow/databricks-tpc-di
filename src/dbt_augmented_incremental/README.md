# dbt Augmented Incremental TPC-DI

A dbt port of the [Augmented Incremental TPC-DI](../incremental_batches/augmented_incremental/README.md)
benchmark. Same 730-day daily streaming workload, same source data, same
business logic — expressed as a dbt project so we can compare:

- **dbt vs SDP** on Databricks (R7 vs R1 in the product benchmark plan)
- **dbt incremental vs SDP MV/ST** materializations on Databricks (R8 vs R1)
- **Cross-CDW** dbt performance (R1 / R2 / R3 / R4 — Databricks vs Snowflake / BigQuery / Redshift)

## Layout

```
dbt_augmented_incremental/
├── dbt_project.yml
├── packages.yml
├── profiles.yml.template
├── macros/
│   ├── _helpers.sql              # tgt_db(), fq(), since_last_load(), etc.
│   ├── clone_staging.sql         # one-shot clone of the per-SF staging schema
│   └── read_files_dispatch.sql   # adapter dispatch: Databricks read_files vs Snowflake stage
├── models/
│   ├── 10_static/                # 7 static dims (table)
│   ├── 20_finwire/               # 4 FinWire-derived (table)
│   ├── 30_historical/            # 6 historical SCD2 builds (table)
│   ├── 40_bronze/                # 7 daily ingestion (incremental, append)
│   ├── 50_silver/                # 4 SCD2/SCD1 dims (incremental, merge)
│   └── 60_gold/                  # 4 fact tables (mix of strategies)
└── driver/
    └── dbt_runner.py             # per-batch loop wrapper (Databricks notebook)
```

## End-to-end flow

Stage 0 (data prep) is unchanged — uses the same `augmented_staging` job to
build the per-SF shared staging schema and the partitioned-CSV staging tree
under `_staging/sf={sf}/{Dataset}/_pdate={date}/`.

Stage 1 (per-batch dbt loop) replaces the SDP / Classic notebooks:

```
setup.py            (reuses augmented_incremental/setup.py — clones staging, resets dirs)
   │
   ├─ run-once: dbt run-operation clone_staging  (or: keep setup.py's clone)
   ├─ run-once: dbt run --select 10_static 20_finwire 30_historical
   │
   └─ for batch_date in dates_730:
        simulate_filedrops.py --batch_date {batch_date}
        dbt run --select 40_bronze 50_silver 60_gold --vars '{batch_date: {batch_date}}'
```

Per-batch concurrency stays at the dbt thread level (`threads: 8` default)
plus dbt's parallel model-graph execution.

## Adapter targets

The same project graph runs against both adapters. Per-model dispatch
hooks (`read_files_dispatch`, etc.) handle the differences:

| Concern | Databricks | Snowflake |
|---|---|---|
| Daily file ingest | `read_files('<path>', schema => ...)` | `select $1::T from @stage/.../<file>` |
| Materialization | Delta tables w/ partitions | Snowflake transient/permanent tables |
| `merge` strategy | native MERGE | native MERGE |
| `delete+insert` strategy | native | native |
| `insert_overwrite` strategy | dynamic partition overwrite | (no native) |
| `microbatch` strategy | partition by date | partition by date |

## Re-run safety

Every bronze model uses the `since_last_load(date_col)` macro:

```sql
{{ since_last_load('update_dt') }}
```

This expands to a `WHERE date_col > coalesce(MAX(date_col), '1900-01-01')`
guard inside `is_incremental()` blocks — same idempotency semantics as
the Auto Loader checkpoint in the Classic build, just expressed in SQL.

## How to run

```bash
# Install dbt-databricks (or dbt-snowflake) and packages
pip install dbt-databricks dbt-snowflake
cd src/dbt_augmented_incremental
cp profiles.yml.template ~/.dbt/profiles.yml  # fill in credentials

dbt deps

# Stage 0 + clone (one-time)
dbt run-operation clone_staging --vars '{wh_db: shannon_barrow, scale_factor: "10"}'
dbt run --select 10_static 20_finwire 30_historical \
  --vars '{wh_db: shannon_barrow, scale_factor: "10"}'

# Per batch (orchestrated by driver/dbt_runner.py)
dbt run --select 40_bronze 50_silver 60_gold \
  --vars '{wh_db: shannon_barrow, scale_factor: "10", batch_date: "2015-07-06"}'
```

## What's NOT here

Same as the augmented_incremental README — BatchDate / Prospect / Audit
are intentionally out of scope.
