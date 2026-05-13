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
Databricks notebooks — `setup_dbt.py` next to this directory. dbt
enters when the daily loop starts.

## Layout

```
src/incremental_batches/augmented_incremental/dbt/
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

All models use **stock dbt-databricks strategies** — no custom macros
for materialization. Liquid clustering is the project default (the
partition-by approach has been retired across the whole benchmark);
every Liquid-clustered table is **pre-created by `setup_dbt.py`** so
dbt model configs deliberately omit `liquid_clustered_by` /
`tblproperties` ("setup-owns-layout" pattern — see next section).

| Layer | Strategy | Notes |
|---|---|---|
| **bronze** (7) | `incremental` `append` | Tables pre-created in setup_dbt.py with CLUSTER BY + dataSkippingNumIndexedCols=34 |
| **silver** dimcustomer / dimaccount | `incremental` `merge` | (target pre-CTAS'd Liquid by setup_dbt.py on `enddate`) |
| **silver** dimtrade | `incremental` `merge` + `incremental_predicates=['DBT_INTERNAL_DEST.sk_closedateid IS NULL']` | predicate matches the Liquid cluster column for data-skipping prune |
| **silver** factwatches | `incremental` `merge` + `incremental_predicates=['DBT_INTERNAL_DEST.removed = false', 'DBT_INTERNAL_DEST.sk_dateid_dateremoved IS NULL']` | first predicate is the business-logic prune; second is pinned on the Liquid cluster column for skipping |
| **gold** factholdings | `incremental` `append` | target pre-CTAS'd Liquid on `sk_dateid` |
| **gold** factmarkethistory | `incremental` `merge` + `unique_key=['sk_securityid','sk_dateid']` | target pre-CTAS'd Liquid on `sk_dateid` |
| **gold** factcashbalances | `incremental` `merge` + `unique_key=['sk_accountid','sk_dateid']` | target pre-created empty by setup_dbt.py with CLUSTER BY (sk_dateid) |
| **gold** currentaccountbalances | `incremental` `insert_overwrite` (no partition_by) | small running-aggregate; CREATE OR REPLACE TABLE AS SELECT each batch — unclustered |

The 11 read-only static + FinWire reference tables are populated by Stage
0 + CLONEd into the run schema by the setup notebook; the dbt project
`source()`s them but doesn't write to them.

## Setup-owns-layout pattern

`setup_dbt.py` is responsible for the table layout (cluster columns,
tblproperties); dbt is responsible only for writing data. This avoids
dbt-databricks's per-batch `ALTER TABLE CLUSTER BY` /
`ALTER TABLE SET TBLPROPERTIES` (which it issues whenever a model
declares `liquid_clustered_by` / `tblproperties` to "synchronize"
target state to model config, even when nothing has drifted).

Concretely, `setup_dbt.py`:
- **CTAS**s the dim/fact tables with `CLUSTER BY` (matches the SDP
  pipeline's choices: `enddate` for dimcustomer/dimaccount,
  `sk_closedateid` for dimtrade, `sk_dateid_dateremoved` for factwatches,
  `sk_dateid` for factholdings + factmarkethistory + factcashbalances,
  `dm_date` for bronzedailymarket).
- **Pre-creates the 6 bronze tables empty** (account / customer /
  cashtransaction / holdings / trade / watches) with `CLUSTER BY` on
  the per-batch ingest column (update_dt / event_dt) +
  `delta.dataSkippingNumIndexedCols = 34` (bronzecustomer's
  cluster column is past the default 32-col stats window).
- **Pre-creates factcashbalances** empty with `CLUSTER BY (sk_dateid)`
  so the dbt MERGE has a Liquid target to write into.
- Does NOT pre-create `currentaccountbalances` — dbt's `insert_overwrite`
  without `partition_by` does `CREATE OR REPLACE TABLE AS SELECT` each
  batch, which would wipe any cluster_by we set.

## Adapter targets

Per-model dispatch hooks handle the differences:

| Concern | Databricks | Snowflake |
|---|---|---|
| Daily file ingest | `read_files('<path>', schema => …)` | `select $1::T from @stage/…/<file>` |
| Materialization | Delta tables with Liquid clustering | Snowflake transient/permanent tables |
| `merge` strategy | native MERGE | native MERGE |
| `insert_overwrite` strategy | full-table replace via CREATE OR REPLACE TABLE AS SELECT (no partition_by — `currentaccountbalances` model already handles state carryover via the `prior` CTE on `{{ this }}`) | (no native; falls back to TABLE materialization) |

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
cd src/incremental_batches/augmented_incremental/dbt
cp profiles.yml.template ~/.dbt/profiles.yml   # fill in credentials

# Per batch (orchestrated by tools/workflow_builders/augmented_dbt.py
# in production; for local dev:)
dbt run --vars '{
  batch_date: "2016-07-06",
  scale_factor: "10",
  wh_db: "shannon_barrow_AugmentedIncremental_DBT",
  catalog: "main",
  tpcdi_directory: "/Volumes/main/tpcdi_raw_data/tpcdi_volume/"
}'
```

## What's NOT here

Same as the augmented_incremental README — BatchDate / Prospect / Audit
are intentionally out of scope.
