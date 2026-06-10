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
| **bronze** `account_updates_from_customer` | `incremental` `append` | Derived rows from bronzecustomer 'U' events joined to dimaccount AS-OF batch start. Mirrors SDP's `account_updates_from_customers` flow and Cluster's per-batch notebook — keeps bronzeaccount pure (file drops only); dimaccount UNIONs both at MERGE time |
| **silver** dimcustomer / dimaccount | `incremental` `merge` | Liquid layout inherited from staging on the business key (`customerid` / `accountid`) |
| **silver** dimtrade | `incremental` `merge` + `incremental_predicates=['DBT_INTERNAL_DEST.sk_closedateid IS NULL']` | clustered on `sk_customerid`; the predicate is a logical open-trades prune (not cluster-aligned) |
| **silver** factwatches | `incremental` `merge` + `incremental_predicates=['DBT_INTERNAL_DEST.removed = false', 'DBT_INTERNAL_DEST.sk_dateid_dateremoved IS NULL']` | clustered on `customerid`; both predicates are logical business prunes (not cluster-aligned) |
| **gold** factholdings | `incremental` `append` | Liquid layout inherited from staging on `sk_customerid` |
| **gold** factmarkethistory | `incremental` `merge` + `unique_key=['sk_securityid','sk_dateid']` | Liquid layout inherited from staging on `sk_securityid` |
| **gold** factcashbalances | `incremental` `merge` + `unique_key=['sk_accountid','sk_dateid']` | Liquid layout inherited from staging on `sk_accountid` |
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
- **DEEP CLONE**s the dim/fact tables (incl. factcashbalances +
  bronzedailymarket) from the `tpcdi_incremental_staging_{sf}` schema,
  inheriting that schema's `CLUSTER BY`. The dim/fact tables cluster on
  the **business/entity key** so the background clustering service has
  real recluster work each batch (see "Clustering keys" below):
  `customerid` (dimcustomer), `accountid` (dimaccount), `sk_customerid`
  (dimtrade, factholdings), `customerid` (factwatches), `sk_securityid`
  (factmarkethistory), `sk_accountid` (factcashbalances); `dm_date`
  stays on bronzedailymarket (append-only, date-range read).
- **Pre-creates the 6 bronze tables empty** (account / customer /
  cashtransaction / holdings / trade / watches) with `CLUSTER BY` on
  the per-batch ingest column (update_dt / event_dt) +
  `delta.dataSkippingNumIndexedCols = 34` (bronzecustomer's
  cluster column is past the default 32-col stats window). Bronze stays
  on the date column: it is append-only and read by date-range filters,
  so date clustering both prunes those reads and stays naturally ordered.
- Does NOT pre-create `currentaccountbalances` — dbt's `insert_overwrite`
  without `partition_by` does `CREATE OR REPLACE TABLE AS SELECT` each
  batch, which would wipe any cluster_by we set.

## Clustering keys

The dim/fact tables cluster on their **business/entity key** rather than a
load-date key. This is deliberate for the Predictive-Optimization-vs-
Automatic-Clustering comparison: a date key (the prior choice) stays
naturally time-ordered as each daily batch appends a new date, so the
background maintenance service has almost nothing to recluster. An entity
key scatters each batch's writes across the whole key range (a random
subset of customers/accounts/securities is touched per day), continuously
fragmenting the layout so both Databricks Predictive Optimization and
Snowflake Automatic Clustering have ongoing work — and it matches the
realistic BI access pattern ("all rows for customer/account/security X").

| Table | Cluster key | Why it scatters |
|---|---|---|
| dimcustomer | `customerid` | daily SCD2 versions for a random subset of customers |
| dimaccount | `accountid` | daily SCD2 versions for a random subset of accounts |
| dimtrade | `sk_customerid` | a customer's trades land across all 365 batches |
| factwatches | `customerid` | watch ACTV/CNCL events scatter by customer |
| factmarkethistory | `sk_securityid` | one row per security per day, all securities each batch |
| factcashbalances | `sk_accountid` | balances for the day's touched accounts |
| factholdings | `sk_customerid` | holdings for the day's touched customers |
| bronze* + bronzedailymarket | date (`update_dt`/`event_dt`/`dm_date`) | append-only, date-range reads — kept on date so reads prune and the layout stays ordered |

The **same keys are mirrored on Snowflake** (`snowflake/sf_staging_bootstrap.py::CLUSTER_KEYS`)
so the two engines are compared on an identical layout. The Databricks
keys live in `historical/*.sql` (inherited by every Databricks variant via
DEEP CLONE); change both sides together to keep the comparison fair.

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
