# Augmented Incremental TPC-DI

A 730-day daily-streaming reshaping of the standard TPC-DI benchmark. Splits
the bulk historical batches into per-day file drops and runs the pipeline
incrementally — exercising CDC + SCD2 + cumulative compaction the way a
production daily pipeline does.

> **Why?** Standard TPC-DI is heavily biased toward a single bulk historical
> load — Batch 2 and Batch 3 together are <0.5% of the total benchmark data,
> so most runs effectively measure a single full load. The Augmented
> Incremental variant keeps the official business rules, table outputs,
> and statistical fidelity of TPC-DI while reshaping the runtime profile to
> match modern incremental pipelines.

The original design doc is on [Google Docs][design-doc]. Architecture has
evolved since the doc was written (notably: the Spark data generator
replaced the manual per-table prep notebooks). This README is the current
source of truth.

[design-doc]: https://docs.google.com/document/d/1C9QgjnaR5ZtKXTbyT_6TigWq7Y_Ewtz1tkbXC5WNQPQ/edit

---

## End-to-end flow

```
┌─ Stage 0: Data prep (one-time per SF) ─────────────────────────────┐
│  Driver → augmented_staging job:                                   │
│    1. spark_runner (augmented mode) writes 7 datasets to Delta     │
│       at {catalog}.tpcdi_raw_data.{dataset}{sf}, with stg_target   │
│       column splitting rows into 'tables' (< 2015-07-06) and       │
│       'files' (the 730-day window).                                │
│    2. stage_tables: builds the shared staging schema               │
│       tpcdi_incremental_staging_{sf} from stg_target='tables'      │
│       (DimCustomer/DimAccount/DimTrade Historical, FactCash/       │
│       Holdings/WatchesHistorical, CompanyYearEPS, plus the 7       │
│       static dim tables ingested from {volume}/Batch1/).           │
│    3. stage_files: 7 notebooks each filter their dataset's Delta   │
│       table to stg_target='files' and write Spark-native           │
│       partitioned-CSV at {volume}/_staging/sf={sf}/{Dataset}/      │
│       _pdate={date}/part-*.csv.                                    │
│    4. cleanup_stage0: drops the 7 temp Delta tables and removes    │
│       the spark-gen Batch1/2/3 leftovers.                          │
└────────────────────────────────────────────────────────────────────┘
              │
              ▼
┌─ Stage 1: Benchmark (one parent job per run) ──────────────────────┐
│  Driver → augmented_classic OR augmented_sdp parent job:           │
│    - setup.py: CLONE staging schema → per-user run schema; reset   │
│      the autoloader watch dir + checkpoints; emit the 730-day      │
│      date list as a job task value.                                │
│    - Loop (for_each_task over the 730 dates), each iteration:      │
│        a. simulate_filedrops.py: dbutils.fs.cp the day's per-      │
│           dataset part files from _staging into _dailybatches.     │
│        b. bronze ingest (Auto Loader) for all 7 datasets.          │
│        c. silver/gold MERGEs (DimCustomer/Account/Trade            │
│           Incremental, FactCashBalances/Watches/Holdings/          │
│           MarketHistory Incremental).                              │
│    - cleanup (gated by delete_when_finished_TRUE_FALSE).           │
└────────────────────────────────────────────────────────────────────┘
```

The Stage 0 work is implemented under `src/tools/augmented_staging/` and
built by `src/tools/workflow_builders/augmented_staging.py`. Stage 1 lives
in this directory and is built by `augmented_classic.py` / `augmented_sdp.py`.

---

## Directory layout

```
augmented_incremental/
├── setup.py                       # Stage 1 setup: CLONE staging → run schema, reset dirs
├── teardown.py                    # Drop the run's schema and wipe its checkpoints
├── create_dates_loop.py           # Emits the 730-date list as a job task value
├── simulate_filedrops.py          # Per-batch: copy day's part files into autoloader watch dir
├── bronze/
│   ├── ingest_bronze.py           # Auto Loader stream for the 7 raw datasets
│   └── account_updates_from_customer.py  # DimCustomer events that also touch DimAccount
├── historical/                    # Pre-2015-07-06 SCD2 builds (read from spark-gen Delta)
│   ├── DimCustomerHistorical.sql
│   ├── DimAccountHistorical.sql
│   ├── DimTradeHistorical.sql
│   ├── FactCashBalancesHistorical.sql
│   ├── FactHoldingsHistorical.sql
│   ├── FactWatchesHistorical.sql
│   └── CompanyYearEPS.sql
├── incremental/                   # Per-batch SCD2 / aggregate MERGEs
│   ├── DimCustomer Incremental.py
│   ├── DimAccount Incremental.py
│   ├── DimTrade Incremental.py
│   ├── FactCashBalances Incremental.py
│   ├── FactHoldings Incremental.py
│   ├── FactWatches Incremental.py
│   ├── FactMarketHistory Incremental.py
│   └── currentaccountbalances Incremental.py
└── DLT/                           # SDP variant
    ├── pipelines_setup.py
    ├── update_pipeline_notebook.py    # Library-swap: dlt_historical → dlt_incremental
    ├── dlt_ingest_bronze.py
    ├── dlt_historical.sql
    └── dlt_incremental.sql
```

---

## Stage 0 (data prep) — augmented_staging job

**Triggered manually** from the Driver (or `databricks jobs run-now`). One
run per SF. Outputs are reusable across many benchmark runs at that SF.

| Job parameter        | Default                  | Notes |
|----------------------|--------------------------|-------|
| `scale_factor`       | (required)               | 10 / 100 / 1000 / 5000 / 10000 / 20000 |
| `catalog`            | `main`                   | Target catalog |
| `data_gen_type`      | `augmented_incremental`  | Routes spark_runner into Delta-only mode (skips Batch2/3, writes to `tpcdi_raw_data.{dataset}{sf}`). |
| `regenerate_data`    | `NO`                     | `YES` forces a rebuild even if the early-exit check would otherwise skip. |
| `log_level`          | `INFO`                   | DEBUG for verbose stage_runner traces. |

**Early-exit check.** `spark_runner` skips stage 0 when both:

1. The 19 expected staging tables exist in
   `tpcdi_incremental_staging_{sf}`, AND
2. Each of the 7 dataset directories at
   `_staging/sf={sf}/{Dataset}/` has Spark's `_SUCCESS` marker.

The marker is dropped by Spark only on a clean partitioned-write completion,
so a half-finished dataset can't false-positive into a skip. `regenerate_data=YES`
short-circuits the check and forces a rebuild.

**Customer event distribution.** The CustomerMgmt scheduler runs 434 update
windows across the full 10-year CM range; ~88 of those windows fall in the
730-day augmented window. Each window's actions (NEW / ADDACCT / UPDACCT /
CLOSEACCT / UPDCUST / INACT) are randomly interleaved across the row positions
within the window via a deterministic permutation, so timestamps for any single
ActionType are spread uniformly across the window — mirroring DIGen's
`CustomerMgmtScheduler` behavior. Without this (an earlier slab-packed
implementation), Customer events only landed on ~404 of 730 dates because
NEW/UPDCUST/INACT were each packed into <1-day sub-slabs of each window.

---

## Stage 1 (benchmark) — augmented_classic / augmented_sdp parent

**Triggered from the Driver** with SKU = `Cluster` or `SDP` and Batch Type
= `Augmented Incremental`. The parent job:

1. Runs `setup.py`: CLONEs the per-SF shared staging schema into the run's
   per-user schema (`{catalog}.{wh_db}_AugmentedIncremental_{Cluster|SDP}_{sf}`),
   resets `_dailybatches/{wh_db}_{sf}/` and `_checkpoints/{wh_db}_{sf}/`, and
   emits the 730-date list via `create_dates_loop.py` as a task value.
2. Runs the date loop (Databricks `for_each_task`). Each iteration is a
   child job that:
   - `simulate_filedrops` — `dbutils.fs.cp` the day's `_staging/sf={sf}/
     {Dataset}/_pdate={batch_date}/part-*.csv` files into the Auto Loader
     watch dir at `_dailybatches/{wh_db}_{sf}/{batch_date}/`, renamed to
     `{Dataset}.{file_ext}` (job param `file_ext` default `txt`).
   - Bronze auto-loader stream picks up the new files (one notebook per
     dataset, all reading from the same `cloudFiles` source).
   - Silver / gold incremental MERGEs (one notebook per target table).
3. Optional cleanup, gated by `delete_when_finished_TRUE_FALSE`.

### Re-run semantics

`simulate_filedrops` uses `dbutils.fs.cp` (not `mv`) so the staging tree
survives. Any individual batch can be re-run without regenerating Stage 0.
The auto-loader watch dir is wiped before each batch is staged, so the
Auto Loader checkpoint sees only the day's intended files.

### Job parameters (parent)

| Parameter                           | Default | Notes |
|-------------------------------------|---------|-------|
| `scale_factor`                      | (req.)  | Must match a Stage 0 run. |
| `catalog`                           | `main`  | Same as Stage 0. |
| `wh_db`                             | (user)  | Per-user prefix for the run schema, dailybatches, checkpoints. |
| `tpcdi_directory`                   | (req.)  | Path to the volume containing `augmented_incremental/_staging/sf={sf}/`. |
| `delete_when_finished_TRUE_FALSE`   | `FALSE` | A full 730-day run takes hours; default keeps the result tables for inspection. |
| `file_ext`                          | `txt`   | Output file extension at filedrop time. `read_file_ext` becomes `csv` when this is `txt` (the CSV writer produces `.csv` part files we rename), or matches `file_ext` for any other value. |

---

## Data shape (SF=20000, validated 2026-04-30)

| Dataset         | Total rows  | Rows/day | Source range       |
|-----------------|------------:|---------:|--------------------|
| Customer        | 8,692,760   | 11,908   | 730 days           |
| Account         | 17,385,945  | 23,816   | 730 days           |
| Trade           | (large)     | ~3.5M    | 730 days           |
| CashTransaction | (large)     | ~1.3M    | 730 days           |
| HoldingHistory  | (large)     | ~1.3M    | 730 days           |
| DailyMarket     | (large)     | ~14.8M   | 730 days           |
| WatchHistory    | (large)     | ~3.3M    | 730 days           |

DIGen baseline (Customer/Account, B2+B3 portion that lands in the same
2015-07-06 onward window): 11,886 / 23,809 rows/day at SF=20k. Augmented
matches DIGen within 0.2% on per-day density; the +30-day extra coverage
(augmented runs 730 days vs DIGen's 700) accounts for the total-row gap.

---

## Tables NOT covered

Three TPC-DI tables are intentionally out of scope:

| Table     | Reason |
|-----------|--------|
| BatchDate | Augmented uses dates instead of batch numbers; the table is meaningless. |
| Prospect  | Spec applies whole snapshots in 3 batches with no history; doesn't fit the daily streaming model. Could be re-added with synthetic per-day generation. |
| Audit     | Official audit checks compare per-batch row counts; the daily model doesn't have batches. |

The 4 FinWire-derived tables (DimCompany / DimSecurity / Financial /
CompanyYearEPS) and 7 static dim tables (DimDate / DimTime / TaxRate /
StatusType / TradeType / Industry / DimBroker) are loaded as part of
Stage 0 and CLONEd into each benchmark run.

---

## CDC schema

Every incremental file has these two columns prepended:

| Column     | Meaning |
|------------|---------|
| `cdc_flag` | `'I'` (insert / first observation of this entity) or `'U'` (update). For Customer/Account, derived from the ActionType. For Trade, derived from `row_number() over (partition by tradeid order by th_dts) = 1`. WatchHistory / CashTransaction / DailyMarket are always `'I'`. |
| `cdc_dsn`  | Sequence number. Never used in pipeline logic; preserved because the spec includes it. Allocated as a Delta IDENTITY column at Stage 0 write time. |

A per-row date column (`event_dt`, `update_dt`, etc.) drives the
partition split — see each dataset's stage_files notebook for the
specific source column.

---

## Pointers

- **Top-level architecture / gotchas:** [`CLAUDE.md`](../../../CLAUDE.md) at repo root
- **Workflow builders:** `src/tools/workflow_builders/augmented_staging.py`,
  `augmented_classic.py`, `augmented_sdp.py`
- **Stage 0 entry:** `src/tools/spark_runner.py`
  (`augmented_incremental=True` branch)
- **Stage 0 file staging:** `src/tools/augmented_staging/_stage_ingestion.py`
  + 7 notebooks under `stage_files/`
- **Driver:** `src/TPC-DI Driver.py` — set SKU = `Cluster` or `SDP` and
  Batch Type = `Augmented Incremental`
