# Claude / AI Agent Context

> **PRIMARY USE CASE — AI/AGENT CONSUMPTION.** This codebase is being
> deliberately built and curated for use as a *skills asset* by AI agents,
> specifically:
> - **Claude** (Claude Code, claude.ai Projects, Claude Code skills/plugins)
> - **Databricks Genie** (workspace-native conversational analytics)
>
> Every artifact in the repo — the workflow builders, the audit module, the
> static audit snapshots, the unified `data_gen` entry point, the README's
> flow diagram, and especially this file — is designed so an AI agent can
> reason about the TPC-DI benchmark, answer questions about it, and make
> well-informed code/SQL suggestions without re-discovering the architecture
> from scratch. When making changes, prioritize keeping this file (and
> README) accurate over leaving cruft. Outdated docs hurt agent answers
> directly.

This document captures project-specific context for AI agents (Claude Code,
Genie, claude.ai Projects) working on this repo. Always-on context for Claude
Code when this file lives at the repo root.

The user-facing introduction is in `README.md`. This file is for behind-the-
scenes design decisions, gotchas, and operational knowledge that an AI needs
to make good suggestions or answer questions about the codebase.

## Project goal

Replace the single-threaded `DIGen.jar` data generator (Java, runs on one
node) with a distributed PySpark implementation that runs on Databricks
serverless or classic Photon clusters. Targets:
- Linear scaling across executors so large scale factors (SF=10000+) finish
  in a fraction of the JAR's time.
- No DBR / node-type / UC cluster restrictions.
- Statistical fidelity to DIGen, not byte-parity (see "DIGen vs ours" below).

The existing `main` branch holds the original DIGen-based pipeline. The
`augmented_incremental` branch now carries **both** generators behind a single
`data_generator` widget on the Driver:

- `spark` (default) — distributed PySpark, runs on serverless.
- `digen` — DIGen.jar wrapped by `src/tools/digen_runner.py`, runs on a
  classic single-node cluster (Java subprocess can't run on serverless).

Both flow through a single unified entry-point notebook —
`src/tools/data_gen_tasks/data_gen.py`. For `data_gen_type=native` it
imports `digen_runner.py` and runs the JAR inline (single-task workflow).
For `data_gen_type=spark` / `augmented_incremental` it does first-task
init work (creates `_stage` schema, ensures `tpcdi_raw_data` + volume,
wipes prior outputs on `regenerate_data=YES`); the actual generation is
done by per-dataset downstream tasks in the same DAG (see "Data
generator architecture" below).

`generate_datagen_workflow()` dispatches via a `_BUILDERS` dict to
`workflow_builders/datagen_spark.py` or `workflow_builders/datagen_digen.py`
(both pure Python). The DIGen builder additionally requires
`default_dbr_version` and `default_worker_type` (its forced
non-serverless DBR 15.4 + Photon cluster spec).

**Output paths.** All three modes write under
`/Volumes/{catalog}/tpcdi_raw_data/tpcdi_volume/`. DIGen writes raw files
under `sf={SF}/Batch*`; Spark writes raw files under the same Batch1/2/3
layout; augmented writes Delta tables to
`{catalog}.tpcdi_raw_data.{dataset}{SF}` (no Batch dirs).

## Data generator architecture

The Spark and augmented_incremental paths run as a multi-task DAG
defined in `workflow_builders/datagen_spark.py` /
`workflow_builders/augmented_staging.py`:

```
data_gen (entry: schema+volume init, wipe when regenerate=YES)
  ├── gen_reference / gen_hr / gen_finwire / gen_prospect (wave 1)
  │   └── copy_hr / copy_finwire / copy_prospect (parallel with downstream)
  ├── gen_customer (← gen_hr)            └── copy_customer
  ├── gen_daily_market (← gen_finwire)   └── copy_daily_market
  ├── gen_trade_base (← gen_finwire)     V6 trade decomposition
  │   ├── gen_trade            ──→ copy_trade
  │   ├── gen_tradehistory     ──→ copy_tradehistory
  │   ├── gen_cashtransaction  ──→ copy_cashtransaction
  │   └── gen_holdinghistory   ──→ copy_holdinghistory
  └── gen_watch_history (← gen_finwire + gen_customer) └── copy_watch_history
audit_emit (ALL_SUCCESS, ← all gens — standard mode only)
cleanup_intermediates (ALL_SUCCESS, ← all gens + copies + audit_emit)
```

**Trade decomposition (V6).** `gen_trade_base` materializes a
``_gen_trade_df`` Delta table in `{wh_db}_{sf}_stage` containing the
minimum cross-task column set: `t_id, t_qty, t_ca_id, t_s_symb,
_is_canceled, _is_buy, _is_limit, _base_ts, _submit_ts, _complete_ts,
_cash_ts`. The 4 leaves read this Delta in parallel and re-derive
single-consumer columns from `t_id` (cheap hash ops). Notably *not*
staged: `_ct_name_raw` (~300 GB at SF=20k — expensive to materialize,
cheap to re-derive in the CT leaf), `_trade_val`, `_broker_idx`,
`_qty_val`. Inter-leaf state (`_hh_hist_batch{N}`,
`_ct_hist_batch{N}`, `_t_submit_hist_batch{N}` temp views from the
legacy code) is *not* shared — each leaf computes the equivalent
filter directly from `_gen_trade_df`. The leaves are independent.

Each gen task is a thin notebook in `src/tools/data_gen_tasks/` that
imports the per-table generator from `src/tools/tpcdi_gen/`. Each
self-skips when its output Delta is intact and `regenerate_data=NO`, so
a re-trigger only redoes missing datasets — repair-runs at per-dataset
granularity. Cross-task intermediates (`_gen_brokers`, `_gen_symbols`,
`_gen_customer_dates`, `_dc_*` from `disk_cache`) live as Delta tables
in `{catalog}.{wh_db}_{sf}_stage` (matching `dw_init.sql`'s `_stage`
schema convention) with no-stats / no-auto-optimize TBLPROPERTIES.

`copy_*` tasks decouple the staging→final UC Volume copies from the
generators so they run in parallel with downstream gens (the gen_* sets
`utils._DEFER_COPIES["enabled"]=True` so the inline
`register_copies_from_staging` call in the underlying generator is a
no-op; the dedicated `copy_*` task does the staging→final copy
synchronously and waits on background daemon threads before exiting).

`audit_emit` (standard mode only) aggregates each gen's `record_counts`
task value via `dbutils.jobs.taskValues.get(...)` and feeds the merged
dict into `audit.generate()` to produce the per-batch `*_audit.csv` +
`Generator_audit.csv` files.

Per-table generators live in `src/tools/tpcdi_gen/`:

- `reference.py` — small static tables (StatusType, TaxRate, Date, Time,
  Industry, TradeType, BatchDate). Constants/dictionaries materialized.
- `hr.py` — HR.csv. Each employee gets a hash-derived job code; ~30%
  are brokers (jobcode 314).
- `finwire.py` — FINWIRE quarterly fixed-width files (CMP/SEC/FIN). 202
  quarters covered. Splits CMP/SEC/FIN compute into parallel writes.
  Cross-joins active companies × quarters for FIN; pre-repartitions
  `cmp_quarters` to `max(8, cfg.sf // 25)` partitions before the cross-join.
  Emits `_symbols` temp view early via a `symbols_ready_event` so Trade /
  WatchHistory / DailyMarket can start before FINWIRE finishes.
- `customer.py` — CustomerMgmt.xml + Customer.txt + Account.txt.
  Calls `customer_scheduler_v2.build_schedule_df()` (V8 — Spark-native
  port of DIGen's `GrowingOffsetPermutation`) to emit the unified
  schedule DataFrame directly on Spark executors via Pandas UDFs, then
  joins it against the per-row bijected `all_df` to assign final
  ActionTypes. Same DIGen algorithm as before but with the
  `_resolve_skip_vec` driver-side numpy loop replaced by closed-form
  recursion in vectorized numpy on each executor partition.
- `customer_scheduler_v2.py` — implements the
  `GrowingOffsetPermutation`. Driver-side computes per-generation
  bijection params (b, c, perm_max, offsets, offset_sums) into broadcast-
  friendly numpy arrays, then a Pandas UDF runs the recursion at scale.
  Same-update non-collision between INACT/UPDCUST/ADDACCT (and
  CLOSEACCT/UPDACCT) achieved via DIGen's slot-offset trick: each
  action type gets a disjoint slot range on the same per-generation
  bijection, so uniqueness within a slot range plus disjoint ranges
  guarantees no two action types pick the same entity in the same
  update.
- `prospect.py` — Prospect.csv (B1/B2/B3). Slices B1 by `p_id` ordinal for
  B2/B3 incremental churn — does NOT use a `Window.orderBy()` (that forces
  single-partition shuffle).
- `market_data.py` — DailyMarket.txt (~5.5B rows at SF=10000).
- `watch_history.py` — WatchHistory.txt. Bijection-based pair selection
  over the customer × security cross-product. Per-update LCG with `bij_b`
  capped at `2^62 / max_pos` to keep `pos × b` in bigint range
  (Photon ANSI raises ARITHMETIC_OVERFLOW otherwise at SF≥20000).
- `trade_split.py` — Trade.txt + TradeHistory.txt + CashTransaction.txt
  + HoldingHistory.txt, decomposed into 5 functions (V6):
  `materialize_base()` writes the `_gen_trade_df` Delta with the
  minimum cross-task columns; `gen_trade()` /
  `gen_tradehistory()` / `gen_cashtransaction()` /
  `gen_holdinghistory()` are independent leaves that read the Delta
  and write their dataset's outputs. Trade
  `t_ca_id = _va_idx.cast(string)` directly — the hash-derived index
  *is* the account ID (sequential 0..n_valid-1). Account pool size is
  analytical (`cfg.n_available_accounts`); no CustomerMgmt dependency.
- `audit.py` — emits `*_audit.csv` files. Two paths:
  1. **Static snapshot** at `static_audits/sf={SF}/` — pre-computed
     CSVs committed to the repo, copied into the volume after generation.
     Avoids recomputing exact counts on every run.
  2. **Dynamic regeneration** — when no static snapshot exists for the SF,
     audit module emits values from the `record_counts` dict that each
     generator populates during execution. No Spark scans needed; counts
     come from in-memory dict lookups + analytical formulas.

`utils.py` holds shared helpers: `disk_cache` (Delta-table staging on
serverless via `{wh_db}_{sf}_stage._dc_NNN_*`, `persist(DISK_ONLY)` on
classic), `dict_join` / `dict_join_batch` for broadcast-style dictionary
lookups, and `register_copies_from_staging` for the V8 Spark-distributed
staging→final rename (see "Spark-distributed rename" below).

## Key design decisions

### Bijection-based generation
Every table that needs unique random selections without replacement uses
a bijective permutation `f(x) = (b*x + c) % modulus`. With `b` coprime to
`modulus`, this is mathematically guaranteed to be collision-free over the
full domain. Used in:
- WatchHistory pair selection (customer × security cross-product)
- CustomerMgmt action scheduling (INACT/UPDCUST/etc. picks per update)
- WH historical (per-update `b`/`c` differ by RNG seed)

The earlier hash-modulo approach drifted vs DIGen by 5-25% on dedup-heavy
tables at large SFs; bijection eliminates that drift.

### No purposeful DQ injection
Per `project_no_dq_injection` memory note: we deliberately do NOT inject
the data-quality test cases DIGen does (C_DOB_TO/TY out-of-range birthdays,
C_TIER_INV invalid tiers, T_InvalidCharge/Commission). Our generator
produces uniformly valid data. Both audit values and ETL alert counts are 0,
so the audit checks pass with `0 = 0`. This is deliberate and matches
`project_no_dq_injection` — DON'T "fix" by injecting bad data.

Exception: B1 historical Trade.txt has the chrg/comm injection (~0.01%
trades) per the historical pattern. Incrementals don't.

### Analytical pools (no Spark scan-back at gen time)
- `cfg.n_available_accounts` — count of accounts created before
  TRADE_BEGIN_DATE. Computed from CM scaling constants. Trade uses
  `hash % n_available_accounts` to pick an account, and the hash result
  IS the CA_ID (0..n_valid-1, no enumeration needed).
- `cfg.n_brokers_estimate` — count of brokers via analytical
  `int(hr_rows × BROKER_PCT)`. Trade uses this when `static_audits_available`,
  exact count when regenerating audits dynamically.

These let Trade start as soon as FINWIRE `_symbols` is ready, with no
dependency on CustomerMgmt or HR completion. Saves ~30 min on the critical
path at SF=20000.

### Spark-distributed rename (V8)
`register_copies_from_staging` moves Spark part files from each
generator's staging dir (`{batch_path}/{base}/part-*`) to the flat
DIGen-compatible layout (`{batch_path}/{base}_K{ext}`). UC Volume FUSE
serializes rename syscalls per-pod, so within-pod parallelism doesn't
help — instead we repartition the (src, dst) pairs across `N=32`
executor pods and run `subprocess.run(["mv", ...])` on each pod. Each
pod has its own FUSE driver, so the system-wide concurrent rename
count is `N`. Per-file mv has retry-with-backoff (~2 min total budget)
to absorb transient ABFS rename throttling. Resume-safe: a re-run
detects existing `{base}_K{ext}` files at the destination and starts
the next K past the highest existing one.

At SF=20k the long-pole copy task drops from ~80m (single-pod-serial
under contention) to ~2m. Prior approaches that didn't survive:
- V0: `dbutils.fs.cp` thread pool — Files API ~405 ms/file ceiling
- V5: same-dir bash `mv` loop — 500 ms/file but FUSE bottleneck
- V7: within-pod `ThreadPoolExecutor mv` — EAGAIN cascades, 2× slower

### Spark-native CustomerMgmt scheduler (V8)
`customer_scheduler_v2.build_schedule_df` ports DIGen's
`GrowingOffsetPermutation` into a Pandas UDF that runs on Spark
executors. Replaces the `~22-26 min driver-side numpy` loop with a
~3-4 min distributed pass. Output IDs differ from the prior driver
scheduler (different bijection params + DIGen's slot-offset semantics
for ADDACCT/UPDCUST), so the static_audits/ snapshots were
regenerated against this scheduler — every audit count remains within
0.0005% of the validated DIGen baseline (the ADDACCT drift is the
only non-zero one and is well below any audit tolerance).

### Account pool safety margin
`n_available_accounts = max(hist_size, int(_n_created * _trade_fraction))`
where `_trade_fraction` subtracts a 30-day safety margin from
TRADE_BEGIN_DATE. Without this, account creation_ts has sub-day positional
offsets that fuzz the boundary, and a few trades pick accounts created
*after* their trade_dts — failing the silver SCD2 temporal join check
(DimTrade SK_AccountID Bad join). At SF=10000 we saw 9 such trades; SF=20000
would see ~18. Pool shrinks ~0.8%, drift goes to zero.

### Broadcast threshold + targeted broadcast hints
- `spark.sql.autoBroadcastJoinThreshold` set to 250m (up from default 10m).
- `spark.databricks.adaptive.autoBroadcastJoinThreshold` matched.
- ALL manual `F.broadcast()` hints have been REMOVED. The optimizer makes
  broadcast-vs-shuffle decisions against the 250MB threshold based on
  runtime stats. Manual hints caused Photon broadcast OOMs at SF=20000
  (e.g., the CustomerMgmt inactive-customers anti-join's 4.34M-row build
  side ballooned to 2 GiB hash table).

### `_symbols._idx` cast to long
Cast to `long` in the FINWIRE staging so downstream Trade/WH/DM joins
have matching key types. If left as `int`, Spark inserts an implicit
`cast(_idx AS bigint)` into the join condition that can prevent
auto-broadcast even when `_symbols` fits under the threshold.

### Parallel range partitioning
`spark.range(0, cfg.trade_total, numPartitions=max(8, cfg.sf // 8))` for
the Trade base. Default partitioning at SF=20000 (~24 tasks) put ~108M
rows/task. Pipeline's 30+ withColumns + the `_symbols` join piled up
working memory until tasks spilled 237 GB. With 2500 partitions
(~1M rows/task), zero spill, 6m42s wall clock vs 30 min before.

Same logic in CustomerMgmt: `schedule_df.coalesce(max(8, cfg.sf // 2500))`
fans out the Arrow-deserialized schedule across writers.

### DIGen vs ours — counter semantics
For each audit attribute, the DIGen reference logic lives in:
`src/tools/datagen/pdgf/config/tpc-di-Audit/*-Audit.xml` (counterTemplate).
We mirror these exactly:

- `C_NEW` — `cdc_flag == "I"`
- `C_UPDCUST` — `cdc_flag == "U" AND c_st_id != "INAC"`
- `C_INACT` — `c_st_id == "INAC"` (every row)
- `C_TIER_INV` — `c_tier in {0, 4-9, null, ""}` (i.e., not 1/2/3)
- `CA_ADDACCT` — `cdc_flag == "I"`
- `CA_CLOSEACCT` — `cdc_flag == "U" AND ca_st_id == "INAC"`
- `CA_UPDACCT` — `cdc_flag == "U" AND ca_st_id == "ACTV"`
- `T_Records` / `T_NEW` (B1) — every row in Trade.txt
- `T_NEW` (B2/B3) — `cdc_flag == "I"`
- `T_CanceledTrades` — `t_st_id == "CNCL"` (or `status` for incrementals)
- `T_InvalidCharge/Commission` — `fee/commission > qty * tradeprice`
- `WH_RECORDS` — every row in WatchHistory.txt
- `WH_ACTIVE` — `count(w_action == 'ACTV')` (NOT net active = ACTV - CNCL).
  Silver groups by `(c_id, sym)` via bijection invariant; ACTV count == distinct pairs.
- `HR_BROKERS` — `count(employeejobcode == "314")`
- `FW_CMP/SEC/FIN` — count by rectype byte slice (positions 16-18 of each line)

If an audit check fails, the FIRST debugging step is to verify our counter
matches the DIGen XML. The second is to check whether the generated data
produces the same SCD2 outcome under silver's filtering rules.

## Benchmark architecture

The benchmark comes in three runtime variants — `CLUSTER` (job cluster or
serverless), `DBSQL` (SQL warehouse), and `SDP` (Spark Declarative
Pipelines, the Databricks runtime previously branded "DLT"). The Driver
creates one job per `(scale_factor, batched, exec_type, data_generator)`
combination using Python builders under `src/tools/workflow_builders/`:

- `datagen_spark.py` / `datagen_digen.py` — datagen workflows
- `workflows_single_batch.py` / `workflows_incremental.py` — Cluster + DBSQL
  benchmark workflows (one-shot vs auditable per-batch variants)
- `sdp_pipeline.py` / `sdp_workflow.py` — SDP pipeline definition + the
  Jobs-API workflow that runs it
- `augmented_staging.py` — Stage 0 for Augmented Incremental (one-time
  data prep per SF: per-dataset gen DAG in Delta-only mode + per-dataset
  partitioned-CSV trees + cleanup of spark-gen leftovers)
- `augmented_classic.py` / `augmented_sdp.py` — Augmented Incremental
  benchmark workflows (730-day daily streaming loop). See
  `src/incremental_batches/augmented_incremental/README.md` for the full
  architecture.
- `warehouse.py` — DBSQL warehouse spec
- `_workflow_common.py` — shared helpers (`make_task`, `make_cleanup_tasks`,
  cluster specs, etc.)

**Job naming convention** (Driver-built):
- Datagen: `{base}-SF{N}-{SparkGen|NativeGen}`
- Augmented Incremental Stage 0 (`augmented_staging`): `{base}-SF{N}-AugmentedGen`
- Cluster/DBSQL benchmark: `{base}-SF{N}-{Incremental|SingleBatch}-{Cluster|DBSQL}-{SparkGen|NativeGen}`
- SDP benchmark: `{base}-SF{N}-{SDP-CORE|SDP-PRO|SDP-ADVANCED}-{SparkGen|NativeGen}`
- Augmented benchmark parent: `{base}-SF{N}-AugmentedIncremental-{Cluster|SDP}-Parent`

Every created job carries a `data_generator: spark|native_jar` tag so the
Jobs UI / API can filter without parsing the name.

**Schema names** preserve the long form (`spark_data_gen` / `native_data_gen`)
so already-materialized schemas survive the job-name refactor:
`{catalog}.{wh_db}_{exec_type}_{datagen_label}_{batched_label}_{sf}` —
e.g. `main.shannon_barrow_TPCDI_CLUSTER_spark_data_gen_incremental_10`.
SDP variants use `_SDP_{edition}_{datagen_label}_` instead.

**Cleanup** runs as a final task on every benchmark workflow. A
`delete_when_finished_TRUE_FALSE` `condition_task` gates a SQL notebook
(`tools/cleanup_after_benchmark.sql`) that drops the run's `_stage` and
final schemas. Setting the `delete_tables_when_finished` job parameter to
`FALSE` short-circuits the gate so no cleanup compute spins up.

**Auditable benchmark — incremental batches.** Key SQL files under
`src/incremental_batches/`:

- `bronze/*.sql` — file ingestion. Tables are read via `read_files(...
  fileNamePattern => "{Customer.txt,Customer_[0-9]*.txt}", schema => ...)`.
  The brace-alternation glob matches **both** generators' output: DIGen's
  single `Customer.txt` (no suffix) and Spark's split `Customer_1.txt`,
  `Customer_2.txt`, etc., while excluding `Customer_audit.csv`. FINWIRE is
  special — DIGen names like `FINWIRE2017Q3` (no extension, year+quarter)
  and Spark names like `FINWIRE_1.txt` are caught by
  `{FINWIRE[0-9][0-9][0-9][0-9]Q[1-4],FINWIRE_[0-9]*.txt}`. The same
  brace pattern is used in `sdp_pipeline._BRONZE_TABLES_JSON` for the
  SDP bronze table — without it, Spark's numbered FINWIRE files miss
  the pattern and DimCompany/DimSecurity/Financial come out empty.
- `silver/*.sql` — DimCustomer, DimAccount, DimTrade, FactWatches, etc.
  Each is an SCD2 / aggregate transformation.
- `gold/*.sql` — FactCashBalances, FactMarketHistory.
- `audit_validation/automated_audit.sql` — the canonical audit checks.
  Compares `DIMessages` (validation/alert messages emitted by silver) vs
  `Audit` table values from the generated `*_audit.csv` files.
- `dw_init.sql` — bootstraps schemas, creates the **`Audit` Delta TABLE**
  (loaded from `*_audit.csv` via `read_files`). Was a VIEW; turned into a
  TABLE so a subsequent datagen run wiping the volume doesn't break a
  benchmark mid-run.

### Audit check shapes
- **Row count checks**: actual silver row count (or DIMessages-reported)
  delta per batch == sum of relevant attribute (e.g., T_NEW per batch).
- **Match-Audit-table**: `count(DIMessages where MessageText='X')` ==
  `sum(Audit where Attribute='Y')` per batch. Often cumulative.
- **Bad join**: `count(*) from DimTable` == `count(*) from DimTable INNER JOIN dim_lookup ON sk_id WHERE temporal_range`.
  Strict equality on temporal SCD2 joins; any drift fails.

These checks are mostly EXACT EQUALITY. Even 1 row off fails. Build
generators with that in mind.

### Augmented Incremental — what's different from Single-Batch / Incremental

`src/incremental_batches/augmented_incremental/README.md` is the source of
truth. Quick orientation for AI agents:

- **Stage 0** is a separate workflow (`augmented_staging`) run once per
  SF. The per-dataset gen tasks write the 7 datasets to Delta tables at
  `tpcdi_raw_data.{dataset}{sf}` with an `stg_target` column that splits
  rows into 'tables' (pre-2015-07-06, source for the historical SCD2
  builds) and 'files' (the 730-day window, source for the per-day file
  drops). 7 `stage_files/*.py` notebooks then write Spark-native
  partitioned-CSV at `_staging/sf={sf}/{Dataset}/_pdate={date}/part-*.csv`.
  No post-write rename or concat at stage time.
- **Early-exit** on Stage 0 uses Spark's `_SUCCESS` marker per dataset
  dir, NOT a date-dir count. Authoritative — half-written datasets can't
  false-positive.
- **Stage 1** (the benchmark) is a parent job that loops 730 dates via
  `for_each_task`. Per iteration: `simulate_filedrops.py` does
  `dbutils.fs.cp` (NOT `mv`) of one day's part files into the auto-loader
  watch dir, renamed to `{Dataset}.{file_ext}` (job param `file_ext`,
  default `txt`). `read_file_ext = 'csv' if file_ext == 'txt' else
  file_ext` bridges the writer/extension mismatch. Auto-loader → bronze
  → silver/gold MERGEs follow.
- **No audit step.** Augmented_staging has no `audit_emit` task;
  `static_audits_available()` returns True unconditionally when
  `cfg.augmented_incremental` is set, short-circuiting all per-generator
  dynamic-audit-regen blocks (those blocks read legacy CSV staging paths
  or B2/B3 temp views that don't exist in augmented mode).
- **Customer event spread.** `customer.py` recently dropped
  `_sort_key` / `_ordered_pos`; ActionTS now derives directly from
  `pos_in_update`. Each ActionType (NEW / UPDCUST / INACT) spreads
  uniformly across each 8.4-day update window — Customer touches all
  730 dates instead of the prior 404. Mirrors DIGen's per-row-index
  timestamping with action-type permutation.
- **B2/B3 generation is skipped** in augmented mode (gated at the call
  sites in `customer.py`, `trade.py`, `watch_history.py`, `prospect.py`).
  The 730-day window's events come from the `'files'` partition of the
  Stage 0 Delta tables.

## Common workflows

### Generate data for a scale factor
```
databricks jobs run-now --profile tpc-di \
  --json '{"job_id": 1017619735160060, "job_parameters": {"scale_factor": "10000", "regenerate_data": "YES"}}'
```
- Job `1017619735160060` (cloned, faster cold-start).
- Job `218882370760159` (original, has OOM-promotion flag — better for SF≥20000).
- `regenerate_data=YES` wipes the SF directory and rebuilds.
- These jobs target the **Spark** generator (the unified entry-point
  task is `tools/data_gen_tasks/data_gen`; per-dataset downstream gen +
  copy tasks do the actual generation). For the **DIGen.jar** path,
  regenerate the datagen workflow from the Driver with
  `data_generator=digen` — that posts `workflow_builders/datagen_digen.py`
  with a single `data_gen` task (which imports `digen_runner` and runs
  the JAR inline) on a classic single-node DBR 15.4 + Photon cluster,
  since the Java subprocess can't run on serverless.

### Run the benchmark
```
databricks jobs run-now --profile tpc-di \
  --json '{"job_id": 798957941168162, "job_parameters": {"scale_factor": "10000"}}'
```
- Has `max_concurrent_runs=1` — second trigger while one is running gets
  SKIPPED with `MAXIMUM_CONCURRENT_RUNS_REACHED`.

### Run only N days of the augmented benchmark
```
databricks jobs run-now --profile tpc-di --json '{
  "job_id": <augmented_parent_job_id>,
  "job_parameters": {"scale_factor": "10", "incremental_batches_to_run": "30"}
}'
```
- `incremental_batches_to_run` (default `730`) caps the daily-streaming
  loop to the first N days. Useful for smoke runs without committing
  ~a week of compute. Wired into both `augmented_classic` and
  `augmented_sdp` parent jobs; `setup.py` and `DLT/pipelines_setup.py`
  clamp the value to [1, 730].

### Regenerate audits without re-running datagen
Open `src/tools/regenerate_audits` notebook in workspace, set widgets,
run on serverless. Scans existing data files in the volume and rewrites
every `*_audit.csv` using DIGen's exact counter semantics.

### Check audit failures after a benchmark
```sql
-- Schema name pattern (single source of truth for schema label):
--   {catalog}.{wh_db}_{exec_type}_{datagen_label}_{batched_label}_{sf}
-- e.g. main.shannon_barrow_TPCDI_CLUSTER_spark_data_gen_incremental_10
SELECT * FROM main.shannon_barrow_TPCDI_CLUSTER_spark_data_gen_incremental_${SF}.automated_audit_results
WHERE result != 'OK' ORDER BY test, batch
```

### Sync local + workspace for a code change
```
git add … && git commit -am "…" && git push origin augmented_incremental
databricks repos update --profile tpc-di 1177342250018739 --branch augmented_incremental
```
This is non-negotiable BEFORE triggering any job. Workspace repo id
`1177342250018739` is what jobs execute against.

## Known gotchas

- **Serverless session expiry is idle, not TTL.** Earlier we thought
  Spark Connect serverless sessions had a hard ~60 min TTL. The actual
  cause of session-expired failures was a stalled driver re-compute
  loop from suppressed broadcast OOMs. Active jobs on serverless can run
  hours.
- **UC Volume FUSE EAGAIN.** Concurrent file ops via FUSE return
  `Resource temporarily unavailable` under load. Worked around by
  replacing `bash cat` with `shutil.copyfileobj` + 10-retry backoff in
  CustomerMgmt XML concat and `register_copies_from_staging`.
- **Photon ANSI arithmetic overflow.** At SF≥20000, naive bijection `b`
  values in `[1, modulus)` can produce `pos * b > 2^63 - 1`. WH bijection
  caps `b` at `2^62 / max_pos` to keep products in bigint.
- **`max_concurrent_runs=1` on the benchmark job.** Don't trigger a
  second benchmark while one is running — it'll be SKIPPED.
- **OOM-promoted drivers stick per job_id.** If a job ever OOM'd the
  driver, the job is flagged for big-driver promotion forever; the
  scheduler will queue runs until a big driver is available (sometimes
  4-5 min wait). Cloning to a fresh job_id resets the flag.
- **Avoid manual `F.broadcast()` hints.** They override the optimizer's
  decision based on runtime stats, sometimes forcing 2 GiB Photon hash
  tables. Trust the 250MB `autoBroadcastJoinThreshold`.
- **Don't sync to the wrong workspace path.** `/Workspace/Users/.../tpc-di/`
  is a stale flat-upload dir. `/Workspace/Users/.../databricks-tpc-di-augmented/`
  (repo `1177342250018739`) is the source of truth that jobs run against.

## File map

```
src/
  TPC-DI Driver.py                # entry-point notebook the user runs
  tools/
    digen_runner.py               # DIGen.jar wrapper (called inline from data_gen task in native mode)
    setup_context.py              # tpcdi_config bootstrap (api, cloud, defaults)
    generate_datagen_workflow.py  # dispatches to workflow_builders.datagen_{spark,digen}
    generate_benchmark_workflow.py# dispatches to workflow_builders for Cluster/DBSQL/SDP
    cleanup_after_benchmark.sql   # final cleanup task (SQL — runs on warehouse too)
    regenerate_audits.py          # standalone audit recompute notebook
    workflow_builders/
      _workflow_common.py         # shared helpers (make_task, make_cleanup_tasks, …)
      _node_picker.py             # cloud-aware ARM-preferred node selection
      datagen_spark.py            # Spark datagen workflow JSON
      datagen_digen.py            # DIGen.jar datagen workflow JSON
      workflows_single_batch.py   # Cluster+DBSQL all-batches-at-once benchmark
      workflows_incremental.py    # Cluster+DBSQL per-batch auditable benchmark
      sdp_pipeline.py             # SDP pipeline definition (was dlt_pipeline)
      sdp_workflow.py             # SDP wrapping workflow (was dlt_workflow)
      augmented_staging.py        # Augmented Incremental Stage 0 (data prep)
      augmented_classic.py        # Augmented Incremental Cluster benchmark parent
      augmented_sdp.py            # Augmented Incremental SDP benchmark parent
      warehouse.py                # DBSQL warehouse spec
    augmented_staging/            # Stage 0 notebooks for Augmented Incremental
      _stage_ingestion.py         # stage_to_files() helper — partitioned-CSV writer
      stage_files/{Dataset}.py    # 7 per-dataset stage_files notebooks
      cleanup_stage0.py           # drop temp Delta + remove spark-gen Batch1/2/3
    data_gen_tasks/               # Per-dataset task notebooks for the spark/augmented data_gen DAG
      data_gen.py                 # unified entry: native mode runs DIGen inline; spark/augmented inits intermediates
      _shared.py                  # bootstrap helper (cfg + dicts + sys.path) for every task
      _copy_helper.py             # copy_dataset() — staging→final UC Volume copy + wait
      gen_*.py                    # 8 per-dataset generator tasks (reference, hr, finwire, customer, daily_market, trade, watch_history, prospect)
      copy_*.py                   # 7 per-dataset copy tasks (parallel with downstream gens)
      audit_emit.py               # standard mode only — aggregates record_counts task values, calls audit.generate
      cleanup_intermediates.py    # drops _gen_* / _dc_* temps from {wh_db}_{sf}_stage
    tpcdi_gen/
      audit.py                    # static-snapshot copy + dynamic regen
      config.py                   # ScaleConfig — all scaling constants
      customer.py                 # CustomerMgmt + Customer.txt + Account.txt orchestration
      customer_scheduler_v2.py    # V8 Spark-native GrowingOffsetPermutation (Pandas UDF)
      finwire.py                  # FINWIRE quarterly files
      hr.py                       # HR.csv + _brokers temp view
      market_data.py              # DailyMarket.txt
      prospect.py                 # Prospect.csv (B1 + B2/B3 churn)
      reference_tables.py         # static reference tables
      trade_split.py              # V6 trade decomposition: base + 4 leaves
      utils.py                    # shared helpers (V8 Spark-distributed rename)
      watch_history.py            # WatchHistory.txt with bijection
      static_audits/sf={SF}/      # pre-computed audit snapshots (regenerated under V8)
    datagen/pdgf/                 # DIGen.jar + PDGF config (reference only)
  incremental_batches/
    bronze/                       # file → staging table SQL
    silver/                       # SCD2 dimension builds
    gold/                         # fact tables
    audit_validation/             # automated_audit.sql, batch_validation.sql, audit_alerts.sql
    dw_init.sql                   # schema + Audit table bootstrap
    augmented_incremental/        # Augmented Incremental benchmark — see its README
      README.md                   # source of truth for this variant
      setup.py / teardown.py      # Stage 1 setup/cleanup
      simulate_filedrops.py       # per-batch cp from _staging into auto-loader watch dir
      bronze/ historical/ incremental/ DLT/  # SQL + notebooks
  single_batch/
    SQL/                          # all-batches-in-one variant (Cluster + DBSQL)
    spark_declarative_pipelines/  # SDP notebooks (was delta_live_tables/)
tests/
  test_workflow_builders.py       # unit tests for workflow JSON shape + naming
  smoke_run_workflows.py          # integration smoke test (creates+runs 4 jobs)
```

## Active branch

`data-gen-decomposition`. Carries the V6 trade-decomposition + V8
customer scheduler + V8 Spark-distributed rename. Targets `main`.

## Status of validated scale factors

As of the most recent validation pass, the Spark generator + benchmark
audits pass at SF=10/100/1000/5000/10000/20000. SF=20k full pipeline
finishes in ~19 m on serverless (down from ~105 m pre-V8). End-to-end
smoke (SF=10 × {Cluster/Inc, DBSQL/Single, SDP-CORE} × Spark, plus
Augmented at SF=10) all SUCCEED. Audit `*_audit.csv` snapshots were
regenerated against V8 output and committed at every SF tier.

## DLT → SDP rename

Databricks rebranded "Delta Live Tables" to "Spark Declarative Pipelines".
This repo follows that rename:

- workflow keys: `DLT-CORE/PRO/ADVANCED` → `SDP-CORE/PRO/ADVANCED`
- schema labels: `..._DLT_{edition}_...` → `..._SDP_{edition}_...`
- task keys: `TPC-DI-DLT-PIPELINE` → `TPC-DI-SDP-PIPELINE`
- file/dir names: `dlt_pipeline.py`/`dlt_workflow.py` → `sdp_*.py`,
  `single_batch/delta_live_tables/` → `single_batch/spark_declarative_pipelines/`
- prose in README, Driver markdown, mermaid diagram

The Python SDK module `dlt` (`import dlt`, `@dlt.table`, `dlt.apply_changes(...)`)
is **not** renamed — that's still the actual library Databricks exposes.

## SDP CustomerMgmt routing for Spark vs DIGen

`sdp_pipeline.build()` decides whether `CustomerMgmtRaw` runs as a library
inside the SDP pipeline (creating LIVE customermgmt) vs being read from a
staging schema populated by an upstream task:

- **Spark datagen, any SF** → in-pipeline (split XML files re-parse cheaply)
- **DIGen, SF < 1000** → in-pipeline
- **DIGen, SF ≥ 1000** → upstream `ingest_customermgmt_cluster` task
  ingests the single big DIGen XML via the spark-xml maven library on a
  SingleNode classic cluster, writes to `..._SDP_{edition}_..._stage`,
  and the SDP pipeline reads from there.

If you change either side, keep the `_libraries()` and `cust_mgmt_schema`
decisions in sync — they currently both branch on
`data_generator == "spark" or scale_factor < 1000`.
