# Claude / AI Agent Context

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
- `digen` — DIGen.jar wrapped by `src/tools/data_generator.py`, runs on a
  small classic single-node cluster (Java subprocess can't run on serverless).

`generate_datagen_workflow()` dispatches via a `_TEMPLATES` dict to either
`datagen_workflow.json` (Spark) or `datagen_workflow_digen.json` (DIGen) and
posts the rendered template to the Jobs API. The DIGen branch additionally
requires `default_dbr_version` and `default_worker_type` (its classic
single-node cluster spec).

**Output paths differ.** The Driver chooses `tpcdi_directory` based on the
selected generator and forwards that to the benchmark workflow:
- `spark` → `/Volumes/{catalog}/tpcdi_raw_data/tpcdi_volume/spark_datagen/`
- `digen` → `/Volumes/{catalog}/tpcdi_raw_data/tpcdi_volume/` (legacy path
  preserved so workspaces with prior DIGen output don't have to regenerate)

Each generator hardcodes its own write path internally; the datagen Jinja
templates don't take `tpcdi_directory`. Only the benchmark workflow does.

## Data generator architecture

Entry point: `src/tools/spark_data_generator.py` (notebook). It calls
`spark_generate()` which orchestrates per-table generators living in
`src/tools/tpcdi_gen/`:

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
  Implements a **bijection-based scheduler** (numpy on the driver) that
  picks INACT/CLOSEACCT/UPDCUST/UPDACCT/ADDACCT victims via per-update
  `f(x) = (b*x + c) % modulus` with `b` coprime to `modulus` (collision-free
  within an update). The scheduler emits four numpy arrays
  (`_sched_update`, `_sched_action`, `_sched_pos`, `_sched_id`) that get
  Arrow-converted to a Spark DataFrame, then joined against the bijected
  `all_df` to assign final ActionTypes.
- `prospect.py` — Prospect.csv (B1/B2/B3). Slices B1 by `p_id` ordinal for
  B2/B3 incremental churn — does NOT use a `Window.orderBy()` (that forces
  single-partition shuffle).
- `market_data.py` — DailyMarket.txt (~5.5B rows at SF=10000).
- `watch_history.py` — WatchHistory.txt. Bijection-based pair selection
  over the customer × security cross-product. Per-update LCG with `bij_b`
  capped at `2^62 / max_pos` to keep `pos × b` in bigint range
  (Photon ANSI raises ARITHMETIC_OVERFLOW otherwise at SF≥20000).
- `trade.py` — Trade.txt + TradeHistory.txt + CashTransaction.txt +
  HoldingHistory.txt. Trade `t_ca_id = _va_idx.cast(string)` directly —
  the hash-derived index *is* the account ID (sequential 0..n_valid-1).
  Account pool size is analytical (`cfg.n_available_accounts`); no
  CustomerMgmt dependency.
- `audit.py` — emits `*_audit.csv` files. Two paths:
  1. **Static snapshot** at `static_audits/sf={SF}/` — pre-computed
     CSVs committed to the repo, copied into the volume after generation.
     Avoids recomputing exact counts on every run.
  2. **Dynamic regeneration** — when no static snapshot exists for the SF,
     audit module emits values from the `record_counts` dict that each
     generator populates during execution. No Spark scans needed; counts
     come from in-memory dict lookups + analytical formulas.

`utils.py` holds shared helpers: `disk_cache` (parquet staging on serverless,
persist+count on classic), `dict_join` / `dict_join_batch` for broadcast-style
dictionary lookups, `register_copies_from_staging` for parallel UC Volume
file copies with EAGAIN retry logic.

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

The benchmark is a 47-task workflow (`Shannon-Barrow-TPCDI-CLUSTER` job)
that ingests generated data, builds the silver/gold dimensional model, and
runs validation checks. Key SQL files under `src/incremental_batches/`:

- `bronze/*.sql` — file ingestion. Tables are read via `read_files(...
  fileNamePattern => "{Customer.txt,Customer_[0-9]*.txt}", schema => ...)`.
  The brace-alternation glob matches **both** generators' output: DIGen's
  single `Customer.txt` (no suffix) and Spark's split `Customer_1.txt`,
  `Customer_2.txt`, etc., while excluding `Customer_audit.csv`. FINWIRE is
  special — DIGen names like `FINWIRE2017Q3` (no extension, year+quarter)
  and Spark names like `FINWIRE_1.txt` are caught by `FINWIRE[12_][0-9]*`
  (the `[12_]` class accepts year-1, year-2, or `_`; the `[0-9]` requirement
  excludes `FINWIRE_audit.csv`).
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

## Common workflows

### Generate data for a scale factor
```
databricks jobs run-now --profile tpc-di \
  --json '{"job_id": 1017619735160060, "job_parameters": {"scale_factor": "10000", "regenerate_data": "YES"}}'
```
- Job `1017619735160060` (cloned, faster cold-start).
- Job `218882370760159` (original, has OOM-promotion flag — better for SF≥20000).
- `regenerate_data=YES` wipes the SF directory and rebuilds.
- These jobs target the **Spark** generator (notebook
  `tools/spark_data_generator`). For the **DIGen.jar** path, regenerate the
  datagen workflow from the Driver with `data_generator=digen` — that submits
  a different workflow (template `datagen_workflow_digen.json`) running on a
  classic single-node cluster, since Java subprocess can't run on serverless.

### Run the benchmark
```
databricks jobs run-now --profile tpc-di \
  --json '{"job_id": 798957941168162, "job_parameters": {"scale_factor": "10000"}}'
```
- Has `max_concurrent_runs=1` — second trigger while one is running gets
  SKIPPED with `MAXIMUM_CONCURRENT_RUNS_REACHED`.

### Regenerate audits without re-running datagen
Open `src/tools/regenerate_audits` notebook in workspace, set widgets,
run on serverless. Scans existing data files in the volume and rewrites
every `*_audit.csv` using DIGen's exact counter semantics.

### Check audit failures after a benchmark
```sql
SELECT * FROM main.shannon_barrow_tpcdi_cluster_${SF}.automated_audit_results
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
  tools/
    spark_data_generator.py       # Spark generator orchestrator notebook (default)
    data_generator.py             # DIGen.jar wrapper notebook (legacy path)
    generate_datagen_workflow.py  # builds the datagen workflow; dispatches spark|digen
    regenerate_audits.py          # standalone audit recompute notebook
    tpcdi_gen/
      audit.py                    # static-snapshot copy + dynamic regen
      config.py                   # ScaleConfig — all scaling constants
      customer.py                 # CustomerMgmt + Customer.txt + Account.txt
      finwire.py                  # FINWIRE quarterly files
      hr.py                       # HR.csv + _brokers temp view
      market_data.py              # DailyMarket.txt
      prospect.py                 # Prospect.csv (B1 + B2/B3 churn)
      reference.py                # static reference tables
      trade.py                    # Trade + TradeHistory + CashTransaction + HoldingHistory
      utils.py                    # shared helpers
      watch_history.py            # WatchHistory.txt with bijection
      static_audits/sf={SF}/      # pre-computed audit snapshots (committed)
    datagen/pdgf/                 # DIGen.jar + PDGF config (reference only)
    jinja_templates/              # job workflow templates
  incremental_batches/
    bronze/                       # file → staging table SQL
    silver/                       # SCD2 dimension builds
    gold/                         # fact tables
    audit_validation/             # automated_audit.sql, batch_validation.sql, audit_alerts.sql
    dw_init.sql                   # schema + Audit table bootstrap
  single_batch/SQL/               # all-batches-in-one variant of the pipeline
```

## Active branch

`augmented_incremental`. The `main` branch holds the original DIGen-based
pipeline. The `data_generator` widget merge is now in place on
`augmented_incremental` — Driver dispatches to either the Spark generator
or DIGen.jar based on the widget value. Eventual goal: merge
augmented_incremental → main.

## Status of validated scale factors

As of the most recent validation pass, the Spark generator + benchmark
audits pass at SF=10/100/1000/5000. SF=10000 / SF=20000 validation in
progress (see audit pass tally in conversation history).
