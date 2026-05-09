# Augmented Incremental: 730 → 365-day window refactor

## Why
1. **Never finish 730.** Two years of daily batches takes too long on every variant; we always cut early.
2. **Steady-state inconsistency.** FactMarketHistory looks back 365 days for 52-week high/low. With benchmark start = 2015-07-06 (= DailyMarket data start), batches 1..364 have only partial-window lookback. The benchmark only hits "real" workload from batch 365 onward — exactly when we usually stop running.

## What changes

Shift the benchmark window so **all incremental tables are at steady-state from batch 1**:

| Boundary | Before | After |
|---|---|---|
| `AUG_FILES_DATE_START` | 2015-07-06 | **2016-07-06** |
| `AUG_FILES_DATE_END_EXCL` | 2017-07-05 | 2017-07-05 (unchanged) |
| `AUG_FILES_DAYS` | 730 | **365** |
| Default `incremental_batches_to_run` cap | 730 | **365** |

The first year of generator output (DailyMarket, Trade, etc. for 2015-07-06 → 2016-07-05) becomes "historical" load instead of daily file drops. Setup pre-stages it; the benchmark consumes a populated state from batch 1.

## Impact by dataset

`stg_target` partitioning shifts: rows in `[2015-07-06, 2016-07-06)` move from `'files'` (daily drops) → `'tables'` (historical load).

| Dataset | New historical content | New historical loader |
|---|---|---|
| **DailyMarket** | year of pre-window prices | NEW `DailyMarketHistorical.sql` |
| **FactMarketHistory** | computed FMH for that year using rolling 365-day window | NEW `FactMarketHistoryHistorical.sql` |
| Customer/Account/Trade/Holdings/CashTransaction/Watches | shifts year of incremental → historical | existing historical files (no logic change, just larger inputs) |

## File-level changes

### Generator (Phase 1 — start here)
- `src/tools/tpcdi_gen/config.py`
  - `AUG_FILES_DATE_START = "2016-07-06"`
  - `AUG_FILES_DAYS = 365`
  - Update commentary
- `src/tools/tpcdi_gen/market_data.py:152-157`
  - Currently labels everything `'files'` because DM_BEGIN_DATE = AUG_FILES_DATE_START.
  - Switch to standard `when(dm_date < AUG_FILES_DATE_START, 'tables').otherwise('files')`.
- `src/tools/tpcdi_gen/trade_split.py:675-676`
  - `cutoff_2015 = int(datetime(2016, 7, 6).timestamp())` — rename or just retarget the constant.
  - All other generators (customer, watch_history, prospect, the other trade splits) already key off `AUG_FILES_DATE_START` via the config import — no change needed in their bodies.

### Setup notebooks
- `src/incremental_batches/augmented_incremental/setup.py:13,19`: 730 → 365
- `src/incremental_batches/augmented_incremental/setup_dbt.py:38,44`: 730 → 365
- `src/incremental_batches/augmented_incremental/DLT/pipelines_setup.py:13,19`: 730 → 365
- All three loops `range(0, n_batches)` already start at `2016, 7, 6` (after we update the start_date)

### Historical loaders (NEW)
- `src/incremental_batches/augmented_incremental/historical/DailyMarketHistorical.sql`
  - Reads `tpcdi_raw_data.dailymarket{sf}` where `stg_target='tables'`
  - Writes to `{staging}.bronzedailymarket_historical` (or whatever name SDP can append-flow from)
  - For Classic: writes to `{wh_db}_{sf}.bronzedailymarket` directly (becomes the seed)
- `src/incremental_batches/augmented_incremental/historical/FactMarketHistoryHistorical.sql`
  - Computes FMH for `[2015-07-06, 2016-07-05]` using same windowed logic as the incremental
  - Writes to `{wh_db}_{sf}.factmarkethistory` for Classic, plus `{staging}.factmarkethistoryhistorical` for SDP clone
  - May also need `{staging}.factmarkethistorystghistorical` (the per-symbol array MV state as of 2016-07-05) for SDP

### SDP DLT (per-pipeline)
- `dlt_ingest_bronze.py` (and `_modclust` clone)
  - Add `bronzedailymarket_backfill` append_flow_once that loads from `{staging}.bronzedailymarketstaging`
  - (already has `bronzecashtransaction_backfill` as the template)
- `dlt_historical.sql` (and `_modclust` clone)
  - Add `factmarkethistory` declaration with `INSERT INTO ONCE` flow pointing at staging
  - May add `factmarkethistorystg` if we need to seed the MV state
- `pipelines_setup.py` (and `_modclust` clone)
  - Add new historical-staged tables to `shallow_tbls` clone list

### dbt
- `src/dbt_augmented_incremental/setup_dbt.py`: clone the new staged FMH + bronzedailymarket-historical into the run schema
- `src/dbt_augmented_incremental/models/`: factmarkethistory's `bronzedailymarket` reference works fine if bronze is pre-seeded with the year of historical data

### Audit / regen
- Static audit snapshots at `src/tools/tpcdi_gen/static_audits/sf={SF}/` — per-batch counts unchanged (daily activity rate didn't change), but TOTALS over the benchmark window will be smaller. Need to regen at SF=20k after this.

### Workflow builders
- No changes to the *jobs* themselves — they already accept `incremental_batches_to_run` as a parameter. Just lower the defaults in the parent specs.

## Order of execution

1. **Phase 1 — Generator config + market_data + trade_split** (today, since the user said this won't disrupt running benchmarks).
2. **Phase 2 — Historical loaders** (new SQL files for DailyMarket / FMH historical).
3. **Phase 3 — SDP DLT updates** (bronze backfill, dlt_historical FMH flow).
4. **Phase 4 — Setup notebooks** (clone lists, batch caps).
5. **Phase 5 — Regen at SF=20k + new static audits**.

## Open questions
- For SDP factmarkethistorystg pre-seeding: the existing MV does `where dm_date >= max(dm_date) - 380`. If `bronzedailymarket_backfill` seeds the table with the prior year of data, the MV will compute the right window from batch 1. **Probably no extra staging needed for factmarkethistorystg itself — just bronzedailymarket.** Need to verify.
- Total wall-clock for full 365 batches at SF=20k: classic ~7 min/batch × 365 ≈ 43 hours; dbt Medium ~3.5 min/batch × 365 ≈ 21 hours; dbt Small ~3.5 × 365 ≈ 21 hours; SDP ~5-9 min × 365 ≈ 30-55 hours. Tighter than 730 but still long — consider whether to keep `incremental_batches_to_run` overridable for smoke tests (yes, leave that mechanism in place).
