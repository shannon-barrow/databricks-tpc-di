# DLT Incremental Pipeline — Revision Change Log

Source: 14 dated workspace revisions of `dlt_incremental` between 2025-11-05 and 2025-12-11, plus current (`v99_current`). Files at `/tmp/dlt_incremental_revisions/`.

> **About the commented-out `_historical` blocks across v1–v14.** The notebook used to handle both historical backfill and per-day incrementals. The author would manually comment out the `_historical` flows or the `_incremental` flows depending on which mode they were running. So toggling `_historical` blocks comment-on/comment-off across these revisions reflects which mode the file was last set up to run in — not dead code. The eventual split into a separate `dlt_historical.sql` notebook is what made those blocks truly dead, hence v99_current's deletion.

## Per-revision change list

| # | Revision | Lines | Δ | What changed |
|---|---|---:|---:|---|
| 1 | `2025-11-05 13:48` | 580 | — | **Initial port.** All dim/fact targets defined; each has a parked `_historical` flow alongside its `_incremental` flow. factmarkethistory is a single MV with inline `min_by/max_by(...) OVER (ROWS BETWEEN 364 PRECEDING AND CURRENT ROW)` window functions. |
| 2 | `2025-12-07 16:11` | 584 | +4 | **Cosmetic.** 3 blank lines + `-- COMMAND ----------` separator added before factmarkethistory — workflow-toggle scaffolding for the next day. |
| 3 | `2025-12-08 09:31` | 600 | +16 | **factmarkethistory: split into staging + final.** New MV `factmarkethistorystg` materializes the 365-day window via a self-join + `min_by/max_by` aggregate. Final `factmarkethistory` rewired to read from staging. |
| 4 | `2025-12-08 09:48` | 658 | +58 | **factmarkethistory: try as STREAMING TABLE.** Final flipped MV → STREAMING TABLE; staging logic re-inlined as a `WITH dm AS (...)` CTE. Prior MV-pair form left commented in place. |
| 5 | `2025-12-08 10:15` | 663 | +5 | **Revert to MV-pair.** ST attempt commented out (with new `Streaming Table Version of the Code` DBTITLE label); MV staging+final re-activated. Net same as #3. |
| 6 | `2025-12-08 10:46` | 722 | +59 | **factmarkethistory entirely commented out.** Adds another commented variant with `TBLPROPERTIES('delta.enableDeletionVectors'=true)`. No active factmarkethistory exists in this revision — used to run upstream tables only. |
| 7 | `2025-12-08 11:04` | 725 | +3 | **Re-activate MV pair, add deletion vectors.** Staging MV gains `TBLPROPERTIES('delta.enableDeletionVectors'=true)` permanently. |
| 8 | `2025-12-08 14:17` | 746 | +21 | **Re-activate ST inline-CTE form** + add a `/* */` block comment with the SDP error JSON (`PLAN_NOT_DETERMINISTIC` flagged on `MinBy`/`MaxBy`). Confirms why incrementalization is failing. |
| 9 | `2025-12-10 14:01` | 782 | +36 | **Staging MV → STREAMING TABLE.** factmarkethistorystg flipped to STREAMING TABLE with `STREAM(bronzedailymarket)`. Final ST inline-CTE form from #8 commented out. |
| 10 | `2025-12-11 11:11` | 815 | +33 | **Staging table radically rewritten.** `min_by/max_by` self-join thrown out; staging now produces per-symbol `array_sort(collect_list(struct(date, high, low)))`. Adds debug filter `WHERE dm_date IN ('2017-06-03','2017-06-04')` to test a single batch. Final factmarkethistory re-activated as STREAMING TABLE consuming `STREAM(factmarkethistorystg)`. |
| 11 | `2025-12-11 11:14` | 815 | 0 | **One-character tweak.** `date_sub(min(dm_date), 365)` → `date_sub(min(dm_date), 380)`. Debug-window tuning. |
| 12 | `2025-12-11 11:23` | 810 | -5 | **CTE wiring cleanup.** Drops the inline `WITH dm AS (...)` + `min_dt/max_dt` join; replaces with `WHERE dm_date >= (SELECT date_sub(max(dm_date), 380) FROM bronzedailymarket)`. Strips a ~36-line commented MV-staging variant. |
| 13 | `2025-12-11 11:26` | 876 | +66 | **Wire array-shaped staging into final.** factmarkethistory rewritten to consume the staging array via `filter(date_high_low, x -> x.dm_date BETWEEN ... AND ...)`, then `array_max / array_min / array_position` for 52-week extrema. Three commented-out prior variants kept as graveyard. |
| 14 | `2025-12-11 11:28` | 869 | -7 | **Inline simplification.** Intermediate "dm" CTE that pre-extracted `array_max/array_min/array_position` is dropped; those operations inlined into the outer SELECT. |
| 15 | `v99_current` | 476 | **-393** | **Sweeping cleanup.** All commented `_historical` blocks deleted (DimCustomer ~37 lines, DimAccount ~21, DimTrade ~28, FactHoldings ~18, FactWatches ~17). All commented factmarkethistory experiments deleted (~150+ lines). Debug `WHERE dm_date IN (...)` filter removed. Diagnostic JSON comment removed. **One semantic flip:** factmarkethistorystg reverted STREAMING TABLE → MATERIALIZED VIEW (final factmarkethistory still streams from bronze, batch-joined to the staging MV). |

## Per-target-table summary

### Untouched across all 15 revisions (v99 only deletes their `_historical` blocks)
- **DimCustomer** — STREAMING TABLE, SCD2 via `AUTO CDC INTO` from `STREAM(bronzecustomer)` joined to TaxRate twice. Phone1/2/3 string concat. No logic change ever.
- **DimAccount** — STREAMING TABLE, SCD2 with two flows: `dimaccount_incremental` from `STREAM(bronzeaccount)`, plus `account_updates_from_customers` to handle customer surrogate-key shifts. No logic change ever.
- **DimTrade** — STREAMING TABLE, SCD1 via `AUTO CDC INTO` with `IGNORE NULL UPDATES`, joined to dimaccount + dimsecurity (effective-date). No logic change ever.
- **FactHoldings** — STREAMING TABLE, append-only `INSERT INTO BY NAME`, joined to dimtrade. No logic change ever.
- **FactWatches** — STREAMING TABLE, SCD1, joined to dimsecurity + dimcustomer (current). Cosmetic whitespace touch in #2; otherwise unchanged.
- **DailyTransactionsTotals** — MATERIALIZED VIEW, sums `ct_amt` from bronzecashtransaction. Untouched.
- **FactCashBalances** — MATERIALIZED VIEW, joins dailytransactionstotals to dimaccount + computes running total. Untouched.

### Heavily reworked

**FactMarketHistory + FactMarketHistoryStg** — the only target with real logic churn. Net before/after:

| Aspect | v1 (11-05) | Final (v99) |
|---|---|---|
| Staging table | none | `factmarkethistorystg` MV (pre-aggregates 380-day window of `array_sort(collect_list(struct(date,high,low)))` per symbol) |
| Final table type | MATERIALIZED VIEW | STREAMING TABLE |
| 52-week computation | `min_by/max_by(...) OVER (ROWS BETWEEN 364 PRECEDING AND CURRENT ROW)` | `array_max(filter(...))` + `array_min(filter(...))` + `array_position()` |
| Source consumption | direct `bronzedailymarket` | `STREAM(bronzedailymarket)` joined to staging MV |
| Driver of change | — | `min_by/max_by` flagged `PLAN_NOT_DETERMINISTIC` by SDP incrementalization analyzer |

## Quick categorization of each transition

- **Real architectural changes:** #3 (intro staging table), #4 (try ST), #8 (diagnostic-driven re-attempt), #9 (staging → ST), #10 (array_sort restructure), #13 (array filter/array_max in final), #15 (cleanup + ST→MV revert on staging).
- **Reverts:** #5 (back to MV pair), #14 (drop intermediate CTE).
- **Run-mode workarounds (not architecture):** #2 (cosmetic), #6 (everything commented to skip factmarkethistory), #11 (debug 365→380), #12 (CTE rewrite), #15's `_historical` deletions.
- **Parameter sweep day:** Most of 12-08 (#3–#8) — toggling MV/ST and CTE/staging permutations, leaving each prior attempt commented in place. Net at end of day: roughly back where #3 started, with deletion vectors added.
- **Real progress day:** 12-11 (#10–#14) — direct response to the `PLAN_NOT_DETERMINISTIC` blocker from #8.
