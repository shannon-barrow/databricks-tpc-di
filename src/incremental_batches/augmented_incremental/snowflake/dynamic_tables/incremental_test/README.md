# DT incremental-refresh test harness

Empirical test of which `OVER` patterns Snowflake will incrementalize for
FactMarketHistory-shaped queries (52-week rolling MIN/MAX).

## Test DTs (REFRESH_MODE = AUTO; Snowflake picks)

| DT | Pattern |
|---|---|
| A — `fmhtest_a_no_window` | no window (control) |
| B — `fmhtest_b_minmax_partition` | `MIN/MAX OVER (PARTITION BY sym)` |
| C — `fmhtest_c_minmax_orderby_default_frame` | `MIN/MAX OVER (PARTITION BY sym ORDER BY date)` — default RANGE frame |
| D — `fmhtest_d_minmax_sliding_52w` | `MIN/MAX OVER (... ROWS BETWEEN 364 PRECEDING AND CURRENT ROW)` — the FMH shape |
| E — `fmhtest_e_minby_partition` | `MIN_BY/MAX_BY OVER (PARTITION BY sym)` — tests 2026-03-19 GA |
| F — `fmhtest_f_minby_sliding_52w` | `MIN_BY/MAX_BY OVER (... ROWS BETWEEN 364 PRECEDING AND CURRENT ROW)` — natural FMH shape |

## Run order

```sh
snow sql -c tpcdi_kp -f 01_setup_source.sql        # ~1.4M rows, < 2016-01-01
snow sql -c tpcdi_kp -f 02_create_test_dts.sql     # blocks on each INITIALIZE
snow sql -c tpcdi_kp -f 03_check_refresh_modes.sql # what AUTO picked + why
snow sql -c tpcdi_kp -f 04_load_one_day.sql        # +1 day of source rows
# wait ~90s for the 1-minute scheduler to fire refreshes
snow sql -c tpcdi_kp -f 05_check_refresh_history.sql
snow sql -c tpcdi_kp -f 99_cleanup.sql             # drop everything
```

## What to look for in step 03

- `refresh_mode = INCREMENTAL` → that pattern can be maintained incrementally.
- `refresh_mode = FULL` with `refresh_mode_reason` populated → that's the
  exact string Snowflake uses to explain why it fell back. Capture it.

## What to look for in step 05

- `refresh_action = 'INCREMENTAL'` and `rows_inserted ~ 7434` (1 day × N symbols)
  → small delta only.
- `refresh_action = 'REINITIALIZE'` and `rows_inserted ~ full output size`
  → FULL refresh, recomputed everything.
- `wall_ms` quantifies the cost difference between AUTO-chosen INCREMENTAL
  and FULL for the same data shape.
