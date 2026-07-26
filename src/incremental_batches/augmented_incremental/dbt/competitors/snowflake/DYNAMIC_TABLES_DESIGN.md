# Snowflake Dynamic Tables — variant design

Pre-implementation notes for a **new** Snowflake variant that uses
Dynamic Tables as the pipeline engine — **no dbt in the loop**.
Replaces the existing dbt-Snowflake variant for the purpose of testing
Snowflake's declarative-pipeline product head-to-head against Databricks
SDP (Spark Declarative Pipelines).

## Scope decision: no dbt

The 16 transformations are expressed directly as `CREATE DYNAMIC TABLE`
statements, created once at variant setup time. dbt is not on the
critical path. We lose Jinja (`ref()`, `source()`, `var()`), `dbt test`,
dbt docs — none of which the benchmark needs. What we measure is the
engine, not the orchestrator.

- `dbt_task` is removed from the workflow.
- `run_dbt.py`, `profiles.yml.template`, and the `snowflake_models/`
  tree are not used by this variant. (Existing dbt variant stays as-is
  for the dbt-on-Snowflake comparison.)
- New files live under `snowflake/dynamic_tables/`:
  - `dt_create.sql` — all 22 `CREATE OR REPLACE DYNAMIC TABLE`
    statements, run once per setup.
  - `dt_wait_refresh.py` — synchronous trigger / poll helper called
    each iteration of the for_each loop.

## Why this is interesting

The SDP variant on Databricks is the "no orchestration, just declare the
pipeline" benchmark. Dynamic Tables are Snowflake's equivalent. The whole
per-batch loop (for_each, simulate_filedrops, dbt child job) collapses
into:

1. A raw source table that gets new rows appended each batch
2. A DAG of dynamic tables that Snowflake refreshes on TARGET_LAG

Measured metric changes from "per-batch wall" to **end-to-end freshness**:
file lands → all gold tables reflect it. Closer to a streaming benchmark.

## What Dynamic Tables can / can't incremental-refresh

Cross-checked from the [supported queries docs](https://docs.snowflake.com/en/user-guide/dynamic-tables-supported-queries):

| Construct | Incremental? | Used in our pipeline? |
|---|---|---|
| INNER JOIN, CROSS JOIN, self-join | ✅ | Yes |
| LEFT/RIGHT/FULL OUTER JOIN with equality predicates | ✅ | factmarkethistory left-joins companyyeareps on equality (good) |
| OUTER JOIN with **non-equality / range** predicates | ❌ → FULL refresh | None of ours; dimsecurity range join is INNER (still need to verify INNER also incrementalizes range) |
| GROUP BY MIN, MAX, COUNT, SUM, AVG | ✅ | All gold facts |
| **MIN_BY, MAX_BY** | ✅ | **factmarkethistory** — the 1390s query |
| ROW_NUMBER, RANK, DENSE_RANK, FIRST/LAST/NTH_VALUE | ✅ | SCD logic via QUALIFY |
| OBJECT_CONSTRUCT, STRUCT-like values inside MIN_BY | ✅ (documented as part of MIN_BY support) | factmarkethistory |
| UNION ALL | ✅ | Not currently used |
| UNION (DISTINCT) | ❌ | Not used |
| ORDER BY / LIMIT / SAMPLE / PIVOT | ❌ | Not at top level |
| Subqueries in FROM | ✅ | Yes (CTEs) |
| Correlated subqueries / `WHERE EXISTS` | ❌ | Not used |
| Window with PERCENT_RANK or sliding-frame RANK | ❌ | Not used |
| CURRENT_TIMESTAMP/CURRENT_DATE in **WHERE/HAVING** | ✅ | n/a |
| CURRENT_TIMESTAMP in SELECT | non-deterministic — forces FULL | We don't need this |
| **`var('batch_date')` Jinja substitution** | n/a (no dbt; substitution becomes a real column / param) | Pattern changes — see below |
| MERGE / DML | n/a — dynamic tables are read-only declarations | Forces rewrite of all silver/gold |

## Per-model port (16 transformations)

All models below are expressed as `CREATE OR REPLACE DYNAMIC TABLE` —
no dbt config blocks, no Jinja, no `{{ ref() }}`. Cross-table references
are bare Snowflake FQNs (`{catalog}.{schema}.{table}`).

### Bronze (7) — straightforward

Bronze can't read directly from a stage file in a dynamic table (the
"source" must be a regular table or another dynamic table). The plumbing
changes:

- `simulate_filedrops_sf.py` no longer triggers dbt. Instead, a new
  `seed_raw.py` task `COPY INTO`s the day's CSV into a *raw* table
  (`bronzewatches_raw`).
- The bronze dynamic table is `CREATE DYNAMIC TABLE bronzewatches AS
  SELECT * FROM bronzewatches_raw` — dedup via `QUALIFY ROW_NUMBER()`
  to handle batch-replay (Snowflake doesn't otherwise know the COPY is
  idempotent).
- Effectively the raw table is the "stream source" and the bronze
  dynamic table is the first link in the DAG.

The `since_last_load` dbt macro is gone — dynamic tables track changes
via underlying streams natively.

### Silver SCD1 (dimcustomer, dimaccount) — rewrite from MERGE → QUALIFY

Current pattern (MERGE keyed on customerid/accountid). Replace with:

```sql
CREATE DYNAMIC TABLE dimcustomer
  TARGET_LAG = '1 minute'
  WAREHOUSE  = BARROW_MED_GEN2
AS
SELECT … cols …
FROM bronzecustomer
QUALIFY ROW_NUMBER() OVER (PARTITION BY customerid ORDER BY update_dt DESC, cdc_dsn DESC) = 1
```

That's it — Snowflake maintains the "latest row per customerid"
incrementally. Same logical contract as the existing MERGE.

### Silver SCD2 (dimtrade) — also QUALIFY-able

The dbt model uses `max_by(object_construct(...), t_dts)` to pick the
latest-state-per-tradeid, then derives sk_closedateid. Same pattern in
a dynamic table — QUALIFY ROW_NUMBER() to pick the latest, then add the
derived close_ts logic. The `incremental_predicates: ['DBT_INTERNAL_DEST.sk_closedateid IS NULL']`
that prunes closed trades in dbt MERGE has no direct analog in dynamic
tables, but it doesn't need one — the dynamic table only processes
**changed** tradeids each refresh, so closed trades aren't touched
unless a new row arrives for them.

### Silver factwatches — QUALIFY

Per-watch latest action. Identical pattern to dimcustomer.

### Gold factmarkethistory — **biggest potential win**

Current per-batch cost: 1390s scan of 365 days × 16M securities = 5.8B
rows. Reason: dbt MERGE rebuilds the aggregate from scratch every batch.

Dynamic table semantics: `MIN_BY` is documented as incremental-refresh
eligible. **If** the rolling 365-day filter (`dm_date > dateadd(day,
-365, batch_date)`) can be expressed in a way Snowflake recognizes as
incrementally maintainable, this query becomes O(new rows) instead of
O(365 days). Expected per-refresh cost: seconds instead of minutes.

**Caveat — needs benchmarking**: Sliding-window aggregates (where rows
age OUT as well as IN) traditionally require negative-tuple support.
Snowflake's incremental engine handles MIN/MAX over inserts cleanly,
but the eviction case (last year's value drops out) might force the
table into FULL refresh mode. Concretely:

- Try formulation A: `WHERE dm_date BETWEEN dateadd(-365, current_date) AND current_date`.
  Sliding lower bound is moving — likely FULL refresh.
- Try formulation B: drop the lower bound entirely; let bronze data
  expire via separate retention. Aggregate becomes "min of all time"
  per symbol. Semantically different from 52-week, but incremental.
- Try formulation C: precompute a `daily_market_low_running` dynamic
  table that holds per-symbol-per-day min-since-day. Final fact joins
  to that. Same architecture as the "rolling pre-aggregation" option
  in the FMH perf discussion — but Snowflake owns the refresh.

The right formulation depends on whether we want 52-week-exact (worth
working harder for) or all-time-min (much simpler).

### Gold factcashbalances / factholdings — straightforward

Both aggregate cashtransaction / holding rows per account. Pure GROUP
BY SUM-style aggregation, fully incremental-eligible.

### Gold currentaccountbalances — change CREATE-OR-REPLACE → dynamic

Currently `insert_overwrite` (full rewrite each batch). Dynamic table:
just declare the same aggregate, let Snowflake maintain it.

## TARGET_LAG and orchestration

Realistic settings for the benchmark:

```sql
CREATE DYNAMIC TABLE bronzewatches
  TARGET_LAG    = '1 minute'
  WAREHOUSE     = BARROW_MED_GEN2
  REFRESH_MODE  = INCREMENTAL
AS SELECT * FROM bronzewatches_raw;

CREATE DYNAMIC TABLE factwatches
  TARGET_LAG    = DOWNSTREAM     -- triggered when downstream needs it
  WAREHOUSE     = BARROW_MED_GEN2
  REFRESH_MODE  = INCREMENTAL
AS SELECT … FROM bronzewatches … QUALIFY ROW_NUMBER() = 1;
```

`TARGET_LAG = DOWNSTREAM` on intermediate dynamic tables means they
only refresh when something downstream of them needs a fresh value —
gives Snowflake room to fuse refresh steps. The leaf gold tables get a
concrete lag like `'1 minute'`.

For the benchmark, the parent job's loop simplifies dramatically:

| Today (per-batch dbt) | Dynamic Tables variant |
|---|---|
| `for batch_date in dates:` | same loop |
| 1. `setup_sf` (CLONE staging + 22 DT DDLs — ~15s) | 1. Same, plus run `dt_create.sql` (one-shot DDL for all dynamic tables) |
| 2. `simulate_filedrops` (cp files) | 2. Same |
| 3. `seed_staging` (COPY INTO bronze raw) | 3. Same |
| 4. `dbt run --target snowflake` (770s on Med GEN2) | 4. **Removed.** Replaced by `dt_wait_refresh.py`: synchronously `ALTER DYNAMIC TABLE <leaf> REFRESH` on each gold table (or just the leaves and let DOWNSTREAM cascade), then poll `INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY` until all refreshes report `SUCCEEDED` for this batch's data. |
| 5. Audit | 5. Same |

End-to-end wall per batch = file copy + COPY INTO bronze_raw + dynamic
table refresh cascade. If the cascade incrementalizes well (especially
the FMH win), this should be substantially faster than 770s/batch.

## Cost attribution

Dynamic tables don't carry `QUERY_TAG` the way dbt-driven queries do —
refresh queries are issued by the Snowflake service, not the user
session. Use instead:

- `INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY` — per-refresh
  warehouse credits, rows scanned, refresh start/end. This is the
  per-table equivalent of our existing per-batch task time extract.
- `ACCOUNT_USAGE.QUERY_HISTORY` — refresh queries appear with
  `QUERY_TAG` populated by Snowflake to the dynamic table FQN. Useful
  for digging into specific slow refreshes.
- `ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY` — bills against whatever
  warehouse the dynamic table's `WAREHOUSE = …` points to. Set a
  dedicated `BARROW_DT_MED` warehouse so the attribution matches today's
  variant-per-warehouse pattern.

## What's hard / risky

1. **factmarkethistory sliding-window incremental refresh** — biggest
   potential win, biggest risk of falling back to FULL. Worth a 1-day
   spike to test on SF=1k before committing.
2. **No MERGE semantics → SCD rewrites** — all 4 silver and 4 gold
   models lose their MERGE definitions. QUALIFY ROW_NUMBER() is
   straightforward but the conversion is tedious and easy to get wrong
   for the close_ts / iscurrent / enddate logic on dimtrade.
3. **Bronze loses re-run guard** — the dbt `since_last_load` macro
   becomes irrelevant (dynamic tables track changes via stream), but
   it also means the existing rerun-safety pattern is gone. If a
   batch's CSV is re-COPYed, you'll double-count unless you load into
   a per-batch raw table per day and `UNION ALL` them. Cleanest
   approach: single raw table, dedup via QUALIFY on bronze dynamic
   table.
4. **Determinism check** — the SCD2 rewrite has to produce exactly the
   same DimTrade rows the MERGE variant does, or downstream audits
   fail. Plan a parity validation against dbt output before declaring
   the variant valid.
5. **No `--vars batch_date` substitution** — dynamic tables are SQL
   text fixed at CREATE time. Anywhere the dbt models do
   `cast('{{ var("batch_date") }}' as date)`, the dynamic table has to
   either drop the filter (rely on the source stream's natural
   per-batch deltas) or use `CURRENT_DATE` (only allowed in
   WHERE/HAVING/QUALIFY). The model logic is rewritten to "all rows
   from the source table — Snowflake figures out the deltas."

## Suggested implementation order

1. **Plumbing rebuild**: new `seed_raw.py` → COPY INTO `bronze*_raw`
   tables. Replaces the seed-into-dbt-source pattern.
2. **`dt_create.sql`** — all 22 dynamic table DDLs in dependency
   order. Idempotent (CREATE OR REPLACE).
3. **Bronze dynamic tables** (1-1 SELECT + QUALIFY for dedup) — sanity
   check the DAG fires.
4. **Silver SCD1** (dimcustomer, dimaccount) — QUALIFY pattern.
5. **factcashbalances, factholdings, currentaccountbalances** —
   straightforward aggregates.
6. **factwatches** — QUALIFY pattern.
7. **dimtrade** — SCD2 rewrite, hardest determinism case.
8. **factmarkethistory** — spike on incremental refresh, decide
   formulation (A/B/C above).
9. **`dt_wait_refresh.py`** — synchronous refresh trigger + poll
   helper.
10. **Orchestrator** — `augmented_snowflake_dt.py` workflow builder.
    Setup_sf stays (clone staging + run `dt_create.sql`),
    simulate_filedrops stays, `seed_raw` replaces `seed_staging`,
    `dt_wait_refresh` replaces `run_dbt`.

## Open questions for later

- Same dedicated WH per dynamic table, or one shared "DT" warehouse?
  Per-table is the cleanest cost attribution but spins up multiple
  WHs. Shared is cheaper but conflates refresh queues.
- For the benchmark, do we trigger refresh manually via `ALTER … REFRESH`
  for deterministic per-batch measurement, or rely on TARGET_LAG and
  measure end-to-end latency?
- Can the variant share `STAGING_SF{sf}` clone-and-reuse pattern? Yes
  — dimsecurity/dimcompany/dimaccount static tables stay as cloned
  regular tables; only the streaming-update tables become dynamic.

## References

- [Dynamic Tables overview](https://docs.snowflake.com/en/user-guide/dynamic-tables-about)
- [Supported queries for incremental refresh](https://docs.snowflake.com/en/user-guide/dynamic-tables-supported-queries)
- [Refresh modes / TARGET_LAG](https://docs.snowflake.com/en/user-guide/dynamic-tables-refresh)
- [Dynamic Tables + dbt blog (Snowflake/Medium, 2026)](https://medium.com/snowflake/dynamic-tables-dbt-a-powerful-combination-f550ebc23d60)
- [Comparison: Dynamic Tables vs dbt incremental](https://www.metaplane.dev/blog/snowflake-dynamic-tables-vs-dbt)
- [CDC patterns in Snowflake (Streams/Tasks/Dynamic Tables)](https://medium.com/snowflake/mastering-cdc-in-snowflake-change-tracking-merge-streams-tasks-and-dynamic-tables-c9eab9f0497a)
