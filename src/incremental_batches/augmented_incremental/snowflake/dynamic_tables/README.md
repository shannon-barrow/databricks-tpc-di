# Snowflake Dynamic Tables variant — runtime files

The "no dbt" Snowflake variant. Tests Snowflake's declarative-pipeline
product (Dynamic Tables) head-to-head against Databricks SDP. Design
rationale lives in the sibling `../DYNAMIC_TABLES_DESIGN.md`.

## File map

| File | Role |
|---|---|
| `dt_create.sql` | One-shot DDL: 16 `CREATE OR REPLACE DYNAMIC TABLE` statements covering the same model surface as the dbt variant. Uses Python str.format placeholders (`{catalog}`, `{schema}`, `{warehouse}`, `{target_lag}`) — `setup_sf_dt.py` substitutes and executes. |
| `setup_sf_dt.py` | Run **once per parent run**. Clones the 14 reference / dim tables from `STAGING_SF{sf}`, pre-creates the 6 `bronze*_raw` tables, executes `dt_create.sql`, emits `batch_date_ls` for the for_each loop. |
| `seed_raw.py` | Run **per batch**. `COPY INTO` each `bronze*_raw` table from the day's `@stage/.../{batch_date}/*.txt` files. Also appends today's DailyMarket.txt rows into the cloned `bronzedailymarket` table. |
| `dt_wait_refresh.py` | Run **per batch**, after `seed_raw`. Triggers `ALTER DYNAMIC TABLE <leaf> REFRESH` on the 4 leaf gold DTs (factwatches, factcashbalances, factholdings, factmarkethistory) which cascades through DOWNSTREAM intermediates. Polls `INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY` until terminal state. Per-leaf durations emitted as a task value for downstream attribution. |

## DAG

```
setup_sf_dt        (once)
  CLONE 14 reference tables
  CREATE 6 bronze_raw tables
  CREATE 16 DYNAMIC TABLEs
  emit batch_date_ls
       │
       ▼ for each batch_date:
  ┌─ simulate_filedrops_sf  (copy .txt → external volume)
  ├─ seed_raw               (COPY INTO bronze_raw tables, idempotent)
  └─ dt_wait_refresh        (ALTER REFRESH + poll history)
```

## Creating the jobs

Mirrors the dbt-snowflake variant's posting pattern — there's no
central driver for the Snowflake variants today, so jobs are created
manually via the Databricks API:

```python
import json, requests
from tools.workflow_builders import augmented_snowflake_dt as dt

# child first
child_spec = dt.build_child(
    job_name="TPCDI-SF20000-AugmentedIncremental-SF-DT-Child",
    repo_src_path="/Workspace/Users/shannon.barrow@databricks.com/databricks-tpc-di-augmented/src",
    catalog="TPCDI_TEST",
    scale_factor=20000,
    tpcdi_directory="/Volumes/main/tpcdi_raw_data/tpcdi_benchmarking/",
    wh_db="shannon_barrow_augmentedincremental_sf_dt",
    snowflake_warehouse="BARROW_MED_GEN2",
    interactive_cluster_id="<cluster-id-here>",
)
# POST child_spec via Databricks jobs/create API, get back child_job_id

# then parent referencing child_job_id
parent_spec = dt.build_parent(
    job_name="TPCDI-SF20000-AugmentedIncremental-SF-DT-Parent",
    child_job_id=<child_job_id>,
    repo_src_path="/Workspace/Users/shannon.barrow@databricks.com/databricks-tpc-di-augmented/src",
    catalog="TPCDI_TEST",
    scale_factor=20000,
    tpcdi_directory="/Volumes/main/tpcdi_raw_data/tpcdi_benchmarking/",
    wh_db="shannon_barrow_augmentedincremental_sf_dt",
    snowflake_warehouse="BARROW_MED_GEN2",
    target_lag="1 minute",
    interactive_cluster_id="<cluster-id-here>",
)
# POST parent_spec via Databricks jobs/create API
```

## Prereqs (one-time)

Same as the dbt-snowflake variant:
- Databricks secret scope `tpcdi_snowflake` populated (account, user, role,
  warehouse, private_key PEM).
- `STAGING_SF{sf}` schema seeded by `../onetime_stg_snowflake_tables.py`.
- Snowflake `STORAGE INTEGRATION` + `STAGE TPCDI_TEST.{wh_db}_{sf}.TPCDI_STAGE`.
- UC external volume `main.tpcdi_raw_data.tpcdi_benchmarking`.
- Interactive cluster ID for the orchestration tasks (no Snowflake compute
  runs there; just notebooks dispatching SQL).

## Open verifications (do at first run)

1. **dimcustomer / dimaccount LAST_VALUE-with-offset-frame** — verify
   Snowflake accepts this in incremental refresh mode. If it falls
   back to FULL, swap to MIN(update_dt) OVER (... ROWS BETWEEN 1
   FOLLOWING AND UNBOUNDED FOLLOWING) which uses an unbounded suffix
   frame (which IS in the supported list).

2. **factmarkethistory MIN_BY/MAX_BY with 364-preceding-row window** —
   biggest risk. If incremental, this single change wins back the
   1390s-per-batch FMH scan. If it falls back to FULL, swap in
   formulation A (WHERE filter to 365-day window) or C (drop the
   52-week semantics — all-time MIN/MAX) per the design doc.

3. **MIN_BY accepting OBJECT_CONSTRUCT inside a window frame** — the
   docs explicitly list MIN_BY/MAX_BY as supported aggregates; this
   formulation uses MIN_BY as a window function. If syntax error,
   fall back to per-day aggregate join pattern.

4. **dimtrade MAX_BY-per-tradeid** — used as aggregate (not window).
   This is a standard pattern documented as supported. Should work
   first try; if not, swap to QUALIFY ROW_NUMBER on full row +
   downstream join.

5. **bronze* QUALIFY ROW_NUMBER dedup with PARTITION BY cdc_dsn** —
   should incrementalize cleanly because cdc_dsn is unique per CDC
   event (no actual dedup work needed in practice; just a defensive
   measure for batch replay).

Watch `INFORMATION_SCHEMA.DYNAMIC_TABLE_GRAPH_HISTORY` after `setup_sf_dt`
to see the chosen REFRESH_MODE per DT — if any went to FULL, that's
where to investigate.
