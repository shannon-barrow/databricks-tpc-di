#!/usr/bin/env python3
"""Local BQ INFORMATION_SCHEMA queries for the augmented-incremental BigQuery
benchmark. Pulls per-batch / per-model / live activity / cost rollups from
`region-us-central1.INFORMATION_SCHEMA.JOBS_BY_PROJECT` via the local
`bq` CLI (which uses your gcloud user creds).

No Databricks workspace dependency — runs purely against BigQuery's
INFORMATION_SCHEMA from your laptop.

Conventions used to scope queries to "our" work:
- `dataset_id` of the destination_table is `{wh_db}_{scale_factor}` (e.g.
  `shannon_aug_bq_dbt_10`). The dbt models, the bronze pre-creates, and the
  setup_bq DDLs all write into this single dataset, so this is the cleanest
  filter — independent of which labels make it through.
- `dbt_invocation_id` label is auto-attached to every dbt-emitted job and
  is unique per dbt run, i.e. per (parent_run, batch_date). Aggregating
  by invocation_id gives us per-batch rollups without having to fix the
  profiles.yml labels (which currently aren't propagating — TODO).
- Model name = strip the `__dbt_tmp` suffix from destination_table.table_id.
  dbt-bigquery's incremental materializations write to `<model>__dbt_tmp`
  first, then MERGE/COPY into `<model>`. Both are billable bytes/slot.

Cost math (on-demand pricing, us-central1):
  $6.25 per TiB of total_bytes_billed (after the monthly free tier)

Slot math (capacity / autoscaler pricing):
  total_slot_ms is what we'd be charged for under reserved-slot pricing.

Usage:
  python3 bq_metrics.py per-batch [wh_db] [sf]
  python3 bq_metrics.py per-model [wh_db] [sf] [--invocation_id ID]
  python3 bq_metrics.py live   [wh_db]
  python3 bq_metrics.py cost   [wh_db] [sf]
  python3 bq_metrics.py wall   [wh_db] [sf]
  python3 bq_metrics.py raw    [wh_db] [sf]   # last 50 queries verbatim

Defaults: wh_db='shannon_aug_bq_dbt', sf='10'.
"""
from __future__ import annotations

import argparse
import subprocess
import sys

PROJECT = "databricks-sandbox-perfeng"
REGION = "region-us-central1"
DEFAULT_WH_DB = "shannon_aug_bq_dbt"
DEFAULT_SF = "10"

ON_DEMAND_PRICE_PER_TIB = 6.25  # USD


# ----------------------------------------------------------------------
# Query templates — each is a callable that returns a SQL string given
# the bound params. Keep them in one place so they're easy to copy/paste
# into BQ console or adapt elsewhere.
# ----------------------------------------------------------------------

def q_per_batch(*, wh_db: str, sf: str) -> str:
    """One row per dbt_invocation_id = per batch_date. Includes wall-clock
    (MIN start ... MAX end across all dbt-emitted queries for the batch)
    + total bytes + total slot + query count."""
    return f"""
WITH labeled AS (
  SELECT
    job_id,
    creation_time, start_time, end_time,
    total_bytes_processed,
    total_bytes_billed,
    total_slot_ms,
    statement_type,
    destination_table.dataset_id AS dataset_id,
    destination_table.table_id   AS dest_table,
    (SELECT value FROM UNNEST(labels) WHERE key='dbt_invocation_id') AS invoc_id
  FROM `{REGION}`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
  WHERE creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
    AND destination_table.dataset_id = '{wh_db}_{sf}'
    AND (SELECT value FROM UNNEST(labels) WHERE key='dbt_invocation_id') IS NOT NULL
)
SELECT
  invoc_id,
  TIMESTAMP_DIFF(MAX(end_time), MIN(start_time), SECOND) AS wall_sec,
  MIN(start_time) AS first_query_at,
  MAX(end_time)   AS last_query_at,
  COUNT(*)        AS query_count,
  ROUND(SUM(total_bytes_processed)/POW(2,30), 2) AS gb_processed,
  ROUND(SUM(total_bytes_billed   )/POW(2,30), 2) AS gb_billed,
  ROUND(SUM(total_slot_ms)/1000, 1)              AS slot_sec,
  ROUND(SUM(total_bytes_billed)/POW(2,40) * {ON_DEMAND_PRICE_PER_TIB}, 4) AS est_cost_usd
FROM labeled
GROUP BY invoc_id
ORDER BY first_query_at
"""


def q_per_model(*, wh_db: str, sf: str, invocation_id: str | None) -> str:
    """One row per (invoc_id, model). dbt-bigquery emits 2-3 queries per
    incremental model (CTAS into _dbt_tmp + MERGE into target + DROP);
    we aggregate them under the model name (strip __dbt_tmp)."""
    invoc_filter = (
        f"AND (SELECT value FROM UNNEST(labels) WHERE key='dbt_invocation_id') = "
        f"'{invocation_id}'"
        if invocation_id else ""
    )
    return f"""
WITH labeled AS (
  SELECT
    creation_time, start_time, end_time,
    total_bytes_processed,
    total_bytes_billed,
    total_slot_ms,
    statement_type,
    destination_table.table_id AS dest_table,
    REGEXP_REPLACE(destination_table.table_id, r'__dbt_tmp$', '') AS model_name,
    (SELECT value FROM UNNEST(labels) WHERE key='dbt_invocation_id') AS invoc_id
  FROM `{REGION}`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
  WHERE creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
    AND destination_table.dataset_id = '{wh_db}_{sf}'
    AND destination_table.table_id IS NOT NULL
    {invoc_filter}
)
SELECT
  invoc_id,
  model_name,
  COUNT(*) AS n_queries,
  STRING_AGG(statement_type, ', ') AS stmt_types,
  ROUND(SUM(total_bytes_processed)/POW(2,30), 3) AS gb_processed,
  ROUND(SUM(total_bytes_billed   )/POW(2,30), 3) AS gb_billed,
  ROUND(SUM(total_slot_ms)/1000, 2)              AS slot_sec,
  TIMESTAMP_DIFF(MAX(end_time), MIN(start_time), SECOND) AS model_wall_sec
FROM labeled
GROUP BY invoc_id, model_name
ORDER BY invoc_id, slot_sec DESC
"""


def q_live(*, wh_db: str) -> str:
    """Last 5 min of activity touching the target dataset family. Useful
    while a benchmark run is in flight."""
    return f"""
SELECT
  TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), creation_time, SECOND) AS sec_ago,
  state,
  statement_type,
  destination_table.dataset_id AS dataset,
  destination_table.table_id   AS dest_table,
  ROUND(IFNULL(total_slot_ms,0)/1000, 1) AS slot_sec,
  ROUND(IFNULL(total_bytes_processed,0)/POW(2,20), 1) AS mb_processed
FROM `{REGION}`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
WHERE creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 5 MINUTE)
  AND (destination_table.dataset_id LIKE '{wh_db}%'
       OR destination_table.dataset_id LIKE 'tpcdi_staging_sf%')
ORDER BY creation_time DESC
LIMIT 25
"""


def q_cost(*, wh_db: str, sf: str) -> str:
    """One-row rollup of total cost / bytes / slot for the per-run dataset.
    Includes setup_bq + dbt + simulate_filedrops (all queries against the
    target or staging dataset family)."""
    return f"""
SELECT
  COUNT(*) AS total_queries,
  COUNTIF((SELECT value FROM UNNEST(labels) WHERE key='dbt_invocation_id') IS NOT NULL)
    AS dbt_queries,
  ROUND(SUM(total_bytes_processed)/POW(2,30), 2) AS gb_processed,
  ROUND(SUM(total_bytes_billed   )/POW(2,30), 2) AS gb_billed,
  ROUND(SUM(total_slot_ms)/1000, 1)              AS slot_sec,
  ROUND(SUM(total_bytes_billed)/POW(2,40) * {ON_DEMAND_PRICE_PER_TIB}, 4) AS est_cost_usd,
  MIN(creation_time) AS first_query,
  MAX(end_time)      AS last_query,
  TIMESTAMP_DIFF(MAX(end_time), MIN(creation_time), SECOND) AS span_sec
FROM `{REGION}`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
WHERE creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
  AND (destination_table.dataset_id IN ('{wh_db}_{sf}', '{wh_db}_{sf}_bronze',
                                          'tpcdi_staging_sf{sf}')
       OR referenced_tables[SAFE_OFFSET(0)].dataset_id
           IN ('{wh_db}_{sf}', '{wh_db}_{sf}_bronze', 'tpcdi_staging_sf{sf}'))
"""


def q_wall(*, wh_db: str, sf: str) -> str:
    """dbt wall-clock per batch (mirrors the SF/DBX `dbt wall = MIN..MAX
    query_history` methodology)."""
    return f"""
SELECT
  (SELECT value FROM UNNEST(labels) WHERE key='dbt_invocation_id') AS invoc_id,
  COUNT(*) AS n_queries,
  MIN(start_time) AS dbt_wall_start,
  MAX(end_time)   AS dbt_wall_end,
  TIMESTAMP_DIFF(MAX(end_time), MIN(start_time), SECOND) AS dbt_wall_sec,
  ROUND(SUM(total_slot_ms)/1000, 1) AS slot_sec_total,
  ROUND(SUM(total_slot_ms)/1000.0 / NULLIF(TIMESTAMP_DIFF(MAX(end_time), MIN(start_time), SECOND), 0), 2)
    AS effective_concurrency
FROM `{REGION}`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
WHERE creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
  AND destination_table.dataset_id = '{wh_db}_{sf}'
  AND (SELECT value FROM UNNEST(labels) WHERE key='dbt_invocation_id') IS NOT NULL
GROUP BY invoc_id
ORDER BY dbt_wall_start
"""


def q_raw(*, wh_db: str, sf: str) -> str:
    """Last 50 queries against the per-run dataset family — for ad-hoc
    inspection of statement types / destination tables / costs."""
    return f"""
SELECT
  creation_time,
  TIMESTAMP_DIFF(end_time, start_time, SECOND) AS sec,
  statement_type,
  destination_table.table_id AS dest_table,
  ROUND(IFNULL(total_bytes_processed,0)/POW(2,20), 1) AS mb,
  ROUND(IFNULL(total_slot_ms,0)/1000, 1)              AS slot_sec,
  state
FROM `{REGION}`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
WHERE creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
  AND (destination_table.dataset_id IN ('{wh_db}_{sf}', '{wh_db}_{sf}_bronze')
       OR referenced_tables[SAFE_OFFSET(0)].dataset_id IN ('{wh_db}_{sf}', '{wh_db}_{sf}_bronze'))
ORDER BY creation_time DESC
LIMIT 50
"""


# ----------------------------------------------------------------------
# CLI plumbing
# ----------------------------------------------------------------------

def run_query(sql: str, fmt: str = "pretty") -> int:
    """Pipe SQL through `bq` CLI. Returns exit code."""
    cmd = [
        "bq", f"--project_id={PROJECT}", "query",
        "--use_legacy_sql=false",
        f"--format={fmt}",
        "--max_rows=500",
        sql,
    ]
    return subprocess.call(cmd)


def main(argv: list[str]) -> int:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("command",
                   choices=["per-batch", "per-model", "live", "cost", "wall", "raw"])
    p.add_argument("wh_db", nargs="?", default=DEFAULT_WH_DB)
    p.add_argument("sf",    nargs="?", default=DEFAULT_SF)
    p.add_argument("--invocation_id", help="filter per-model to one dbt invocation")
    p.add_argument("--format", default="pretty",
                   choices=["pretty", "csv", "json", "prettyjson"])
    args = p.parse_args(argv)

    if args.command == "per-batch":
        sql = q_per_batch(wh_db=args.wh_db, sf=args.sf)
    elif args.command == "per-model":
        sql = q_per_model(wh_db=args.wh_db, sf=args.sf,
                          invocation_id=args.invocation_id)
    elif args.command == "live":
        sql = q_live(wh_db=args.wh_db)
    elif args.command == "cost":
        sql = q_cost(wh_db=args.wh_db, sf=args.sf)
    elif args.command == "wall":
        sql = q_wall(wh_db=args.wh_db, sf=args.sf)
    elif args.command == "raw":
        sql = q_raw(wh_db=args.wh_db, sf=args.sf)
    else:
        return 2

    return run_query(sql, fmt=args.format)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
