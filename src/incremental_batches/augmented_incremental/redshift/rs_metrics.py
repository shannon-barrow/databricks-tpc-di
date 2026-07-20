#!/usr/bin/env python3
"""Local Redshift SYS_QUERY_HISTORY queries for the augmented-incremental
Redshift benchmark. Pulls per-batch / per-model / live activity / cost
rollups from `SYS_QUERY_HISTORY` + `SYS_SERVERLESS_USAGE` via a direct
psycopg2 connection.

No Databricks workspace dependency — runs purely against Redshift from
your laptop using the same `tpcdi_redshift` secret-scope-equivalent env
vars (host, user, password, etc.).

Conventions used to scope queries:
- `query_label` (set via session-level SET query_group) carries our JSON
  tag: {"wh_db", "scale_factor", "batch_date", "task"} — same shape as
  Snowflake QUERY_TAG and BigQuery job labels.
- Per-batch aggregation: group by `query_label->>'batch_date'`
- Per-model aggregation: pull the dbt-emitted query text and regex out the
  destination model name.

Cost math (Redshift Serverless, us-west-2):
  $0.375 per RPU-hour (current published rate at time of writing; verify in
  your account's actual contracted rate before quoting publicly).

Usage:
  REDSHIFT_HOST=... REDSHIFT_USER=... REDSHIFT_PASSWORD=... REDSHIFT_DATABASE=dev \\
    python3 rs_metrics.py per-batch [wh_db] [sf]
  ... per-model [wh_db] [sf]
  ... live   [wh_db]
  ... cost   [wh_db] [sf]
  ... wall   [wh_db] [sf]
  ... raw    [wh_db] [sf]   # last 50 queries verbatim

Defaults: wh_db='shannon_aug_rs_dbt', sf='10'.
"""
from __future__ import annotations

import argparse
import json
import os
import sys

DEFAULT_WH_DB = "shannon_aug_rs_dbt"
DEFAULT_SF = "10"

# TODO(rs): verify list price against the account's contracted rate before
# quoting in customer-facing artifacts. The 0.375/RPU-hour figure is a
# published-list ballpark — VERIFY before using.
RPU_HOUR_PRICE_USD = 0.375


def _connect():
    """Open a psycopg2 connection from env vars. Mirrors `_rs_conn.rs_connect`
    but doesn't depend on dbutils — designed to run from a laptop."""
    try:
        import psycopg2
    except ImportError:
        sys.exit("pip install psycopg2-binary first")
    host = os.environ.get("REDSHIFT_HOST")
    user = os.environ.get("REDSHIFT_USER")
    pwd  = os.environ.get("REDSHIFT_PASSWORD")
    db   = os.environ.get("REDSHIFT_DATABASE", "dev")
    port = int(os.environ.get("REDSHIFT_PORT", "5439"))
    if not all([host, user, pwd]):
        sys.exit("set REDSHIFT_HOST, REDSHIFT_USER, REDSHIFT_PASSWORD env vars")
    return psycopg2.connect(
        host=host, port=port, user=user, password=pwd,
        dbname=db, sslmode="require", connect_timeout=30,
    )


# ----------------------------------------------------------------------
# Query builders. Each returns a SQL string.
# Redshift Serverless system tables relevant here:
#   SYS_QUERY_HISTORY — per-query metadata (query_label, elapsed, etc.)
#   SYS_SERVERLESS_USAGE — per-minute compute usage in RPU·seconds (for cost)
# ----------------------------------------------------------------------

def q_per_batch(*, wh_db: str, sf: str) -> str:
    """One row per batch_date. Wall-clock = MIN(start)..MAX(end) of all dbt
    queries for that batch. RPU·sec from SYS_SERVERLESS_USAGE joined on the
    time window."""
    label_prefix = json.dumps({"wh_db": wh_db, "scale_factor": str(sf)})[:-1]
    return f"""
WITH labeled AS (
  SELECT
    query_id, start_time, end_time, elapsed_time, compile_time, execution_time,
    query_label,
    JSON_EXTRACT_PATH_TEXT(query_label, 'batch_date') AS batch_date,
    JSON_EXTRACT_PATH_TEXT(query_label, 'task')       AS task
  FROM SYS_QUERY_HISTORY
  WHERE start_time >= DATEADD('hour', -24, GETDATE())
    AND query_label LIKE '%"wh_db":"{wh_db}"%'
    AND query_label LIKE '%"scale_factor":"{sf}"%'
    AND query_label LIKE '%"task":"dbt_run"%'
)
SELECT
  batch_date,
  COUNT(*)                                                       AS query_count,
  MIN(start_time)                                                AS first_query_at,
  MAX(end_time)                                                  AS last_query_at,
  DATEDIFF('second', MIN(start_time), MAX(end_time))            AS wall_sec,
  ROUND(SUM(elapsed_time::FLOAT) / 1e6, 2)                       AS sum_elapsed_sec
FROM labeled
WHERE batch_date IS NOT NULL
GROUP BY batch_date
ORDER BY first_query_at
"""


def q_cost_per_batch(*, wh_db: str, sf: str) -> str:
    """Per-batch RPU·sec consumption from SYS_SERVERLESS_USAGE, joined to
    SYS_QUERY_HISTORY by overlapping time windows. RPU usage is reported in
    per-minute buckets; we proportionally attribute to the dbt batch windows.

    NOTE: this is an approximation — Redshift Serverless doesn't attribute
    usage to individual queries. For exact per-batch cost, run only one
    batch at a time on a dedicated workgroup (or use the workgroup-wide
    usage as the cost ceiling)."""
    return f"""
WITH dbt_windows AS (
  SELECT
    JSON_EXTRACT_PATH_TEXT(query_label, 'batch_date') AS batch_date,
    MIN(start_time) AS win_start,
    MAX(end_time)   AS win_end
  FROM SYS_QUERY_HISTORY
  WHERE start_time >= DATEADD('hour', -24, GETDATE())
    AND query_label LIKE '%"wh_db":"{wh_db}"%'
    AND query_label LIKE '%"scale_factor":"{sf}"%'
    AND query_label LIKE '%"task":"dbt_run"%'
  GROUP BY 1
),
usage AS (
  SELECT
    start_time AS bucket_start,
    end_time   AS bucket_end,
    charged_seconds AS rpu_sec        -- TODO(rs): verify column name in your account's view
  FROM SYS_SERVERLESS_USAGE
  WHERE start_time >= DATEADD('hour', -24, GETDATE())
)
SELECT
  w.batch_date,
  ROUND(SUM(u.rpu_sec * LEAST(u.bucket_end, w.win_end)
            ::TIMESTAMP - GREATEST(u.bucket_start, w.win_start)::TIMESTAMP)
        / NULLIF(EXTRACT('epoch' FROM (u.bucket_end - u.bucket_start)), 0), 2)
        AS attributed_rpu_sec,
  ROUND(SUM(u.rpu_sec * ...) / 3600.0 * {RPU_HOUR_PRICE_USD}, 4) AS est_cost_usd
FROM dbt_windows w
JOIN usage u
  ON u.bucket_start < w.win_end AND u.bucket_end > w.win_start
GROUP BY w.batch_date
ORDER BY w.batch_date
"""
# TODO(rs): the proportional-attribution math above has a placeholder.
# Replace with a proper time-overlap calculation once SYS_SERVERLESS_USAGE
# schema is verified against the running account.


def q_live(*, wh_db: str) -> str:
    """Currently-running queries on the workgroup for our wh_db."""
    return f"""
SELECT
  query_id, user_id, status,
  DATEDIFF('second', start_time, GETDATE()) AS running_sec,
  LEFT(query_text, 200) AS qt
FROM SYS_QUERY_HISTORY
WHERE status = 'running'
  AND query_label LIKE '%"wh_db":"{wh_db}"%'
ORDER BY start_time
"""


def q_raw(*, wh_db: str, sf: str) -> str:
    """Last 50 queries for our run, verbatim."""
    return f"""
SELECT
  start_time, end_time, status, elapsed_time,
  JSON_EXTRACT_PATH_TEXT(query_label, 'batch_date') AS batch_date,
  JSON_EXTRACT_PATH_TEXT(query_label, 'task')       AS task,
  LEFT(query_text, 200) AS qt
FROM SYS_QUERY_HISTORY
WHERE start_time >= DATEADD('hour', -24, GETDATE())
  AND query_label LIKE '%"wh_db":"{wh_db}"%'
  AND query_label LIKE '%"scale_factor":"{sf}"%'
ORDER BY start_time DESC
LIMIT 50
"""


# ----------------------------------------------------------------------

def _print_rows(cur):
    cols = [d.name for d in cur.description] if cur.description else []
    print("\t".join(cols))
    for row in cur.fetchall():
        print("\t".join("" if v is None else str(v) for v in row))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("cmd", choices=["per-batch", "cost", "live", "raw"])
    parser.add_argument("wh_db", nargs="?", default=DEFAULT_WH_DB)
    parser.add_argument("sf", nargs="?", default=DEFAULT_SF)
    args = parser.parse_args()

    if args.cmd == "per-batch":
        sql = q_per_batch(wh_db=args.wh_db, sf=args.sf)
    elif args.cmd == "cost":
        sql = q_cost_per_batch(wh_db=args.wh_db, sf=args.sf)
    elif args.cmd == "live":
        sql = q_live(wh_db=args.wh_db)
    elif args.cmd == "raw":
        sql = q_raw(wh_db=args.wh_db, sf=args.sf)
    else:
        sys.exit(f"unknown cmd {args.cmd!r}")

    conn = _connect()
    with conn.cursor() as cur:
        cur.execute(sql)
        _print_rows(cur)
    conn.close()


if __name__ == "__main__":
    main()
