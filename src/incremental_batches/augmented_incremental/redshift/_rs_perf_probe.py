# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# dependencies = ["psycopg2-binary"]
# ///
# Diagnostic notebook: investigate where Redshift Serverless time was spent
# during the recent SF=20k bootstrap (the 4 re-seeded tables). Writes a
# human-readable report to a volume log.

dbutils.widgets.text("database",     "dev")
dbutils.widgets.text("secret_scope", "tpcdi_redshift")

database     = dbutils.widgets.get("database")
secret_scope = dbutils.widgets.get("secret_scope")

import psycopg2
def _get(k): return dbutils.secrets.get(scope=secret_scope, key=k)

conn = psycopg2.connect(
    host=_get("host"), port=int(_get("port") or "5439"),
    user=_get("user"), password=_get("password"),
    dbname=database, sslmode="require", connect_timeout=30,
)
conn.autocommit = True
lines = []

def q(sql, label):
    lines.append(f"\n=== {label} ===")
    with conn.cursor() as cur:
        cur.execute(sql)
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
        if not rows:
            lines.append("  (no rows)")
            return
        lines.append("  " + " | ".join(f"{c}" for c in cols))
        for r in rows:
            lines.append("  " + " | ".join(str(v)[:80] for v in r))

# 1) Recent COPY commands — load history (last 4 hrs)
q("""
SELECT
  query_id,
  start_time,
  end_time,
  DATEDIFF(second, start_time, end_time) AS elapsed_s,
  data_source,
  bytes_scanned,
  ROUND(bytes_scanned / 1024.0 / 1024.0 / 1024.0, 2) AS gb_scanned,
  loaded_rows,
  ROUND(loaded_rows / NULLIF(DATEDIFF(second, start_time, end_time), 0)::float, 0) AS rows_per_s,
  ROUND((bytes_scanned/1024.0/1024.0) / NULLIF(DATEDIFF(second, start_time, end_time), 0), 1) AS mb_per_s
FROM SYS_LOAD_HISTORY
WHERE start_time > GETDATE() - INTERVAL '6 hours'
ORDER BY start_time DESC
LIMIT 30
""", "SYS_LOAD_HISTORY (recent COPY commands)")

# 2) Serverless RPU usage by minute (last 4 hrs) — see if autoscaling kicked in
q("""
SELECT
  DATE_TRUNC('minute', start_time) AS minute_bucket,
  ROUND(SUM(charged_seconds), 1) AS charged_seconds,
  ROUND(SUM(compute_seconds), 1) AS compute_seconds,
  ROUND(SUM(charged_seconds)/60.0, 2) AS rpus_avg_over_min
FROM SYS_SERVERLESS_USAGE
WHERE start_time > GETDATE() - INTERVAL '4 hours'
GROUP BY 1
ORDER BY 1 DESC
LIMIT 60
""", "SYS_SERVERLESS_USAGE (per-minute RPU)")

# 3) Query history for COPYs — see queue time vs execution time
q("""
SELECT
  query_id,
  query_type,
  start_time,
  DATEDIFF(second, start_time, end_time) AS elapsed_s,
  DATEDIFF(second, start_time, execution_start_time) AS queue_s,
  DATEDIFF(second, execution_start_time, end_time) AS exec_s,
  status,
  SUBSTRING(query_text, 1, 120) AS query_text
FROM SYS_QUERY_HISTORY
WHERE start_time > GETDATE() - INTERVAL '6 hours'
  AND query_type IN ('LOAD','CTAS','SELECT','UTILITY')
  AND elapsed_time > 30000000      -- > 30 sec, microseconds
ORDER BY elapsed_time DESC
LIMIT 30
""", "SYS_QUERY_HISTORY (long queries, > 30s)")

# 4) Current running queries
q("""
SELECT
  query_id,
  start_time,
  DATEDIFF(second, start_time, GETDATE()) AS running_s,
  query_type,
  status,
  SUBSTRING(query_text, 1, 100) AS query_text
FROM SYS_QUERY_HISTORY
WHERE status = 'running'
ORDER BY start_time
""", "currently running queries")

report = "\n".join(lines)
print(report)
log_path = "/Volumes/main/tpcdi_raw_data/tpcdi_volume/_dbt_run_logs/_rs_perf_probe.log"
dbutils.fs.put(log_path, report, overwrite=True)
dbutils.notebook.exit(f"wrote {log_path}")
