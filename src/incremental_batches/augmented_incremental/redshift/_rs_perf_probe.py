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
    try:
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
    except Exception as e:
        lines.append(f"  QUERY FAILED: {type(e).__name__}: {str(e)[:300]}")

# 0) Discover actual column schemas first
q("""
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_schema = 'pg_catalog'
  AND table_name IN ('sys_load_history', 'sys_query_history', 'sys_serverless_usage')
ORDER BY table_name, ordinal_position
""", "schema of sys_load_history / sys_query_history / sys_serverless_usage")

# 1) Recent COPY commands — load history (last 6 hrs) — use SELECT *
q("""
SELECT *
FROM SYS_LOAD_HISTORY
WHERE start_time > GETDATE() - INTERVAL '6 hours'
ORDER BY start_time DESC
LIMIT 30
""", "SYS_LOAD_HISTORY (recent COPY commands, ALL COLUMNS)")

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

# 3) Query history — long queries
q("""
SELECT *
FROM SYS_QUERY_HISTORY
WHERE start_time > GETDATE() - INTERVAL '6 hours'
ORDER BY elapsed_time DESC NULLS LAST
LIMIT 20
""", "SYS_QUERY_HISTORY (top 20 by elapsed)")


report = "\n".join(lines)
print(report)
log_path = "/Volumes/main/tpcdi_raw_data/tpcdi_volume/_dbt_run_logs/_rs_perf_probe.log"
dbutils.fs.put(log_path, report, overwrite=True)
dbutils.notebook.exit(f"wrote {log_path}")
