# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# dependencies = ["psycopg2-binary"]
# ///

dbutils.widgets.text("database",     "dev")
dbutils.widgets.text("wh_db",        "shannon_aug_rs_dbt")
dbutils.widgets.dropdown("scale_factor", "10", ["10","100","1000"])
dbutils.widgets.text("secret_scope", "tpcdi_redshift")

database     = dbutils.widgets.get("database")
wh_db        = dbutils.widgets.get("wh_db")
sf           = dbutils.widgets.get("scale_factor")
secret_scope = dbutils.widgets.get("secret_scope")
target_schema = f"{wh_db}_{sf}".lower()

import psycopg2

def _get(k): return dbutils.secrets.get(scope=secret_scope, key=k)

conn = psycopg2.connect(
    host=_get("host"), port=int(_get("port") or "5439"),
    user=_get("user"), password=_get("password"),
    dbname=database, sslmode="require", connect_timeout=30,
)
conn.autocommit = True

output_lines = []
tables = ["currentaccountbalances", "factcashbalances", "factmarkethistory",
          "factwatches", "factholdings", "dimcustomer", "dimaccount", "dimtrade",
          "bronzecashtransaction", "bronzecustomer", "bronzeaccount", "bronzeholdings",
          "bronzetrade", "bronzewatches", "bronzedailymarket",
          "account_updates_from_customer"]
with conn.cursor() as cur:
    output_lines.append("\n=== ROW COUNTS ===")
    for t in tables:
        try:
            cur.execute(f'SELECT COUNT(*) FROM "{target_schema}"."{t}"')
            n = cur.fetchone()[0]
            output_lines.append(f"  {t:35s} {n:>12,}")
        except Exception as e:
            output_lines.append(f"  {t:35s} ERROR: {e}")
    # Sanity: currentaccountbalances latest_batch=true count should equal
    # factcashbalances per-batch insertions.
    cur.execute(f'SELECT COUNT(*) FROM "{target_schema}".currentaccountbalances WHERE latest_batch = true')
    output_lines.append(f"\n  currentaccountbalances WHERE latest_batch=true: {cur.fetchone()[0]:,}")
    cur.execute(f'SELECT COUNT(*) FROM "{target_schema}".currentaccountbalances WHERE latest_batch = false')
    output_lines.append(f"  currentaccountbalances WHERE latest_batch=false: {cur.fetchone()[0]:,}")
    cur.execute(f'SELECT MIN(ct_date), MAX(ct_date) FROM "{target_schema}".currentaccountbalances')
    output_lines.append(f"  currentaccountbalances ct_date range: {cur.fetchone()}")
    # Compare to what staging had
    cur.execute(f'SELECT COUNT(*) FROM "tpcdi_staging_sf{sf}".currentaccountbalances')
    output_lines.append(f"  staging.currentaccountbalances count: {cur.fetchone()[0]:,}")

conn.close()
report = "\n".join(output_lines)
print(report)
log_path = "/Volumes/main/tpcdi_raw_data/tpcdi_volume/_dbt_run_logs/_describe_rs.log"
dbutils.fs.put(log_path, report, overwrite=True)
dbutils.notebook.exit(f"wrote {log_path}")
