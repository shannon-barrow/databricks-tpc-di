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
staging_schema = f"tpcdi_staging_sf{sf}".lower()
# All 22 staging tables — verify each has >0 rows. The 5 we suspect of being
# empty shells from the cancelled run: bronzedailymarket, factmarkethistory,
# factwatches, dimtrade, factholdings.
STAGING_TABLES = [
    "bronzedailymarket", "factmarkethistory", "factwatches", "dimtrade",
    "factholdings", "factcashbalances", "cashtransactionhistorical",
    "financial", "companyyeareps", "dimaccount", "dimcustomer",
    "currentaccountbalances", "dimbroker", "dimsecurity", "dimcompany",
    "dimtime", "dimdate", "taxrate", "industry", "tradetype",
    "statustype", "batchdate",
]
with conn.cursor() as cur:
    output_lines.append(f"\n=== {staging_schema} ROW COUNTS ===")
    for t in STAGING_TABLES:
        try:
            cur.execute(f'SELECT COUNT(*) FROM "{staging_schema}"."{t}"')
            n = cur.fetchone()[0]
            marker = "  EMPTY ←" if n == 0 else ""
            output_lines.append(f"  {t:32s} {n:>15,}{marker}")
        except Exception as e:
            output_lines.append(f"  {t:32s} ERROR: {e}")

conn.close()
report = "\n".join(output_lines)
print(report)
log_path = "/Volumes/main/tpcdi_raw_data/tpcdi_volume/_dbt_run_logs/_describe_rs.log"
dbutils.fs.put(log_path, report, overwrite=True)
dbutils.notebook.exit(f"wrote {log_path}")
