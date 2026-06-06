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
with conn.cursor() as cur:
    for t in ["dimsecurity", "dimcustomer", "dimaccount", "dimtrade",
              "factmarkethistory", "factwatches", "factholdings", "factcashbalances",
              "bronzeaccount", "account_updates_from_customer", "bronzecustomer"]:
        cur.execute("""
            SELECT column_name, data_type, character_maximum_length, numeric_precision
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
        """, (target_schema, t))
        rows = cur.fetchall()
        output_lines.append(f"\n=== RS {target_schema}.{t} ===")
        for r in rows:
            output_lines.append(f"  {r[0]:30s} {r[1]:20s} len={r[2]}")

conn.close()
report = "\n".join(output_lines)
print(report)
log_path = "/Volumes/main/tpcdi_raw_data/tpcdi_volume/_dbt_run_logs/_describe_rs.log"
dbutils.fs.put(log_path, report, overwrite=True)
dbutils.notebook.exit(f"wrote {log_path}")
