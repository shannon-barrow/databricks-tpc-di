# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# dependencies = ["psycopg2-binary"]
# ///
# Quick describe of dimsecurity/dimcustomer/factmarkethistory column types in
# Redshift, to figure out which column is varchar where the dbt models expect
# bigint. One-off diagnostic — delete once the type issues are resolved.

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

tables_of_interest = ["dimsecurity", "dimcustomer", "dimaccount", "factmarkethistory",
                       "factwatches", "factholdings", "factcashbalances", "dimtrade"]

with conn.cursor() as cur:
    for t in tables_of_interest:
        cur.execute("""
            SELECT column_name, data_type, character_maximum_length, numeric_precision
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
        """, (target_schema, t))
        rows = cur.fetchall()
        print(f"\n=== {target_schema}.{t} ===")
        for r in rows:
            print(f"  {r[0]:30s} {r[1]:20s} len={r[2]}  prec={r[3]}")

conn.close()
