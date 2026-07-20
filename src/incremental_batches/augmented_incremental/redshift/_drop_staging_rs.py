# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# dependencies = ["psycopg2-binary"]
# ///
# One-off: drop tpcdi_staging_sf{sf} schema in Redshift so the next setup_rs
# re-seeds it with the current (correct) _TYPE_MAP. Delete once not needed.

dbutils.widgets.text("database",     "dev")
dbutils.widgets.dropdown("scale_factor", "10", ["10","100","1000"])
dbutils.widgets.text("secret_scope", "tpcdi_redshift")

database     = dbutils.widgets.get("database")
sf           = dbutils.widgets.get("scale_factor")
secret_scope = dbutils.widgets.get("secret_scope")
staging_schema = f"tpcdi_staging_sf{sf}".lower()

import psycopg2

def _get(k): return dbutils.secrets.get(scope=secret_scope, key=k)

conn = psycopg2.connect(
    host=_get("host"), port=int(_get("port") or "5439"),
    user=_get("user"), password=_get("password"),
    dbname=database, sslmode="require", connect_timeout=30,
)
conn.autocommit = True

with conn.cursor() as cur:
    print(f"dropping {database}.{staging_schema}...")
    cur.execute(f'DROP SCHEMA IF EXISTS "{staging_schema}" CASCADE')
    print(f"OK")

conn.close()
