# Databricks notebook source
# Quick smoke check: SELECT COUNT(*) from each of the 7 external tables in
# {wh_db}_{N}_bronze. Used to verify setup_bq.py's wildcard external table
# URIs actually resolve to the current batch's CSV files after
# simulate_filedrops_bq has run.

# COMMAND ----------

# MAGIC %pip install --quiet google-cloud-bigquery

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("catalog",      "databricks-sandbox-perfeng")
dbutils.widgets.text("wh_db",        "smoke_bq_dbt")
dbutils.widgets.dropdown("scale_factor", "10", ["10","100","1000","5000","10000","20000"])
dbutils.widgets.text("secret_scope", "tpcdi_bigquery")
dbutils.widgets.text("bq_location",  "us-central1")

bq_project   = dbutils.widgets.get("catalog")
wh_db        = dbutils.widgets.get("wh_db")
scale_factor = dbutils.widgets.get("scale_factor")
secret_scope = dbutils.widgets.get("secret_scope")
bq_location  = dbutils.widgets.get("bq_location")

bronze_dataset = f"{wh_db}_{scale_factor}_bronze"
print(f"checking {bq_project}.{bronze_dataset}")

# COMMAND ----------

# MAGIC %run ./_bq_conn

# COMMAND ----------

client = bq_connect(
    project=bq_project, location=bq_location, secret_scope=secret_scope,
    query_label={"task": "smoke_check_externs", "wh_db": wh_db,
                 "scale_factor": scale_factor},
)

DATASETS = ["Customer", "Account", "Trade", "CashTransaction",
            "HoldingHistory", "DailyMarket", "WatchHistory"]
summary = []
for name in DATASETS:
    fq = f"`{bq_project}.{bronze_dataset}.{name}`"
    try:
        n = list(client.query(f"SELECT COUNT(*) AS n FROM {fq}").result())[0]["n"]
        line = f"  {name:18s} rows = {n:,}"
    except Exception as e:
        line = f"  {name:18s} FAIL: {type(e).__name__}: {str(e)[:200]}"
    print(line)
    summary.append(line)
dbutils.notebook.exit("\n".join(summary))
