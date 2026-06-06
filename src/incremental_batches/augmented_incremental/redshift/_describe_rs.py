# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# dependencies = ["psycopg2-binary"]
# ///

dbutils.widgets.text("scale_factor", "10")
sf = dbutils.widgets.get("scale_factor")
src_schema = f"main.tpcdi_incremental_staging_{sf}"

output_lines = []
for t in ["dimsecurity", "dimcustomer", "dimaccount", "dimtrade",
         "factmarkethistory", "factwatches", "factholdings", "factcashbalances"]:
    try:
        df = spark.read.table(f"{src_schema}.{t}")
        output_lines.append(f"\n=== DELTA {src_schema}.{t} ===")
        for f in df.schema.fields:
            output_lines.append(f"  {f.name:30s} {f.dataType.simpleString()}")
    except Exception as e:
        output_lines.append(f"\n=== {src_schema}.{t} ERROR: {e}")

report = "\n".join(output_lines)
print(report)
log_path = "/Volumes/main/tpcdi_raw_data/tpcdi_volume/_dbt_run_logs/_describe_delta.log"
dbutils.fs.put(log_path, report, overwrite=True)
dbutils.notebook.exit(f"wrote {log_path}")
