# Databricks notebook source
# MAGIC %md
# MAGIC # TPC-DI Cleanup
# MAGIC
# MAGIC Final task in the benchmark workflow. Drops the schemas produced by
# MAGIC the run **when `delete_tables_when_finished=TRUE`** (the default the
# MAGIC Driver bakes into each created job — users override per-run via the
# MAGIC job parameter).
# MAGIC
# MAGIC Schemas dropped:
# MAGIC - `{catalog}.{wh_db}_{scale_factor}` — final dimensional model.
# MAGIC - `{catalog}.{wh_db}_{scale_factor}_stage` — staging tables (may not
# MAGIC   exist for DLT runs at low SF; DROP IF EXISTS makes it a no-op).
# MAGIC
# MAGIC Runs `run_if=ALL_DONE` in the workflow, so a partial-failure run still
# MAGIC has its debris cleaned up (default behavior). Set
# MAGIC `delete_tables_when_finished=FALSE` to retain the schemas — useful when
# MAGIC inspecting audit results, debugging mismatches, or comparing two
# MAGIC variants side-by-side.

# COMMAND ----------

dbutils.widgets.text("catalog", "main", "Target Catalog")
dbutils.widgets.text("wh_db", "", "Target Database (the wh_db job parameter)")
dbutils.widgets.text("scale_factor", "10", "Scale Factor")
dbutils.widgets.text("delete_tables_when_finished", "TRUE",
                     "Delete benchmark schemas when finished (TRUE/FALSE)")

# COMMAND ----------

catalog       = dbutils.widgets.get("catalog").strip()
wh_db         = dbutils.widgets.get("wh_db").strip()
scale_factor  = dbutils.widgets.get("scale_factor").strip()
flag_raw      = dbutils.widgets.get("delete_tables_when_finished").strip().upper()
should_delete = flag_raw in ("TRUE", "YES", "T", "Y", "1")

target_schema = f"{catalog}.{wh_db}_{scale_factor}"
stage_schema  = f"{catalog}.{wh_db}_{scale_factor}_stage"

print(f"delete_tables_when_finished={flag_raw!r}")
print(f"target schema: {target_schema}")
print(f"stage schema:  {stage_schema}")

if not should_delete:
    print()
    print("Skipping cleanup — schemas left in place.")
    dbutils.notebook.exit("skipped")

# COMMAND ----------

print()
print(f"Dropping {target_schema} CASCADE...")
spark.sql(f"DROP SCHEMA IF EXISTS {target_schema} CASCADE")
print(f"Dropping {stage_schema} CASCADE...")
spark.sql(f"DROP SCHEMA IF EXISTS {stage_schema} CASCADE")
print()
print("Cleanup complete.")
