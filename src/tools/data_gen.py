# Databricks notebook source
# MAGIC %md
# MAGIC # TPC-DI Unified Data Generator (entry point)
# MAGIC
# MAGIC Single dispatch notebook for data generation. Reads the `data_generator`
# MAGIC job parameter and routes to either:
# MAGIC
# MAGIC - **`spark`** (default) — `./spark_data_generator` (distributed PySpark
# MAGIC   generator on serverless or any classic Photon cluster)
# MAGIC - **`digen`** — `./data_generator` (legacy single-threaded DIGen.jar
# MAGIC   wrapper; requires non-serverless DBR 15.4 + Photon)
# MAGIC
# MAGIC The switch is purely a job parameter — the same job (with a cluster that
# MAGIC supports both implementations, e.g. classic Photon DBR 15.4) can flip
# MAGIC between generators at run time. Pre-existing job_id stays stable; only
# MAGIC the `data_generator` value changes.

# COMMAND ----------

dbutils.widgets.dropdown("data_generator", "spark", ["spark", "digen"], "Data Generator")
dbutils.widgets.dropdown("scale_factor", "10",
                         ["10", "100", "1000", "5000", "10000", "20000"],
                         "Scale Factor")
dbutils.widgets.text("catalog", "main", "Target Catalog")
dbutils.widgets.dropdown("regenerate_data", "NO", ["YES", "NO"], "Regenerate Data")
dbutils.widgets.text("tpcdi_directory", "",
                     "TPCDI Output Directory (defaults to /Volumes/{catalog}/tpcdi_raw_data/tpcdi_volume/)")
dbutils.widgets.dropdown("log_level", "INFO", ["DEBUG", "INFO", "WARN"],
                         "Log Level (Spark generator only)")

# COMMAND ----------

data_generator = dbutils.widgets.get("data_generator")

# Forward all widgets to the child notebook as arguments. Both child notebooks
# self-init their widgets when run as a standalone job, so the arguments dict
# becomes the widget values inside the child.
_args = {
    "scale_factor": dbutils.widgets.get("scale_factor"),
    "catalog": dbutils.widgets.get("catalog"),
    "regenerate_data": dbutils.widgets.get("regenerate_data"),
    "tpcdi_directory": dbutils.widgets.get("tpcdi_directory"),
}

if data_generator == "spark":
    _args["log_level"] = dbutils.widgets.get("log_level")
    _target = "./spark_data_generator"
elif data_generator == "digen":
    _target = "./data_generator"
else:
    raise ValueError(
        f"Unknown data_generator={data_generator!r}; expected 'spark' or 'digen'"
    )

print(f"data_gen dispatch → {_target} (data_generator={data_generator!r})")
print(f"  forwarding args: {_args}")

# timeout_seconds=0 → no timeout (Databricks default for long-running gen jobs)
result = dbutils.notebook.run(_target, timeout_seconds=0, arguments=_args)
print(f"Child notebook completed (result preview: {result[:120] if isinstance(result, str) else result})")
