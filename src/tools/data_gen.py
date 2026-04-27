# Databricks notebook source
# MAGIC %md
# MAGIC # TPC-DI Unified Data Generator (entry point)
# MAGIC
# MAGIC Single dispatch notebook for data generation. Reads the
# MAGIC `spark_or_native_datagen` job parameter and routes to either:
# MAGIC
# MAGIC - **`spark`** (default) — `./spark_data_generator` (distributed PySpark
# MAGIC   generator on serverless or any classic Photon cluster). Writes to
# MAGIC   `/Volumes/{catalog}/tpcdi_raw_data/tpcdi_volume/spark_datagen/sf={SF}/`
# MAGIC   so it doesn't clobber DIGen output.
# MAGIC - **`native`** — `./data_generator` (legacy single-threaded DIGen.jar
# MAGIC   wrapper). Writes to `/Volumes/{catalog}/tpcdi_raw_data/tpcdi_volume/sf={SF}/`.
# MAGIC
# MAGIC The output directory is computed inside this notebook from `catalog` +
# MAGIC the generator choice — users do not pass it explicitly. The selection is
# MAGIC purely a job parameter, so a single job (with a cluster that supports
# MAGIC both implementations — e.g. classic Photon DBR 15.4) can flip between
# MAGIC generators at run time. (Today DIGen.jar can't run on serverless; when
# MAGIC that limitation is removed, the same job with the serverless config
# MAGIC will be able to flip without any config change either.)

# COMMAND ----------

dbutils.widgets.dropdown("spark_or_native_datagen", "spark",
                         ["spark", "native"], "Spark or Native (DIGen) data generator")
dbutils.widgets.dropdown("scale_factor", "10",
                         ["10", "100", "1000", "5000", "10000", "20000"],
                         "Scale Factor")
dbutils.widgets.text("catalog", "main", "Target Catalog")
dbutils.widgets.dropdown("regenerate_data", "NO", ["YES", "NO"], "Regenerate Data")
dbutils.widgets.dropdown("log_level", "INFO", ["DEBUG", "INFO", "WARN"],
                         "Log Level (Spark generator only)")

# COMMAND ----------

# Normalize the generator choice — accept any case + leading/trailing whitespace.
choice = dbutils.widgets.get("spark_or_native_datagen").strip().lower()
if choice not in ("spark", "native"):
    raise ValueError(
        f"spark_or_native_datagen must be 'spark' or 'native' "
        f"(got {dbutils.widgets.get('spark_or_native_datagen')!r})"
    )

catalog = dbutils.widgets.get("catalog")
_volume_base = f"/Volumes/{catalog}/tpcdi_raw_data/tpcdi_volume/"

if choice == "spark":
    tpcdi_directory = f"{_volume_base}spark_datagen/"
    target = "./spark_data_generator"
else:  # native
    tpcdi_directory = _volume_base
    target = "./data_generator"

_args = {
    "scale_factor": dbutils.widgets.get("scale_factor"),
    "catalog": catalog,
    "regenerate_data": dbutils.widgets.get("regenerate_data"),
    "tpcdi_directory": tpcdi_directory,
}
if choice == "spark":
    _args["log_level"] = dbutils.widgets.get("log_level")

print(f"data_gen dispatch → {target} (spark_or_native_datagen={choice!r})")
print(f"  output directory: {tpcdi_directory}sf={_args['scale_factor']}/")

# timeout_seconds=0 → no timeout
result = dbutils.notebook.run(target, timeout_seconds=0, arguments=_args)
print(f"Child notebook completed (result preview: {result[:120] if isinstance(result, str) else result})")
