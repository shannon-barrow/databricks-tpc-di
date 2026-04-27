# Databricks notebook source
# MAGIC %md
# MAGIC # TPC-DI Unified Data Generator (entry point)
# MAGIC
# MAGIC Single dispatch notebook for data generation. Reads the
# MAGIC `spark_or_native_datagen` job parameter and runs **inline** in this
# MAGIC notebook's process — both runners are imported as Python modules so
# MAGIC there is no `dbutils.notebook.run` indirection, no child notebook
# MAGIC context, and no risk of a new cluster being spun up on serverless.
# MAGIC
# MAGIC Choices:
# MAGIC - **`spark`** (default) — `tools/spark_runner.py` (distributed PySpark
# MAGIC   generator). Runs on serverless or any classic Photon cluster. Output
# MAGIC   goes under `tpcdi_volume/spark_datagen/sf={SF}/` to avoid clobbering
# MAGIC   DIGen output.
# MAGIC - **`native`** — `tools/digen_runner.py` (legacy DIGen.jar wrapper). Hard
# MAGIC   pre-flight: requires non-serverless cluster + DBR ≤ 15.4 + Java; aborts
# MAGIC   without touching the volume otherwise. Output goes to
# MAGIC   `tpcdi_volume/sf={SF}/`.
# MAGIC
# MAGIC The output directory is computed in this notebook from `catalog` + the
# MAGIC generator choice — users do not pass it explicitly.

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

import sys

# Normalize the generator choice — accept any case + leading/trailing whitespace.
_choice = dbutils.widgets.get("spark_or_native_datagen").strip().lower()
if _choice not in ("spark", "native"):
    raise ValueError(
        f"spark_or_native_datagen must be 'spark' or 'native' "
        f"(got {dbutils.widgets.get('spark_or_native_datagen')!r})"
    )

_catalog = dbutils.widgets.get("catalog")
_scale_factor = int(dbutils.widgets.get("scale_factor"))
_regenerate = dbutils.widgets.get("regenerate_data") == "YES"
_log_level = dbutils.widgets.get("log_level")

_volume_base = f"/Volumes/{_catalog}/tpcdi_raw_data/tpcdi_volume/"
_tpcdi_directory = f"{_volume_base}spark_datagen/" if _choice == "spark" else _volume_base

_nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
_workspace_src_path = f"/Workspace{_nb_path.split('/src')[0]}/src"
_tools_dir = f"{_workspace_src_path}/tools"
if _tools_dir not in sys.path:
    sys.path.insert(0, _tools_dir)

print(f"data_gen dispatch → {_choice!r}")
print(f"  output directory: {_tpcdi_directory}sf={_scale_factor}/")
print(f"  regenerate_data={_regenerate}, log_level={_log_level}")

if _choice == "spark":
    from spark_runner import run as spark_run
    spark_run(
        scale_factor=_scale_factor,
        catalog=_catalog,
        tpcdi_directory=_tpcdi_directory,
        regenerate_data=_regenerate,
        log_level=_log_level,
        workspace_src_path=_workspace_src_path,
        dbutils=dbutils,
        spark=spark,
    )
else:  # native
    from digen_runner import run as digen_run
    digen_run(
        scale_factor=_scale_factor,
        catalog=_catalog,
        tpcdi_directory=_tpcdi_directory,
        regenerate_data=_regenerate,
        workspace_src_path=_workspace_src_path,
        dbutils=dbutils,
        spark=spark,
    )
