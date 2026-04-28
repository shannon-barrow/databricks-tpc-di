# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Welcome to the TPC-DI Spec Implementation on Databricks!
# MAGIC
# MAGIC ## Please refer to the README for additional documentation.
# MAGIC
# MAGIC ## How to run
# MAGIC 1. **First**, run the *Setup* cell below — it bootstraps `tpcdi_config` and imports the workflow generators.
# MAGIC 2. **Then**, run the *Widgets* cell to render the dropdowns above the notebook.
# MAGIC 3. **Pick your options**, starting with **Workflow Type**, then run the two cells at the bottom — they create:
# MAGIC    - a **data-generation workflow** (Spark or DIGen.jar — see widget below)
# MAGIC    - a **benchmark workflow** that ingests the generated data and builds the dimensional model.
# MAGIC
# MAGIC Each cell prints a link to the created workflow when it finishes.
# MAGIC
# MAGIC ## Choosing a workflow — SKU × Batch Type
# MAGIC The Driver splits the workflow choice into two widgets so the dropdown
# MAGIC stays short. Pick a **SKU** (compute shape) and a **Batch Type** (how
# MAGIC the TPC-DI data is fed in). Combinations the SKU doesn't support are
# MAGIC hidden automatically.
# MAGIC
# MAGIC | SKU \\ Batch Type | Single Batch | Incremental | Augmented Incremental |
# MAGIC |---|:-:|:-:|:-:|
# MAGIC | **Cluster** (classic / serverless job cluster) | ✓ | ✓ | ✓ |
# MAGIC | **DBSQL** (serverless SQL warehouse) | ✓ | ✓ | — |
# MAGIC | **SDP** (Spark Declarative Pipelines) | ✓ + edition | — | ✓ |
# MAGIC
# MAGIC When **SKU=SDP × Batch Type=Single Batch**, an **Edition** dropdown
# MAGIC appears: CORE (declarative SDP pipeline), PRO (adds `APPLY CHANGES
# MAGIC INTO` for SCD Type 1/2), ADVANCED (adds Data Quality constraints).
# MAGIC
# MAGIC **Augmented Incremental** is a 730-day daily-streaming variant — see
# MAGIC `src/incremental_batches/augmented_incremental/` and CLAUDE.md for
# MAGIC details. It assumes the shared staging schema
# MAGIC `tpcdi_incremental_staging_{sf}` is already populated; if not, run the
# MAGIC tools under `src/tools/incremental_file_splitting/` first. Only
# MAGIC SF=20000 is staged today.
# MAGIC
# MAGIC ## Widget reference
# MAGIC - **SKU** — Cluster / DBSQL / SDP. Picks the compute shape.
# MAGIC - **Batch Type** — Single Batch / Incremental / Augmented Incremental.
# MAGIC   Options dynamically filter to what the SKU supports.
# MAGIC - **SDP Edition** (only when SKU=SDP × Single Batch) — CORE / PRO / ADVANCED.
# MAGIC - **Data Generator** (`spark_or_native_datagen` on the created job) — `spark` (default) or `native`.
# MAGIC   - `spark` → distributed PySpark generator on serverless. Output goes to `…/tpcdi_volume/spark_datagen/sf={SF}/`.
# MAGIC   - `native` → legacy single-threaded DIGen.jar wrapped in `tools/digen_runner`. Forces a non-serverless DBR 15.4 + Photon cluster (Java subprocess can't run on serverless). Output goes to `…/tpcdi_volume/sf={SF}/`. Worker count scales with SF: single-node up to SF=1000; +1 worker per 1000 of SF above that.
# MAGIC   - The benchmark reads either format via `{Customer.txt,Customer_[0-9]*.txt}`-style globs, so the rest of the pipeline is identical.
# MAGIC - **Scale Factor** — How much data to generate. Total file/table count and DAG shape are unchanged; only per-file row counts scale. Roughly **SF=10 ≈ 1 GB raw**, **SF=100 ≈ 10 GB**, **SF=1000 ≈ 100 GB**, **SF=10000 ≈ 1 TB**.
# MAGIC - **Batch Type** detail:
# MAGIC   - **Single Batch** — all 3 TPC-DI batches in one pass (faster, **no audit checks**).
# MAGIC   - **Incremental** — batches sequentially with audit checks at each boundary (spec validation; CLUSTER + DBSQL only).
# MAGIC   - **Augmented Incremental** — 730-day daily streaming pipeline (CLUSTER + SDP only). Skips the datagen workflow; reads pre-staged per-day files.
# MAGIC - **Serverless** — `YES` runs the benchmark on serverless compute (workflow or SDP). `NO` provisions a classic cluster sized by scale factor. Note: DBSQL is always serverless; DIGen datagen is always non-serverless regardless of this setting.
# MAGIC - **Predictive Optimization** — `ENABLE` lets Databricks auto-run maintenance ops (OPTIMIZE / VACUUM) on the result tables based on usage. `DISABLE` to opt out.
# MAGIC - **Job Name & Target Database** — Pattern is `{firstname}-{lastname}-TPCDI`. The benchmark workflow appends `-SF{N}-{exec_type}-{datagen}-{batched}` to the job name and `_{exec_type}_{datagen}_{batched}` to `wh_db` so concurrent variants don't collide.
# MAGIC - **Worker Type / Driver Type / DBR** — Only shown for non-serverless runs. Best defaults are auto-selected; the dropdowns let you override per cloud / per workspace.
# MAGIC - **Optimize For UC Features or Fastest Performance** — `Feature-Rich` (default) keeps PK/FK constraints, optimized writes, and data-skipping indexes that improve the downstream user experience. `Fastest Performance` strips them to compare Databricks against platforms that can't or won't implement those features. In production scenarios we recommend leaving this as `Feature-Rich`.
# MAGIC - **Regenerate Data** — On the *generated datagen job's* parameters (not on this Driver). `YES` wipes the existing volume output and regenerates from scratch; `NO` (default) skips if the SF directory already exists.
# MAGIC
# MAGIC ## Cluster / warehouse sizing
# MAGIC Serverless clusters and SDP pipelines auto-scale. Classic clusters and SQL Warehouses are sized for best TCO at the chosen scale factor. To override, edit the cluster spec on the workflow this Driver creates.
# MAGIC
# MAGIC The DIGen datagen job ignores the *Serverless* widget — it always provisions a non-serverless DBR 15.4 Photon cluster (DIGen.jar is a Java subprocess that can't run on serverless). The Spark datagen job is always serverless.

# COMMAND ----------

# DBTITLE 1,Setup: bootstrap tpcdi_config + import workflow-generator functions
# MAGIC %run ./tools/setup

# COMMAND ----------

# DBTITLE 1,Declare Widgets and Assign to Variables EXCEPT Worker Count
# Databricks notebook UI sorts widgets alphabetically by NAME, not by creation order. Number-prefixing the names forces the visual order: What → Where → How → Tuning. Labels (4th arg) stay human-readable.
#
# One-shot reset of any unprefixed widgets left over from prior versions of this notebook. Safe to keep — no-op once the workspace state is clean.
for _legacy in ("workflow_type", "batched", "sku", "batch_type", "edition",
                "pred_opt", "scale_factor", "job_name", "wh_target",
                "catalog", "perf_or_features", "data_generator",
                "serverless", "worker_type", "driver_type", "dbr"):
  try: dbutils.widgets.remove(_legacy)
  except Exception: pass

# --- 01_sku / 02_batch_type / 03_edition (What) ---
# `batch_type` options are dynamically constrained to what each SKU actually supports: DBSQL has no Augmented Incremental, SDP has no per-day Incremental. `edition` only appears for SDP × Single Batch.
dbutils.widgets.dropdown("01_sku", "Cluster", ["Cluster", "DBSQL", "SDP"], "SKU")
_sku_choice = dbutils.widgets.get("01_sku")

if _sku_choice == "SDP":
  _batch_options = ["Single Batch", "Augmented Incremental"]
elif _sku_choice == "DBSQL":
  _batch_options = ["Single Batch", "Incremental"]
else:  # Cluster
  _batch_options = ["Single Batch", "Incremental", "Augmented Incremental"]
dbutils.widgets.dropdown("02_batch_type", _batch_options[0], _batch_options, "Batch Type")
batch_type = dbutils.widgets.get("02_batch_type")
if batch_type not in _batch_options:
  batch_type = _batch_options[0]

if _sku_choice == "SDP" and batch_type == "Single Batch":
  dbutils.widgets.dropdown("03_edition", "CORE", ["CORE", "PRO", "ADVANCED"], "SDP Edition")
  _edition = dbutils.widgets.get("03_edition")
else:
  try: dbutils.widgets.remove("03_edition")
  except Exception: pass
  _edition = "CORE"  # placeholder — only consumed when SDP × Single Batch

# Compute wf_key in the format the dispatcher expects.
if batch_type == "Augmented Incremental":
  wf_key = f"AUGMENTED-{_sku_choice.upper()}"
elif _sku_choice == "SDP":
  wf_key = f"SDP-{_edition}"
else:
  wf_key = _sku_choice.upper()  # CLUSTER or DBSQL
workflow_type = tpcdi_config.workflows_dict.get(wf_key, wf_key)
sku           = wf_key.split('-')
incremental   = (batch_type == "Incremental")

# --- 04 / 05 / 06 / 07 / 08 (more What + Where) ---
dbutils.widgets.dropdown("04_scale_factor", tpcdi_config.default_sf, tpcdi_config.default_sf_options, "Scale Factor")
dbutils.widgets.dropdown("05_data_generator", "spark", ["spark", "digen"], "Data Generator")
dbutils.widgets.text("06_catalog", tpcdi_config.default_catalog, "Target Catalog")
dbutils.widgets.text("07_wh_target", tpcdi_config.default_wh, "Target Database")
dbutils.widgets.text("08_job_name", tpcdi_config.default_job_name, "Job Name")

scale_factor   = int(dbutils.widgets.get("04_scale_factor"))
data_generator = dbutils.widgets.get("05_data_generator")
catalog        = dbutils.widgets.get("06_catalog")
wh_target      = dbutils.widgets.get("07_wh_target")

_volume_base = f"/Volumes/{catalog}/tpcdi_raw_data/tpcdi_volume/"
# AUGMENTED variants read from the volume root (their staging tree `augmented_incremental/_staging/sf={sf}/` is path-appended inside the notebooks). Other Spark-datagen variants point at `spark_datagen/`.
if sku[0] == "AUGMENTED":
  tpcdi_directory = _volume_base
else:
  tpcdi_directory = f"{_volume_base}spark_datagen/" if data_generator == "spark" else _volume_base

# --- 09 / 10 / 11 / 12 (How — compute) ---
dbutils.widgets.dropdown("09_serverless", tpcdi_config.default_serverless, ["YES", "NO"], "Enable Serverless")
dbutils.widgets.dropdown("10_worker_type", tpcdi_config.default_worker_type, list(tpcdi_config.node_types.keys()), "Worker Type")
dbutils.widgets.dropdown("11_driver_type", tpcdi_config.default_driver_type, list(tpcdi_config.node_types.keys()), "Driver Type")
dbutils.widgets.dropdown("12_dbr", tpcdi_config.default_dbr, list(tpcdi_config.dbrs.values()), "Databricks Runtime")
serverless       = "YES" if sku[0] not in ["CLUSTER","SDP"] else dbutils.widgets.get("09_serverless")
worker_node_type = dbutils.widgets.get("10_worker_type")
driver_node_type = dbutils.widgets.get("11_driver_type")
dbr_version_id   = list(tpcdi_config.dbrs.keys())[list(tpcdi_config.dbrs.values()).index(dbutils.widgets.get("12_dbr"))]

# --- 13 / 14 (Tuning) ---
dbutils.widgets.dropdown("13_perf_or_features", tpcdi_config.features_or_perf[0], tpcdi_config.features_or_perf, "Optimize For UC Features or Fastest Performance")
dbutils.widgets.dropdown("14_pred_opt", "DISABLE", ["ENABLE", "DISABLE"], "Predictive Optimization")
perf_opt_flg = (dbutils.widgets.get("13_perf_or_features") == tpcdi_config.features_or_perf[1])
pred_opt     = dbutils.widgets.get("14_pred_opt")

# --- Conditional widget hide-on-irrelevant ---
if serverless == "YES":
  for _w in ("10_worker_type", "11_driver_type", "12_dbr"):
    try: dbutils.widgets.remove(_w)
    except Exception: pass

if sku[0] == "DBSQL":
  try: dbutils.widgets.remove("09_serverless")
  except Exception: pass

if sku[0] not in ["CLUSTER","DBSQL"]:
  try: dbutils.widgets.remove("13_perf_or_features")
  except Exception: pass

# AUGMENTED variants always use Spark-staged data; data_generator widget doesn't apply.
if sku[0] == "AUGMENTED":
  try: dbutils.widgets.remove("05_data_generator")
  except Exception: pass
  data_generator = "spark"

# Build job_name(s) with suffixes after all widget logic settles incremental.
# - Datagen job depends only on data_generator + SF (same output reused across all benchmark variants at that SF), so its name omits exec_type/batched.
# - Benchmark job: batched + exec + gen suffixes. SDP has no batched concept, so omit that suffix there. The data_generator is also stamped on every created job as a `data_generator` tag so jobs are queryable by generator.
# - `_datagen_label` (long form) is preserved for schema names — changing it would break already-materialized `..._spark_data_gen_*` schemas.
_datagen_label   = "spark_data_gen" if data_generator == "spark" else "native_data_gen"
_gen_label       = "SparkGen" if data_generator == "spark" else "NativeGen"
_batched_label   = "Incremental" if incremental else "SingleBatch"
_exec_label      = "Cluster" if sku[0] == "CLUSTER" else wf_key
_base_name       = dbutils.widgets.get('08_job_name')
datagen_job_name = f"{_base_name}-SF{scale_factor}-{_gen_label}"
if sku[0] in ['CLUSTER','DBSQL']:
  job_name = f"{_base_name}-SF{scale_factor}-{_batched_label}-{_exec_label}-{_gen_label}"
elif sku[0] == "AUGMENTED":
  # AUGMENTED creates parent + child (+ pipeline for SDP). job_name here is the PARENT name; the dispatcher derives child by stripping `-Parent` and pipeline (SDP only) by appending `-Pipeline`.
  _aug_variant = "Cluster" if sku[1] == "CLUSTER" else "SDP"
  job_name = f"{_base_name}-SF{scale_factor}-AugmentedIncremental-{_aug_variant}-Parent"
else:
  job_name = f"{_base_name}-SF{scale_factor}-{_exec_label}-{_gen_label}"

# COMMAND ----------

# DBTITLE 1,Create the Data Generation Workflow (serverless, single notebook task)
# AUGMENTED variants consume pre-staged per-day files from the shared `tpcdi_incremental_staging_{sf}` schema (built once-per-SF by the Phase B file-splitting tools), so they don't need a datagen workflow.
if sku[0] == "AUGMENTED":
  displayHTML(
    "<h2>Data Generation Workflow — skipped for AUGMENTED variants</h2>"
    "<p>The augmented benchmark reads from the shared staging schema "
    f"<code>{catalog}.tpcdi_incremental_staging_{scale_factor}</code>. "
    "If that schema isn't populated for this SF, run the file-splitting "
    "tools under <code>src/tools/incremental_file_splitting/</code> first.</p>"
  )
else:
  datagen_job_id = generate_datagen_workflow(
      job_name=datagen_job_name,
      scale_factor=scale_factor,
      catalog=catalog,
      regenerate_data="NO",
      log_level="INFO",
      repo_src_path=tpcdi_config.repo_src_path,
      workspace_src_path=tpcdi_config.workspace_src_path,
      api_call=tpcdi_config.api_call,
      data_generator=data_generator,
      default_dbr_version=tpcdi_config.default_dbr_version,
      default_worker_type=tpcdi_config.default_worker_type,
      serverless=serverless,
      node_types=tpcdi_config.node_types,
      cloud_provider=tpcdi_config.cloud_provider,
  )
  displayHTML(f"<h2><a href=/#job/{datagen_job_id}>Data Generation Workflow</a></h2>")

# COMMAND ----------

# DBTITLE 1,Create the Benchmark Workflow (ingest + dim/fact pipeline)
benchmark_job_id = generate_benchmark_workflow(
    wf_key=wf_key,
    workflow_type=workflow_type,
    job_name=job_name,
    catalog=catalog,
    wh_target=wh_target,
    scale_factor=scale_factor,
    tpcdi_directory=tpcdi_directory,
    repo_src_path=tpcdi_config.repo_src_path,
    workspace_src_path=tpcdi_config.workspace_src_path,
    cloud_provider=tpcdi_config.cloud_provider,
    serverless=serverless,
    pred_opt=pred_opt,
    perf_opt_flg=perf_opt_flg,
    incremental=incremental,
    data_generator=data_generator,
    api_call=tpcdi_config.api_call,
    worker_node_type=worker_node_type if serverless != "YES" else None,
    driver_node_type=driver_node_type if serverless != "YES" else None,
    dbr_version_id=dbr_version_id if serverless != "YES" else None,
    default_dbr_version=tpcdi_config.default_dbr_version,
    default_worker_type=tpcdi_config.default_worker_type,
    cust_mgmt_type=tpcdi_config.cust_mgmt_type,
    worker_cores_mult=tpcdi_config.worker_cores_mult,
    node_types=tpcdi_config.node_types,
)
displayHTML(f"<h2><a href=/#job/{benchmark_job_id}>Benchmark Workflow</a></h2>")