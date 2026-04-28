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
# MAGIC ## Choices
# MAGIC
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
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Rules and reasoning
# MAGIC
# MAGIC The Driver creates this datagen job through `workflow_builders/datagen_spark.py`
# MAGIC or `workflow_builders/datagen_digen.py`, both of which call into
# MAGIC `workflow_builders/_node_picker.py`. The rules they encode:
# MAGIC
# MAGIC ### Cluster mode by generator
# MAGIC
# MAGIC | Generator | Cluster | Why |
# MAGIC | --- | --- | --- |
# MAGIC | **Spark** | Serverless by default; respects the user's *Serverless* widget. | Distributed PySpark runs anywhere; serverless is fastest cold-start. |
# MAGIC | **Native (DIGen)** | Always non-serverless, always single-node, **DBR 15.4 + Photon** forced. | DIGen.jar is a Java subprocess (can't run on serverless). The audit logic is validated against DBR 15.4. Generation is sequential — extra workers do nothing. |
# MAGIC
# MAGIC ### Native (DIGen) cluster sizing
# MAGIC
# MAGIC DIGen is sequential, so we scale the **driver** by SF and pick a
# MAGIC storage-optimized SKU when the dataset gets large enough that local-disk
# MAGIC throughput dominates wall-clock:
# MAGIC
# MAGIC | scale_factor | driver | category |
# MAGIC | --- | --- | --- |
# MAGIC | ≤ 100 | 8-core | General Purpose |
# MAGIC | 1000 | 16-core | Storage Optimized |
# MAGIC | ≥ 5000 | 32-core | Storage Optimized |
# MAGIC
# MAGIC SF=100 ≈ 10 GB raw — fits comfortably on a small general-purpose VM.
# MAGIC SF=1000+ at 100+ GB benefits noticeably from local-NVMe throughput.
# MAGIC
# MAGIC ### Spark non-serverless cluster sizing (when user opts out of serverless)
# MAGIC
# MAGIC All general-purpose; Spark's parallelism distributes the work, so per-
# MAGIC node disk doesn't dominate the way it does for sequential DIGen.
# MAGIC
# MAGIC | scale_factor | driver | workers | layout |
# MAGIC | --- | --- | --- | --- |
# MAGIC | ≤ 100 | 8-core | 0 | single-node |
# MAGIC | 1000 | 16-core | 0 | single-node |
# MAGIC | 5000 | 32-core | 5 × 16-core | multi-node |
# MAGIC | 10000 | 64-core | 10 × 16-core | multi-node |
# MAGIC | 20000 | 64-core | 20 × 16-core | multi-node |
# MAGIC
# MAGIC General rule above SF=1000: 1 × 16-core worker per 1000 of SF.
# MAGIC
# MAGIC ### Node-type selection (cross-cloud)
# MAGIC
# MAGIC `_node_picker.pick_node()` queries the workspace's available node types
# MAGIC and scores candidates by the tuple **(is_arm, has_local_disk, generation)**
# MAGIC — higher tuple wins. Filtering passes:
# MAGIC
# MAGIC 1. **Match cores** + **match category** (Storage Optimized vs General
# MAGIC    Purpose). Drop the category constraint as a fallback.
# MAGIC 2. Exclude specialized SKUs (GPU, ML, IPU, Gaudi).
# MAGIC 3. Among the survivors, pick the one with the highest score:
# MAGIC    - **ARM preferred** (lower TCO, latest gens are first-class):
# MAGIC      - Azure: `Standard_*p*_v6` (the `p` indicates ARM; e.g. `Standard_D8pds_v6`).
# MAGIC      - AWS: Graviton families — `m8g`, `m8gd`, `c8g`, `r8g` (the `g` after the
# MAGIC        family digit indicates Graviton ARM).
# MAGIC      - GCP: `c4a-` (Axion) or `t2a-` (Tau T2A).
# MAGIC    - **Local NVMe disk preferred** (fast shuffle / DIGen tmp output):
# MAGIC      - Storage Optimized category always has it.
# MAGIC      - Azure: `d` in the SKU suffix (`pds` / `ds` / `pld` / `ld`).
# MAGIC      - AWS: `d` in the family-suffix (`m8gd`, `c6gd`, `r7gd`, …); the `i*`
# MAGIC        storage families always have it.
# MAGIC      - GCP: `-lssd` suffix on the node-type name
# MAGIC        (e.g. `c4a-standard-16-lssd`).
# MAGIC    - **Latest generation** wins ties — Azure highest `_v{N}`, AWS / GCP
# MAGIC      highest digit in the family (`m8` beats `m7`; `c4a` beats `c3`).
# MAGIC
# MAGIC ### Local disk fallback
# MAGIC
# MAGIC When the picked node lacks built-in local NVMe:
# MAGIC
# MAGIC - **Azure / AWS** — `enable_elastic_disk: True` on the cluster spec auto-
# MAGIC   attaches managed disks (Azure managed disks / EBS) on demand.
# MAGIC - **GCP** — explicit `gcp_attributes.local_ssd_count` is set to
# MAGIC   `max(1, target_cores // 4)`. Each local SSD on GCP is 375 GB, so this
# MAGIC   gives ~94 GB per core: 8c → 2 SSDs (750 GB), 16c → 4 (1.5 TB),
# MAGIC   32c → 8 (3 TB), 64c → 16 (6 TB).
# MAGIC
# MAGIC ### Storage-Optimized ARM caveats
# MAGIC
# MAGIC As of writing, storage-optimized ARM SKUs are sparse:
# MAGIC - Azure has **no** storage-opt ARM (L-series is x86 only), so SF≥1000
# MAGIC   DIGen on Azure picks `Standard_L*s_v3` (x86).
# MAGIC - AWS has `i7gd`/`r7gd` (Graviton storage-opt) where available; otherwise
# MAGIC   the picker falls back to x86 `i7i`/`i4i`.
# MAGIC - GCP doesn't expose a clean ARM storage-opt family; the `c4a-*-lssd`
# MAGIC   variants cover that role.
# MAGIC
# MAGIC ### Pre-flight (DIGen only)
# MAGIC
# MAGIC Before any volume side effect, `digen_runner` verifies:
# MAGIC 1. `java -version` succeeds (must, for the JAR).
# MAGIC 2. A writable scratch dir exists. Prefers `/local_disk0` (large fast NVMe);
# MAGIC    falls back to `/tmp` only when SF ≤ 100 (small enough to fit the OS root
# MAGIC    partition; SF=100 ≈ 10 GB).
# MAGIC 3. DBR version ≤ 15.4 (the audit logic depends on DBR 15.4 behaviors).
# MAGIC
# MAGIC If any check fails, the job hard-aborts with a **"switch to SINGLE_USER
# MAGIC access mode"** hint and **no volume data is touched**. This avoids a
# MAGIC `regenerate_data=YES` run wiping existing data on a cluster that can't
# MAGIC actually regenerate it.
# MAGIC
# MAGIC ### Defaults
# MAGIC
# MAGIC - `spark_or_native_datagen=spark` (Spark generator preferred — faster,
# MAGIC   serverless-friendly, no DBR pinning).
# MAGIC - `Serverless=YES` widget default.
# MAGIC - `regenerate_data=NO` (re-running the job at the same SF is a no-op
# MAGIC   when output already exists — safe to re-trigger).

# COMMAND ----------

dbutils.widgets.dropdown("spark_or_native_datagen", "spark",
                         ["spark", "native", "augmented_incremental"],
                         "Spark or Native (DIGen) data generator")
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
if _choice not in ("spark", "native", "augmented_incremental"):
    raise ValueError(
        f"spark_or_native_datagen must be 'spark', 'native', or "
        f"'augmented_incremental' "
        f"(got {dbutils.widgets.get('spark_or_native_datagen')!r})"
    )

# Job-level parameters are free-form text on retrigger — strip whitespace and normalize case before comparing against literal sentinels so 'Yes', ' yes ', 'YES' all do the right thing.
_catalog = dbutils.widgets.get("catalog").strip()
_scale_factor = int(dbutils.widgets.get("scale_factor").strip())
_regenerate = dbutils.widgets.get("regenerate_data").strip().upper() == "YES"
_log_level = dbutils.widgets.get("log_level").strip().upper()

_volume_base = f"/Volumes/{_catalog}/tpcdi_raw_data/tpcdi_volume/"
# Per-mode target directory drives spark_runner's "skip if already exists" early-exit. - spark    → spark_datagen/sf={sf}/   (raw .txt/.xml/.csv files) - native   → sf={sf}/                (DIGen-native layout) - augmented → augmented_incremental/_staging/sf={sf}/   (Phase 2 per-day files; presence means the full augmented pipeline has already produced the persisted artifacts and stage 0 can short-circuit. Stage 0's temp Delta tables in tpcdi_raw_data are dropped by cleanup_stage0 anyway, so they aren't a useful marker)
if _choice == "augmented_incremental":
    _tpcdi_directory = f"{_volume_base}augmented_incremental/_staging/"
elif _choice == "spark":
    _tpcdi_directory = f"{_volume_base}spark_datagen/"
else:
    _tpcdi_directory = _volume_base

_nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
_workspace_src_path = f"/Workspace{_nb_path.split('/src')[0]}/src"
_tools_dir = f"{_workspace_src_path}/tools"
if _tools_dir not in sys.path:
    sys.path.insert(0, _tools_dir)

print(f"data_gen dispatch → {_choice!r}")
print(f"  scale factor:     {_scale_factor}")
print(f"  catalog:          {_catalog}")
if _choice == "augmented_incremental":
    print(f"  output target:    Delta tables at "
          f"{_catalog}.tpcdi_raw_data.{{dataset}}{_scale_factor} "
          f"(no volume files)")
else:
    print(f"  output directory: {_tpcdi_directory}sf={_scale_factor}/")
print(f"  regenerate data:  {_regenerate}")
print(f"  log level:        {_log_level}")
print()  # blank line before the runner output begins

# Both runner.run() functions take the same kwargs. log_level is consumed by spark_runner and silently ignored by digen_runner via **_unused. augmented_incremental flips spark_runner into Delta-only mode and skips B2/B3.
_run_kwargs = dict(
    scale_factor=_scale_factor,
    catalog=_catalog,
    tpcdi_directory=_tpcdi_directory,
    regenerate_data=_regenerate,
    log_level=_log_level,
    workspace_src_path=_workspace_src_path,
    dbutils=dbutils,
    spark=spark,
    augmented_incremental=(_choice == "augmented_incremental"),
)

if _choice in ("spark", "augmented_incremental"):
    from spark_runner import run as runner
else:  # native
    from digen_runner import run as runner

runner(**_run_kwargs)
