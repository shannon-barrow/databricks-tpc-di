# Databricks TPC-DI

Databricks TPC-DI (Data Integration) is an implementation of specifications derived from the [TPC-DI](http://tpc.org/tpcdi/default5.asp) Benchmark.  
This repo includes multiple implementations and interpretations of the **TPC-DI v1.1.0**.  We suggest executing any of the workflow types on the Databricks Runtime **14.1** or higher. 

## Note About Data Generation
**Data generation defaults to a distributed PySpark implementation** that runs
on **Databricks Serverless** and scales across the executor pool so large scale
factors finish in a fraction of the time the original DIGen.jar required. The
legacy single-threaded **DIGen.jar** path is still available — selectable via
the Driver's `Data Generator` widget (parameter `spark_or_native_datagen`,
values `spark` or `native`) — for byte-compatible output with the upstream
TPC-DI reference implementation. See [Data Generation](#data-generation) below.

[![DatabricksRuntime](https://img.shields.io/badge/Databricks%20Runtime-14.1-orange)](https://docs.databricks.com/release-notes/runtime/releases.html)
[![Benchmark](https://img.shields.io/badge/Benchmark-TPC--DI%20v1.1.0-blue)](http://tpc.org/tpcdi/default5.asp)

## Flow at a glance

<img src="/src/tools/readme_images/tpcdi_flow.svg" alt="TPC-DI execution flow — Setup, Benchmark, and Audit groups" width="100%">

## Summary
Historically, the process of synchronizing a decision support system with data from operational systems has been referred to as Extract, Transform, Load (ETL) and the tools supporting such process have been referred to as ETL tools. Recently, ETL was replaced by the more comprehensive acronym, data integration (DI). DI describes the process of extracting and combining data from a variety of data source formats, transforming that data into a unified data model representation and loading it into a data store. The TPC-DI benchmark combines and transforms data extracted from an On-Line Transaction Processing (OTLP) system along with other sources of data, and loads it into a data warehouse. The source and destination data models, data transformations and implementation rules have been designed to be broadly representative of modern data integration requirements.

* The current TPC-DI specification can be found on the [TPC Documentation Webpage](https://www.tpc.org/tpc_documents_current_versions/current_specifications5.asp).  
* The current implementation follows the [TPC-DI v1.1.0 spec](https://www.tpc.org/TPC_Documents_Current_Versions/pdf/TPC-DI_v1.1.0.pdf). 

The TPC-DI spec does not provide code, instead relying on documenting business rules and requirements of the eventual table outputs of the data warehouse.   
This repo is an implementation of the spec only - one that executes only on the cloud-based Databricks platform.   

### LIMITATIONS
* The code base is implemented to execute on the Databricks platform.
* This code requires:
  * Databricks E2 Deployment. 
  * Databricks Repos functionality. 
  * Access to GitHub via public internet access. 
* Serverless has been introduced as an optional execution mechanism but requires access to serverless compute
* Unity Catalog will be required to use a catalog outside of hive_metastore. Additionally, UC is a prerequisite for lineage and primary/foreign key constraints
* The legacy DIGen.jar (native) data-generation path requires a non-serverless cluster with **DBR 15.4** and Photon. The Driver creates this cluster automatically when you choose `native`. The Spark generator path runs on serverless and has no DBR restrictions.

### Notes on Scoring/Metrics and Publication
As of writing, the TPC-DI has not modified its submittal/scoring metrics to accomodate cloud-based platforms and infrastructure. Furthermore, as of writing, there has never been an official submission to this benchmark.  Therefore, no throughput metrics are mentioned or discussed in this code repository.  Databricks is attempting to work with the TPC to amend the rules for submission and modernize the throughput metrics for a future "official submittal" to be published by the TPC.  Until then, this code has been opened for demonstrating how such a Data Integration ETL application could be built upon the Databricks Lakehouse - without any performance metrics being published.

# Execution Mechanisms
The benchmark has been implemented in several ways to demonstrate the versatility of the Databricks Lakehouse platform. Users can reference each implementation side-by-side to better enable them to build other similar data engineering pipelines:

1. **Workspace Cluster Workflow** — classic-cluster (or serverless) job with the auditable, batch-aware ETL.
2. **DBSQL Warehouse Workflow** — same DAG, executed against a serverless SQL Warehouse instead of a job cluster.
3. **Delta Live Tables Pipeline (CORE)** — declarative DLT pipeline.
4. **Delta Live Tables Pipeline (PRO)** — adds `APPLY CHANGES INTO` for SCD Type 1/2 ingestion.
5. **Delta Live Tables Pipeline (ADVANCED)** — adds Data Quality constraints to the PRO pipeline.

The following toggles apply across the deployment choices above:

1. **Data Generator** (`spark_or_native_datagen`): `spark` (default, distributed PySpark on serverless) or `native` (legacy DIGen.jar wrapped by `tools/digen_runner`, runs on a non-serverless DBR 15.4 + Photon cluster). Output paths differ — `spark` writes under `…/tpcdi_volume/spark_datagen/sf={SF}/`, `native` writes to `…/tpcdi_volume/sf={SF}/`. The benchmark reads either format via `{Customer.txt,Customer_[0-9]*.txt}`-style globs, so the rest of the pipeline is identical.
2. **Scale Factor**: How much data to generate. Total file/table count and DAG shape are unchanged; only per-file row counts scale. Roughly **SF=10 ≈ 1 GB raw**, **SF=100 ≈ 10 GB**, **SF=1000 ≈ 100 GB**, **SF=10000 ≈ 1 TB**. Defaults to 10.
3. **Serverless**: Faster performance and startup with serverless Workflows / DLT pipelines (DLT includes Enzyme as a standard feature). DBSQL is always serverless. The DIGen native datagen ignores this widget — it always uses a non-serverless cluster.
4. **Collective Batch or Incremental Batches**: Single-batch runs all 3 TPC-DI batches in one pass (faster, **no audit checks**). Incremental processes batches sequentially with audit checks at each batch boundary — required for spec validation. Only available for `CLUSTER` and `DBSQL` workflow types; DLT pipelines always run all batches in a single DLT pass.
5. **Predictive Optimization**: `ENABLE` lets Databricks auto-run maintenance ops (OPTIMIZE / VACUUM) on the result tables based on usage. `DISABLE` to opt out.
6. **Job Name & Target Database**: Pattern is `[firstname]-[lastname]-TPCDI`. The benchmark workflow appends `-SF{N}-{exec_type}-{datagen}-{batched}` to the job name and `_{exec_type}_{datagen}_{batched}` to `wh_db` so concurrent variants don't collide.
7. **Various cluster options**: When not choosing serverless, the DBR and worker/driver type dropdowns let you override the cloud-aware defaults the Driver picks for you.


## Components of Execution
The TPC-DI has an initial "setup" step, followed by the benchmark execution.  

![Execution Journey](/src/tools/readme_images/tpcdi_execution_journey.png "Execution Journey")

### Step 1) Setup: Data Generation and Workflow Generation  
This portion is **NOT benchmarked**.  The sole purpose of this step is to:
1. Create the necessary raw data and store them in UNITY CATALOG
2. Create a Databricks Workflow (this created workflow is what executes the ingestion and transformations for the benchmark).  

#### Data Generation
Two interchangeable implementations are supported, selected via the Driver's
**Data Generator** widget (job parameter `spark_or_native_datagen`):

- **`spark` (default)** — distributed PySpark generator. The unified entry
  notebook (`src/tools/data_gen.py`) imports `spark_runner` (which calls into
  `src/tools/tpcdi_gen/`) and runs everything inline on serverless. Outputs
  are split files like `Customer_1.txt`, `Customer_2.txt`, etc.
- **`native`** — the legacy single-threaded DIGen.jar wrapped by
  `src/tools/digen_runner.py` (also invoked from `data_gen.py`). Forced to
  a non-serverless DBR 15.4 + Photon cluster (a Java subprocess can't run
  on serverless). Worker count scales with SF: single-node up to SF=1000;
  +1 worker per 1000 of SF above that. Outputs are single files per table
  (`Customer.txt`, `Trade.txt`, etc.).

The benchmark ingestion code reads either format via brace-alternation globs
of the form `{Customer.txt,Customer_[0-9]*.txt}`, so the rest of the pipeline
is identical.

##### Interface
The Driver creates a datagen job with parameters:
  - `spark_or_native_datagen` — `spark` (default) or `native` (case-insensitive, trimmed)
  - `scale_factor` — `10`, `100`, `1000`, `5000`, `10000`, `20000`
  - `catalog` — target catalog for the output volume (default `main`)
  - `regenerate_data` — `YES` to wipe and regenerate; `NO` (default) to no-op if output already exists
  - `log_level` — `DEBUG` / `INFO` / `WARN` (consumed by the Spark runner; ignored by DIGen)

Output path:
  - `spark` → `/Volumes/{catalog}/tpcdi_raw_data/tpcdi_volume/spark_datagen/sf={scale_factor}/`
  - `native` → `/Volumes/{catalog}/tpcdi_raw_data/tpcdi_volume/sf={scale_factor}/`

Native pre-flight: before any volume side effect, `digen_runner` verifies (a)
`java` is callable, (b) a writable scratch directory is available
(`/local_disk0` preferred; `/tmp` fallback for SF≤100), and (c) DBR ≤ 15.4.
If any check fails the job hard-aborts with a clear "switch the cluster to
SINGLE_USER access mode" hint and **no volume data is touched**.

![Type of Raw Data Generated](/src/tools/readme_images/data_gen.png "Type of Raw Data Generated")

##### Architecture highlights
- **Dependency-graph scheduling** — Reference / HR / FINWIRE / Prospect start
  concurrently; CustomerMgmt starts as soon as HR publishes the `_brokers`
  temp view; Trade / DailyMarket / WatchHistory start as soon as FINWIRE
  publishes `_symbols` (via an early threading-event + Parquet-staged view).
  On serverless this means all four heavy phases overlap.
- **Serverless-compatible caching** — Spark Connect forbids `persist()` /
  `UNCACHE TABLE`, so large intermediates are staged to Parquet in
  `_staging/` and read back. `cleanup_staging` removes these at end of run.
- **Bijection-based ID scheduling** — CustomerMgmt C_IDs and CA_IDs for
  INACT / CLOSEACCT / UPDCUST / UPDACCT / ADDACCT actions are pre-computed
  at the driver via a numpy-vectorised implementation of DIGen's
  `GrowingOffsetPermutation` (pool bijection with skip-resolution over
  cumulative deletions). Row counts match DIGen exactly at every scale.
- **FINWIRE split + repartition** — CMP, SEC, and FIN subsets write to
  independent staging dirs in parallel instead of being union'd into a
  single 244 M-row stream. FIN is repartitioned to `max(8, sf // 25)`
  partitions ahead of the cross-join so it fans out across the executor
  pool instead of stacking on spark.range default partitioning.
- **Static audit snapshots** — pre-computed `*_audit.csv` files for the
  common scale factors live at `src/tools/tpcdi_gen/static_audits/sf={sf}/`
  and are copied at the end of a run instead of being regenerated. Unknown
  scale factors fall back to dynamic audit regeneration.
- **FUSE-hardened file concat** — output parts bin-packed into ~128 MB
  files via pure-Python `shutil.copyfileobj` with per-source retries
  (handles `EAGAIN` from the UC Volume FUSE mount under heavy parallel I/O).

##### Notes
- The generator is deterministic — same scale factor always yields the
  same row counts and audit values.
- Partition sizing has been tuned per phase (Prospect, FINWIRE/FIN,
  CustomerMgmt schedule DF) to avoid spill on serverless while leaving
  executor capacity for concurrent phases.
- If the target scale factor directory already exists and
  `regenerate_data=NO`, the step is a no-op (safe to re-run).

#### Workflow Creation
The Databricks TPC-DI is developed in a way to make the execution simple and easy.  
- The TPC-DI Driver will provide simple default dropdown widgets. These widgets are populated based on your environment after running the setup command in the first cell.
  - Some are set to defaults that we suggest changing - such as workflow type and serverless.
  - Others are set to the suggested values and pre-populated with the actual options available.  These include DBR, Worker/Driver Type, Job Name, Target Catalog (we suggest only changing this one in cases where you don't have permissions to create a catalog), and target database
- If you prefer to change some values such as DBR, Worker/Driver Types, Scale Factor, etc, then the widgets give you the flexibility to do so.
  - Note that the widgets are dynamic and are added, removed, and populated based on your Databricks workspace configuration, cluster configuration, workflow type chosen, and whether Serverless is chosen.  For example, worker/driver type and DBR are removed as widgets if choosing serverless.
- Workflow creation should take less than 1 second.  When complete you will be given a link to open your workflow.

##### CLUSTER/WAREHOUSE SIZING
###### DBSQL Warehouse Size
- A ***SERVERLESS*** DBSQL Warehouse will **automatically** be generated for you, if one doesn't exist already.
- Additionally, to keep down on the number of warehouses created if multiple users will be launching this benchmark in the workspace, the warehouse names are generic and are intended to be shared across other users.
- All created warehouses are only as big as they need to be and are configured with an extremelely low auto-terminate value of 1 minute to keep costs as low as possible.
- The names and sizes are below:  
  | Scale Factor | Name and Size  |
  | ------------ | -------------- |
  | 10           | TPCDI_2X-Small |
  | 100          | TPCDI_2X-Small |
  | 1000         | TPCDI_Small    |
  | 5000         | TPCDI_Large    |
  | 10000        | TPCDI_X-Large  |

###### Cluster Size
*The following applies only when **not choosing SERVERLESS** as the compute choice. Serverless will automatically scale to meet the demands of the job*
- The workflow that is created by running these notebooks will size the cluster according to your scale factor and node type selected.  
- To reduce complexity in making this "generic" approach to creating a dynamic job, we abstracted away the sizing of the cluster (number of worker nodes).  Repeated tests have revealed the optimal number of cores per scale factor to achieve the BEST TCO (not necessarily the fastest job but the cheapest way to execute - which is the goal of the benchmark).  A single node cluster could handle all the way up to a scale factor of 5000, depending on the size of that node.  The chart is below.
    | Scale Factor | Total Raw Data | Optimal Worker Cores|
    | ------------ | -------------- | ------------------- |
    | 10           | .961 GB        | As low as 1         |
    | 100          | 9.66 GB        | As low as 2         |
    | 1000         | 97 GB          | 16                  |
    | 10000        | 970 GB         | 144                 |
- **IF** you truly want to change these values, we suggest building your workflow using the workflow_builder and then changing the cluster configuration from the workflow created.  

### Step 2) TPC-DI Workflow Execution
- After opening the link provided for your workflow, to begin execution click the **Run** button. To watch execution, open the new Job Run.  

# Footnote Disclaimer
As the Databricks implementation of the TPC-DI is an unpublished TPC Benchmark, the following is required legalese per TPC Fair Use Policy:  
*The Databricks TPC-DI is derived from the TPC-DI and as such is not comparable to published TPC-DI results.  The current scoring metrics for the TPC-DI preclude any official submission for cloud-based execution and therefore The Databricks TPC-DI can not OFFIALLY be submitted under current scoring metrics.  Databricks withholds the ability to submit an official submission to the TPC for this benchmark upon future revision of its scoring metrics. Prior to that, we maintain that this implementation follows all guidelines, rules, and audits required of the official TPC-DI specification.*
