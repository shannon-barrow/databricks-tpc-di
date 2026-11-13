# Spark-engine competitor benchmarks

The Spark-engine competitors run the **same Augmented Incremental TPC-DI
workload** as the Databricks Cluster variant — the same PySpark transforms, run
as batch notebooks — on a competitor's managed Spark, reading the **same source
data** Databricks generated. They're the Spark counterpart to the dbt warehouse
competitors in [`../dbt/competitors/`](../dbt/competitors/README.md); the same
**Competitor Driver** notebook creates both kinds.

| Engine | Cloud | Variants | Auth |
|---|---|---|---|
| **Microsoft Fabric Spark** | Azure | `fabric_spark` (JVM Spark), `fabric_nee` (Native Execution Engine: Velox via Gluten) | Entra service principal (client credentials) |
| **Amazon EMR (on EC2)** | AWS | `emr` (EMR Spark 3.5, Delta via delta-spark, Glue catalog) | an AWS identity: UC service credential, instance profile, or (laptop driver) an AWS CLI profile |

`fabric_spark` and `fabric_nee` run byte-identical notebooks. `batch_runner`
sets `spark.native.enabled` for the session from the job's `enable_nee`
parameter, so NEE on/off is the only difference between the two.

## How they're created and run

In the **`src/TPC-DI Competitor Driver`** notebook, pick `fabric_spark` or
`fabric_nee` (offered on Azure workspaces), fill the Fabric inputs and run the
Create cell. It creates two jobs:

```
Parent  {prefix}-SF{sf}-AugmentedIncremental-{FabricSpark|FabricNEE}-Parent
  setup_fabric      0. provision Fabric: capacity check, fixed-size custom
    │                  Spark pool, Environment (runtime 2.0 on that pool),
    │                  deploy notebooks/ as Fabric notebooks
    │               1. DEEP CLONE main.tpcdi_incremental_staging_{sf} into the
    │                  lakehouse as staging_sf{sf} (once per SF)
    │               2. run the Fabric `setup` notebook: reset {wh_db}_{sf}
    │                  (SHALLOW CLONE staging, create empty bronze tables)
    │               3. emit batch_date_ls
    └─ for_each_task over batch_date_ls (sequential)
         └─ Child  {prefix}-SF{sf}-AugmentedIncremental-{...}-Child
              simulate_filedrops_fabric   copy the day's staged files into
                │                         the lakehouse's OneLake Files
                └─ run_fabric             run Fabric `batch_runner` for that
                                          date (bronze → silver/gold DAG via
                                          runMultiple in one Fabric session)
  cleanup  (gated by delete_tables_when_finished): drop {wh_db}_{sf} + the
           run's file drops; staging_sf{sf} and the Fabric items are kept
```

Headless equivalent: `fabric/create_jobs.py <sf> --variant spark|nee ...`.

**Prerequisite:** the Databricks Augmented Incremental Stage 0
(`augmented_staging`) has run for this scale factor.

**Smoke test:** trigger the parent with `incremental_batches_to_run=2`,
`delete_tables_when_finished=FALSE`. A pass is `setup_fabric` SUCCESS plus 2
`for_each` iterations SUCCESS.

## One-time Fabric prerequisites

1. A Fabric **workspace on a running capacity**. setup fails fast if it can
   see the capacity is paused; a service principal can't always see the
   capacity, in which case it just warns.
2. A **schema-enabled Lakehouse** in that workspace (its id goes in the
   Driver).
3. An Entra **service principal** that is a member of the workspace:
   **Contributor** to create the environment and notebooks, **Admin** if setup
   has to create the custom Spark pool (Contributor is enough when a pool of
   that name already exists — setup resizes it to the requested size).
   The tenant setting that lets service principals call Fabric APIs must be on.
4. The SP's client secret as a **UC secret** in `main.tpcdi_raw_data`, named
   `fabric_<client_id>_sp_secret` (the Driver's default). Fabric DW uses the
   same secret.

## Inputs (Driver widgets)

| Widget | Meaning |
|---|---|
| `fab_workspace_id`, `fab_lakehouse_id` | from the Fabric item URLs |
| `fab_tenant_id`, `fab_client_id` | the service principal |
| `fab_pool_name` | custom pool to create or resize (default `tpcdi_spark_pool`) |
| `fab_node_size`, `fab_node_count` | pool size; default **16 × Medium = one F64** (64 CU = 128 base vCores; a Medium node is 8 vCores) |
| secret catalog / schema / name | the SP client secret's UC path |

The pool is fixed-size (no autoscale, no dynamic allocation) so every batch
runs on the same compute — the Fabric analog of picking a warehouse size.
Driver and executors take a whole node (Medium = 8 cores / 56 GB).

## Compute (Databricks side)

The Databricks tasks only orchestrate and copy files — the transforms run in
Fabric — but they need **classic compute**: the OneLake writes go through
`fs.azure.*` Spark confs, which serverless doesn't allow. With no
`interactive_cluster_id` each job gets a single-node DBR 18 LTS job cluster
with dedicated access (17.3 LTS is the floor for UC secrets on classic). That
cluster starts once per child run — several minutes per batch, outside the
Fabric-measured time but real wall-clock over 365 batches — so for long runs
pass an interactive cluster (DBR 17.3 LTS+, dedicated access).

Don't run the Spark and NEE jobs at the same time in one workspace: they share
the pool and the deployed notebooks.

## Measuring results

Fabric reports per-batch time and capacity usage itself — use the Fabric
Monitor's **Running duration** per `batch_runner` run (Total = Queued +
Running), not the Databricks task duration (which includes the job-cluster
start and REST polling). TCO at fixed capacity = running hours × the capacity's
hourly rate (F64 pay-as-you-go: 64 CU × $0.18/CU-hr = $11.52/hr).

## Validation status

Both variants passed a `scale_factor=10`, 2-batch smoke test end to end from
jobs created by the Competitor Driver, on default job clusters (no interactive
cluster), with setup provisioning the environment and deploying the notebooks
as the service principal. Checked on the Fabric side: the batch sessions ran
on Runtime 2.0 on the custom pool (15 executors x 8 cores, dynamic allocation
off), and the NEE run's plans were native (`*Transformer`,
`VeloxColumnarToRow`, `ColumnarExchange`).

## Amazon EMR (on EC2)

The same 16-node per-batch DAG as Fabric's `batch_runner`, run as **one EMR
step per batch** on a fixed-size on-demand cluster (no managed scaling, no
dynamic allocation, one whole-node executor per core node, driver on the
primary node in client mode). Tables are Delta (EMR's bundled delta-spark) in
the Glue database `{wh_db}_{sf}`, under `{s3}_emr/warehouse/`.

**What's shared with Fabric.** `emr/jobs/tpcdi_sql.py` holds the transforms
as SQL builders. `tests/test_emr_variant.py` checks they match the Fabric
notebooks token for token. The one change is DimAccount: Fabric's
`SELECT * except(...)` becomes an explicit column list, because OSS Spark 3.5
has no star-except. The bronze tables drop Fabric's `CLUSTER BY` because OSS
Delta only clusters on OPTIMIZE. They keep `dataSkippingNumIndexedCols=34`,
so the per-batch date filters skip earlier days' files. `emr/jobs/tpcdi_dag.py`
is the runMultiple DAG (same edges), run as threads on one SparkSession in
FAIR mode.

**Staging.** Same flow as Fabric. Once per SF, Databricks DEEP CLONEs the 20
`main.tpcdi_incremental_staging_{sf}` tables to Delta on S3 at
`{s3}_emr/staging_delta/sf={sf}/{table}` (`emr/_emr_staging.py`). `setup_emr`
does this as its first step, and `emr/materialize_staging_emr.py` does it as a
standalone one-time job (`scale_factor` = one SF or a comma list,
`force_materialize`). Tables already there are skipped unless
`force_materialize=true`. Each run's EMR `setup` step then SHALLOW CLONEs every
table into the Glue database (`CREATE TABLE db.t SHALLOW CLONE
delta.\`s3://.../t\` LOCATION '{db location}/t'`), a zero-copy reset, and
creates the 6 empty bronze tables.

How EMR still differs from Fabric:
- The copies must be readable and writable by EMR's open-source delta-spark
  3.x, because the shallow clones keep the source's protocol and the batches
  MERGE into them. The materialize turns deletion vectors off on each copy and
  purges any (REORG), so EMR only clones plain parquet paths and its MERGEs
  rewrite files. It then checks each copy's `tableFeatures` against what
  delta-spark 3.x implements. A copy with anything else is deleted and
  rewritten as a plain CTAS with those features off: a CREATE OR REPLACE
  can't drop a protocol feature. Column mapping (`name`), v2 checkpoints, row
  tracking and liquid clustering are all in delta-spark 3.x and stay.
- A UniForm (Iceberg-enabled) staging table can't be DEEP CLONEd to a path
  outside Unity Catalog (`DELTA_UNIFORM_NOT_SUPPORTED`). The tpcdi-fresh
  staging tables are UniForm, so their copy is a plain CTAS rewrite instead,
  still written once per SF.
- The clone target is tried as the `/Volumes` path first, then the `s3://`
  uri; the first form Databricks accepts is used.
- No ANALYZE after the clone: OSS Delta sizes a table from its log, so the
  stat-less clones don't trip the broadcast misestimate Fabric guards against.
- Don't force a re-clone while a run is using that SF: its shallow clones read
  the staging's files. Nothing VACUUMs the staging.

**Two ways to drive it** (same `emr/_emr_conn.py` underneath):

| | Databricks jobs (Competitor Driver → `emr`) | Laptop driver `emr/emr_bench.py` |
|---|---|---|
| Shape | parent `setup_emr` → for_each child (`simulate_filedrops_emr` = the Redshift drop notebook, `run_emr`) → `teardown_emr` (ALL_DONE: cleanup if `delete_tables_when_finished`, report, terminate) | `plan` / `create` / `setup` / `run` / `report` / `terminate` / `cleanup` / `smoke` |
| AWS identity | UC service credential (`aws_service_credential`, serverless env v5) or an interactive cluster's instance profile; needs EMR + `iam:PassRole`. S3 goes through the UC volume | any boto3 credential (SSO profile) with EMR, S3 and Glue on the account |
| Use when | the workspace has such an identity | it doesn't (the case today, see below) |

`create`, `run` and `smoke` refuse to start without `--yes`. `plan` makes no
AWS calls: it validates the RunJobFlow spec offline against the EMR API model
and prints the cost estimate.

**Measuring.** Per batch: the EMR step's start/end (`DescribeStep`), plus the
runner's own per-table start/end/seconds (`batch={date}.json`). Both land in
`{s3}_emr/metrics/{db}/{run_id}/` as `batches.csv` and `task_times.csv`.
Batch cost = step seconds x the fleet's on-demand $/hr. The run total is
`cluster_cost.json`: every instance's lifetime from `ListInstances`, billed
per second with EC2's 60 s minimum. Rates are EC2 Linux on-demand + the EMR
per-instance uplift + the gp3 root volume. The rates come from
`emr/prices_snapshot.json` (AWS Pricing API, us-west-2, fetched 2026-11-13);
`--prices live` re-fetches them. For example, m8gd.2xlarge = $0.46128 EC2 +
$0.11532 EMR + 32 GB gp3 ($0.0035) = **$0.5801/hr per node**.

**Sizing.** For comparison with Fabric's F64 (16 Medium nodes = 128 vCores),
use 16 x m8gd.2xlarge core nodes (128 vCPUs) plus 1 primary: $9.86/hr. The
SF=10 smoke test uses 2 core nodes + 1 primary: $1.74/hr.

**Smoke test (laptop):**
`python emr_bench.py smoke --yes --batches 2 --core-count 2 --profile <p> --s3-root s3://<bucket>/<prefix>/ --wh-db <prefix>_emr --sf 10 --subnet <subnet> --master-sg <sg> --slave-sg <sg>`.
It creates the cluster, runs setup and 2 batches, and terminates the cluster
even if a step fails. Then it writes the cost report. A pass is both batches
reporting `SUCCESS`, with 16 SUCCESS rows each in `task_times.csv`.

**Validated (laptop driver, 2026-11-13, m8gd.2xlarge, 4 batches each, all
SUCCESS):**

| SF | Core nodes | Setup | Step per batch | $ per batch | Cluster total |
|---|---|---|---|---|---|
| 10 | 2 | 124 s | 58-60 s | $0.03 | $0.26 (2 batches) |
| 100 | 4 | 86 s | 64-70 s | $0.05 | $0.56 |
| 1000 | 8 | 84 s | 74-82 s | $0.11 | $1.12 |
| 20000 | 16 | 174 s | 276-278 s | $0.76 | $4.57 |

Step = the EMR step's wall (DescribeStep), file drop excluded. At SF=20000
this is the F64-equivalent sizing above.

## Layout

```
spark_competitors/
├── README.md                       this file
└── fabric/
    ├── PORT_NOTES.md               design notes + gotchas from the port
    ├── _fabric_conn.py             Fabric REST: SP token, provisioning (pool,
    │                               environment, notebook deploy), job runs
    ├── setup_fabric.py             parent setup (Databricks side)
    ├── simulate_filedrops_fabric.py / run_fabric.py   child tasks
    ├── teardown_fabric.py          gated cleanup
    ├── create_jobs.py              headless job creation
    └── notebooks/                  deployed INTO Fabric by setup_fabric
        ├── setup.py / batch_runner.py / ingest_bronze.py
        ├── account_updates_from_customer.py
        └── incremental/*.py        the 8 silver/gold transforms
```

```
└── emr/
    ├── _emr_conn.py                boto3: cluster spec, steps, file drops, pricing, cost
    ├── emr_bench.py                laptop driver (CLI)
    ├── setup_emr.py / run_emr.py / teardown_emr.py   Databricks tasks
    ├── prices_snapshot.json        on-demand EC2 + EMR + gp3 rates
    └── jobs/                       uploaded to S3, run ON EMR by spark-submit
        ├── emr_runner.py           setup | batch | drop
        ├── tpcdi_sql.py            the transforms (Fabric port)
        └── tpcdi_dag.py            per-batch DAG + scheduler
```

Builders: `src/tools/workflow_builders/augmented_fabric_spark.py`,
`src/tools/workflow_builders/augmented_emr.py`.
