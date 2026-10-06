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

Builder: `src/tools/workflow_builders/augmented_fabric_spark.py`.
