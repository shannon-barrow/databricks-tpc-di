# Cross-CDW competitor benchmarks

The competitor benchmarks run the **same Augmented Incremental TPC-DI workload**
(365-day daily-streaming dbt project) against a non-Databricks cloud data
warehouse, reading the **same source data** Databricks generated. Run a
competitor next to the Databricks Augmented Incremental dbt variant to compare
Databricks vs Snowflake / Amazon Redshift Serverless / Google BigQuery on
identical data and business logic.

| Engine | Cloud | Auth | Data access |
|---|---|---|---|
| **Snowflake** | any | PEM keypair (or password/MFA) + Databricks PAT for UC federation | external stage over the UC volume |
| **Redshift Serverless** | AWS | password over PG wire (port 5439) | `COPY` from S3 via an IAM role |
| **BigQuery** | GCP | service-account key JSON | external tables over the GCS-backed volume |

## How they're created and run

Use the **`src/TPC-DI Competitor Driver`** notebook (see its own header for the
full input reference). It runs *as you* — no service principal — and creates
two Databricks Jobs per engine; it does **not** run them.

```
Parent  {prefix}-SF{sf}-AugmentedIncremental-{Engine}-Parent
  setup_{engine}                         self-bootstraps the engine's staging +
    │                                    per-run schema from the Databricks
    │                                    tpcdi_incremental_staging_{sf} schema
    └─ for_each_task over batch_date_ls
         └─ Child  {prefix}-SF{sf}-AugmentedIncremental-{Engine}-Child
              simulate_filedrops_{engine}   drop the day's staged files into
                │                           the shared UC volume
                └─ dbt_run                  bronze + silver + gold for that batch
  cleanup / teardown  (gated by delete_tables_when_finished)
```

**Prerequisite:** the Databricks Augmented Incremental Stage 0
(`augmented_staging`) must have run for this scale factor first, so the per-day
staged files and `main.tpcdi_incremental_staging_{sf}` schema exist. The
competitor's `setup_{engine}` seeds its own staging from that.

**Smoke test** (what we validate a fresh port with): trigger the parent with
`scale_factor=10`, `incremental_batches_to_run=2`,
`delete_tables_when_finished=FALSE`. A pass is `setup_{engine}` SUCCESS plus 2
`for_each` iterations SUCCESS (each = `simulate_filedrops` + `dbt_run` against
the engine).

## Credentials — Unity Catalog secrets

Only genuine secrets (passwords / keys / tokens) are UC secrets; hosts, users,
accounts, warehouses, ARNs, and project ids are plain job parameters.

- **Home:** `main.tpcdi_raw_data` (the driver's default secret schema). Do not
  use `main.default` (often not writable) and do not create a new schema — use
  the existing `tpcdi_raw_data`.
- **Named for what they unlock.** Defaults derive from the target
  login/account/project so a secret is **created once per deployment and reused**
  by the whole team (collisions are intended, like the shared staged data):
  - Redshift: `redshift_<user>_pw_secret`
  - Snowflake: `snowflake_<user>_cred_secret` + `snowflake_<account>_dbx_pat_secret`
  - BigQuery: `bigquery_<project>_sa_json_secret`
- **Consumer-only.** The driver/notebooks never create secrets. Create them once
  via Catalog Explorer (schema → Create secret) or
  `POST /api/2.1/unity-catalog/secrets`. The driver validates each referenced
  secret (exists+readable / exists-but-no-access / missing) but never blocks.
- **Runtime requirement:** UC secret reads need **serverless env v5+** (or
  DBR 17.3 LTS+). Below v5, `dbutils.secrets.get` silently returns an empty
  value. The workflow builders pin the serverless env to v5.

## Compute

Every task defaults to **serverless** (env v5) — zero cluster config. The child
tasks (`simulate_filedrops_*`, `dbt_run`) accept an `interactive_cluster_id` to
run on a classic cluster instead; both paths are supported. Notebooks that need
an engine client library declare it in their serverless env **and** defensively
`pip install` when run on a classic cluster, so either compute works OOTB.

dbt adapter pins (in the builders' serverless env specs): `dbt-redshift==1.10.1`
+ `dbt-core==1.11.8`, `dbt-snowflake==1.9.*` + `dbt-core==1.9.*`,
`dbt-bigquery==1.11.1`.

## Layout

```
dbt/competitors/
├── README.md                     this file
├── snowflake/
│   ├── PORT_NOTES.md / PLAN.md / DYNAMIC_TABLES_DESIGN.md
│   ├── _sf_conn.py               snowflake.connector factory (keypair/MFA sniff)
│   ├── setup_sf.py               per-run: federation + CLONE staging → run schema
│   ├── simulate_filedrops_sf.py  per-batch file drop into the UC volume
│   ├── run_dbt.py                per-batch dbt run --target snowflake
│   ├── seed_staging*.py / sf_staging_bootstrap.py   one-time staging seed
│   └── dynamic_tables/           Snowflake Dynamic Tables variant (design + notebooks)
├── redshift/
│   ├── PORT_NOTES.md
│   ├── _rs_conn.py               psycopg2 factory (cold-start retry)
│   ├── setup_rs.py / rs_staging_bootstrap.py
│   ├── simulate_filedrops_rs.py / run_dbt.py / teardown_rs.py
│   └── rs_metrics.py / make_staging_share.py
├── bigquery/
│   ├── PORT_NOTES.md
│   ├── _bq_conn.py               google.cloud.bigquery client factory
│   ├── setup_bq.py / bq_staging_bootstrap.py
│   └── simulate_filedrops_bq.py / run_dbt.py / teardown_bq.py
└── {snowflake,redshift,bigquery}_models/   per-engine dbt model trees
```

The workflow builders live in `src/tools/workflow_builders/augmented_{engine}.py`;
the driver's generator is `src/tools/generate_competitor_workflow.py`. The dbt
project itself (models, macros, adapter dispatch) is documented in
[`../README.md`](../README.md).

## Validation status

Each engine has passed a `scale_factor=10`, 2-batch smoke test on serverless
(`setup_{engine}` + both batch iterations SUCCESS):

| Engine | Workspace | Notes |
|---|---|---|
| Redshift | AWS (serverless-only) | connection retry absorbs workgroup cold-start |
| Snowflake | AWS | dual secret (keypair + PAT), external-stage federation |
| BigQuery | GCP | serverless-default child tasks; no numpy ABI issue on env v5 |

See each engine's `PORT_NOTES.md` for design decisions and the SQL-dialect
translation detail.
