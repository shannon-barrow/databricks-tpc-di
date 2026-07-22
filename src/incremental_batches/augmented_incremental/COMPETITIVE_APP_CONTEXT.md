# Competitive benchmark app — context & prerequisites

Seed context for a Databricks App that helps a user set up and run the
competitive (non-Databricks) TPC-DI benchmarks.

The app does NOT orchestrate runs — the emitted **workflows** orchestrate
themselves (parent -> for_each -> child), exactly like the native driver.
The app's job is:

1. **Collect** the prerequisites each run type needs (a native Databricks
   run derives almost everything; a competitor run can't — workspace,
   bucket, account, warehouse, credentials, federation, etc.).
2. **Emit** the workflow(s) by calling the per-engine `create()` in each
   port's `create_jobs.py`.
3. **Adjust / diagnose** the workflows it created — param overrides (e.g.
   "only run 50 batches"), and read-only failure triage (explain why a run
   failed, usually a prerequisite that isn't wired).

Rule for credentials: the app never handles secret values. Each port reads
its credentials from **Unity Catalog secrets** — the operator creates the
secrets under a `{catalog}.{schema}` they own, and the app collects only that
catalog + schema, passing the pair as `secret_catalog` / `secret_schema` job
params. At run time the port reads each key via
`dbutils.secrets.get(catalog=…, schema=…, key=…)`. The app is never a
credential store, and no secret value flows through job params. (Requires a
runtime that supports UC secrets: DBR 17.3 LTS+ or serverless env v4+.)

---

## Native Databricks — almost everything derives

| Input | Kind | Notes |
|---|---|---|
| scale_factor | pick | |
| variant (Cluster / DBSQL / SDP / dbt) | pick | |
| user / repo_src_path / catalog / wh_target | derived | via `current_user()` + notebook context, as the driver does today |
| incremental_batches_to_run | param, default 365 | override to run fewer |

No external prerequisites. This is why the native path "just works" from the driver.

---

## Redshift

Maps to `redshift/create_jobs.py::create()`.

**Prerequisites that must exist before a run** (the app should verify each):

- A Redshift Serverless **workgroup** (default db `dev`), reachable on 5439.
- An **IAM role** attached to the workgroup with S3 read on the staging bucket
  — its trust policy must include the workgroup's principal (COPY assumes it).
- An **S3 bucket** for staging, in the same region as the workgroup, surfaced
  as the UC external volume the Spark datagen writes to.
- **UC secrets** under `{secret_catalog}.{secret_schema}` (default
  `main.tpcdi_redshift`) holding keys: host, port, database, user, password,
  iam_role.
- One-time **staging seed** (`tpcdi_staging_sf{sf}`) — expensive; setup_rs
  self-bootstraps it and skips if already present with matching row counts.

**App form fields:** scale_factor; target-workspace profile; `s3_volume_prefix`
(required); aws_region; wh_db/database/catalog (defaults fine); secret_catalog +
secret_schema (the UC location where you created the host/user/password/iam_role
secrets).

---

## BigQuery

Maps to `bigquery/create_jobs.py::create()`.

**Prerequisites:**

- A **BigQuery project** (passed as `catalog`).
- A **GCS bucket** for staging, in the same region, surfaced as the UC
  external volume.
- A **service-account JSON key** with BigQuery Data Editor + Job User, stored
  as a **UC secret** under `{secret_catalog}.{secret_schema}` (default
  `main.tpcdi_bigquery`, key `sa_json`).
- One-time staging seed (bootstrap step).

**App form fields:** scale_factor; GCP-workspace profile; `catalog` (BQ project,
required); `gcs_volume_prefix` (required); bq_location; wh_db (default fine);
secret_catalog + secret_schema (the UC location where you created the `sa_json`
secret).

---

## Snowflake — the heaviest setup (catalog federation)

Maps to `snowflake/setup_sf.py` + `sf_staging_bootstrap.py`. Snowflake reads
the Databricks-generated data over **UC Iceberg-REST catalog federation**, so
there's a whole bridge to stand up beyond "account + warehouse + creds".

**Databricks side (must exist first — NOT created by the port code):**

- Source tables in `main.tpcdi_incremental_staging_{sf}` have **UniForm enabled
  and deletion vectors off** — Databricks' Iceberg-REST endpoint only exposes
  Delta tables with UniForm on. (`_enable_uniform_on_sources` ALTERs these.)
- A **Databricks PAT** with access to that UC catalog, for Snowflake to
  authenticate to the Iceberg-REST endpoint.

**Snowflake side:**

- A **CATALOG INTEGRATION** (e.g. `TPCDI_DBX_UC_SF10_INT`) pointing at the
  Databricks UC Iceberg-REST endpoint, its `REST_AUTHENTICATION` bearer token
  = the Databricks PAT. The integration is generic — target any namespace via
  `CATALOG_NAMESPACE` on the per-table `CREATE ICEBERG TABLE`.
- **Federated Iceberg tables** — `CREATE OR REPLACE ICEBERG TABLE ... CATALOG =
  <integration> CATALOG_NAMESPACE = 'tpcdi_incremental_staging_{sf}'` per source
  table (setup builds these; also the fix for stale-federation errors).
- A **warehouse** to run the models (or a Dynamic Tables warehouse).
- A **stage** for the per-batch file drops.
- **UC secrets** under `{secret_catalog}.{secret_schema}` (default
  `main.tpcdi_snowflake`): account, user, password (or keypair), warehouse, and
  the Databricks PAT key.

**Known failure the app should recognize:** `SHOW SCHEMAS` works but
`SELECT`/`CREATE ICEBERG TABLE ... CATALOG=<int>` fails to vend S3 creds ->
the catalog integration's PAT is expired. Fix: `ALTER CATALOG INTEGRATION
<int> SET REST_AUTHENTICATION = (BEARER_TOKEN='<fresh PAT>')` (only that field
is alterable; TYPE is not). Refresh via `dbx_pat_secret_key`.

**App form fields:** scale_factor; Snowflake account, warehouse, stage;
target database (catalog) + wh_db; catalog-integration name; secret_catalog +
secret_schema (the UC location where you created the Snowflake user/password,
keypair, and Databricks-PAT secrets). Plus a pre-flight that confirms UniForm
is enabled on the sources.

---

## Adjust & diagnose modes

- **Adjust:** distinguish a **param override** (e.g. `incremental_batches_to_run
  = 50` -> a `run-now` param, no rebuild) from a **rebuild** (warehouse size,
  new variant -> re-emit via `create()`). Prefer the override when it suffices.
- **Diagnose:** read run state + the per-engine failure signal, correlate, and
  explain in plain language. Any *mutation* (re-run, re-seed) is an explicit
  confirmed action, never auto-executed.

Where each engine surfaces failures:

| Engine | Failure signal |
|---|---|
| Redshift | per-batch volume `.log` from run_dbt; `SYS_LOAD_ERROR_DETAIL` (Serverless, not STL_LOAD_ERRORS); `SYS_QUERY_HISTORY` |
| BigQuery | BQ job errors; `INFORMATION_SCHEMA.JOBS_BY_PROJECT` |
| Snowflake | query errors; catalog-integration credential-vending failures (usually expired PAT) |
| All | Jobs API run/task state; expired workspace/CLI token symptoms |
