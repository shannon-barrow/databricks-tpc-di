# TPC-DI Augmented Incremental — Snowflake variant: architecture

> **Secret home (current).** The two secrets below (`sf_credential_secret`,
> `dbx_pat_secret`) now live in **`main.tpcdi_raw_data`**, not the old
> `main.tpcdi_snowflake` scope this doc names. They're **named for what they
> unlock** (defaults `snowflake_<user>_cred_secret` +
> `snowflake_<account>_dbx_pat_secret`) and created once + reused. The two-secret
> split and PEM/MFA sniffing described here are still accurate. See
> [`../README.md`](../README.md) for the authoritative cross-engine secret model.

Cross-platform benchmark of the TPC-DI Augmented Incremental workload running on
Snowflake, driven by a Databricks workflow. The dbt project is the same one
that powers the Databricks dbt variant; only the `--target` changes.

## Components

### One-time setup (manual, per scale factor)

1. **AWS / UC plumbing.** A UC external location and managed external volume
   exist for a customer-controlled S3 bucket in `us-west-2` (matching the
   Snowflake account's region). The bucket's IAM role trusts Snowflake's
   IAM user (per the `STORAGE_AWS_IAM_USER_ARN` + external ID from a
   `DESC STORAGE INTEGRATION` on the Snowflake side). Snowflake has a
   matching `STORAGE INTEGRATION` and `STAGE` pointing at a prefix under
   that bucket.

2. **Staging-table seed** (`seed_staging.py`). One-time Spark→Snowflake JDBC
   copy of `main.tpcdi_incremental_staging_{sf}` (Databricks) →
   `TPCDI_TEST.STAGING_SF{sf}` (Snowflake). The same reference + dimension
   tables the Databricks `setup_dbt.py` clones from on the Databricks side,
   but materialized into Snowflake so the Snowflake-side `setup_sf.py` can
   `CLONE` from it cheaply on every parent run. Cost: ~minutes per SF, paid
   once.

### Per-run setup (`setup_sf.py`)

Runs on an interactive cluster (no serverless required — this notebook just
dispatches SQL to Snowflake). Equivalent to the Databricks `setup_dbt.py` but
the work happens in Snowflake.

1. `CREATE OR REPLACE SCHEMA TPCDI_TEST.{wh_db}_{sf}`
2. For each of the ~12 reference/dimension tables, run
   `CREATE OR REPLACE TABLE TPCDI_TEST.{wh_db}_{sf}.<table> CLONE TPCDI_TEST.STAGING_SF{sf}.<table>`.
   Zero-copy and instant.
3. Pre-create the 16 bronze/silver/gold target tables (same DDL as our
   `setup.py` already has) with the right `CLUSTER BY` keys.
4. Emit `batch_date_ls` task value (list of ISO dates) for the parent's
   `for_each_task` to loop over — same convention as `augmented_sdp.py`.

### Per-batch tasks

Both run on the same interactive cluster (same as `simulate_filedrops` on the
existing Databricks variants).

1. **`simulate_filedrops_sf.py`** — copies that day's `.txt` files from the
   per-SF `_staging/` tree to the per-`(wh_db, sf, batch_date)` directory under
   the UC external volume. Databricks writes via UC; Snowflake reads the same
   bytes via its storage integration. Path convention follows our existing
   `_dailybatches/{wh_db}_{scale_factor}/{batch_date}/` layout, just rooted at
   the new external volume's `tpcdi_directory` instead of the managed Volume.

2. **`dbt_run.py`** — pip-checks `dbt-snowflake==1.9.*` (no-op if already on
   the cluster as a library), writes a per-run `profiles.yml` from the
   UC secrets under `main.tpcdi_snowflake` (catalog.schema), then runs:

   ```
   dbt run --target snowflake \
       --profiles-dir <tmp> \
       --project-dir <workspace_repo>/src/incremental_batches/augmented_incremental/dbt \
       --vars '{catalog: TPCDI_TEST, wh_db: <wh>, scale_factor: <sf>,
                batch_date: <iso>, snowflake_stage: TPCDI_STAGE,
                tpcdi_directory: /Volumes/main/tpcdi_raw_data/tpcdi_benchmarking/}'
   ```

   No per-batch model-list selection — dbt's DAG handles bronze→silver→gold
   for the date. Same vars contract as the Databricks dbt variant (just
   different `tpcdi_directory` + the extra `snowflake_stage`).

### Workflow shape (`augmented_snowflake.py`)

Mirrors `augmented_sdp.py`:

```
Parent (augmented_snowflake_parent):
  setup_sf
    └─ run_if=ALL_SUCCESS → loop_incremental_tpcdi (for_each over batch_date_ls)
                              └─ Child job per batch_date
  cleanup (gated by delete_tables_when_finished, ALL_DONE)

Child (augmented_snowflake_child):
  simulate_filedrops_sf  (interactive cluster)
    └─ dbt_run            (same interactive cluster, depends_on simulate_filedrops_sf)
```

Both tasks pinned to the same interactive cluster ID. No serverless.

## Secrets / config

Only genuine secrets live in UC; everything else is a plain job param.

Plain params (job parameters / widgets — NOT secrets):

| param | value |
|---|---|
| `account` | `<org>-<account>` |
| `sf_user` | `TPCDI_SVC` (service user with KEY_PAIR auth policy) |
| `role` | `ACCOUNTADMIN` (optional; empty = connector default) |
| `snowflake_warehouse` | `BARROW_XS_GEN2` (or whichever warehouse the benchmark uses) |

Real secrets — each a FULL UC secret path (`catalog.schema.key`) resolved via
`_secret_from_path(path)` in `_sf_conn.py`:

| param | example value | holds |
|---|---|---|
| `sf_credential_secret` | `main.tpcdi_snowflake.password` | password OR PEM private key — auth mode decided by PEM-sniffing the resolved value |
| `dbx_pat_secret` | `main.tpcdi_snowflake.dbx_pat` | Databricks PAT for catalog-integration federation token refresh |

`_sf_conn.py`'s `sf_connect(...)` takes `account` / `user` / `warehouse` /
`role` as plain values plus `sf_credential_secret` (a secret path), resolves
the credential once, and branches keypair-vs-password on the PEM sniff.

## What changes vs the Databricks dbt variant

| | Databricks dbt | Snowflake dbt |
|---|---|---|
| Compute that runs dbt | Databricks SQL Warehouse (`dbt_task` native resource) | Interactive cluster running a notebook that shells dbt |
| Compute that runs models | Databricks SQL Warehouse | Snowflake warehouse (set in profile) |
| File reading | `read_files()` on UC Volume | `@stage` on Snowflake-managed external stage |
| Staging tables | `CREATE … CLONE …` (Delta SHALLOW CLONE) | `CREATE … CLONE …` (Snowflake zero-copy clone) |
| Auth | PAT / OAuth M2M to workspace | Keypair auth via `TPCDI_SVC` |

## Open items (resolved as the work progresses)

- IAM role trust setup on the target bucket (waiting on Opal power-user creds
  or bucket-owner cooperation)
- `STAGING_SF{sf}` schema name finalization — defaulting to `STAGING_SF20000`
  for first pass; bake the SF into the macro if we'll multi-SF this
- Cluster library list: confirm `dbt-snowflake==1.9.*` + `dbt-core==1.9.*`
  pinned correctly; defensive pip in `dbt_run.py` until library is verified
