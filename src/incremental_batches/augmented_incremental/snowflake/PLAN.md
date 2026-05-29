# TPC-DI Augmented Incremental — Snowflake variant: architecture

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
   the cluster as a library), writes a per-run `profiles.yml` from a
   Databricks secret scope, then runs:

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

Databricks secret scope `tpcdi_snowflake` (one-time create via
`databricks secrets create-scope`):

| key | value |
|---|---|
| `account` | `kponzso-bwa95870` |
| `user` | `TPCDI_SVC` (service user with KEY_PAIR auth policy) |
| `role` | `ACCOUNTADMIN` |
| `warehouse` | `BARROW_XS_GEN2` (or whichever warehouse the benchmark uses) |
| `private_key` | PEM contents of `~/.snowflake_keys/tpcdi_kp.pem` |

`_sf_conn.py` (already present) reads these.

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
