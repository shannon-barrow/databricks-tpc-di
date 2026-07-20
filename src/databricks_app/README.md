# TPC-DI Benchmark Builder — Databricks App

A Databricks App for setting up and emitting TPC-DI benchmark workflows.
Its flagship use case is the **competitive** (non-Databricks) benchmarks:
Redshift, BigQuery, and Snowflake runs need many inputs that a native
Databricks run can just derive (workspace, bucket, account, warehouse,
credentials, catalog federation). The app collects those, writes any secrets
to the target secret scope, and emits the parent+child workflow by calling
each port's `create_jobs.create()`.

See `../incremental_batches/augmented_incremental/COMPETITIVE_APP_CONTEXT.md`
for the per-engine prerequisites and failure signals this app is built around.

## Layout

| File | Role |
|---|---|
| `app.py` | Streamlit UI — the adaptive per-engine form |
| `models.py` | Per-engine input schema (source of truth for the form + backend) |
| `backend.py` | Toggle: mock vs real backend |
| `backend_mock.py` | No-workspace, no-spend backend for local dev |
| `backend_real.py` | Writes secrets (SDK) + calls the ports' `create_jobs.create()` |
| `app.yaml` | Databricks Apps config (command + env) |

## Run locally (no workspace, no spend)

The mock backend lets you exercise the whole form + Create flow on your laptop:

```bash
cd src/databricks_app
databricks apps run-local --prepare-environment
# open the printed URL; USE_MOCK_BACKEND defaults to true
```

## Deploy

```bash
databricks apps create tpcdi-benchmark-builder
databricks workspace import-dir . /Workspace/Users/<you>/apps/tpcdi-benchmark-builder
databricks apps deploy tpcdi-benchmark-builder \
  --source-code-path /Workspace/Users/<you>/apps/tpcdi-benchmark-builder
```

Set `USE_MOCK_BACKEND=false` in `app.yaml` (or the app's env) to make it write
secrets and emit real workflows. The app authenticates as its service
principal via the SDK `Config()`.

## Scope

- **v1 — Create mode**, Redshift + BigQuery wired via their `create_jobs`.
  Native Databricks and Snowflake wire-ups, plus **Adjust** (param overrides
  like "run 50 batches") and **Diagnose** (read run state + explain failures),
  are later phases.
- **Single-workspace** for now: the app targets the workspace it authenticates
  to. Cross-workspace runs use the `profile` seam in `create_jobs` (later).

## Security

The app never stores credentials. Secret fields (🔑 in the form) are written
straight into the target Databricks secret scope and only the scope/key
reference is retained. Secrets never flow into job parameters or results.
