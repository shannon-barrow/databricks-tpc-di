# Fabric Data Warehouse — dbt variant port notes

The Fabric DW competitor for the TPC-DI Augmented Incremental benchmark. Same
dbt project as the other CDWs, run with `--target fabric` (the `dbt-fabric`
T-SQL adapter). Mirrors `competitors/redshift/` in shape; this file records
where Fabric DW forced a different decision.

## Architecture

Databricks (Azure `tpc-di`) orchestrates; the **Fabric Data Warehouse**
`tpcdi_fabric_dw` (workspace `e0a370b6…`, SQL endpoint
`skrtph5o6caeff4w6gdeuehp7q-…​.datawarehouse.fabric.microsoft.com`) runs the
models. Source data reuses the `tpcdi_fabric` Lakehouse's `staging_sf{sf}`
OneLake Delta tables the fabric_ss/nee runs already materialized (same
workspace → cross-database query, no re-copy).

Parent: `setup_fabric` → `for_each` day → gated `cleanup` (teardown_fabric).
Child: `simulate_filedrops_fabric` → `run_dbt`.

## Model tree

`competitors/fabric_models/fab_{bronze,silver,gold}` (committed separately).
Translated from the **Redshift** tree — Redshift and Fabric DW both lack
`MAX_BY`/`MIN_BY`/`STRUCT`, so the `ROW_NUMBER()=1` rewrites for
dimtrade/factmarkethistory carry over. Bronze reads the day's OneLake CSV via
`OPENROWSET(BULK)` (the `fabric__read_daily_csv` macro), NOT the Redshift
COPY-into-temp pre_hook.

## Decisions that differ from Redshift

1. **Everything runs on a CLASSIC job cluster** (`augmented_fabric.py`
   `job_clusters` + `init_msodbcsql18.sh`), not serverless. `dbt-fabric` uses
   pyodbc + the `msodbcsql18` ODBC driver, which serverless can't `apt`-install.
   setup/teardown also need OneLake OAuth (SP) + UC single-user to read `main`
   for the one-time DEEP CLONE. So one shared single-node UC cluster with the
   init script serves all tasks.

2. **Auth = Entra service principal only** (no SQL logins). `_fab_conn.py`
   mints an SP token via MSAL and hands it to ODBC via
   `SQL_COPT_SS_ACCESS_TOKEN`; `run_dbt` writes a dbt-fabric profile with
   `authentication: ServicePrincipal`. SP creds live in the `tpcdi_fabric` UC
   secret scope (client_id/client_secret); never job params.

3. **No datashare / COPY.** Redshift needed a Delta→parquet→COPY seed + a
   datashare. Fabric's staging is already Delta in OneLake, read in place via
   cross-DB query. `fabric_staging_bootstrap.ensure_onelake_staging` only DEEP
   CLONEs UC→OneLake for any missing `staging_sf{sf}` table (idempotent).

4. **Historical baseline via cross-DB CTAS.** setup CTASes the 22 baseline +
   reference tables `[tpcdi_fabric].[staging_sf{sf}].[t]` →
   `[{wh_db}_{sf}].[t]` (CTAS `CREATE TABLE … AS SELECT`). No DISTKEY (Fabric
   has no distribution concept).

5. **Clustering (preview) — kept, translated from SORTKEY.** Fabric DW supports
   `WITH (CLUSTER BY (≤4 cols))`, set at CREATE only (no ALTER; recluster =
   CTAS). setup pre-creates the clustered tables (setup-owns-layout: dbt models
   stay clustering-free and MERGE/append into them). Keys match the
   Databricks/Liquid choices: dimcustomer/dimaccount `enddate`; dimtrade
   `sk_closedateid`; factwatches `sk_dateid_dateremoved`;
   factmarkethistory/factholdings/factcashbalances `sk_dateid`;
   bronzecustomer/bronzeaccount `update_dt`; bronzetrade/cashtransaction/
   holdings/watches `event_dt`; bronzedailymarket `dm_date`. Reference tables
   (taxrate/dimdate/…) are unclustered (small / low-cardinality).
   - Supported cluster types: bigint/int/smallint/decimal/numeric/float/real/
     date/datetime2/time/char/varchar — all our keys qualify. NOT bit,
     varchar(max), varbinary, uniqueidentifier, or IDENTITY columns.
   - NO `dataSkippingNumIndexedCols` (that's Delta/Databricks-only).
   - **CAVEAT:** clustering adds ingestion overhead and its synchronous
     file-skipping benefit needs ≥1M-row DML batches; our per-batch incremental
     writes are small (may cluster asynchronously). We cluster primarily for the
     big one-time historical CTAS loads (dimtrade/factmarkethistory/…) so the
     read-heavy paths (dim lookups, the FMH 52-week window) prune — same
     rationale as the Databricks Liquid choice.

6. **factholdings = `append`** (in the model), because Fabric Delta forbids
   subqueries in DELETE, which dbt's delete+insert would emit. Append is correct
   for the append-only fact (batches run once).

## Ingestion path

`simulate_filedrops_fabric` copies each day's `_staging/sf=N/` CSVs into OneLake
`Files/…/_dailybatches/{wh_db}_{sf}/{batch_date}/{Dataset}.txt` (via
`dbutils.fs.cp` with SP OneLake OAuth on the JVM hadoop conf). The bronze
`OPENROWSET(BULK 'https://onelake…/Files/…')` reads the same bytes.

## OPEN ITEMS — verify at first SF=10 smoke (couldn't test headless)

- **SP access to the WH.** The SP is workspace Contributor; if connects are
  rejected, grant it inside the WH: `CREATE USER [tpcdi-fabric-sp] FROM EXTERNAL
  PROVIDER; ALTER ROLE db_owner ADD MEMBER [tpcdi-fabric-sp];`.
- **Token resource.** `_fab_conn` uses `https://database.windows.net/.default`;
  if rejected, try `https://analysis.windows.net/powerbi/api/.default`.
- **Cross-DB source schema.** Assumed `[tpcdi_fabric].[staging_sf{sf}].[t]`
  (schema-enabled lakehouse surfaces `Tables/staging_sf{sf}/` as schema
  `staging_sf{sf}`). If cross-DB resolution fails, check the SQL-endpoint schema
  and set `lakehouse_src_schema`.
- **OneLake OPENROWSET (preview) URL form.** `run_dbt` builds
  `https://onelake.dfs.fabric.microsoft.com/{ws}/{lh}/Files/…`; if OPENROWSET
  rejects it, try the `…/blob…` host or the `<ws>/<lh>.Lakehouse/…` form.
- **`dbt-fabric` `incremental_predicates` / `DBT_INTERNAL_DEST`** support (used
  by dimtrade/factwatches) — RS/BQ/SF honor it; confirm on dbt-fabric.
- **msodbcsql18 on the chosen DBR** — the init script installs it (Ubuntu 22.04
  repo codename `jammy`); if the DBR's Ubuntu release differs, update the
  codename.
- **CU-second cost** is not in `queryinsights` — pull it from the Fabric
  Capacity Metrics app, or hand-calc total duration × F64 hourly (as fabric_ss/
  nee did). `fabric_metrics.py` reports duration only.
