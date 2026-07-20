# dbt-Redshift port — design notes

How the 16-model augmented-incremental dbt project is ported from Databricks / Snowflake / BigQuery to Amazon Redshift Serverless.

Environment it expects:

- A Redshift Serverless workgroup (default database `dev`), reachable over the PG wire protocol on port 5439.
- An IAM role attached to the workgroup with S3 read on the staging bucket, for `COPY`.
- The staging bucket is the same S3 path the Spark datagen writes to, surfaced as the UC external volume.

All of these (host, role ARN, creds) come from the `tpcdi_redshift` secret scope — see `_rs_conn.py`.

Orchestrator: same as the SF and BQ ports — a Databricks workspace that owns the UC external-location volume on `s3://<your-bucket>/tpcdi/`.
The 365 daily filedrop CSVs are therefore already on S3 in the same region as the Redshift workgroup — no cross-region egress.

## Project layout

Match the existing per-engine tree pattern:

```
dbt/
  models/                   (databricks)
  snowflake_models/sf_*     (snowflake)
  bigquery_models/bq_*      (bigquery)
  redshift_models/rs_*      (redshift)         <-- new
```

`dbt_project.yml` adds a fourth config block gated on
`target.type == 'redshift'`. `rs_bronze` / `rs_silver` / `rs_gold` subdir
names mirror the BQ + SF prefix discipline so dbt's config resolution
doesn't double-apply.

```yaml
# new in dbt_project.yml
rs_bronze:
  +enabled: "{{ target.type == 'redshift' }}"
  +materialized: incremental
  +incremental_strategy: append    # see "insert_overwrite" notes below
  +on_schema_change: ignore
rs_silver:
  +enabled: "{{ target.type == 'redshift' }}"
  +materialized: incremental
  +incremental_strategy: merge     # Redshift MERGE (GA 2023+)
  +on_schema_change: ignore
rs_gold:
  +enabled: "{{ target.type == 'redshift' }}"
  +materialized: incremental
  +on_schema_change: ignore
```

## Adapter feature parity

The matrix expanded for Redshift:

| Config / feature | Databricks | Snowflake | BigQuery | Redshift | Notes |
|---|---|---|---|---|---|
| `merge` strategy | ✅ | ✅ | ✅ | ✅ (2023+) | dbt-redshift uses Redshift's native MERGE statement (`MERGE INTO target USING source ON ... WHEN MATCHED ... WHEN NOT MATCHED ...`). Honors `unique_key`, `merge_update_columns`, `incremental_predicates`. |
| `insert_overwrite` | ✅ | ✅ (fallback) | ✅ (metadata partition swap) | ✅ via `delete+insert` | dbt-redshift implements `delete+insert` strategy — DELETE the target rows matching `unique_key`, then INSERT the new ones. NOT a metadata-only partition swap; physically rewrites blocks. Bronze can use either `append` or `delete+insert`. |
| `append` | ✅ | ✅ | ✅ | ✅ | Bronze tables, no change. |
| **Zero-copy clone** | DEEP CLONE | CREATE TABLE … CLONE | CREATE TABLE … CLONE | ❌ **No native zero-copy clone.** Datashares are read-only. | This is the biggest structural difference. The "setup-owns-layout via CLONE from `STAGING_SF{sf}`" pattern can't carry over verbatim — see "No-clone workaround" section below. |
| Auto-optimize / cluster | Liquid + setup-owned | auto-cluster + setup-owned | Auto-clustering on `cluster_by`, free | DISTKEY (data distribution across nodes) + SORTKEY (column-major zone maps) | Both declared at CREATE TABLE time; **immutable** without recreate. Redshift Serverless still has the concept of distribution under the hood. |
| Transient / temp | n/a | `+transient: false` | n/a | n/a | No equivalent setting. |

## No-clone workaround (the central decision)

The Snowflake and BQ ports both rely on:

```
{project_or_db}.tpcdi_staging_sf{sf}   <-- one-time materialized once per SF
        │
        ▼ CLONE on every parent run (zero-copy, instant)
{project_or_db}.{wh_db}_{sf}            <-- per-run starting point
```

That guarantees every parent run starts from a clean, layout-correct
snapshot of the 22 historical/reference tables. Redshift has no such
mechanism. Three options ranked from best to worst:

### Option A (recommended): CTAS-based reset, paid once per parent run

Per-run setup does `CREATE TABLE {wh_db}_{sf}.<t> AS SELECT * FROM tpcdi_staging_sf{sf}.<t>` for each of the 22 tables, **including the DISTKEY/SORTKEY declarations**. This is a real materialization, but:

- Tables are small relative to facts (largest is `dimcustomer` ≈ 4-10 M rows at SF=20k).
- All 22 in parallel via separate JDBC threads from the setup notebook.
- Cost is roughly equivalent to running 22 medium INSERT statements once per parent run. At SF=20k, expect ~1-3 min total.

This keeps the setup-owns-layout invariant exactly the same as BQ/SF —
dbt models declare zero DDL.

### Option B: `CREATE TABLE LIKE` + `INSERT INTO ... SELECT`

Splits structure (instant) from data (the same materialization). No real
benefit over (A), just two statements per table. Skip.

### Option C: Datashare from a producer cluster

Read-only, can't be used as the dbt target. Could work for the staging
source but adds a second cluster to manage. Overkill.

**Decision: go with (A).** Document the "one CTAS per table per parent
run" cost explicitly; it shows up in the per-batch cost numbers as a
small per-parent overhead, not per-batch.

## DISTKEY / SORTKEY strategy

Both are declared at CREATE TABLE time. Once set, can't be ALTERed —
have to recreate. The setup notebook owns this; dbt models declare no
table-level layout. Per-table:

| Table | DISTKEY | SORTKEY (≤ 4 cols, compound) | Notes |
|---|---|---|---|
| `dimcustomer` | `customerid` | `enddate, customerid` | Most queries probe by customerid or filter on iscurrent/enddate. Distribute by customerid for join co-location; SORTKEY hot range first. |
| `dimaccount` | `accountid` | `enddate, accountid` | Symmetric with dimcustomer. |
| `dimtrade` | `sk_securityid` | `sk_closedateid, sk_brokerid, sk_securityid` | Fact-side joins on sk_securityid (factholdings, factmarkethistory). Sort-prune on close date for time-range queries. |
| `factwatches` | `sk_customerid` | `sk_dateid_dateremoved, sk_customerid, sk_securityid` | SCD2 update flips `removed` — sort key gets stale fast; rely on per-batch sort+vacuum off-hours (or AUTO TABLE OPTIMIZATION on Serverless). |
| `factmarkethistory` | `sk_securityid` | `sk_dateid, sk_securityid, sk_companyid` | The bigger one. Sort by sk_dateid keeps time-window prior-year scans selective. |
| `factholdings` | `sk_customerid` | `sk_dateid, sk_customerid, sk_securityid` | Distributed on customerid keeps `factcashbalances` join co-located. |
| `factcashbalances` | `sk_customerid` | `sk_dateid, sk_customerid` | Co-locate with factholdings. |
| `currentaccountbalances` | `customerid` | `customerid` | Small snapshot — DISTSTYLE ALL might actually be better; small table, every node gets a copy, all joins local. Decide after first benchmark run. |

**Bronze tables:** skip DISTKEY (DISTSTYLE EVEN or AUTO is fine — these
are append-mostly and read once per batch). SORTKEY on the date column
the bronze model filters on (`update_dt` / `event_dt` / `dm_date`).

**`DISTSTYLE ALL` candidates:** `currentaccountbalances`, `dimbroker`,
small dim/reference tables under ~5 M rows. Worth flagging in setup.

## Auth & profile

Connection details live in a `tpcdi_redshift` Databricks secret scope (mirrors `tpcdi_snowflake`), with these keys:

| key | example |
|---|---|
| `host` | `<workgroup>.<account-id>.<region>.redshift-serverless.amazonaws.com` |
| `port` | `5439` |
| `database` | `dev` |
| `user` | a Redshift service account |
| `password` | that account's password |
| `iam_role` | `arn:aws:iam::<account-id>:role/<role-name>` |

`_rs_conn.py` reads these and exposes a `rs_connect()` factory returning a `psycopg2` connection (dbt-redshift uses psycopg2 under the hood, so connection semantics match what dbt will see).

dbt-redshift's `profiles.yml` template:

```yaml
dbt_augmented_incremental:
  target: redshift
  outputs:
    redshift:
      type: redshift
      method: database         # password-based (TODO: switch to IAM later)
      host: "{{ env_var('REDSHIFT_HOST') }}"
      port: 5439
      dbname: "{{ env_var('REDSHIFT_DATABASE') }}"
      schema: "{{ env_var('DBT_SCHEMA') }}"        # = {wh_db}_{sf}
      user: "{{ env_var('REDSHIFT_USER') }}"
      password: "{{ env_var('REDSHIFT_PASSWORD') }}"
      threads: 8
      ra3_node: true
      connect_timeout: 30
      sslmode: require
```

(`ra3_node: true` doesn't apply to Serverless but doesn't hurt; dbt
ignores it on Serverless workgroups. Skip if dbt-redshift's latest
adapter complains about it.)

`run_dbt.py` (analog of SF/BQ) reads the scope, exports env vars, shells
out to `dbt run --target redshift`. Same pattern as `_sf_conn.py` /
`_bq_conn.py`.

## Cost attribution (Redshift's equivalent of QUERY_TAG / job labels)

Redshift Serverless has two attribution mechanisms:

1. **`SET query_group = '...'`** — session-level string tag. Lands in
   `SYS_QUERY_HISTORY.query_label`. We set this once per dbt session via
   the `query-comment` macro mechanism, embedding a JSON blob:

   ```yaml
   # dbt_project.yml
   query-comment:
     comment: "{{ query_comment(node) }}"
     append: true     # appends as SQL comment; query_group set separately
   ```

   ```jinja
   {# macros/query_comment.sql — add Redshift branch #}
   {% macro query_comment(node) %}
     {%- if target.type == 'redshift' -%}
       {# Redshift truncates query_group at 320 chars; keep it terse #}
       {{- '/* ' ~ tojson({
         'wh_db': var('wh_db'),
         'scale_factor': var('scale_factor'),
         'batch_date': var('batch_date'),
         'task': 'dbt_run',
       }) ~ ' */' -}}
     {%- elif target.type == 'snowflake' -%}
       ...
     {%- endif -%}
   {% endmacro %}
   ```

   A dbt `on-run-start` hook sets `SET query_group = '...'` once per
   invocation. The hook reads vars + builds the string.

2. **`SYS_QUERY_HISTORY` system view** — the canonical query-history
   surface on Serverless. Has `query_text`, `query_label` (from
   query_group), `elapsed_time`, `execution_time`, `compile_time`, etc.

`rs_metrics.py` reads `SYS_QUERY_HISTORY` (joined with
`SYS_SERVERLESS_USAGE` for RPU consumption) and emits the same
per-task / per-batch breakdown the SF / BQ metrics scripts produce.

## Per-batch bronze ingestion (the second biggest difference)

The other three engines each have their own bronze read path:

- Databricks: `read_files(volume_path, format=>csv, schema=>'...', ...)`
- Snowflake: positional `$1::T` over `@stage/path/{batch_date}/File.txt`
- BigQuery: external table over wildcard GCS prefix

Redshift loads with `COPY` into the native bronze tables, and the COPY runs **inside the dbt run as a pre_hook** on each bronze model (macro `rs_bronze_copy_prehook`, in `dbt/macros/rs_bronze_copy_prehook.sql`).
Per batch, each bronze model's pre_hook creates a temp table `LIKE` the bronze target, `COPY`s that day's CSV from S3 into it, then the model body `INSERT`s into the persistent bronze table (dbt `append` strategy).

Keeping the COPY inside dbt (rather than a separate load task) means bronze ingestion is billed on the same Redshift compute as silver+gold, so the per-batch cost is apples-to-apples with the other engines.
The COPY still surfaces in `SYS_LOAD_HISTORY` / `SYS_QUERY_HISTORY` for per-step timing.

`setup_rs.py` pre-creates the 6 streaming bronze tables empty so the pre_hook's `CREATE TEMP TABLE ... (LIKE this)` has a schema to copy.
`bronzedailymarket` is the exception: setup_rs CTAS's it from staging with a year of history for the FactMarketHistory MIN/MAX lookback.

Alternatives considered and rejected: **Spectrum / federated external tables** avoid the load step but bill S3 scans via RPU, impose an external-table optimizer barrier (no DISTKEY join co-location), and would re-scan S3 on every bronze->silver MERGE — slower and not apples-to-apples.

## SQL dialect translation table (Snowflake → Redshift)

Most of the BQ translation table also applies; the deltas are:

| Snowflake | Redshift | Notes |
|---|---|---|
| `to_char(d, 'YYYYMMDD')` | `TO_CHAR(d, 'YYYYMMDD')` | Same syntax. ✅ |
| `to_char(d, 'HH24MISS')` | `TO_CHAR(d, 'HH24MISS')` | ✅ |
| `to_date(ts)` | `TRUNC(ts)` or `CAST(ts AS DATE)` | `TO_DATE(string)` exists but takes string, not timestamp |
| `::number` / `::int` / `::float` | `::numeric` / `::int` / `::float8` | Cast shorthand works; type names differ |
| `iff(c, t, f)` | `DECODE(c, true, t, f)` or `CASE WHEN c THEN t ELSE f END` | No IF/IFF — use DECODE or CASE |
| `decode(val, k1, v1, ...)` | `DECODE(val, k1, v1, ...)` | ✅ same syntax |
| `object_construct('k', v, ...)` | **`JSON_OBJECT('k': v, ...)` (RS 2023+)** or build via JSON_PARSE/CONCAT | Redshift SUPER type. Less ergonomic than SF VARIANT. |
| `max_by(object_construct(...), key)` | Subquery with `ROW_NUMBER() OVER (PARTITION BY ... ORDER BY key DESC) = 1` | No MAX_BY in Redshift. Falls back to window. |
| `:field::type` access | `s.k::type` on SUPER, or pre-extract into columns | SUPER access syntax is similar but less universally supported |
| `div0(a, b)` | `CASE WHEN b = 0 THEN 0 ELSE a/b END` or `NULLIF(b, 0)` | No DIV0; matches BQ's lack of it |
| `IDENTIFIER(:catalog \|\| '.' \|\| ...)` | `"{{ var('catalog') }}"."{{ var('schema') }}".tbl` | Double-quote identifiers; dotted FQN |
| `current_timestamp()` | `CURRENT_TIMESTAMP` (no parens) or `GETDATE()` | Drop parens — Redshift parses `current_timestamp` as keyword |
| `dateadd(day, -n, d)` | `DATEADD(day, -n, d)` | ✅ same syntax (PostgreSQL-compatible) |
| `GROUP BY ALL` | Spell columns out | Not supported |
| `SELECT * EXCLUDE col` | Spell columns out | Not supported in Redshift |
| `transient` table | n/a | Ignore |
| `LIMIT (expr)` | `LIMIT <integer literal>` | Same restriction as Snowflake — pre-compute in Python before embedding |
| `QUALIFY` | Wrap in subquery + WHERE | Not supported; standard workaround |

The **biggest model-translation pain** vs SF/BQ:

- No `QUALIFY` → every silver MERGE that uses `QUALIFY ROW_NUMBER() = 1`
  needs an extra subquery layer
- No `IFF` → every `iff(...)` becomes a `CASE WHEN`
- `MAX_BY(struct)` → window function rewrite (same as BQ but Redshift's
  ARRAY_AGG ordering semantics are slightly different)
- No `SELECT * EXCEPT` — every dim/fact projection that uses it has to
  spell columns out

## Models that need the most translation work (delta from BQ port)

Same five clusters as BQ, plus Redshift-specific overhead:

1. **dimtrade** — already heaviest; gains `DECODE` (still fine) but loses
   `IFF`/`QUALIFY` simplifications.
2. **factmarkethistory** — `max_by(struct(...))` → window function;
   `div0` → `CASE/NULLIF`; prior-year window scan stays the same shape.
3. **factwatches** — `GROUP BY ALL` → explicit (same fix as BQ).
4. **dimcustomer / dimaccount** — `iff` → `CASE`, `SELECT * EXCEPT(...)`
   → spell columns.
5. **All 7 bronze** — schema lives in setup notebook (COPY DDL); dbt
   models are trivial pass-throughs.

## Per-batch staging "clone" workaround (in setup_rs.py)

Translated from the SF / BQ pattern but with CTAS instead of CLONE:

- `rs_staging_bootstrap.py` — pure Python module imported inline by setup_rs.py; one-time per SF load of Databricks staging
  → S3 (UNLOAD-shape via Spark), then COPY into
  `tpcdi_staging_sf{sf}.<table>` with DISTKEY/SORTKEY declared at
  CREATE. ~10-20 min at SF=20k (~150 GB of staging data).
- `setup_rs.py` — per-parent: CTAS from `tpcdi_staging_sf{sf}.<t>` →
  `{wh_db}_{sf}.<t>`, re-declaring DISTKEY/SORTKEY. ~1-3 min at SF=20k.

The CTAS pattern carries the layout because Redshift's `CREATE TABLE
AS` accepts `DISTKEY (col) SORTKEY (cols...)` in the same DDL.

## Workflow shape (`augmented_redshift.py`)

Mirrors `augmented_snowflake.py`:

```
Parent (augmented_redshift_parent):
  setup_rs                        (interactive cluster)
    └─ for_each_task loop over batch_date_ls
         └─ Child job per batch_date
  cleanup (gated, ALL_DONE)

Child (augmented_redshift_child):
  simulate_filedrops_rs   (S3 cp daily files into _dailybatches/{wh_db}_{sf}/...)
    └─ dbt_run            (bronze COPY pre_hooks + silver + gold via dbt-redshift)
```

Two child tasks: the bronze COPY runs inside dbt_run (as a pre_hook on each bronze model), so there's no separate load task — it stays on the same Redshift compute as silver+gold for apples-to-apples cost.

## RPU sizing (Serverless equivalent of warehouse size)

`xsmall-8rpu-workgroup` = 8 RPU base. Redshift Serverless auto-scales.
For comparability with the BQ port (where slot allocation is dynamic)
and SF port (where warehouse size is explicit), default to **leaving
auto-scaling on with `MaxRPU` set explicitly per SF** instead of
forcing a fixed RPU. See `_dbt_wh_size()` in
`generate_benchmark_workflow.py` for the SF / BQ / SDP precedent.

Suggested MaxRPU (decide after first run):

| Scale factor | MaxRPU |
|---|---|
| SF=10 | 8 (current workgroup default) |
| SF=100 | 16 |
| SF=1000 | 32 |
| SF=10000 | 64 |
| SF=20000 | 128 |

The 8 RPU base matches Snowflake's BARROW_XS_GEN2 + BQ's small slot
reservation in compute units (rough approximation; not 1:1 with slot
hours).

## Tooling files to create

Mirrors the BQ structure (which has 10 files including PORT_NOTES.md):

```
src/incremental_batches/augmented_incremental/redshift/
  PORT_NOTES.md                   (this file)
  _rs_conn.py                     (psycopg2 connection factory; reads tpcdi_redshift secret scope)
  setup_rs.py                     (per-parent: CTAS 22 tables from tpcdi_staging_sf{sf} → {wh_db}_{sf})
  rs_staging_bootstrap.py         (pure Python module imported inline by setup_rs; self-bootstraps tpcdi_staging_sf{sf})
  simulate_filedrops_rs.py        (per-batch: cp files into _dailybatches/{wh_db}_{sf}/{batch_date}/)
  run_dbt.py                      (per-batch: runs dbt in-process; bronze COPY pre_hooks + silver + gold)
  teardown_rs.py                  (drop {wh_db}_{sf} schema at end of parent)
  rs_metrics.py                   (SYS_QUERY_HISTORY + SYS_SERVERLESS_USAGE extract)
  make_staging_share.py           (optional: datashare staging from a producer workgroup to a consumer)
  create_jobs.py                  (Databricks Jobs API JSON for parent + child)
```

Plus the `dbt/redshift_models/rs_{bronze,silver,gold}/` tree (16 .sql
files mirroring the SF/BQ counterparts) and the `dbt_project.yml` +
`profiles.yml.template` + `macros/query_comment.sql` updates.

## Compute decision: serverless-only

The workspace that owns the UC external volume (mapping to the S3 bucket Redshift COPYs from) only exposes serverless compute — no interactive cluster access.
Both child-job tasks (`simulate_filedrops_rs`, `run_dbt`) therefore run on serverless via the workflow builder's `serverless_rs` env (deps: `dbt-core==1.11.8`, `dbt-redshift==1.10.1`, `psycopg2-binary`).

**Known tradeoff:** serverless cold-starts add variable latency (~30-60s per child run) that pollutes per-batch timing.
Moving to a classic cluster would require a different workspace with its own cross-mounting of the UC volume / S3 access, so the port accepts this for now.
For competitive comparisons, factor cold-start in, or re-run from a workspace that supports interactive clusters.

The workflow builder still accepts an `interactive_cluster_id` parameter for forward compatibility — passing it switches both child tasks to that classic cluster and skips the serverless env definition.

## Operational notes

- **Credentials:** connection creds (host, port, database, user, password, iam_role) come from the `tpcdi_redshift` secret scope — never hardcode them.
Use a service account rather than the workgroup admin.
- **S3 IAM role:** COPY requires the Serverless workgroup to assume the IAM role, so the role's trust policy must include the workgroup's principal.
- **dbt versions:** `dbt-core==1.11.8` + `dbt-redshift==1.10.1` (the dbt Cloud "compatible track" pairing for Redshift; keep in sync with `run_dbt.py`).
- **Cross-region:** keep the Serverless workgroup and the S3 bucket in the same region — the COPY IAM-role match assumes it, and it avoids cross-region transfer.
- **Delimiter:** `augmented_staging/_stage_ingestion.py` writes uncompressed CSV with `delimiter="|"`, so the pre_hook uses `COPY ... DELIMITER '|'`.
- **COPY errors:** Serverless reports COPY failures in `SYS_LOAD_ERROR_DETAIL` (not `STL_LOAD_ERRORS`).

## Validation plan (parallel to BQ #96)

1. Scaffold per-engine tooling + `redshift_models/`. Pre-flight smoke at
   SF=10 (≤5 min per parent run on 8 RPU).
2. End-to-end SF=10 with the dbt benchmark variant. Compare row counts
   per dim/fact against the Databricks dbt variant. Same correctness
   bar as BQ and SF.
3. SF=100 + SF=1000 to confirm scaling pattern. Time per task and RPU
   utilization vs MaxRPU.
4. SF=10000 anchor run to populate the cross-CDW dashboard.

Same scope discipline as BQ #96 — no SF=20k validation until lower SFs
are clean.
