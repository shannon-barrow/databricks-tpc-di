# dbt-BigQuery port — research notes

Pre-implementation reference for porting the 16-model augmented-incremental
dbt project from Databricks/Snowflake to BigQuery. Plumbing decisions
(GCS staging, Databricks→BQ load, simulate_filedrops) are out of scope here.

## Project layout

Match the existing two-tree pattern (`models/` for Databricks,
`snowflake_models/sf_{bronze,silver,gold}/` for Snowflake). Add a third tree:

```
dbt/
  models/                  (databricks)
  snowflake_models/sf_*    (snowflake)
  bigquery_models/bq_*     (bigquery)         <-- new
```

`dbt_project.yml` adds a third config block gated on
`target.type == 'bigquery'`. `+enabled` switch keeps each tree isolated to
its target. Subdir names must be unique to avoid config-collision (same
reason `sf_bronze` is prefixed).

## Adapter feature parity (the 3 things that matter)

| Config / feature        | Databricks | Snowflake | BigQuery | Notes |
|---|---|---|---|---|
| `merge` strategy        | ✅ | ✅ | ✅ | All three honor `unique_key` (list since dbt-core 1.1), `merge_update_columns`, `incremental_predicates`. The earlier worry that BQ wouldn't is wrong — dbt-bigquery's macro layer handles all three. |
| `insert_overwrite`      | ✅ | ✅ (fallback to TABLE) | ✅ first-class | BQ's `copy_partitions=true` is metadata-only partition swap — much cheaper than MERGE for partitioned facts. Worth using for `factmarkethistory` / `factholdings` / `currentaccountbalances`. |
| `append`                | ✅ | ✅ | ✅ | Bronze tables, no change. |
| Zero-copy clone         | DEEP CLONE | CREATE TABLE … CLONE | `CREATE TABLE … CLONE` | BQ has clone; setup-owns-layout pattern carries over verbatim. |
| Auto-optimize / cluster | Liquid + setup-owned | auto-cluster + setup-owned | Auto-clustering on `cluster_by`, free | BQ does background re-clustering automatically once `cluster_by` is set. No equivalent to PO costs to track. |

## Partition + cluster strategy (new for BQ)

BQ rewards partitioning; we should add it. Suggested per-table layout
(decide after running benchmark once with no partitioning to measure):

| Table | partition_by | cluster_by (≤ 4) |
|---|---|---|
| `dimcustomer` | none (small) | `enddate, customerid` |
| `dimaccount` | none | `enddate, accountid` |
| `dimtrade` | `range_bucket(sk_closedateid)` or `date(close_ts)` | `sk_brokerid, sk_securityid` |
| `factwatches` | `date(timestamp_seconds(...))` keyed off `sk_dateid_dateremoved` | `sk_customerid, sk_securityid` |
| `factmarkethistory` | `range_bucket(sk_dateid, 0..10M, 1000)` | `sk_securityid, sk_companyid` |
| `factholdings` | `sk_dateid` range | `sk_customerid, sk_securityid` |
| `factcashbalances` | `sk_dateid` range | `sk_customerid` |
| `currentaccountbalances` | none (current snapshot) | `customerid` |

Bronze tables: skip partitioning. They're per-batch append, dropped each
batch when setup CLONE happens. `event_dt` / `update_dt` clustering only.

## Auth & profile

BQ profile invoked from Databricks `dbt_task` needs service-account-json
from a Databricks secret scope:

```yaml
# profiles.yml.template
dbt_augmented_incremental:
  target: bigquery
  outputs:
    bigquery:
      type: bigquery
      method: service-account-json
      project: <gcp-project>
      dataset: "{{ env_var('DBT_DATASET') }}"   # ignored — _helpers overrides
      keyfile_json: "{{ env_var('BQ_KEYFILE_JSON') | as_native }}"
      location: US
      threads: 8
      priority: interactive      # batch is cheaper but blocks on slot avail
      job_execution_timeout_seconds: 7200
      job_retries: 1
      job_creation_timeout_seconds: 60
```

The `run_dbt.py` notebook (analog of Snowflake's) reads the JSON from a
Databricks secret scope, exports it as `BQ_KEYFILE_JSON`, then shells out
to `dbt run --target bigquery`. Same pattern as `_sf_conn.py`.

## Cost attribution (BQ equivalent of Snowflake query_tag)

BigQuery attribution is via **job labels**, not session tags. Set them
via dbt's `query-comment` machinery with `job-label: true` so every dbt
query stamps the labels onto the BQ job:

```yaml
# dbt_project.yml
query-comment:
  comment: "{{ query_comment(node) }}"
  job-label: true
```

```jinja
{# macros/query_comment.sql #}
{% macro query_comment(node) %}
  {%- do return(tojson({
    'wh_db':        var('wh_db'),
    'scale_factor': var('scale_factor'),
    'batch_date':   var('batch_date'),
    'table_format': var('table_format'),
    'task':         'dbt_run',
  })) -%}
{% endmacro %}
```

Cost extract reads `INFORMATION_SCHEMA.JOBS_BY_PROJECT` and parses
`labels` — same shape as our SF `QUERY_HISTORY.QUERY_TAG` extract.

## SQL dialect translation table (Snowflake → BQ)

Each row appears 1+ times across the silver/gold models.

| Snowflake | BigQuery | Notes |
|---|---|---|
| `to_char(d, 'YYYYMMDD')` | `FORMAT_DATE('%Y%m%d', d)` | Strftime format codes, not Java patterns |
| `to_char(d, 'HH24MISS')` | `FORMAT_TIME('%H%M%S', TIME(d))` | If `d` is timestamp, `TIME(d)` first |
| `to_char(...)::number` | `CAST(FORMAT_DATE(...) AS INT64)` | One extra cast |
| `to_date(ts)` | `DATE(ts)` | Function call, not cast |
| `::number` / `::int` / `::float` | `CAST(x AS INT64 / FLOAT64)` | No `::type` shorthand |
| `iff(c, t, f)` | `IF(c, t, f)` | BQ has IF natively |
| `decode(val, k1, v1, k2, v2, ..., else)` | **CASE WHEN** — no DECODE | Affects dimtrade status/type maps |
| `object_construct('k', v, ...)` | `STRUCT(v AS k, ...)` | Typed at construction; cleaner |
| `max_by(object_construct(...), key)` | `ANY_VALUE(STRUCT(...) HAVING MAX key)` or `ARRAY_AGG(STRUCT(...) ORDER BY key DESC LIMIT 1)[OFFSET(0)]` | BQ's `MAX_BY` (GA 2024) takes scalar not struct; for struct-of-fields the array_agg pattern is the idiomatic form |
| `:field::type` access | `s.field` — no cast | BQ structs are typed at construct |
| `div0(a, b)` | `SAFE.DIVIDE(a, b)` | SF returns 0 on /0; BQ returns NULL — matches Databricks `try_divide` semantics, so BQ aligns with Databricks not Snowflake here |
| `IDENTIFIER(:catalog \|\| '.' \|\| ...)` | `\`{{var('project')}}.{{var('dataset')}}.tbl\`` | Backtick quoting, dotted FQN |
| `current_timestamp()` | `CURRENT_TIMESTAMP()` | Same |
| `dateadd(day, -n, d)` | `DATE_SUB(d, INTERVAL n DAY)` | Different sigil |
| `GROUP BY ALL` | spell columns out | Not in BQ |
| `SELECT * EXCLUDE col` | `SELECT * EXCEPT (col)` | BQ has EXCEPT; equivalent feature |
| `transient` table | n/a | BQ has no transient tier; ignore the config |
| `$1::type` positional (stage read) | `_FILE_NAME` / external-table column projection | Whole staging story is different — see below |

## Bronze ingestion (hardest part)

Databricks: `read_files(volume_path, format=>csv, schema=>'...', ...)`
Snowflake: positional `$1::T` over `@stage/path/{batch_date}/File.txt`

BigQuery has three options; all are different:

1. **External table over GCS prefix** (recommended). Setup-time:
   ```sql
   CREATE OR REPLACE EXTERNAL TABLE {dataset}.ext_customer_<sf>
   OPTIONS (
     format = 'CSV', field_delimiter = '|', skip_leading_rows = 0,
     uris = ['gs://<bucket>/augmented_incremental/_dailybatches/{wh_db}_{sf}/*/Customer.txt']
   );
   ```
   Then bronze model selects from `ext_customer_<sf> WHERE _FILE_NAME LIKE '%/{batch_date}/%'`.
   Pros: declarative, single external def per dataset, native partition-pruning via hive-style or `_FILE_NAME`.
   Cons: schema inference flaky on pipe-delimited; declare schemas explicitly.

2. **LOAD DATA FROM FILES** in a Python operator at start of dbt run.
   Pros: lands data as native BQ table, all downstream queries are cheap.
   Cons: another pre-step; not a dbt model. (Snowflake parallel: COPY INTO.)

3. **bigframes / google-cloud-bigquery from Python** before dbt runs.
   Same shape as our `seed_staging_py.py` for Snowflake; lands per-batch
   parquet → native BQ table → bronze models just `SELECT * FROM staging`.

The right call depends on whether we want bronze to be a real measured step or pre-loaded. Snowflake currently uses option 2 via `COPY INTO`. For BQ, option 3 (Python-load to native table, bronze just `SELECT * FROM`) is the most apples-to-apples to the Snowflake setup — and avoids external-table query cost surprises.

## Models that need the most translation work

1. **dimtrade** — heavy struct work + multiple `decode`s + multiple
   `to_char(... )::number` casts + struct field re-extraction. Probably
   half a day of careful translation.
2. **factmarkethistory** — `min_by/max_by(struct(...))` pair, the
   prior-year window scan (recall the SF=20k 1392-sec query on the
   1-year scan), `div0` → `SAFE.DIVIDE`, several `to_char`.
3. **factwatches** — uses `GROUP BY ALL`; replace with explicit GROUP BY
   (one place).
4. **dimcustomer / dimaccount** — straight-translation of `to_char`,
   `iff`, `decode`, plus `SELECT * EXCEPT(...)` cleanup.
5. **All 7 bronze** — schema is engine-agnostic but the *read* path is
   100% new (see bronze ingestion section above).

## Per-batch staging clone (carries over)

BQ supports zero-copy clones via `CREATE TABLE ... CLONE`. The
one-time-materialize / per-batch-clone split we built for Snowflake
ports as-is:

- `onetime_stg_bq_tables.py` — load Databricks staging → native BQ
  tables in `{project}.{staging_dataset_sf}`. Run once per SF.
- `setup_bq.py` — per-run CLONE from `{staging_dataset_sf}` to
  `{wh_db}_{sf}`. Same 22 tables.

Both should mirror the Snowflake notebook structure verbatim — just
swap the SQL dialect.

## Open questions for plumbing phase

- Which GCS bucket / project owns the file drops? (memory: `tpcdi-fresh` workspace uses `s3://tpcds-datasets/shannon_tpcdi/` for AWS; need a GCS equivalent.)
- Service-account JSON storage: new Databricks secret scope `tpcdi_bigquery`?
- Cross-cloud egress cost of streaming `_dailybatches/*` from Databricks to GCS each batch. Snowflake side avoided this because the SF stage points at the same S3 bucket; BQ may need files actually living in GCS.
- Run-time: does `dbt_task` on Databricks have `dbt-bigquery` pinned, or do we need an env install? (currently `dbt-databricks==1.11.7`)
