# Databricks notebook source
# ONE-TIME per scale factor. Copies
#   {catalog}.tpcdi_incremental_staging_{sf}.{table}
# (Delta tables Phase A's augmented_staging workflow produced on Databricks)
# to
#   {bq_project}.tpcdi_staging_sf{sf}.{table}
# (native BigQuery tables).
#
# Path: Delta → parquet on GCS (via UC external volume) → `bq load` into a
# native BQ table. The intermediate parquet step exists because direct
# Spark-to-BigQuery connectors are slower and more brittle than GCS-staged
# loads, and Phase B is one-time per SF anyway so the extra hop is cheap.
#
# Suitable for SF<=20000 where each individual staging table fits in driver
# memory. For larger SFs the parquet writer is distributed, so the only
# driver-bound step is the `bq load` job submission.

# COMMAND ----------

# MAGIC %pip install --quiet google-cloud-bigquery

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("catalog", "main", "Source Databricks catalog for the staging Delta tables")
dbutils.widgets.dropdown("scale_factor", "10", ["10","100","1000","5000","10000","20000"])
dbutils.widgets.text("bq_project", "databricks-sandbox-perfeng", "Target BigQuery project")
dbutils.widgets.text("bq_location", "us-central1", "BQ dataset location (must match GCS bucket region)")
dbutils.widgets.text("secret_scope", "tpcdi_bigquery", "Databricks secret scope holding sa_json")
dbutils.widgets.text("gcs_volume_prefix", "gs://shannon-tpcdi/tpcdi/",
                     "GCS URI that maps 1:1 to /Volumes/{catalog}/tpcdi_raw_data/tpcdi_volume/")

src_catalog       = dbutils.widgets.get("catalog")
scale_factor      = dbutils.widgets.get("scale_factor")
bq_project        = dbutils.widgets.get("bq_project")
bq_location       = dbutils.widgets.get("bq_location")
secret_scope      = dbutils.widgets.get("secret_scope")
gcs_volume_prefix = dbutils.widgets.get("gcs_volume_prefix").rstrip("/") + "/"

src_schema   = f"tpcdi_incremental_staging_{scale_factor}"
bq_dataset   = f"tpcdi_staging_sf{scale_factor}"
parquet_root = f"/Volumes/{src_catalog}/tpcdi_raw_data/tpcdi_volume/staging_parquet/sf={scale_factor}"
volume_root  = f"/Volumes/{src_catalog}/tpcdi_raw_data/tpcdi_volume/"

print(f"src     = {src_catalog}.{src_schema}")
print(f"parquet = {parquet_root}")
print(f"sink    = {bq_project}.{bq_dataset} (in {bq_location})")

# COMMAND ----------

# MAGIC %run ./_bq_conn

# COMMAND ----------

client = bq_connect(
    project=bq_project,
    location=bq_location,
    secret_scope=secret_scope,
    query_label={"scale_factor": scale_factor, "task": "seed_staging_py"},
)

from google.cloud import bigquery
ds = bigquery.Dataset(f"{bq_project}.{bq_dataset}")
ds.location = bq_location
client.create_dataset(ds, exists_ok=True)
print(f"[ok] BQ dataset ready: {bq_project}.{bq_dataset} in {bq_location}")

# COMMAND ----------

STAGING_TABLES = [
    # Biggest-first so the ThreadPoolExecutor below kicks off the long-pole
    # tables on its first workers; the dozens of tiny ref tables backfill
    # the pool as the big ones finish. Sequential SF=20k seed took ~75 min
    # because financial (99 GB) alone blocked the queue for 5 min and
    # bronzedailymarket / factmarkethistory (~250 GB each) for 15+ min apiece.
    "bronzedailymarket", "factmarkethistory",        # ~5.35B rows (heaviest)
    "factwatches",                                    # ~2.85B
    "dimtrade",                                       # ~2.10B
    "factholdings", "factcashbalances",               # ~1.94B
    "cashtransactionhistorical",                      # ~1.94B
    "financial", "companyyeareps",                    # ~950M
    "dimaccount", "dimcustomer",                      # 102M, 39M
    "currentaccountbalances", "dimbroker",            # ~30M
    "dimsecurity", "dimcompany",                      # 16M, 10M
    "dimtime", "dimdate",                             # 86K, 26K
    "taxrate", "industry", "tradetype",               # <500
    "statustype", "batchdate",                        # tiny
]

# Per-table partition + cluster overrides applied at load time. These mirror
# the canonical CLUSTER_KEYS dict in sf_staging_bootstrap.py exactly (which
# in turn mirrors the Databricks Liquid clustering layout). Two BQ-specific
# divergences:
#
#   1. bronzedailymarket: SF/DBX cluster on dm_date. On BQ we PARTITION on
#      dm_date (DATE) instead — the dbt model uses insert_overwrite with
#      copy_partitions=true to do a metadata-only partition swap when
#      appending each new batch date. The staging table must already be
#      partitioned the same way so setup_bq.py's CLONE inherits the layout.
#   2. factholdings: SF/DBX cluster on sk_dateid. On BQ we PARTITION on
#      sk_dateid (INT64 range, capped under 10k buckets) — same reason:
#      insert_overwrite-as-append needs partition_by.
#
# All other tables match CLUSTER_KEYS verbatim. Tables not listed here get
# whatever the parquet load defaults produce (unpartitioned, unclustered)
# — fine for small reference tables.
TABLE_LAYOUTS = {
    # Append-via-partition-swap: PARTITION (no cluster).
    "bronzedailymarket": {
        "time_partitioning": bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY, field="dm_date"),
    },
    "factholdings": {
        "range_partitioning": bigquery.RangePartitioning(
            field="sk_dateid",
            range_=bigquery.PartitionRange(start=20160706, end=20170703, interval=1),
        ),
    },
    # Everything else: cluster_by per CLUSTER_KEYS.
    "factmarkethistory":  {"clustering_fields": ["sk_dateid"]},
    "factwatches":        {"clustering_fields": ["sk_dateid_dateremoved"]},
    "factcashbalances":   {"clustering_fields": ["sk_dateid"]},
    "dimcustomer":        {"clustering_fields": ["enddate"]},
    "dimaccount":         {"clustering_fields": ["enddate"]},
    "dimtrade":           {"clustering_fields": ["sk_closedateid"]},
    "companyyeareps":     {"clustering_fields": ["qtr_start_date"]},
}

# COMMAND ----------

# Self-skip: if all 22 expected tables already exist in this staging
# dataset, exit early. Makes the seed task idempotent + cheap to re-run
# in the parent workflow (the seed_staging task always runs but is a
# sub-second no-op when staging is intact).
try:
    rows = client.query(
        f"SELECT table_name FROM `{bq_project}.{bq_dataset}.INFORMATION_SCHEMA.TABLES`"
    ).result()
    _present = {r["table_name"] for r in rows}
except Exception:
    _present = set()
_missing = set(STAGING_TABLES) - _present
if not _missing:
    msg = f"[skip] {bq_project}.{bq_dataset} already has all {len(STAGING_TABLES)} staging tables"
    print(msg)
    dbutils.notebook.exit(msg)
print(f"[seed] {len(_missing)} of {len(STAGING_TABLES)} staging tables missing: {sorted(_missing)}")

# COMMAND ----------

import time
from concurrent.futures import ThreadPoolExecutor, as_completed

# Parallel pool size. Each worker independently runs:
#   Delta read → write parquet → BQ load → row-count parity check
# Both bigquery.Client (Google docs) and the SparkSession are thread-safe;
# Spark dispatches the per-worker .write.parquet() as independent jobs.
# 6 is enough to keep both the cluster's write bandwidth and BQ's load
# slots saturated without driver memory pressure. Bump for very large
# SFs if the cluster has headroom.
MAX_PARALLEL_TABLES = 6

def _seed_one(table: str) -> dict:
    """Per-table pipeline. Returns a result dict (never raises — failures
    are captured in result['error'] so other tables can still proceed)."""
    src_fq       = f"{src_catalog}.{src_schema}.{table}"
    parquet_path = f"{parquet_root}/{table}"
    bq_table_id  = f"{bq_project}.{bq_dataset}.{table}"
    log_lines    = [f"[{table}] starting"]
    t0           = time.time()
    try:
        delta_rows = spark.read.table(src_fq).count()
    except Exception as e:
        return {"table": table, "skipped": True,
                "error": f"{type(e).__name__}: {e}",
                "elapsed": time.time() - t0}
    log_lines.append(f"[{table}] delta_rows={delta_rows:,}, writing parquet → {parquet_path}")
    try:
        (spark.read.table(src_fq)
            .write.mode("overwrite").parquet(parquet_path))
    except Exception as e:
        return {"table": table, "error": f"parquet write failed: {type(e).__name__}: {e}",
                "elapsed": time.time() - t0, "log": "\n".join(log_lines)}

    # Resolve volume path → gs:// URI. The UC external volume at
    # /Volumes/{catalog}/tpcdi_raw_data/tpcdi_volume/ maps 1:1 to the
    # gcs_volume_prefix bucket+prefix.
    gcs_uri = parquet_path.replace(volume_root, gcs_volume_prefix) + "/*.parquet"
    log_lines.append(f"[{table}] loading {gcs_uri} → {bq_table_id}")

    job_kwargs = dict(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition="WRITE_TRUNCATE",
        autodetect=True,
    )
    layout = TABLE_LAYOUTS.get(table, {})
    if "time_partitioning" in layout:
        job_kwargs["time_partitioning"] = layout["time_partitioning"]
        log_lines.append(f"[{table}] layout: time_partition={layout['time_partitioning'].field}")
    if "range_partitioning" in layout:
        rp = layout["range_partitioning"]
        job_kwargs["range_partitioning"] = rp
        log_lines.append(f"[{table}] layout: range_partition={rp.field} "
                         f"({rp.range_.start}..{rp.range_.end}, step={rp.range_.interval})")
    if "clustering_fields" in layout:
        job_kwargs["clustering_fields"] = layout["clustering_fields"]
        log_lines.append(f"[{table}] layout: cluster_by={layout['clustering_fields']}")

    try:
        load_job = client.load_table_from_uri(
            gcs_uri, bq_table_id, job_config=bigquery.LoadJobConfig(**job_kwargs),
        )
        load_job.result()
    except Exception as e:
        return {"table": table, "error": f"bq_load failed: {type(e).__name__}: {e}",
                "elapsed": time.time() - t0, "log": "\n".join(log_lines)}

    bq_rows = client.get_table(bq_table_id).num_rows
    parity = "OK" if bq_rows == delta_rows else f"MISMATCH (delta={delta_rows}, bq={bq_rows})"
    elapsed = time.time() - t0
    log_lines.append(f"[{table}] done in {elapsed:.1f}s — {bq_rows:,} rows  ({parity})")
    return {"table": table, "delta_rows": delta_rows, "bq_rows": bq_rows,
            "parity": parity, "elapsed": elapsed, "log": "\n".join(log_lines)}

results = []
t_start = time.time()
print(f"[parallel] seeding {len(STAGING_TABLES)} tables with {MAX_PARALLEL_TABLES} workers...")
with ThreadPoolExecutor(max_workers=MAX_PARALLEL_TABLES) as ex:
    futures = {ex.submit(_seed_one, t): t for t in STAGING_TABLES}
    for fut in as_completed(futures):
        r = fut.result()
        # Print each table's full log atomically so the per-table trace
        # stays grouped despite interleaved completion order.
        if "log" in r:
            print(r["log"])
        if r.get("skipped"):
            print(f"[{r['table']}] [skip] {r['error']}")
        elif r.get("error"):
            print(f"[{r['table']}] [FAIL] {r['error']}")
        results.append(r)
print(f"\n[parallel] all tables done in {time.time() - t_start:.1f}s")

# COMMAND ----------

print("\n[summary] (sorted by elapsed seconds, biggest first)")
results_sorted = sorted(results, key=lambda r: r.get("elapsed", 0), reverse=True)
for r in results_sorted:
    table = r["table"]
    if r.get("skipped") or r.get("error"):
        status = r.get("error", "skipped")
        print(f"  {table:<32s}  {r.get('elapsed', 0):>6.1f}s  {status}")
        continue
    print(f"  {table:<32s}  {r['elapsed']:>6.1f}s  "
          f"delta={r['delta_rows']:>12,}  bq={r['bq_rows']:>12,}  {r['parity']}")

failures = [r for r in results
            if not r.get("skipped") and (r.get("error") or r.get("parity") != "OK")]
if failures:
    raise RuntimeError(
        "Seed failures: " + ", ".join(
            f"{r['table']}({r.get('error') or r.get('parity')})"
            for r in failures
        )
    )
seeded = sum(1 for r in results if not r.get("skipped") and not r.get("error"))
print(f"\n[done] {bq_project}.{bq_dataset} seeded with {seeded} tables.")
