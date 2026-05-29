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
    # reference + dim tables (pure seed)
    "taxrate", "dimdate", "industry", "tradetype", "dimbroker",
    "dimsecurity", "statustype", "dimcompany", "dimtime", "financial",
    "companyyeareps", "currentaccountbalances", "dimaccount",
    # historical SCD2 dims + facts (the dbt incremental models MERGE/APPEND
    # new rows on top of these — they MUST be pre-loaded for joins like
    # factwatches → dimcustomer to find historical-period customers).
    "dimcustomer", "dimtrade", "factwatches", "factcashbalances",
    "factholdings", "factmarkethistory", "bronzedailymarket",
    "cashtransactionhistorical", "batchdate",
]

# COMMAND ----------

import time
results = []
for table in STAGING_TABLES:
    src_fq        = f"{src_catalog}.{src_schema}.{table}"
    parquet_path  = f"{parquet_root}/{table}"
    bq_table_id   = f"{bq_project}.{bq_dataset}.{table}"

    print(f"\n[{table}]")
    try:
        delta_rows = spark.read.table(src_fq).count()
    except Exception as e:
        print(f"  [skip] {src_fq} not found ({type(e).__name__})")
        continue
    print(f"  delta rows = {delta_rows:,}")

    print(f"  exporting Delta → parquet at {parquet_path}")
    (spark.read.table(src_fq)
        .write.mode("overwrite").parquet(parquet_path))

    # Resolve volume path → gs:// URI. The UC external volume at
    # /Volumes/{catalog}/tpcdi_raw_data/tpcdi_volume/ maps 1:1 to the
    # gcs_volume_prefix bucket+prefix.
    gcs_uri = parquet_path.replace(volume_root, gcs_volume_prefix) + "/*.parquet"
    print(f"  loading parquet → BigQuery {bq_table_id} from {gcs_uri}")

    load_job = client.load_table_from_uri(
        gcs_uri,
        bq_table_id,
        job_config=bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition="WRITE_TRUNCATE",
            autodetect=True,
        ),
    )
    load_job.result()  # blocking wait

    bq_rows = client.get_table(bq_table_id).num_rows
    parity = "OK" if bq_rows == delta_rows else f"MISMATCH (delta={delta_rows}, bq={bq_rows})"
    print(f"  loaded {bq_rows:,} rows  ({parity})")
    results.append((table, delta_rows, bq_rows, parity))

# COMMAND ----------

print("\n[summary]")
for table, delta_rows, bq_rows, parity in results:
    print(f"  {table:<35s}  delta={delta_rows:>12,}  bq={bq_rows:>12,}  {parity}")

mismatches = [r for r in results if r[3] != "OK"]
if mismatches:
    raise RuntimeError(f"Row-count parity failures: {[r[0] for r in mismatches]}")
print(f"\n[done] {bq_project}.{bq_dataset} seeded with {len(results)} tables.")
