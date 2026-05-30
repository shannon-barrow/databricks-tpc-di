"""
bq_staging_bootstrap.py — self-bootstrapping BigQuery staging dataset.

Entry point called by `setup_bq.py` within the same notebook execution
(NOT via dbutils.notebook.run — see [[feedback-no-dbutils-notebook-run]];
mirrors the `sf_staging_bootstrap.py` / `setup_sf.py` pattern):

  ensure_staging_environment(client, *, bq_project, bq_dataset, bq_location,
                              src_catalog, src_schema, parquet_root,
                              volume_root, gcs_volume_prefix, spark, parallel=6) -> dict
    Idempotent. Checks BQ dataset `{bq_project}.{bq_dataset}` exists with
    all 22 expected staging tables. If anything is missing, loads only
    the missing ones in parallel via Delta → parquet (UC external volume
    on GCS) → bq_load → row-count parity check. Failures raise.

The 22 tables have BQ-specific partition / cluster layouts (see
TABLE_LAYOUTS). Two BQ-specific divergences from the SF CLUSTER_KEYS map:

  1. bronzedailymarket: PARTITION on dm_date (DATE), not cluster — the
     bronze dbt model uses insert_overwrite + copy_partitions=true, which
     requires the target be DATE-partitioned. The CLONE in setup_bq
     inherits the layout from this staging table.
  2. factholdings: RANGE PARTITION on sk_dateid INT64 (20160706..20170703
     step 1, 9999 buckets — under BQ's 10K cap) — gold model uses
     insert_overwrite as the BQ-idiomatic equivalent of SF/DBX append.

All other tables match the canonical CLUSTER_KEYS map in
sf_staging_bootstrap.py.
"""
from __future__ import annotations

import concurrent.futures as _cf
import time as _time

# The 22 staging tables, sorted biggest-first so the ThreadPoolExecutor
# kicks off the long-pole tables first.
STAGING_TABLES: tuple[str, ...] = (
    "bronzedailymarket", "factmarkethistory",        # ~5.35B rows
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
)


def _build_layouts():
    """Lazy import — keeps this module importable from any context that
    doesn't have google.cloud.bigquery installed yet (setup_bq.py installs
    it before importing this module)."""
    from google.cloud import bigquery
    return {
        # Append-via-partition-swap: PARTITION (no cluster).
        "bronzedailymarket": {
            "time_partitioning": bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY, field="dm_date"),
        },
        "factholdings": {
            "range_partitioning": bigquery.RangePartitioning(
                field="sk_dateid",
                range_=bigquery.PartitionRange(
                    start=20160706, end=20170703, interval=1),
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


def _seed_one(table, *, client, bq_project, bq_dataset, bq_location,
              src_catalog, src_schema, parquet_root, volume_root,
              gcs_volume_prefix, spark, layouts):
    """Seed one table. Raises on any failure — caller catches and aggregates."""
    from google.cloud import bigquery
    src_fq       = f"{src_catalog}.{src_schema}.{table}"
    parquet_path = f"{parquet_root}/{table}"
    bq_table_id  = f"{bq_project}.{bq_dataset}.{table}"
    log          = [f"[{table}] starting"]
    t0           = _time.time()

    delta_rows = spark.read.table(src_fq).count()
    log.append(f"[{table}] delta_rows={delta_rows:,}, writing parquet → {parquet_path}")
    (spark.read.table(src_fq)
        .write.mode("overwrite").parquet(parquet_path))

    gcs_uri = parquet_path.replace(volume_root, gcs_volume_prefix) + "/*.parquet"
    log.append(f"[{table}] loading {gcs_uri} → {bq_table_id}")

    job_kwargs = dict(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition="WRITE_TRUNCATE",
        autodetect=True,
    )
    layout = layouts.get(table, {})
    if "time_partitioning" in layout:
        job_kwargs["time_partitioning"] = layout["time_partitioning"]
        log.append(f"[{table}] layout: time_partition={layout['time_partitioning'].field}")
    if "range_partitioning" in layout:
        rp = layout["range_partitioning"]
        job_kwargs["range_partitioning"] = rp
        log.append(f"[{table}] layout: range_partition={rp.field} "
                   f"({rp.range_.start}..{rp.range_.end}, step={rp.range_.interval})")
    if "clustering_fields" in layout:
        job_kwargs["clustering_fields"] = layout["clustering_fields"]
        log.append(f"[{table}] layout: cluster_by={layout['clustering_fields']}")

    load_job = client.load_table_from_uri(
        gcs_uri, bq_table_id, job_config=bigquery.LoadJobConfig(**job_kwargs),
    )
    load_job.result()

    bq_rows = client.get_table(bq_table_id).num_rows
    elapsed = _time.time() - t0
    if bq_rows != delta_rows:
        raise RuntimeError(
            f"row-count mismatch for {table}: delta={delta_rows:,}, bq={bq_rows:,}"
        )
    log.append(f"[{table}] done in {elapsed:.1f}s — {bq_rows:,} rows  OK")
    return {"table": table, "delta_rows": delta_rows, "bq_rows": bq_rows,
            "elapsed": elapsed, "log": "\n".join(log)}


def ensure_staging_environment(client, *,
                                bq_project: str,
                                bq_dataset: str,
                                bq_location: str,
                                src_catalog: str,
                                src_schema: str,
                                parquet_root: str,
                                volume_root: str,
                                gcs_volume_prefix: str,
                                spark,
                                parallel: int = 6) -> dict:
    """Idempotent BQ staging bootstrap. Mirrors
    sf_staging_bootstrap.ensure_staging_environment.

    Args:
        client: live google.cloud.bigquery.Client (from _bq_conn helper)
        bq_project / bq_dataset / bq_location: where staging lives in BQ
        src_catalog / src_schema: Databricks-side source
          (e.g. "main" / "tpcdi_incremental_staging_{sf}")
        parquet_root: UC volume path for the parquet staging step
        volume_root: UC volume root used for gcs_uri rewrite
        gcs_volume_prefix: gs:// prefix that maps 1:1 to volume_root
        spark: SparkSession (Spark Connect-compatible)
        parallel: ThreadPoolExecutor max_workers (default 6)

    Returns:
        dict with keys: skipped (bool), n_seeded, elapsed_s, missing (list).

    Raises:
        RuntimeError if any seeded table fails to load with matching
        row counts vs the Delta source.
    """
    from google.cloud import bigquery

    # 1) Ensure dataset exists.
    ds = bigquery.Dataset(f"{bq_project}.{bq_dataset}")
    ds.location = bq_location
    client.create_dataset(ds, exists_ok=True)

    # 2) Check what's already there.
    try:
        rows = client.query(
            f"SELECT table_name FROM `{bq_project}.{bq_dataset}.INFORMATION_SCHEMA.TABLES`"
        ).result()
        present = {r["table_name"] for r in rows}
    except Exception:
        present = set()
    missing = [t for t in STAGING_TABLES if t not in present]

    if not missing:
        msg = f"[bootstrap] {bq_project}.{bq_dataset} already has all {len(STAGING_TABLES)} staging tables — skipping"
        print(msg)
        return {"skipped": True, "n_seeded": 0, "elapsed_s": 0.0, "missing": []}

    # 3) Seed only the missing tables in parallel.
    print(f"[bootstrap] seeding {len(missing)} of {len(STAGING_TABLES)} staging tables: {missing}")
    layouts = _build_layouts()
    t_start = _time.time()
    results: list[dict] = []
    failures: list[tuple[str, str]] = []
    with _cf.ThreadPoolExecutor(max_workers=parallel) as ex:
        futures = {
            ex.submit(
                _seed_one, t,
                client=client,
                bq_project=bq_project,
                bq_dataset=bq_dataset,
                bq_location=bq_location,
                src_catalog=src_catalog,
                src_schema=src_schema,
                parquet_root=parquet_root,
                volume_root=volume_root,
                gcs_volume_prefix=gcs_volume_prefix,
                spark=spark,
                layouts=layouts,
            ): t for t in missing
        }
        for fut in _cf.as_completed(futures):
            t = futures[fut]
            try:
                r = fut.result()
                print(r["log"])
                results.append(r)
            except Exception as e:
                msg = f"[{t}] [FAIL] {type(e).__name__}: {e}"
                print(msg)
                failures.append((t, str(e)))

    elapsed_s = _time.time() - t_start
    print(f"\n[bootstrap] seeded {len(results)} tables, {len(failures)} failures in {elapsed_s:.1f}s")

    # 4) Re-verify after seed.
    try:
        rows = client.query(
            f"SELECT table_name FROM `{bq_project}.{bq_dataset}.INFORMATION_SCHEMA.TABLES`"
        ).result()
        present_after = {r["table_name"] for r in rows}
    except Exception as e:
        raise RuntimeError(f"post-seed staging check failed: {e}")
    still_missing = [t for t in STAGING_TABLES if t not in present_after]
    if still_missing or failures:
        raise RuntimeError(
            f"Seed incomplete. Failures: {failures}. Still missing: {still_missing}"
        )

    return {"skipped": False, "n_seeded": len(results),
            "elapsed_s": elapsed_s, "missing": missing}
