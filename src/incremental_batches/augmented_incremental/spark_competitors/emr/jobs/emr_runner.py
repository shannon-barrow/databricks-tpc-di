"""EMR entry point: `spark-submit emr_runner.py setup|batch|drop ...`.

Runs ON the EMR cluster as one EMR step (client mode, driver on the primary
node). `setup` resets the run database in the Glue catalog: a SHALLOW
CLONE of each Delta staging table plus the 6 empty bronze tables. `batch`
runs one day's bronze -> silver/gold DAG and writes a per-node timing JSON
to S3 next to the run's other metrics. `drop` removes the run database and
its files (the gated cleanup); the Delta staging is never touched.

The two modes are the EMR counterparts of the Fabric `setup` and
`batch_runner` notebooks.
"""
from __future__ import annotations

import argparse
import concurrent.futures
import json
import sys
import time
from datetime import datetime, timezone

import tpcdi_dag
import tpcdi_sql as q


def _spark(app: str, shuffle_partitions: str):
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.appName(app).enableHiveSupport().getOrCreate()
    # Same session-wide value the Fabric batch_runner sets: the FMH 52-week
    # aggregation overflows executor-local disk at SF=20k with 200 partitions.
    # AQE coalesces it back down at small scale factors.
    spark.conf.set("spark.sql.shuffle.partitions", shuffle_partitions)
    return spark


def _hadoop_fs(spark, path: str):
    jvm = spark._jvm
    p = jvm.org.apache.hadoop.fs.Path(path)
    return p.getFileSystem(spark._jsc.hadoopConfiguration()), p


def _write_text(spark, path: str, text: str) -> None:
    fs, p = _hadoop_fs(spark, path)
    out = fs.create(p, True)
    try:
        out.write(bytearray(text.encode("utf-8")))
    finally:
        out.close()


def _delete_path(spark, path: str) -> None:
    fs, p = _hadoop_fs(spark, path)
    if fs.exists(p):
        fs.delete(p, True)


def _iso(ts):
    return None if ts is None else datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()


# --- setup -----------------------------------------------------------------

def run_setup(spark, *, db: str, db_location: str, staging_delta: str) -> dict:
    t0 = time.time()
    spark.sql(q.drop_database(db))
    # A reused location with an old _delta_log would make the clones below fail.
    _delete_path(spark, db_location)
    spark.sql(q.create_database(db, db_location))

    def _clone(t):
        s = time.time()
        spark.sql(q.shallow_clone(db, t, staging_delta, db_location))
        return t, round(time.time() - s, 1)

    with concurrent.futures.ThreadPoolExecutor(max_workers=len(q.STAGING_TABLES)) as ex:
        for t, secs in ex.map(_clone, q.STAGING_TABLES):
            print(f"[clone] {t} {secs}s", flush=True)
    for src in q.EMPTY_BRONZE:
        spark.sql(q.create_bronze(db, src))
        print(f"[bronze] bronze{src}", flush=True)
    secs = round(time.time() - t0, 1)
    print(f"[setup] {db} ready in {secs}s", flush=True)
    return {"db": db, "seconds": secs}


def run_drop(spark, *, db: str, db_location: str) -> None:
    spark.sql(q.drop_database(db))
    _delete_path(spark, db_location)
    print(f"[drop] {db} and {db_location} removed", flush=True)


# --- batch -----------------------------------------------------------------

def _ingest(spark, db: str, source: str, batch_dir: str) -> None:
    df = (spark.read.format("csv")
          .schema(q.BRONZE_SCHEMAS[source])
          .option("header", "false")
          .option("sep", "|")
          .option("pathGlobFilter", q.BRONZE_FILES[source])
          .load(batch_dir))
    view = f"_bronze_ingest_{source}"
    df.createOrReplaceTempView(view)
    spark.sql(q.ingest_bronze(db, source, view))


def _selective_overwrite(spark, db: str, table: str, src_sql: str) -> None:
    src = spark.sql(src_sql)
    view = f"_{table}_src"
    src.createOrReplaceTempView(view)
    ids = [r[0] for r in src.select("sk_dateid").distinct().collect()]
    stmt = q.delete_dateids(db, table, ids)
    if stmt:
        spark.sql(stmt)
    spark.sql(q.insert_from_view(db, table, view))


def node_runner(spark, *, db: str, batch_date: str, batch_dir: str):
    """Map each DAG node name to the Spark work it does."""
    def run(node: str) -> None:
        # Each thread gets its own scheduler pool so FAIR mode shares the
        # executors between concurrent nodes instead of running them FIFO.
        spark.sparkContext.setLocalProperty("spark.scheduler.pool", node)
        spark.sparkContext.setJobGroup(node, f"{batch_date} {node}")
        if node.startswith("bronze"):
            _ingest(spark, db, node[len("bronze"):], batch_dir)
        elif node == "account_updates_from_customer":
            spark.sql(q.account_updates_from_customer(db, batch_date))
        elif node == "factcashbalances":
            _selective_overwrite(spark, db, node, q.factcashbalances_src(db))
        elif node == "factmarkethistory":
            _selective_overwrite(spark, db, node, q.factmarkethistory_src(db, batch_date))
        else:
            spark.sql(getattr(q, node)(db, batch_date))
    return run


def run_batch(spark, *, db: str, batch_date: str, batch_dir: str,
              concurrency: int, metrics_path: str | None) -> dict:
    t0 = time.time()
    records = tpcdi_dag.run_dag(
        tpcdi_dag.DAG, node_runner(spark, db=db, batch_date=batch_date, batch_dir=batch_dir),
        concurrency=concurrency)
    t1 = time.time()
    for r in records:
        r["start"], r["end"] = _iso(r["start"]), _iso(r["end"])
    failed = [r["node"] for r in records if r["status"] != "SUCCESS"]
    result = {"db": db, "batch_date": batch_date, "concurrency": concurrency,
              "start": _iso(t0), "end": _iso(t1), "seconds": round(t1 - t0, 3),
              "status": "FAILED" if failed else "SUCCESS", "failed": failed,
              "tasks": records}
    for r in records:
        print(f"[{r['status']:7s}] {r['node']:32s} {r['seconds'] or 0:8.1f}s"
              + (f"  {r['error']}" if r["error"] else ""), flush=True)
    print(f"[batch] {batch_date} {result['status']} in {result['seconds']:.1f}s", flush=True)
    if metrics_path:
        _write_text(spark, metrics_path, json.dumps(result, indent=1))
    return result


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="mode", required=True)
    for name in ("setup", "batch", "drop"):
        p = sub.add_parser(name)
        p.add_argument("--db", required=True, help="Glue database = {wh_db}_{sf}, lowercase")
        p.add_argument("--shuffle-partitions", default="1000")
    sub.choices["setup"].add_argument("--db-location", required=True)
    sub.choices["drop"].add_argument("--db-location", required=True)
    sub.choices["setup"].add_argument("--staging-delta", required=True)
    b = sub.choices["batch"]
    b.add_argument("--batch-date", required=True)
    b.add_argument("--batch-dir", required=True, help="s3 dir holding this day's {Dataset}.txt files")
    b.add_argument("--concurrency", type=int, default=8)
    b.add_argument("--metrics-path", default=None)
    a = ap.parse_args(argv)

    spark = _spark(f"tpcdi-emr-{a.mode}-{a.db}", a.shuffle_partitions)
    if a.mode == "setup":
        run_setup(spark, db=a.db, db_location=a.db_location, staging_delta=a.staging_delta)
        return 0
    if a.mode == "drop":
        run_drop(spark, db=a.db, db_location=a.db_location)
        return 0
    res = run_batch(spark, db=a.db, batch_date=a.batch_date, batch_dir=a.batch_dir,
                    concurrency=a.concurrency, metrics_path=a.metrics_path)
    return 0 if res["status"] == "SUCCESS" else 1


if __name__ == "__main__":
    sys.exit(main())
