"""Materialize the UC staging tables as OSS-readable Delta on S3, once per SF.

Runs on Databricks (setup_emr step 1 and materialize_staging_emr). The EMR
counterpart of Fabric's OneLake materialize: a distributed DEEP CLONE of
main.tpcdi_incremental_staging_{sf}.{table} to {s3}_emr/staging_delta/sf={sf}/{table}.
Each EMR run then SHALLOW CLONEs from there (emr_runner setup), so the
per-run reset is zero-copy, as on Fabric.

The spark session and the two file helpers are passed in, so the logic is
testable without Databricks.

Table features. A DEEP CLONE keeps the source's protocol, and Databricks
writes features open-source Delta may not know. The copies must be readable
AND writable by EMR's delta-spark 3.x, because the run's shallow clones keep
the source's protocol and the batches MERGE into them. So:
  - deletion vectors are turned off on the copy and purged (REORG), so the
    copy has no DV files: EMR's shallow clone then only carries plain parquet
    paths, and EMR's MERGEs rewrite files as the old CTAS tables did;
  - every copy's tableFeatures are checked against OSS_DELTA_FEATURES;
  - a copy carrying anything else is deleted and rewritten as a plain CTAS
    with those features off. A CREATE OR REPLACE can't drop a feature from
    the protocol, hence the delete first.
columnMapping (the staging tables use mode 'name'), v2Checkpoint, rowTracking,
domainMetadata and liquid clustering are all in delta-spark 3.x, so they are
kept as they are.
"""
from __future__ import annotations

import concurrent.futures
import time
from typing import Callable, Iterable

# Reader + writer table features delta-spark 3.x implements
# (EMR 7.x bundles delta-spark 3.x).
OSS_DELTA_FEATURES = frozenset({
    "appendOnly", "invariants", "checkConstraints", "changeDataFeed", "generatedColumns",
    "columnMapping", "identityColumns", "deletionVectors", "timestampNtz", "domainMetadata",
    "v2Checkpoint", "rowTracking", "clustering", "icebergCompatV1", "icebergCompatV2",
    "vacuumProtocolCheck",
})

CLONE_PROPS = {"delta.enableDeletionVectors": "false"}

# The rewrite fallback: a fresh table with nothing beyond the legacy protocol.
PLAIN_PROPS = {
    "delta.enableDeletionVectors": "false",
    "delta.enableRowTracking": "false",
    "delta.enableInCommitTimestamps": "false",
    "delta.checkpointPolicy": "classic",
    "delta.columnMapping.mode": "none",
}


def _props(props: dict) -> str:
    return "TBLPROPERTIES (" + ", ".join(f"'{k}' = '{v}'" for k, v in props.items()) + ")"


def path_ref(root: str, table: str) -> str:
    return f"delta.`{root.rstrip('/')}/{table}`"


def deep_clone_sql(ref: str, src: str) -> str:
    return f"CREATE OR REPLACE TABLE {ref} DEEP CLONE {src} {_props(CLONE_PROPS)}"


def purge_sql(ref: str) -> str:
    return f"REORG TABLE {ref} APPLY (PURGE)"


def rewrite_sql(ref: str, src: str) -> str:
    return f"CREATE TABLE {ref} USING DELTA {_props(PLAIN_PROPS)} AS SELECT * FROM {src}"


def unsupported_features(detail: dict) -> list[str]:
    """Features in a DESCRIBE DETAIL row that delta-spark 3.x can't handle.
    A legacy protocol (no tableFeatures) implies only features it has."""
    feats = detail.get("tableFeatures") or []
    return sorted(f for f in feats if f not in OSS_DELTA_FEATURES)


def materialize(spark, *, src_db: str, tables: Iterable[str], roots: list[str],
                has_table: Callable[[str], bool], remove: Callable[[str], None],
                force: bool = False, log: Callable[[str], None] = print) -> dict:
    """DEEP CLONE src_db.{t} to {root}/{t} for each table not yet there
    (every table when force), then make sure every copy is OSS-readable.

    roots: the same S3 directory written two ways, tried in order (the
    /Volumes path, then the s3:// uri). The first one Databricks accepts as a
    clone target is used for the rest.
    has_table(t): the copy has a committed Delta log.
    remove(t): delete the copy's directory.
    """
    tables = list(tables)
    good: list[str] = []  # the root that worked, once one has

    def on_root(fn):
        """fn(root) on the settled root, or on each candidate until one works."""
        errors = []
        for root in (good or roots):
            try:
                out = fn(root)
            except Exception as e:  # noqa: BLE001 - the next form may work
                errors.append(f"{root}: {str(e)[:300]}")
                if good:
                    break
                continue
            if not good:
                good.append(root)
                log(f"[materialize] path form {root}")
            return out
        raise RuntimeError("no path form worked:\n" + "\n".join(errors))

    def run(*stmts):
        return lambda root: [spark.sql(s(root)) for s in stmts]

    def clone(t: str) -> str:
        s = time.time()
        try:
            on_root(run(lambda r: deep_clone_sql(path_ref(r, t), f"{src_db}.{t}"),
                        lambda r: purge_sql(path_ref(r, t))))
        except RuntimeError as e:
            # A UniForm (Iceberg) staging table can't be DEEP CLONEd to a path
            # outside Unity Catalog (DELTA_UNIFORM_NOT_SUPPORTED): its copy is
            # the plain rewrite instead, written once per SF like a clone.
            if "DELTA_UNIFORM_NOT_SUPPORTED" not in str(e):
                raise
            on_root(run(lambda r: rewrite_sql(path_ref(r, t), f"{src_db}.{t}")))
            return f"[rewrite] {t:24s} {time.time() - s:6.1f}s (UniForm source)"
        return f"[clone] {t:24s} {time.time() - s:6.1f}s"

    def detail(t: str) -> dict:
        return on_root(lambda r: spark.sql(f"DESCRIBE DETAIL {path_ref(r, t)}")
                       .collect()[0].asDict())

    todo = tables if force else [t for t in tables if not has_table(t)]
    log(f"[materialize] {src_db} -> {roots[-1]}: {len(tables) - len(todo)} present, "
        f"{len(todo)} to DEEP CLONE (force={force})")
    t0 = time.time()
    if todo:
        log(clone(todo[0]))  # serial: settles which path form works
    if len(todo) > 1:
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(todo) - 1) as ex:
            for msg in ex.map(clone, todo[1:]):
                log(msg)

    rewritten = []
    for t in tables:
        bad = unsupported_features(detail(t))
        if not bad:
            continue
        log(f"[materialize] {t}: {bad} not in delta-spark 3.x; rewriting as a plain table")
        remove(t)
        on_root(run(lambda r: rewrite_sql(path_ref(r, t), f"{src_db}.{t}")))
        still = unsupported_features(detail(t))
        if still:
            raise RuntimeError(f"{t} still has {still} after the rewrite")
        rewritten.append(t)
    secs = round(time.time() - t0, 1)
    log(f"[materialize] done in {secs}s; cloned {len(todo)}, rewritten {rewritten}")
    return {"cloned": todo, "rewritten": rewritten, "root": (good or roots)[0], "seconds": secs}


def materialize_sf(spark, dbutils, *, catalog: str, sf: str, staging_delta: str,
                   staging_delta_volume: str, tables: Iterable[str],
                   force: bool = False) -> dict:
    """materialize() for one SF from a Databricks notebook: the copy at
    staging_delta (s3://) is reached for file checks through its /Volumes
    path, and both forms are offered as the clone target."""
    vol = staging_delta_volume.rstrip("/")

    def has_table(t: str) -> bool:
        try:
            return any(f.name.endswith(".json") for f in dbutils.fs.ls(f"{vol}/{t}/_delta_log"))
        except Exception:  # noqa: BLE001 - no such dir yet
            return False

    return materialize(
        spark, src_db=f"{catalog}.tpcdi_incremental_staging_{sf}", tables=tables,
        roots=[vol, staging_delta.rstrip("/")], has_table=has_table,
        remove=lambda t: dbutils.fs.rm(f"{vol}/{t}", True), force=force)
