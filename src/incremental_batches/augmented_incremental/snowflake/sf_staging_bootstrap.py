"""
sf_staging_bootstrap.py — self-bootstrapping Snowflake staging schemas.

Two entry points called by `setup_sf.py` (dbt variant) and
`setup_sf_dt.py` (Dynamic Tables variant):

  ensure_staging_environment(conn, *, catalog, scale_factor, ...)
    Idempotent. Checks STAGING_SF{sf} (native) + STAGING_SF{sf}_DBX
    (federated Iceberg) exist with all expected tables. If anything is
    missing, sets up Iceberg federation against the Databricks UC
    schema `main.tpcdi_incremental_staging_{sf}` (CREATE ICEBERG TABLE
    per table) then CTASs the native silver/gold/ref tables from the
    federation.

  materialize_bronze_into_schema(conn, *, catalog, scale_factor, target_schema, ...)
    Per-run CTAS of the 7 bronze tables from STAGING_SF{sf}_DBX
    (federated Iceberg) into target_schema. Tables keep their source
    names (bronzeaccount, bronzecashtransaction, …) and are created
    with CHANGE_TRACKING = TRUE so downstream silver/gold DTs can
    refresh incrementally against them with no bronze DT pass-through.

The module replaces the obsolete `onetime_stg_snowflake_*` notebooks
with a Python module callable from any setup notebook — the same code
path works for both the dbt-variant and DT-variant first-run setup.

Type fidelity is preserved end-to-end: the source Databricks Delta
tables are Iceberg-UniForm; Snowflake reads them via the catalog
integration; the CTAS is type-preserving. No pandas, no driver-side
data movement.
"""
from __future__ import annotations

import concurrent.futures as _cf
import time as _time
from typing import Callable, Iterable, Optional


# ============================================================================
# Table sets
# ============================================================================

# Native staging tables CLONEable by per-run setup notebooks (both variants).
# These are silver/gold/ref + bronzedailymarket — built by the Databricks-side
# augmented_incremental historical SQL.
NATIVE_STAGING_TABLES: tuple[str, ...] = (
    "taxrate", "dimdate", "industry", "tradetype", "dimbroker",
    "dimsecurity", "statustype", "dimcompany", "dimtime", "financial",
    "companyyeareps", "currentaccountbalances", "dimaccount",
    "dimcustomer", "dimtrade", "factwatches", "factcashbalances",
    "factholdings", "factmarkethistory", "bronzedailymarket",
    "cashtransactionhistorical", "batchdate",
)

# The 7 bronze tables produced by augmented_staging's `bronze_staging_*`
# tasks (with `generate_bronze_staging=YES`). Federated as Iceberg into
# STAGING_SF{sf}_DBX; never materialized natively in STAGING_SF{sf}
# (except bronzedailymarket, which is in both).
BRONZE_FEDERATION_TABLES: tuple[str, ...] = (
    "bronzeaccount", "bronzecashtransaction", "bronzecustomer",
    "bronzedailymarket", "bronzeholdings", "bronzetrade", "bronzewatches",
)

# All tables exposed via federation — used by the one-time setup path to
# CTAS native staging (silver/gold/ref) and by the DT variant to CTAS
# the 7 bronze tables into per-run schemas. UniForm gets enabled on the
# Databricks side at federation-creation time via ALTER TABLE (not at
# data-gen time — data gen writes plain Delta).
FEDERATED_TABLES: tuple[str, ...] = tuple(sorted(set(
    NATIVE_STAGING_TABLES + BRONZE_FEDERATION_TABLES
)))

# Cluster keys mirror the Databricks-side Liquid layout. Setting CLUSTER BY
# in the native CTAS lets subsequent CLONEs inherit the layout.
CLUSTER_KEYS: dict[str, str] = {
    "dimcustomer":           "enddate",
    "dimaccount":            "enddate",
    "dimtrade":              "sk_closedateid",
    "factwatches":           "sk_dateid_dateremoved",
    "factmarkethistory":     "sk_dateid",
    "factcashbalances":      "sk_dateid",
    "factholdings":          "sk_dateid",
    "bronzedailymarket":     "dm_date",
    "companyyeareps":        "qtr_start_date",
    "bronzeaccount":         "update_dt",
    "bronzecashtransaction": "event_dt",
    "bronzecustomer":        "update_dt",
    "bronzeholdings":        "event_dt",
    "bronzetrade":           "event_dt",
    "bronzewatches":         "event_dt",
}


# ============================================================================
# Helpers
# ============================================================================

def _schema_exists(cur, catalog: str, schema: str) -> bool:
    cur.execute(f"SHOW SCHEMAS LIKE '{schema}' IN DATABASE {catalog}")
    return bool(cur.fetchall())


def _tables_in(cur, catalog: str, schema: str) -> set[str]:
    cur.execute(
        f"SELECT LOWER(TABLE_NAME) FROM {catalog}.INFORMATION_SCHEMA.TABLES "
        f"WHERE TABLE_SCHEMA = UPPER('{schema}')"
    )
    return {r[0] for r in cur.fetchall()}


def _missing(actual: set[str], expected: Iterable[str]) -> set[str]:
    return set(expected) - actual


# Snowflake catalog integrations occasionally fail credential vending for a
# few seconds after CREATE OR REPLACE ICEBERG TABLE — the iceberg-rest
# endpoint exists but isn't ready to vend S3 creds yet. The error surfaces
# as "Failed to retrieve credentials from the Catalog ... Please ensure
# that the catalog vends credentials and retry." Retry-with-fixed-delay
# avoids needing the parent job to fail + retry the whole task.
_VEND_RETRY_MARKERS = (
    "Failed to retrieve credentials from the Catalog",
    "catalog vends credentials",
)


def _is_vending_error(exc: BaseException) -> bool:
    msg = str(exc)
    return any(m in msg for m in _VEND_RETRY_MARKERS)


def _retry_on_vending(fn, *, label: str, attempts: int = 3, delay_sec: int = 60):
    """Run fn() up to `attempts` times. Only retry credential-vending errors;
    everything else propagates immediately."""
    for i in range(1, attempts + 1):
        try:
            return fn()
        except Exception as e:
            if _is_vending_error(e) and i < attempts:
                print(f"[retry] {label}: credential vending failed (attempt {i}/{attempts}); "
                      f"sleeping {delay_sec}s — {str(e)[:120]}")
                _time.sleep(delay_sec)
                continue
            raise


# ============================================================================
# Enable UniForm on the Databricks-side bronze sources (one-time per SF)
# ============================================================================

def enable_uniform_on_sources(
    spark, *, databricks_catalog: str, scale_factor: str, tables: Iterable[str],
) -> None:
    """ALTER each source on the Databricks UC side to enable UniForm + drop
    deletion vectors. Required before Snowflake federation `CREATE OR REPLACE
    ICEBERG TABLE ... CATALOG = ...` calls can resolve — Databricks's
    iceberg-rest endpoint only exposes Delta tables with UniForm enabled.

    `databricks_catalog` is the UC catalog name on the Databricks side
    (e.g. "main"), NOT the Snowflake database. Sources live at
    `{databricks_catalog}.tpcdi_incremental_staging_{sf}.<tbl>`.

    Idempotent: ALTER ... SET TBLPROPERTIES is a no-op when the props are
    already in the requested state.
    """
    src_schema = f"{databricks_catalog}.tpcdi_incremental_staging_{scale_factor}"
    tables = list(tables)
    print(f"[uniform] enabling UniForm (Iceberg V2) on {len(tables)} sources under {src_schema}")
    t0 = _time.time()

    def _has_uniform(fq: str) -> bool:
        """True if `delta.universalFormat.enabledFormats` already includes
        'iceberg' (V2 or V3). V3 is a one-way door — non-removable
        feature, non-disable-able property — so we never try to downgrade
        a V3 table. Snowflake's iceberg-rest federation reads both V2
        and V3 sources, so accepting either is safe."""
        try:
            rows = spark.sql(f"SHOW TBLPROPERTIES {fq}").collect()
        except Exception:
            return False
        for r in rows:
            if r["key"] == "delta.universalFormat.enabledFormats" and "iceberg" in (r["value"] or "").lower():
                return True
        return False

    for tbl in tables:
        fq = f"{src_schema}.{tbl}"
        if _has_uniform(fq):
            continue
        # Two-step ALTER: turn DV off FIRST as its own transaction, then
        # enable IcebergCompatV2 + UniForm in a second ALTER. Doing both
        # in one ALTER fails — V2's validator checks the protocol state
        # before the DV-disable has actually committed
        # (DELTA_ICEBERG_COMPAT_VIOLATION.DELETION_VECTORS_SHOULD_BE_DISABLED).
        # Tables created with `delta.enableDeletionVectors=false` at
        # CREATE time never get the DV feature on the protocol, so the
        # disable here is a no-op TBLPROPERTY write (no REORG needed).
        spark.sql(f"ALTER TABLE {fq} SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'false')")
        spark.sql(
            f"ALTER TABLE {fq} SET TBLPROPERTIES ("
            f"  'delta.enableIcebergCompatV2'         = 'true',"
            f"  'delta.universalFormat.enabledFormats'= 'iceberg'"
            f")"
        )
    print(f"[uniform] done in {_time.time() - t0:.1f}s")


# ============================================================================
# Federation setup — CREATE ICEBERG TABLE per federated table
# ============================================================================

def setup_federation(
    conn,
    *,
    catalog: str,
    scale_factor: str,
    catalog_integration: str = "TPCDI_DBX_UC_SF10_INT",
    databricks_catalog_namespace: Optional[str] = None,
    new_pat: Optional[str] = None,
) -> None:
    """Create STAGING_SF{sf}_DBX with all 28 federated Iceberg tables.

    If `new_pat` is provided, refreshes the catalog integration's bearer
    token first (Databricks PATs expire; the integration silently fails
    with 403 until rotated).
    """
    cur = conn.cursor()
    dbx_schema = f"STAGING_SF{scale_factor}_DBX"
    namespace = databricks_catalog_namespace or f"tpcdi_incremental_staging_{scale_factor}"

    if new_pat:
        cur.execute(
            f"ALTER CATALOG INTEGRATION {catalog_integration} "
            f"SET REST_AUTHENTICATION = (BEARER_TOKEN='{new_pat}')"
        )
        print(f"[fed] refreshed bearer token on {catalog_integration}")

    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{dbx_schema}")
    print(f"[fed] creating {len(FEDERATED_TABLES)} federated Iceberg tables in {dbx_schema}")
    t0 = _time.time()
    for tbl in FEDERATED_TABLES:
        cur.execute(
            f"CREATE OR REPLACE ICEBERG TABLE {catalog}.{dbx_schema}.{tbl} "
            f"CATALOG = '{catalog_integration}' "
            f"CATALOG_TABLE_NAME = '{tbl}' "
            f"CATALOG_NAMESPACE = '{namespace}'"
        )
    print(f"[fed] {dbx_schema} ready in {_time.time() - t0:.1f}s "
          f"(namespace={namespace} catalog={catalog_integration})")
    cur.close()


# ============================================================================
# Native staging CTAS — parallel across the 22 silver/gold/ref tables
# ============================================================================

def setup_native_staging(
    conn,
    *,
    catalog: str,
    scale_factor: str,
    new_connection: Optional[Callable[[], object]] = None,
    parallel: int = 8,
) -> None:
    """CTAS the 22 native staging tables from STAGING_SF{sf}_DBX into STAGING_SF{sf}.

    `new_connection` is a callable that returns a fresh Snowflake connection;
    the Snowflake Python connector is not thread-safe across workers, so
    parallel CTAS needs its own connection per future. If omitted, runs
    sequentially using the passed-in `conn`.
    """
    dbx_schema = f"STAGING_SF{scale_factor}_DBX"
    native_schema = f"STAGING_SF{scale_factor}"

    cur = conn.cursor()
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{native_schema}")
    cur.close()

    def _ctas_one(tbl: str) -> tuple[str, float, int]:
        cluster = f" CLUSTER BY ({CLUSTER_KEYS[tbl]})" if tbl in CLUSTER_KEYS else ""

        def _do():
            c = new_connection() if new_connection else conn
            owned = new_connection is not None
            try:
                t0 = _time.time()
                with c.cursor() as cc:
                    cc.execute(
                        f"CREATE OR REPLACE TABLE {catalog}.{native_schema}.{tbl}"
                        f"{cluster} AS "
                        f"SELECT * FROM {catalog}.{dbx_schema}.{tbl}"
                    )
                    cc.execute(
                        f"SELECT ROW_COUNT FROM {catalog}.INFORMATION_SCHEMA.TABLES "
                        f"WHERE TABLE_SCHEMA = UPPER('{native_schema}') "
                        f"AND TABLE_NAME = UPPER('{tbl}')"
                    )
                    n = (cc.fetchone() or [0])[0] or 0
                return (tbl, _time.time() - t0, int(n))
            finally:
                if owned:
                    c.close()

        return _retry_on_vending(_do, label=f"ctas {native_schema}.{tbl}")

    t_start = _time.time()
    print(f"[native] CTAS {len(NATIVE_STAGING_TABLES)} tables "
          f"into {native_schema} (parallel={parallel if new_connection else 1})")
    results: list[tuple[str, float, int]] = []
    if new_connection and parallel > 1:
        with _cf.ThreadPoolExecutor(max_workers=parallel) as ex:
            futures = {ex.submit(_ctas_one, t): t for t in NATIVE_STAGING_TABLES}
            for f in _cf.as_completed(futures):
                tbl, wall, n = f.result()
                cluster_note = f" CLUSTER BY {CLUSTER_KEYS[tbl]}" if tbl in CLUSTER_KEYS else ""
                print(f"[ctas] {tbl:30s} {wall:5.1f}s rows={n:>15,d}{cluster_note}")
                results.append((tbl, wall, n))
    else:
        for tbl in NATIVE_STAGING_TABLES:
            tbl_name, wall, n = _ctas_one(tbl)
            cluster_note = f" CLUSTER BY {CLUSTER_KEYS[tbl_name]}" if tbl_name in CLUSTER_KEYS else ""
            print(f"[ctas] {tbl_name:30s} {wall:5.1f}s rows={n:>15,d}{cluster_note}")
            results.append((tbl_name, wall, n))

    print(f"[native] {native_schema} ready in {_time.time() - t_start:.1f}s, "
          f"total rows = {sum(n for _,_,n in results):,}")


# ============================================================================
# Per-run bronze materialization for the DT variant
# ============================================================================

def materialize_bronze_into_schema(
    conn,
    *,
    catalog: str,
    scale_factor: str,
    target_schema: str,
    new_connection: Optional[Callable[[], object]] = None,
    parallel: int = 7,
) -> None:
    """Per-run CTAS — copy each of the 7 federated bronze Iceberg tables into
    `target_schema` (the per-benchmark schema, e.g. SHANNON_AUG_SF_DT_10).

    Targets keep the source name (no `_raw` suffix). Tables are created
    with `CHANGE_TRACKING = TRUE` so downstream DTs can incrementally
    refresh against them directly — no pass-through DT layer needed.

    `new_connection`: callable returning a fresh Snowflake connection. Same
    thread-safety reason as setup_native_staging.
    """
    dbx_schema = f"STAGING_SF{scale_factor}_DBX"
    sources = list(BRONZE_FEDERATION_TABLES)

    def _ctas_one(name: str) -> tuple[str, float, int]:
        def _do():
            c = new_connection() if new_connection else conn
            owned = new_connection is not None
            try:
                t0 = _time.time()
                with c.cursor() as cc:
                    cc.execute(
                        f"CREATE OR REPLACE TABLE {catalog}.{target_schema}.{name} "
                        f"CHANGE_TRACKING = TRUE AS "
                        f"SELECT * FROM {catalog}.{dbx_schema}.{name}"
                    )
                    cc.execute(
                        f"SELECT ROW_COUNT FROM {catalog}.INFORMATION_SCHEMA.TABLES "
                        f"WHERE TABLE_SCHEMA = UPPER('{target_schema}') "
                        f"AND TABLE_NAME = UPPER('{name}')"
                    )
                    n = (cc.fetchone() or [0])[0] or 0
                return (name, _time.time() - t0, int(n))
            finally:
                if owned:
                    c.close()

        return _retry_on_vending(_do, label=f"ctas {target_schema}.{name}")

    t_start = _time.time()
    print(f"[bronze] CTAS {len(sources)} bronze tables {dbx_schema} → {target_schema}")
    if new_connection and parallel > 1:
        with _cf.ThreadPoolExecutor(max_workers=parallel) as ex:
            futures = {ex.submit(_ctas_one, s): s for s in sources}
            for f in _cf.as_completed(futures):
                name, wall, n = f.result()
                print(f"[bronze] {name:30s} {wall:5.1f}s rows={n:>12,d}")
    else:
        for s in sources:
            name, wall, n = _ctas_one(s)
            print(f"[bronze] {name:30s} {wall:5.1f}s rows={n:>12,d}")
    print(f"[bronze] done in {_time.time() - t_start:.1f}s")


# ============================================================================
# Top-level entry point — idempotent self-bootstrap
# ============================================================================

def ensure_staging_environment(
    conn,
    *,
    catalog: str,
    scale_factor: str,
    catalog_integration: str = "TPCDI_DBX_UC_SF10_INT",
    databricks_catalog_namespace: Optional[str] = None,
    databricks_catalog: str = "main",
    new_pat: Optional[str] = None,
    new_connection: Optional[Callable[[], object]] = None,
    parallel: int = 8,
    spark = None,
) -> dict:
    """Idempotent self-bootstrap. Returns a dict describing what was done.

    Designed for from-zero startup. Prereq: data gen has populated
    `main.tpcdi_incremental_staging_{sf}` with the 28 staging tables
    (silver/gold/ref + 7 bronze) as plain Delta. Nothing on the Snowflake
    side needs to exist beforehand.

    Three states to handle:
      - Native + federation both complete → no-op (steady state)
      - Federation missing → enable UniForm on all 28 Databricks sources,
        create federation (28 CREATE ICEBERG TABLE statements)
      - Native missing → CTAS the 22 silver/gold/ref tables from
        federation into native (parallel)

    On a true cold start, all three steps fire in order: enable UniForm →
    create federation → CTAS native. Subsequent runs short-circuit each
    step that's already in place.

    `new_pat` only needed on the very first bootstrap (or after the PAT
    backing the catalog integration expires).
    """
    cur = conn.cursor()
    dbx_schema = f"STAGING_SF{scale_factor}_DBX"
    native_schema = f"STAGING_SF{scale_factor}"

    result = {"federation_setup": False, "native_setup": False}

    # State probe
    native_complete = False
    if _schema_exists(cur, catalog, native_schema):
        miss = _missing(_tables_in(cur, catalog, native_schema), NATIVE_STAGING_TABLES)
        if not miss:
            native_complete = True
            print(f"[bootstrap] {native_schema} complete")
        else:
            print(f"[bootstrap] {native_schema} missing tables: {sorted(miss)}")
    else:
        print(f"[bootstrap] {native_schema} does not exist")

    fed_complete = False
    if _schema_exists(cur, catalog, dbx_schema):
        miss = _missing(_tables_in(cur, catalog, dbx_schema), FEDERATED_TABLES)
        if not miss:
            fed_complete = True
            print(f"[bootstrap] {dbx_schema} federation complete")
        else:
            print(f"[bootstrap] {dbx_schema} federation missing: {sorted(miss)}")
    else:
        print(f"[bootstrap] {dbx_schema} does not exist")
    cur.close()

    if native_complete and fed_complete:
        return result

    # Federation setup. Required before native CTAS can pull from it.
    if not fed_complete:
        if spark is None:
            raise RuntimeError(
                "spark session required to enable UniForm on Databricks sources. "
                "Pass spark= to ensure_staging_environment() from the notebook."
            )
        # Enable UniForm on every source we're about to federate.
        enable_uniform_on_sources(
            spark, databricks_catalog=databricks_catalog,
            scale_factor=scale_factor,
            tables=FEDERATED_TABLES,
        )
        setup_federation(
            conn, catalog=catalog, scale_factor=scale_factor,
            catalog_integration=catalog_integration,
            databricks_catalog_namespace=databricks_catalog_namespace,
            new_pat=new_pat,
        )
        result["federation_setup"] = True

    # Native staging setup — CTAS silver/gold/ref from the federation we
    # just (re)created. Skipped if native already has all 22 tables.
    if not native_complete:
        setup_native_staging(
            conn, catalog=catalog, scale_factor=scale_factor,
            new_connection=new_connection, parallel=parallel,
        )
        result["native_setup"] = True

    return result
