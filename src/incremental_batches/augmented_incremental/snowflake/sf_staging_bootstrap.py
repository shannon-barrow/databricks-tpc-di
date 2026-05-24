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
    (federated Iceberg) into target_schema. The 6 transactional bronze
    tables are renamed with the `_raw` suffix so they match the bronze
    DT engine's input names; bronzedailymarket keeps its native name
    (factmarkethistory's DT reads it directly).

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

# All tables exposed via the federation schema.
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
        c = new_connection() if new_connection else conn
        owned = new_connection is not None
        try:
            cluster = f" CLUSTER BY ({CLUSTER_KEYS[tbl]})" if tbl in CLUSTER_KEYS else ""
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

# Mapping: source name in _DBX → target name in per-run schema.
# Only bronzedailymarket keeps its name; the 6 transactional bronze tables
# get _raw suffix (the DT engine's bronze*_raw input names).
_BRONZE_RENAME: dict[str, str] = {
    "bronzeaccount":         "bronzeaccount_raw",
    "bronzecashtransaction": "bronzecashtransaction_raw",
    "bronzecustomer":        "bronzecustomer_raw",
    "bronzedailymarket":     "bronzedailymarket",
    "bronzeholdings":        "bronzeholdings_raw",
    "bronzetrade":           "bronzetrade_raw",
    "bronzewatches":         "bronzewatches_raw",
}


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
    The 6 transactional bronze tables get the `_raw` suffix; bronzedailymarket
    keeps its name (factmarkethistory's DT reads it directly).

    `new_connection`: callable returning a fresh Snowflake connection. Same
    thread-safety reason as setup_native_staging.
    """
    dbx_schema = f"STAGING_SF{scale_factor}_DBX"

    def _ctas_one(src: str) -> tuple[str, str, float, int]:
        dst = _BRONZE_RENAME[src]
        c = new_connection() if new_connection else conn
        owned = new_connection is not None
        try:
            cluster = f" CLUSTER BY ({CLUSTER_KEYS[src]})" if src in CLUSTER_KEYS else ""
            t0 = _time.time()
            with c.cursor() as cc:
                cc.execute(
                    f"CREATE OR REPLACE TABLE {catalog}.{target_schema}.{dst}"
                    f"{cluster} AS "
                    f"SELECT * FROM {catalog}.{dbx_schema}.{src}"
                )
                cc.execute(
                    f"SELECT ROW_COUNT FROM {catalog}.INFORMATION_SCHEMA.TABLES "
                    f"WHERE TABLE_SCHEMA = UPPER('{target_schema}') "
                    f"AND TABLE_NAME = UPPER('{dst}')"
                )
                n = (cc.fetchone() or [0])[0] or 0
            return (src, dst, _time.time() - t0, int(n))
        finally:
            if owned:
                c.close()

    t_start = _time.time()
    sources = list(_BRONZE_RENAME.keys())
    print(f"[bronze] CTAS {len(sources)} bronze tables {dbx_schema} → {target_schema}")
    if new_connection and parallel > 1:
        with _cf.ThreadPoolExecutor(max_workers=parallel) as ex:
            futures = {ex.submit(_ctas_one, s): s for s in sources}
            for f in _cf.as_completed(futures):
                src, dst, wall, n = f.result()
                print(f"[bronze] {src:25s} → {dst:30s} {wall:5.1f}s rows={n:>12,d}")
    else:
        for s in sources:
            src, dst, wall, n = _ctas_one(s)
            print(f"[bronze] {src:25s} → {dst:30s} {wall:5.1f}s rows={n:>12,d}")
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
    new_pat: Optional[str] = None,
    new_connection: Optional[Callable[[], object]] = None,
    parallel: int = 8,
) -> dict:
    """Idempotent self-bootstrap. Returns a dict describing what was done.

    Checks in order:
      1. Native STAGING_SF{sf} exists with all NATIVE_STAGING_TABLES → if yes, no-op.
      2. Federation STAGING_SF{sf}_DBX exists with all FEDERATED_TABLES → if no, set it up.
      3. Native STAGING_SF{sf} missing some tables → CTAS the full set from federation.

    `new_pat` only needed on the very first bootstrap (or after the PAT
    backing the catalog integration expires).
    """
    cur = conn.cursor()
    dbx_schema = f"STAGING_SF{scale_factor}_DBX"
    native_schema = f"STAGING_SF{scale_factor}"

    result = {"federation_setup": False, "native_setup": False}

    # 1. Quick exit if native staging is already complete
    if _schema_exists(cur, catalog, native_schema):
        present = _tables_in(cur, catalog, native_schema)
        miss = _missing(present, NATIVE_STAGING_TABLES)
        if not miss:
            print(f"[bootstrap] {native_schema} already complete "
                  f"({len(present)} tables) — skipping setup")
            # Also ensure federation is present for DT variant bronze access
            if not _schema_exists(cur, catalog, dbx_schema):
                print(f"[bootstrap] {dbx_schema} missing — setting up federation")
                setup_federation(
                    conn, catalog=catalog, scale_factor=scale_factor,
                    catalog_integration=catalog_integration,
                    databricks_catalog_namespace=databricks_catalog_namespace,
                    new_pat=new_pat,
                )
                result["federation_setup"] = True
            cur.close()
            return result
        else:
            print(f"[bootstrap] {native_schema} missing tables: {sorted(miss)}")
    else:
        print(f"[bootstrap] {native_schema} does not exist")

    # 2. Ensure federation (covers both DT bronze + CTAS source for native)
    fed_ok = False
    if _schema_exists(cur, catalog, dbx_schema):
        present = _tables_in(cur, catalog, dbx_schema)
        miss = _missing(present, FEDERATED_TABLES)
        if not miss:
            fed_ok = True
            print(f"[bootstrap] {dbx_schema} federation already complete")
        else:
            print(f"[bootstrap] {dbx_schema} federation missing: {sorted(miss)}")
    cur.close()

    if not fed_ok:
        setup_federation(
            conn, catalog=catalog, scale_factor=scale_factor,
            catalog_integration=catalog_integration,
            databricks_catalog_namespace=databricks_catalog_namespace,
            new_pat=new_pat,
        )
        result["federation_setup"] = True

    # 3. CTAS native staging from federation
    setup_native_staging(
        conn, catalog=catalog, scale_factor=scale_factor,
        new_connection=new_connection, parallel=parallel,
    )
    result["native_setup"] = True
    return result
