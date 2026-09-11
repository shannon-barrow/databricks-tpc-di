"""Self-bootstrapping Fabric OneLake staging.

Pure Python module imported by setup_fabric.py. The Fabric analogue of
rs_staging_bootstrap.py, but MUCH simpler: Fabric's staging is Delta in OneLake
that the warehouse reads via cross-database query, so there is NO datashare and
NO parquet+COPY round-trip (Redshift needed those). We just ensure the 22
`staging_sf{sf}` Delta tables exist in the Lakehouse OneLake; if any are
missing we DEEP CLONE them from the Databricks UC staging (the exact mechanism
the fabric_ss / fabric_nee setup used).

`ensure_onelake_staging(...)` is idempotent — it clones only missing tables
(unless force=True). Requires the OneLake OAuth confs to be set on the Spark
session already (setup_fabric sets them from the tpcdi_fabric SP secrets), and a
UC-enabled single-user cluster to read `main`.

Cross-DB read path for the warehouse: [tpcdi_fabric].[staging_sf{sf}].[<table>].
"""
from __future__ import annotations

import time as _time

# Canonical 20 staging tables the DW dbt models actually use (merge targets +
# reference sources/lookups). batchdate + cashtransactionhistorical are dropped:
# no fab_* model references them (batch_date is passed as a var; the cash-history
# seed already lives in the seeded currentaccountbalances/factcashbalances).
STAGING_TABLES: tuple[str, ...] = (
    "bronzedailymarket", "factmarkethistory", "factwatches", "dimtrade",
    "factholdings", "factcashbalances",
    "financial", "companyyeareps", "dimaccount", "dimcustomer",
    "currentaccountbalances", "dimbroker", "dimsecurity", "dimcompany",
    "dimtime", "dimdate", "taxrate", "industry", "tradetype",
    "statustype",
)


def _onelake_tbl_path(ws_id: str, lh_id: str, sf: str, table: str) -> str:
    # BARE GUIDs, no `.lakehouse` suffix (GUID mode; the `.lakehouse` friendly
    # form 400s — confirmed on the fabric_ss materialize).
    return (f"abfss://{ws_id}@onelake.dfs.fabric.microsoft.com/"
            f"{lh_id}/Tables/staging_sf{sf}/{table}")


def _exists(spark, path: str) -> bool:
    try:
        spark.sql(f"DESCRIBE DETAIL delta.`{path}`").collect()
        return True
    except Exception:
        return False


def ensure_onelake_staging(*, spark, dbutils, ws_id: str, lh_id: str,
                           src_catalog: str, scale_factor: str,
                           force: bool = False) -> dict:
    """Ensure the 22 staging tables are present in the Lakehouse OneLake.

    Missing tables are DEEP CLONE'd from
    `{src_catalog}.tpcdi_incremental_staging_{sf}.{t}` -> OneLake. Returns a
    summary dict. Raises if a clone fails.
    """
    sf = str(scale_factor)
    src_schema = f"tpcdi_incremental_staging_{sf}"

    todo = []
    for t in STAGING_TABLES:
        path = _onelake_tbl_path(ws_id, lh_id, sf, t)
        if force or not _exists(spark, path):
            todo.append(t)

    if not todo:
        print(f"[bootstrap] all {len(STAGING_TABLES)} staging tables present in "
              f"OneLake staging_sf{sf} — skipping")
        return {"skipped": True, "n_cloned": 0}

    print(f"[bootstrap] DEEP CLONE {len(todo)} table(s) UC->OneLake: {todo}")
    t_start = _time.time()
    for t in todo:
        path = _onelake_tbl_path(ws_id, lh_id, sf, t)
        src = f"{src_catalog}.{src_schema}.{t}"
        t0 = _time.time()
        # CREATE OR REPLACE handles a stale/partial prior clone. If a target
        # exists with a different columnMapping mode, REPLACE can fail with
        # DELTA_UNSUPPORTED_COLUMN_MAPPING_MODE_CHANGE — delete the OneLake path
        # first (DFS API) if that happens (documented in PORT_NOTES).
        spark.sql(f"CREATE OR REPLACE TABLE delta.`{path}` DEEP CLONE {src}")
        print(f"[clone] {t:28s} {_time.time()-t0:6.1f}s")

    return {"skipped": False, "n_cloned": len(todo),
            "elapsed_s": round(_time.time() - t_start, 1), "cloned": todo}
