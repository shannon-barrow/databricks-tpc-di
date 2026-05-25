# Databricks notebook source
# Per-batch refresh trigger for the Dynamic Tables variant. Replaces
# `dbt run` in the per-batch loop.
#
# What this does:
#   1. Fire `ALTER DYNAMIC TABLE <leaf> REFRESH;` on each of the 4 gold leaves
#      *in parallel*, each on its own connection in its own thread. Each call
#      blocks until that leaf's refresh (and its DOWNSTREAM cascade) completes
#      — ALTER REFRESH is synchronous and returns the refresh statistics in
#      its result set.
#   2. Snowflake's DT engine dedups shared upstream refreshes across the
#      threads: if two leaves both depend on dimcustomer, dimcustomer
#      refreshes once and both threads wait for it.
#   3. Fail loudly on any thread exception so the parent job aborts.
#
# Each batch's wall-clock is now the SLOWEST leaf cascade, not the SUM
# of all four serialized cascades. Per-leaf wall-times are emitted as
# task values for downstream dashboards.

import time, json
import concurrent.futures as _cf

# COMMAND ----------

dbutils.widgets.text("catalog",        "TPCDI_TEST")
dbutils.widgets.text("wh_db",          "")
dbutils.widgets.dropdown("scale_factor", "10", ["10","100","1000","5000","10000","20000"])
dbutils.widgets.text("batch_date",     "")
dbutils.widgets.text("secret_scope",   "tpcdi_snowflake")
dbutils.widgets.text("snowflake_warehouse", "BARROW_MED_GEN2",
                     "Warehouse used for the ALTER REFRESH dispatch + the polling query. The DT refresh itself runs on whatever WH the DT was created with.")
dbutils.widgets.text("poll_interval_seconds", "5")
dbutils.widgets.text("timeout_seconds",       "7200")

catalog          = dbutils.widgets.get("catalog")
wh_db            = dbutils.widgets.get("wh_db")
scale_factor     = dbutils.widgets.get("scale_factor")
batch_date       = dbutils.widgets.get("batch_date")
secret_scope     = dbutils.widgets.get("secret_scope")
warehouse        = dbutils.widgets.get("snowflake_warehouse")
poll_interval    = int(dbutils.widgets.get("poll_interval_seconds"))
timeout_s        = int(dbutils.widgets.get("timeout_seconds"))

if not (wh_db and batch_date):
    raise ValueError("wh_db and batch_date are required")

target_schema = f"{wh_db}_{scale_factor}"
print(f"target = {catalog}.{target_schema}")

# Leaves of the DT DAG. These are the gold tables with concrete TARGET_LAG.
# Triggering REFRESH on each cascades through DOWNSTREAM intermediates
# (bronze* DTs, account_updates_from_customer, dim* DTs, currentaccountbalances).
LEAF_DTS = [
    "factwatches",
    "factcashbalances",
    "factholdings",
    "factmarkethistory",
]

# COMMAND ----------

# MAGIC %run ../_sf_conn

# COMMAND ----------

# Connection params shared by each thread's sf_connect() call.
_SF_KWARGS = dict(
    database=catalog,
    schema=target_schema,
    secret_scope=secret_scope,
    warehouse=warehouse,
)

def _refresh_leaf(leaf: str) -> tuple[str, float, str]:
    """Open a fresh connection, fire ALTER DYNAMIC TABLE <leaf> REFRESH, drain
    the result set, return (leaf, wall_seconds, stats_json). ALTER REFRESH is
    synchronous — it blocks until the refresh + DOWNSTREAM cascade complete."""
    t0 = time.time()
    fq = f"{catalog}.{target_schema}.{leaf}"
    c = sf_connect(**_SF_KWARGS, query_tag={
        "batch_date":   batch_date,
        "wh_db":        wh_db,
        "scale_factor": scale_factor,
        "table_format": "dynamic_tables",
        "task":         "dt_wait_refresh",
        "leaf":         leaf,
    })
    try:
        cc = c.cursor()
        try:
            cc.execute(f"ALTER DYNAMIC TABLE {fq} REFRESH")
            rows = cc.fetchall()
        finally:
            cc.close()
        return leaf, time.time() - t0, json.dumps(rows, default=str)
    finally:
        c.close()

# COMMAND ----------

# Fire ALTER REFRESH on all leaves in parallel. Each thread blocks on its own
# leaf's full cascade; Snowflake's DT engine shares any overlapping upstream
# refreshes (e.g. dimcustomer if multiple leaves depend on it).
print(f"[trigger] firing {len(LEAF_DTS)} leaf REFRESHes in parallel...")
t_batch_start = time.time()
leaf_durations: dict[str, float] = {}
errors: list[tuple[str, BaseException]] = []

with _cf.ThreadPoolExecutor(max_workers=len(LEAF_DTS)) as ex:
    futures = {ex.submit(_refresh_leaf, leaf): leaf for leaf in LEAF_DTS}
    for f in _cf.as_completed(futures):
        leaf = futures[f]
        try:
            name, wall, stats = f.result()
            leaf_durations[name] = wall
            print(f"  ✓ {name:25s} {wall:6.1f}s  {stats[:200]}")
        except BaseException as e:
            errors.append((leaf, e))
            print(f"  ✗ {leaf:25s} ERROR: {type(e).__name__}: {e}")

if errors:
    raise RuntimeError(
        "One or more DT refreshes failed:\n"
        + "\n".join(f"  {leaf}: {type(e).__name__}: {e}" for leaf, e in errors)
    )

# COMMAND ----------

dur_wall  = time.time() - t_batch_start
dur_max   = max(leaf_durations.values())
dur_sum   = sum(leaf_durations.values())
print(f"[done] all {len(LEAF_DTS)} leaf DTs refreshed for {batch_date}")
print(f"  batch_wall={dur_wall:.1f}s  max_leaf_wall={dur_max:.1f}s  sum_leaf_wall={dur_sum:.1f}s")
print(f"  per-leaf durations (s): {json.dumps(leaf_durations, default=str)}")

# Emit per-leaf durations as task values so the parent job / dashboard can
# pick them up without re-querying.
dbutils.jobs.taskValues.set("leaf_refresh_durations_s", leaf_durations)
dbutils.jobs.taskValues.set("batch_wall_s",            dur_wall)

cur.close()
conn.close()
