# Databricks notebook source
# Per-batch refresh trigger + poll for the Dynamic Tables variant. Replaces
# `dbt run` in the per-batch loop.
#
# What this does:
#   1. Issue `ALTER DYNAMIC TABLE <leaf> REFRESH;` for each leaf gold DT —
#      that triggers a cascade through DOWNSTREAM-mode intermediates.
#   2. Poll `INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY` for each leaf
#      until the latest refresh issued at-or-after t_start is in a terminal
#      state (SUCCEEDED / FAILED / CANCELED).
#   3. Fail loudly on FAILED / CANCELED so the parent job aborts.
#
# Each batch's wall-clock is roughly: max(refresh_durations across the
# cascade). The leaves refresh in parallel; intermediates are shared across
# leaves so cost ≈ each intermediate's incremental cost once.

import time, json

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

conn = sf_connect(
    database=catalog,
    schema=target_schema,
    secret_scope=secret_scope,
    warehouse=warehouse,
    query_tag={
        "batch_date":   batch_date,
        "wh_db":        wh_db,
        "scale_factor": scale_factor,
        "table_format": "dynamic_tables",
        "task":         "dt_wait_refresh",
    },
)
cur = conn.cursor()

# COMMAND ----------

# Mark the wall-clock floor so we know which DYNAMIC_TABLE_REFRESH_HISTORY
# rows to wait on. Use Snowflake's own clock to avoid any host-time skew.
cur.execute("SELECT CURRENT_TIMESTAMP()")
t_start = cur.fetchone()[0]
print(f"[t_start] {t_start}")

# COMMAND ----------

# Trigger refresh on each leaf. ALTER … REFRESH is asynchronous; it returns
# immediately and the engine schedules the refresh on the DT's own warehouse.
print(f"[trigger] {len(LEAF_DTS)} leaf refreshes...")
for dt_name in LEAF_DTS:
    fq = f"{catalog}.{target_schema}.{dt_name}"
    cur.execute(f"ALTER DYNAMIC TABLE {fq} REFRESH")
    print(f"  -> ALTER DYNAMIC TABLE {dt_name} REFRESH (queued)")

# COMMAND ----------

# Poll DYNAMIC_TABLE_REFRESH_HISTORY for terminal state on each leaf. The
# INFORMATION_SCHEMA table function is the realtime view (vs the
# 3-hour-lagged ACCOUNT_USAGE one).
#
# Each leaf may have multiple refresh rows after t_start (the DT engine can
# split a refresh into sub-runs). Require the LATEST row per leaf to be
# terminal, AND its REFRESH_TRIGGER to be 'MANUAL' (our ALTER … REFRESH)
# to guarantee we're observing OUR trigger.
TERMINAL = {"SUCCEEDED", "FAILED", "CANCELED"}
FAIL     = {"FAILED", "CANCELED"}

leaves_remaining = set(LEAF_DTS)
deadline = time.time() + timeout_s
leaf_durations = {}

print(f"[poll] every {poll_interval}s, timeout {timeout_s}s")
while leaves_remaining and time.time() < deadline:
    leaves_csv = ",".join(f"'{l}'" for l in leaves_remaining)
    cur.execute(f"""
        WITH ranked AS (
          SELECT
            name,
            state,
            refresh_trigger,
            refresh_start_time,
            refresh_end_time,
            ROW_NUMBER() OVER (PARTITION BY name ORDER BY refresh_start_time DESC) AS rn
          FROM TABLE({catalog}.INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY(
              SCHEMA_NAME => '{catalog}.{target_schema}'
          ))
          WHERE name IN ({leaves_csv})
            AND refresh_start_time >= '{t_start}'
            AND refresh_trigger IN ('MANUAL', 'TASK', 'SCHEDULED')
        )
        SELECT name, state, refresh_trigger, refresh_start_time, refresh_end_time
        FROM ranked WHERE rn = 1
    """)
    rows = cur.fetchall()
    for name, state, trigger, t_st, t_en in rows:
        if state in TERMINAL:
            dur = (t_en - t_st).total_seconds() if t_en and t_st else None
            leaf_durations[name] = dur
            if state in FAIL:
                # Fetch the error detail
                cur.execute(f"""
                    SELECT state, refresh_action, refresh_trigger, statistics, condition
                    FROM TABLE({catalog}.INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY(
                        NAME => '{catalog}.{target_schema}.{name}'
                    ))
                    WHERE refresh_start_time = '{t_st}'
                """)
                detail = cur.fetchall()
                raise RuntimeError(
                    f"DT refresh {state}: {name}\n"
                    f"  duration={dur}s\n"
                    f"  detail={detail}"
                )
            print(f"  ✓ {name:25s} {state:10s} dur={dur:.1f}s")
            leaves_remaining.discard(name)
    if leaves_remaining:
        time.sleep(poll_interval)

if leaves_remaining:
    raise TimeoutError(
        f"Timeout {timeout_s}s waiting for DTs to refresh: {leaves_remaining}"
    )

# COMMAND ----------

print(f"[done] all {len(LEAF_DTS)} leaf DTs refreshed for {batch_date}")
print(f"durations (s): {json.dumps(leaf_durations, default=str)}")

# Emit per-leaf durations as task values so the parent job / dashboard can
# pick them up without re-querying.
dbutils.jobs.taskValues.set("leaf_refresh_durations_s", leaf_durations)

cur.close()
conn.close()
