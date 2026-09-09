# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # One-off: rebuild staging tables with explicit table properties
# MAGIC
# MAGIC The augmented-incremental staging tables were created inheriting the
# MAGIC account default (`autoCompact` ON) and without deletion vectors / row
# MAGIC tracking / Parquet v2. This notebook rewrites each table in place so the
# MAGIC properties are EXPLICIT (not reliant on account defaults) — without a full
# MAGIC data regen (reuses the existing SF data).
# MAGIC
# MAGIC Per table, parallelized:
# MAGIC 1. `CREATE {t}_new LIKE {t}` — copies columns + constraints + TBLPROPERTIES,
# MAGIC    but **NOT** Liquid `CLUSTER BY` (a verified LIKE limitation).
# MAGIC 2. Re-apply the source table's clustering — `ALTER {t}_new CLUSTER BY (...)`
# MAGIC    (detected at runtime; preserves the *current* layout, never invents one).
# MAGIC 3. `ALTER {t}_new SET TBLPROPERTIES (...)` — the explicit target set.
# MAGIC 4. `INSERT INTO {t}_new SELECT * FROM {t}` — writes fresh Parquet v2 files.
# MAGIC 5. **Row-count guard** — verify `_new` == original before touching original.
# MAGIC 6. `DROP {t}`, `RENAME {t}_new -> {t}`, `OPTIMIZE`, `ANALYZE`.
# MAGIC
# MAGIC The guard means a mid-run failure leaves the original staging table intact
# MAGIC (the shared source every benchmark variant DEEP CLONEs from) — worst case is
# MAGIC an orphan `_new` table.

# COMMAND ----------

import concurrent.futures
import traceback

dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema",  "tpcdi_incremental_staging_20000")
dbutils.widgets.text("max_workers", "8")
dbutils.widgets.dropdown("dry_run", "NO", ["NO", "YES"], "YES = plan only, no writes")

catalog     = dbutils.widgets.get("catalog")
schema      = dbutils.widgets.get("schema")
max_workers = int(dbutils.widgets.get("max_workers"))
dry_run     = dbutils.widgets.get("dry_run").upper() == "YES"

# Explicit target properties — the whole point of this notebook. optimizeWrite on
# / autoCompact off (we OPTIMIZE deliberately, not per-write), deletion vectors +
# row tracking on, Parquet v2 for the freshly-written files.
TARGET_PROPS = {
    "delta.autoOptimize.optimizeWrite": "true",
    "delta.autoOptimize.autoCompact":   "false",
    "delta.enableDeletionVectors":      "true",
    "delta.enableRowTracking":          "true",
    "delta.parquet.format.version":     "2.12.0",
}
PROPS_SQL = ", ".join(f"'{k}' = '{v}'" for k, v in TARGET_PROPS.items())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Discover tables + their current clustering

# COMMAND ----------

tables = [r.tableName for r in spark.sql(f"SHOW TABLES IN {catalog}.{schema}").collect()
          if not r.tableName.endswith("_new")]

def current_cluster_cols(fqn):
    """clusteringColumns from DESCRIBE DETAIL, e.g. ['enddate'] or []."""
    row = spark.sql(f"DESCRIBE DETAIL {fqn}").select("clusteringColumns").first()
    return list(row[0]) if row and row[0] else []

plan = {}
for t in tables:
    fqn = f"{catalog}.{schema}.{t}"
    plan[t] = current_cluster_cols(fqn)

for t in sorted(plan):
    ck = ", ".join(plan[t]) if plan[t] else "(unclustered)"
    print(f"{t:30s} CLUSTER BY {ck}")
print(f"\n{len(plan)} tables; dry_run={dry_run}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Per-table rebuild

# COMMAND ----------

def rebuild(t):
    fqn   = f"{catalog}.{schema}.{t}"
    newfqn = f"{catalog}.{schema}.{t}_new"
    cluster_cols = plan[t]
    log = [f"[{t}] start (cluster={cluster_cols or 'none'})"]
    if dry_run:
        return "\n".join(log + [f"[{t}] DRY RUN — skipped"])

    spark.sql(f"DROP TABLE IF EXISTS {newfqn}")
    # 1. LIKE copies schema + constraints + TBLPROPERTIES, but NOT clustering.
    spark.sql(f"CREATE TABLE {newfqn} LIKE {fqn}")
    # 2. Re-apply the source clustering (LIKE dropped it). Single or multi-col.
    if cluster_cols:
        cols = ", ".join(cluster_cols)
        spark.sql(f"ALTER TABLE {newfqn} CLUSTER BY ({cols})")
    # 3. Explicit properties.
    spark.sql(f"ALTER TABLE {newfqn} SET TBLPROPERTIES ({PROPS_SQL})")
    # 4. Copy data (writes Parquet v2 files).
    spark.sql(f"INSERT INTO {newfqn} SELECT * FROM {fqn}")

    # 5. Guard: never drop the original unless the copy is complete.
    old_n = spark.sql(f"SELECT COUNT(*) c FROM {fqn}").first().c
    new_n = spark.sql(f"SELECT COUNT(*) c FROM {newfqn}").first().c
    if old_n != new_n:
        raise RuntimeError(f"[{t}] ROW COUNT MISMATCH old={old_n:,} new={new_n:,} — "
                           f"leaving {fqn} intact and {newfqn} orphaned for inspection")
    log.append(f"[{t}] rows verified {new_n:,}")

    # 6. Swap + compact + stats.
    spark.sql(f"DROP TABLE {fqn}")
    spark.sql(f"ALTER TABLE {newfqn} RENAME TO {fqn}")
    spark.sql(f"OPTIMIZE {fqn}")
    spark.sql(f"ANALYZE TABLE {fqn} COMPUTE STATISTICS FOR ALL COLUMNS")
    log.append(f"[{t}] DONE")
    return "\n".join(log)

# COMMAND ----------

results, errors = {}, {}
with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as ex:
    futs = {ex.submit(rebuild, t): t for t in sorted(plan)}
    for f in concurrent.futures.as_completed(futs):
        t = futs[f]
        try:
            results[t] = f.result()
            print(results[t])
        except Exception as e:
            errors[t] = f"{e}\n{traceback.format_exc()}"
            print(f"[{t}] FAILED: {e}")

# COMMAND ----------

print(f"OK: {len(results)}   FAILED: {len(errors)}")
if errors:
    for t, e in errors.items():
        print(f"\n===== {t} =====\n{e}")
    raise RuntimeError(f"{len(errors)} table(s) failed: {sorted(errors)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify final properties + clustering

# COMMAND ----------

for t in sorted(plan):
    fqn = f"{catalog}.{schema}.{t}"
    props = {r.key: r.value for r in spark.sql(f"SHOW TBLPROPERTIES {fqn}").collect()}
    ck = current_cluster_cols(fqn)
    checks = all(props.get(k) == v for k, v in TARGET_PROPS.items())
    print(f"{t:30s} props_ok={checks}  cluster={ck or '(none)'}")
