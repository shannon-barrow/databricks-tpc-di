# Fabric NEE notebook — diagnose WHY the native execution engine is / isn't offloading.
# Reports the SESSION's actual NEE state (is spark.native.enabled set? is the Gluten
# plugin actually LOADED at session start? — the real failure mode when the conf is set
# but NEE still falls back), then runs a clean Delta scan+aggregate and inspects the
# executed physical plan for Velox/Gluten Transformer nodes. Writes to OneLake
# Files/_diag/nee_diag.txt (read back via the DFS API). Must run BOUND TO the NEE env.
# --- PARAMETER CELL ---
scale_factor = "10"
wh_db        = "shannon_barrow_fabricnee"
# ----------------------

# COMMAND ----------

import json
lines = []
def w(m):
    lines.append(str(m))
    try: notebookutils.fs.put("Files/_diag/nee_diag.txt", "\n".join(lines), True)
    except Exception as e: print("put err", e)

w("=== SESSION NEE STATE ===")
w("spark.version = " + spark.version)
for k in ["spark.native.enabled", "spark.gluten.enabled", "spark.plugins",
          "spark.sql.extensions", "spark.shuffle.manager",
          "spark.gluten.sql.columnar.backend.lib", "spark.sql.ansi.enabled"]:
    w(f"{k} = {spark.conf.get(k, '<unset>')}")

# The decisive check: is the Gluten/Velox plugin actually on the classpath + loaded?
# If spark.native.enabled='true' but these classes are NOT loaded, the env property did
# not cause the session to start with the native backend -> everything falls back to JVM.
nc = {k: v for k, v in spark.sparkContext.getConf().getAll()
      if any(t in k.lower() for t in ("native", "gluten", "velox", "columnar"))}
w("native/gluten/velox confs in session: " + json.dumps(nc, indent=1))
for cls in ["org.apache.gluten.GlutenPlugin", "io.glutenproject.GlutenPlugin",
            "org.apache.gluten.extension.GlutenSessionExtensions",
            "org.apache.spark.sql.execution.ColumnarToRowExec"]:
    try:
        spark._jvm.java.lang.Class.forName(cls); w(f"CLASS {cls}: LOADED")
    except Exception as e:
        w(f"CLASS {cls}: NOT FOUND ({str(e)[:80]})")

# COMMAND ----------

# Clean Delta scan + aggregate (self-contained — does not depend on prior data). If NEE is
# actually engaged the plan shows *Transformer / *NativeFileScan / Velox nodes.
#
# CRITICAL: with AQE on (the default), `queryExecution.executedPlan.toString()` prints the
# AQE *Initial* Plan — vanilla JVM node names (HashAggregate / Scan parquet) — EVEN WHEN the
# Final Plan is fully columnar. Reading that string is how we wrongly concluded "NEE = JVM"
# for weeks. To check correctly, read the AQE FINAL plan via `explainString("formatted")`
# AFTER executing, or disable AQE so executedPlan shows the physical operators directly.
w("")
w("=== CLEAN DELTA SCAN+AGG PROBE (AQE-final; the Initial Plan is misleading) ===")
try:
    spark.sql("CREATE SCHEMA IF NOT EXISTS dbo")
    (spark.range(0, 2_000_000).selectExpr("id", "id % 137 as k")
        .write.mode("overwrite").format("delta").saveAsTable("dbo._nee_probe"))
    df = spark.sql("SELECT k, count(*) c, sum(id) s FROM dbo._nee_probe GROUP BY k")
    df.collect()                                          # force execution (finalizes AQE)
    qe = df._jdf.queryExecution()
    try:
        mode = spark._jvm.org.apache.spark.sql.execution.ExplainMode.fromString("formatted")
        plan = qe.explainString(mode)                     # includes == Final Plan ==
    except Exception:
        plan = qe.executedPlan().toString()
    native = any(x in plan for x in ["Transformer", "NativeFileScan", "Velox",
                                     "ColumnarToRow", "Gluten"])
    w(f"NATIVE OFFLOAD: {'YES' if native else 'NO (pure JVM)'}")
    w("--- physical plan (head; look under '== Final Plan ==') ---")
    for ln in plan.splitlines()[:30]:
        w("  " + ln.rstrip()[:160])
except Exception as e:
    w("probe failed: " + str(e)[:300])

# COMMAND ----------

# Per-operator offload on the ACTUAL pipeline tables, at this scale. Checks the query shapes
# our transforms actually run: a Delta scan+agg, a dim SCD2 join, and min_by/max_by(struct)
# (FMH's 52-week window — the known Velox fallback risk). Reads the AQE FINAL plan per query
# and reports whether it went native + any JVM-fallback nodes (vanilla HashAggregate /
# SortMergeJoin / *Exec without a Transformer suffix, or ColumnarToRow/RowToColumnar bridges).
schema = f"{wh_db}_{scale_factor}"
spark.sql(f"USE {schema}")
w("")
w(f"=== PER-OPERATOR OFFLOAD on real tables ({schema}) ===")
def finalplan(sql):
    df = spark.sql(sql); df.collect()
    qe = df._jdf.queryExecution()
    try:
        mode = spark._jvm.org.apache.spark.sql.execution.ExplainMode.fromString("formatted")
        return qe.explainString(mode)
    except Exception:
        return qe.executedPlan().toString()

# Each returns a SMALL result so .collect() is driver-safe but still forces the operators
# under test to execute (join / struct-agg run in full; only the tiny final tally returns).
CHECKS = {
    "delta-scan-agg": "SELECT sk_dateid, count(*) c FROM factmarkethistory GROUP BY sk_dateid",
    "dim-join":       "SELECT count(*) c FROM dimtrade t JOIN dimsecurity s ON t.sk_securityid = s.sk_securityid",
    "minby-struct":   ("SELECT count(*) n FROM (SELECT dm_s_symb, min_by(struct(dm_low,dm_date),dm_low) lo, "
                       "max_by(struct(dm_high,dm_date),dm_high) hi FROM bronzedailymarket GROUP BY dm_s_symb)"),
}
for label, sql in CHECKS.items():
    try:
        p = finalplan(sql)
        # isolate the Final Plan section (ignore the Initial Plan, which is always JVM-named)
        fp = p.split("== Initial Plan ==")[0]
        native = any(x in fp for x in ["Transformer", "NativeFileScan", "Velox"])
        # crude fallback signal: JVM exec node names appearing in the Final Plan
        fb = [n for n in ["HashAggregate ", "SortMergeJoin", "BroadcastHashJoin ", "ColumnarToRow", "RowToColumnar"]
              if n in fp and (n+"Transformer") not in fp]
        w(f"[{label}] native={'YES' if native else 'NO'}  fallback_nodes={fb}")
        for ln in fp.splitlines()[:14]:
            w("   " + ln.rstrip()[:150])
        w("")
    except Exception as e:
        w(f"[{label}] failed: {str(e)[:200]}")

w("done")
notebookutils.notebook.exit("done")
