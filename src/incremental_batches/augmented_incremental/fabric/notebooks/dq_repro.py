# Fabric Spark notebook — PRE-FLIGHT: prove ANALYZE + injection stops the broadcast.
# ANALYZE dimcustomer (USE + unqualified), enable injection, EXPLAIN COST the DimCustomer
# arm1 join, and check it's now SortMergeJoin with a realistic estimate (not the 103 KiB
# that drove the 8.2 GiB BroadcastHashJoin).
# --- PARAMETER CELL ---
scale_factor = "20000"
wh_db        = "shannon_barrow_fabricss"
batch_date   = "2016-07-07"
# ----------------------

# COMMAND ----------

run_schema = f"{wh_db}_{scale_factor}"
lines = []
def w(m):
    lines.append(str(m))
    try: notebookutils.fs.put("Files/_diag/preflight.txt", "\n".join(lines), True)
    except Exception as e: print("put err", e)

spark.conf.set("spark.microsoft.delta.stats.injection.catalog.enabled", "true")
spark.sql(f"USE {run_schema}")
for t in ["dimcustomer", "taxrate"]:
    spark.sql(f"ANALYZE TABLE {t} COMPUTE STATISTICS FOR ALL COLUMNS")
w("analyzed dimcustomer + taxrate; injection ON")

# Representative arm1 join: this-batch customers JOIN dimcustomer current records.
plan = spark.sql(f"""EXPLAIN COST
  SELECT s.customerid
  FROM (SELECT customerid FROM bronzecustomer WHERE update_dt = date'{batch_date}') s
  JOIN dimcustomer t ON s.customerid = t.customerid
  WHERE t.iscurrent AND t.enddate = DATE'9999-12-31'""").collect()[0][0]

phys = plan.split("== Physical Plan ==")[-1]
w("JOIN TYPE: " + ("BroadcastHashJoin (STILL BROADCASTS - BAD)" if "BroadcastHashJoin" in phys
                   else "SortMergeJoin/ShuffledHashJoin (no broadcast - FIXED)"))
# show the dimcustomer-current estimate line
for ln in plan.splitlines():
    if "Filter (iscurrent" in ln or ("dimcustomer" in ln and "Statistics" in ln):
        w("EST: " + ln.strip()[:200])
w(plan[:2500])
notebookutils.notebook.exit("done")
