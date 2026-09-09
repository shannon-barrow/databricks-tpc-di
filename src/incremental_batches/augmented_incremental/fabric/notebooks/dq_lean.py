# Fabric Spark notebook — lean DQ check. Runs on the STARTER pool (deploy WITHOUT the
# custom-pool env so it starts in seconds), reads the lakehouse bronze tables natively,
# and tests whether the once-per-day source invariant (one row per merge key per day)
# holds after the Fabric ingest. Any dup here = a port-introduced DQ bug. Writes the
# result to OneLake Files/_diag/dq_lean.txt (Job Scheduler returns no exitValue).

# --- PARAMETER CELL ---
scale_factor = "20000"
wh_db        = "shannon_barrow_fabricss"
# ----------------------

# COMMAND ----------

schema = f"{wh_db}_{scale_factor}"
dates  = ["2016-07-06", "2016-07-07", "2016-07-08"]   # batch 1 (ok) + batches 2,3 (failed)
lines  = [f"DQ check schema={schema}"]

def chk(tbl, dcol, key):
    for d in dates:
        r = spark.sql(f"SELECT count(*) t, count(distinct {key}) k "
                      f"FROM {schema}.{tbl} WHERE {dcol}=date'{d}'").collect()[0]
        dup = r['t'] - r['k']
        flag = "   << DUP" if dup > 0 else ""
        lines.append(f"{tbl:22s} {d} rows={r['t']:>12,} distinct[{key}]={r['k']:>12,} dup={dup:>10,}{flag}")
    lines.append("")

chk("bronzecustomer",    "update_dt", "customerid")   # dimcustomer MERGE — strict, this is the one that failed
chk("bronzeaccount",     "update_dt", "accountid")
chk("bronzedailymarket", "dm_date",   "dm_s_symb")

out = "\n".join(lines)
print(out)
notebookutils.fs.put("Files/_diag/dq_lean.txt", out, True)
notebookutils.notebook.exit("dq_ok")
