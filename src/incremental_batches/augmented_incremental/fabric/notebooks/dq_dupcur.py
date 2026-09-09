# Fabric Spark notebook — check for DUPLICATE CURRENT records in dimcustomer (the MERGE
# multi-match cause when the source is clean). A customerid with >1 row where
# iscurrent AND enddate=9999-12-31 makes one incoming row match two targets. Checks both
# the current (post-batch-1) dimcustomer AND the historical seed (staging clone), to tell
# whether the dup came from the seed/historical build or from batch 1's MERGE. Safe
# SELECTs only (no MERGE) so it won't session-cancel. Starter pool.

# --- PARAMETER CELL ---
scale_factor = "20000"
wh_db        = "shannon_barrow_fabricss"
# ----------------------

# COMMAND ----------

schema  = f"{wh_db}_{scale_factor}"
staging = f"staging_sf{scale_factor}"
lines   = [f"dup-current-record check schema={schema}"]

def dupcur(tbl):
    d = spark.sql(f"SELECT customerid, count(*) c FROM {tbl} "
                  f"WHERE iscurrent AND enddate=DATE'9999-12-31' GROUP BY customerid HAVING count(*)>1")
    n = d.count()
    lines.append(f"{tbl}: customerids with >1 CURRENT record = {n:,}")
    if n:
        lines.append("  sample: " + str([r['customerid'] for r in d.limit(10).collect()]))
    return d, n

post, npost = dupcur(f"{schema}.dimcustomer")      # post-batch-1 state (what batch 2 merged into)
seed, nseed = dupcur(f"{staging}.dimcustomer")      # historical seed setup shallow-cloned
lines.append("")
# do the dup-current customers overlap batch 2's incoming customers (2016-07-07)?
if npost:
    post.select("customerid").createOrReplaceTempView("dupcur")
    hit = spark.sql(f"SELECT count(distinct b.customerid) c FROM {schema}.bronzecustomer b "
                    f"JOIN dupcur d ON b.customerid=d.customerid WHERE b.update_dt=date'2016-07-07'").collect()[0]['c']
    lines.append(f"dup-current customers touched by batch 2 (2016-07-07): {hit:,}")

out = "\n".join(lines)
print(out)
notebookutils.fs.put("Files/_diag/dq_dupcur.txt", out, True)
notebookutils.notebook.exit("ok")
