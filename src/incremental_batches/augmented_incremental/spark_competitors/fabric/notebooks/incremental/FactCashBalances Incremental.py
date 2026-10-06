# Fabric Spark NEE notebook — BATCH port of the fabric_ss FactCashBalances transform.
# The fabric_ss version was ALREADY batch (no readStream/foreachBatch — it reads the
# rebuilt currentaccountbalances snapshot), so this NEE version is byte-identical logic;
# it just runs on the NEE-accelerated environment. See PORT_NOTES.md §7.
# MUST run on the NEE-accelerated environment (tpcdi_fabric_nee, spark.native.enabled=true).

# --- PARAMETER CELL (Fabric injects overrides) ---
scale_factor    = "10"
wh_db           = ""
# -------------------------------------------------

sf_ls = ["10", "100", "1000", "5000", "10000", "20000"]

tgt_db          = f"{wh_db}_{scale_factor}"
table           = "factcashbalances"
src_table       = f"{tgt_db}.currentaccountbalances"
tgt_table       = f"{tgt_db}.{table}"

# COMMAND ----------

# Fabric Spark 4.1 has no `INSERT INTO ... REPLACE USING` (Databricks-only selective
# overwrite). Portable equivalent with the same semantics: build this batch's rows,
# DELETE the target rows whose sk_dateid appears in the batch, then INSERT. (Standard
# Delta 4.2 SQL; per-batch idempotent — re-running a batch replaces its sk_dateid rows.)
_src = spark.sql(f"""
  SELECT
    a.sk_customerid,
    a.sk_accountid,
    bigint(date_format(c.ct_date, 'yyyyMMdd')) sk_dateid,
    c.current_account_cash
  FROM {src_table} c
  JOIN {tgt_db}.dimaccount a
    ON
      c.accountid = a.accountid
      AND a.iscurrent
  where c.latest_batch
""")
_src.createOrReplaceTempView("_fcb_src")
# Delta on Fabric rejects subqueries in DELETE (DELTA_UNSUPPORTED_SUBQUERY), so collect
# this batch's distinct sk_dateids and DELETE with a literal IN-list (tiny per batch).
_dateids = [int(r[0]) for r in _src.select("sk_dateid").distinct().collect() if r[0] is not None]
if _dateids:
    spark.sql(f"DELETE FROM {tgt_table} WHERE sk_dateid IN ({','.join(map(str, _dateids))})")
spark.sql(f"INSERT INTO {tgt_table} SELECT * FROM _fcb_src")
