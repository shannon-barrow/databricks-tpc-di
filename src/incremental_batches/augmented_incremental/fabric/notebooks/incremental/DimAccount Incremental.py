# Fabric Spark notebook (ported from augmented_incremental/incremental/DimAccount Incremental.py)
# Silver/gold incremental transform — structured streaming + foreachBatch MERGE.
# Ports 1:1 from Databricks; only the parameter source, catalog naming, and
# checkpoint path change for Fabric. Attach a schema-enabled default lakehouse.

# --- PARAMETER CELL (Fabric injects overrides) ---
scale_factor    = "10"
wh_db           = ""
checkpoint_base = "Files/augmented_incremental/_checkpoints"
# -------------------------------------------------

sf_ls = ["10", "100", "1000", "5000", "10000", "20000"]

tgt_db          = f"{wh_db}_{scale_factor}"
table           = "dimaccount"
src_table       = f"{tgt_db}.bronzeaccount"
tgt_table       = f"{tgt_db}.{table}"
checkpoint_dir  = f"{checkpoint_base}/{tgt_db}/{table}"

# COMMAND ----------

# ONLY Excecute this code if you need to restart the stream over

# notebookutils.fs.rm(f"{checkpoint_dir}", recurse=True)
# notebookutils.fs.mkdirs(f"{checkpoint_dir}")
# spark.sql(f"RESTORE TABLE {tgt_table} TO VERSION AS OF 0")

# COMMAND ----------

def upsertToDelta(microBatchOutputDF, batch_id):
  microBatchOutputDF.createOrReplaceTempView("bronzeaccount")
  microBatchOutputDF.sparkSession.sql(f"""
    with accounts as (
      -- Fabric Spark 4.1 has no QUALIFY (Databricks/Snowflake extension) — use the
      -- standard-Spark equivalent: row_number() in a subquery + WHERE rn = 1.
      SELECT * except(cdc_dsn, _rn) FROM (
        SELECT *,
          row_number() over (partition by update_dt, accountid order by cdc_flag desc) as _rn
        FROM bronzeaccount a
      ) WHERE _rn = 1
    ),
    all_incr_updates as (
      SELECT
        bigint(concat(date_format(a.update_dt, 'yyyyMMdd'), a.accountid)) sk_accountid,
        accountid,
        brokerid sk_brokerid,
        dc.sk_customerid,
        accountdesc,
        taxstatus,
        decode(a.status, 
          'ACTV',	'Active',
          'CMPT','Completed',
          'CNCL','Canceled',
          'PNDG','Pending',
          'SBMT','Submitted',
          'INAC','Inactive',
          a.status) status,
        true iscurrent,
        update_dt effectivedate,
        date('9999-12-31') enddate
      FROM accounts a
      JOIN {tgt_db}.dimcustomer dc
        ON 
          dc.iscurrent
          and dc.customerid = a.customerid
    ),
    matched_accts as (
      SELECT
        s.*
      FROM all_incr_updates s
      JOIN {tgt_db}.DimAccount t
        ON s.accountid = t.accountid
      WHERE t.iscurrent
        AND t.enddate = DATE'9999-12-31'
    )
    MERGE INTO {tgt_table} t USING (
      SELECT
        CAST(NULL AS BIGINT) AS mergeKey,
        *
      FROM all_incr_updates
      UNION ALL
      SELECT
        accountid mergeKey,
        *
      FROM matched_accts
    ) s
    ON t.accountid = s.mergeKey AND t.iscurrent AND t.enddate = DATE'9999-12-31'
    WHEN MATCHED AND t.iscurrent THEN UPDATE SET
      t.iscurrent = false,
      t.enddate = s.effectivedate
    WHEN NOT MATCHED THEN INSERT (sk_accountid, accountid, sk_brokerid, sk_customerid, accountdesc, TaxStatus, status, iscurrent, effectivedate, enddate)
    VALUES (sk_accountid, accountid, sk_brokerid, sk_customerid, accountdesc, TaxStatus, status, iscurrent, effectivedate, enddate)
""")

# COMMAND ----------

(spark.readStream.table(src_table)
  .writeStream
  .option("checkpointLocation", checkpoint_dir)
  .trigger(availableNow=True)
  .foreachBatch(upsertToDelta)
  .outputMode("update")
  .start()
).awaitTermination()   # Fabric: block until the availableNow stream finishes (see ingest_bronze)
