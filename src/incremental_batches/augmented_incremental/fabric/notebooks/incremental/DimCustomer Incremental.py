# Fabric Spark notebook (ported from augmented_incremental/incremental/DimCustomer Incremental.py)
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
table           = "dimcustomer"
src_table       = f"{tgt_db}.bronzecustomer"
tgt_table       = f"{tgt_db}.{table}"
checkpoint_dir  = f"{checkpoint_base}/{tgt_db}/{table}"

# COMMAND ----------

# ONLY Excecute this code if you need to restart the stream over

# notebookutils.fs.rm(f"{checkpoint_dir}", recurse=True)
# notebookutils.fs.mkdirs(f"{checkpoint_dir}")
# spark.sql(f"RESTORE TABLE {tgt_table} TO VERSION AS OF 0")

# COMMAND ----------

def upsertToDelta(microBatchOutputDF, batch_id):
  microBatchOutputDF.createOrReplaceTempView("bronzecustomer")
  microBatchOutputDF.sparkSession.sql(f"""
    with incr_cust as (
      SELECT
        bigint(concat(date_format(c.update_dt, 'yyyyMMdd'), customerid)) sk_customerid,
        customerid,
        taxid,
        decode(status, 
          'ACTV',	'Active',
          'CMPT','Completed',
          'CNCL','Canceled',
          'PNDG','Pending',
          'SBMT','Submitted',
          'INAC','Inactive') status,
        lastname,
        firstname,
        middleinitial,
        if(upper(c.gender) IN ('M', 'F'), upper(c.gender), 'U') gender,
        tier,
        dob,
        addressline1,
        addressline2,
        postalcode,
        city,
        stateprov,
        country,
        nvl2(
          c_local_1,
          concat(
            nvl2(c_ctry_1, '+' || c_ctry_1 || ' ', ''),
            nvl2(c_area_1, '(' || c_area_1 || ') ', ''),
            c_local_1,
            nvl(c_ext_1, '')),
          c_local_1) phone1,
        nvl2(
          c_local_2,
          concat(
            nvl2(c_ctry_2, '+' || c_ctry_2 || ' ', ''),
            nvl2(c_area_2, '(' || c_area_2 || ') ', ''),
            c_local_2,
            nvl(c_ext_2, '')),
          c_local_2) phone2,
        nvl2(
          c_local_3,
          concat(
            nvl2(c_ctry_3, '+' || c_ctry_3 || ' ', ''),
            nvl2(c_area_3, '(' || c_area_3 || ') ', ''),
            c_local_3,
            nvl(c_ext_3, '')),
          c_local_3) phone3,
        email1,
        email2,
        r_nat.tx_name as nationaltaxratedesc,
        r_nat.tx_rate as nationaltaxrate,
        r_lcl.tx_name as localtaxratedesc,
        r_lcl.tx_rate as localtaxrate,
        update_dt effectivedate,
        date('9999-12-31') enddate,
        True iscurrent
      FROM bronzecustomer c
      JOIN {tgt_db}.TaxRate r_lcl 
        ON c.lcl_tx_id = r_lcl.TX_ID
      JOIN {tgt_db}.TaxRate r_nat 
        ON c.nat_tx_id = r_nat.TX_ID
    )
    MERGE INTO {tgt_table} t USING (
      SELECT
        s.customerid AS mergeKey,
        s.*
      FROM incr_cust s
      JOIN {tgt_table} t
        ON s.customerid = t.customerid
      WHERE t.iscurrent
        AND t.enddate = DATE'9999-12-31'
      UNION ALL
      SELECT
        cast(null as bigint) AS mergeKey,
        *
      FROM incr_cust
    ) s
      ON
        t.customerid = s.mergeKey
        AND t.iscurrent
        AND t.enddate = DATE'9999-12-31'
    WHEN MATCHED AND s.sk_customerid is not null THEN UPDATE SET
      t.iscurrent = false,
      t.enddate = s.effectivedate
    WHEN NOT MATCHED THEN INSERT (sk_customerid, customerid, taxid, status, lastname, firstname, middleinitial, gender, tier, dob, addressline1, addressline2, postalcode, city, stateprov, country, phone1, phone2, phone3, email1, email2, nationaltaxratedesc, nationaltaxrate, localtaxratedesc, localtaxrate, effectivedate, enddate, iscurrent)
    VALUES (sk_customerid, customerid, taxid, status, lastname, firstname, middleinitial, gender, tier, dob, addressline1, addressline2, postalcode, city, stateprov, country, phone1, phone2, phone3, email1, email2, nationaltaxratedesc, nationaltaxrate, localtaxratedesc, localtaxrate, effectivedate, enddate, iscurrent)
  """)

# COMMAND ----------

(spark.readStream
  .table(src_table)
  .writeStream
  .option("checkpointLocation", checkpoint_dir)
  .trigger(availableNow=True)
  .foreachBatch(upsertToDelta)
  .outputMode("update")
  .start()
).awaitTermination()   # Fabric: block until the availableNow stream finishes (see ingest_bronze)
