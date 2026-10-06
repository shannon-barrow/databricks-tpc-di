# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# Non-SS batch DimCustomer (mirrors dbt silver/dimcustomer). bronzecustomer ACCUMULATES;
# this transform scopes to THIS batch with `new_events = bronzecustomer where update_dt = batch_date`,
# then the SCD2 MERGE end-dates the matched current row and inserts the new version.
sf_ls = ["10", "100", "1000", "5000", "10000", "20000"]
dbutils.widgets.dropdown("scale_factor", sf_ls[0], sf_ls)
dbutils.widgets.text("tpcdi_directory", "/Volumes/tpcdi/tpcdi_raw_data/tpcdi_volume/")
dbutils.widgets.text("catalog", "tpcdi")
dbutils.widgets.text("wh_db", "")
dbutils.widgets.text("batch_date", "")

catalog         = dbutils.widgets.get("catalog")
scale_factor    = dbutils.widgets.get("scale_factor")
tpcdi_directory = dbutils.widgets.get("tpcdi_directory")
wh_db           = dbutils.widgets.get("wh_db")
batch_date      = dbutils.widgets.get("batch_date")
tgt_db          = f"{wh_db}_{scale_factor}"
table           = "dimcustomer"
tgt_table       = f"{catalog}.{tgt_db}.{table}"

# COMMAND ----------

spark.sql(f"""
  with new_events as (
    -- scope the accumulated bronze to THIS batch (dbt models/silver/dimcustomer.sql)
    select * from {catalog}.{tgt_db}.bronzecustomer
    where update_dt = cast('{batch_date}' as date)
  ),
  incr_cust as (
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
    FROM new_events c
    JOIN {catalog}.{tgt_db}.TaxRate r_lcl
      ON c.lcl_tx_id = r_lcl.TX_ID
    JOIN {catalog}.{tgt_db}.TaxRate r_nat
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
    UNION ALL
    SELECT
      cast(null as bigint) AS mergeKey,
      *
    FROM incr_cust
  ) s
    ON
      t.customerid = s.mergeKey
      AND t.iscurrent
  WHEN MATCHED AND s.sk_customerid is not null THEN UPDATE SET
    t.iscurrent = false,
    t.enddate = s.effectivedate
  WHEN NOT MATCHED THEN INSERT (sk_customerid, customerid, taxid, status, lastname, firstname, middleinitial, gender, tier, dob, addressline1, addressline2, postalcode, city, stateprov, country, phone1, phone2, phone3, email1, email2, nationaltaxratedesc, nationaltaxrate, localtaxratedesc, localtaxrate, effectivedate, enddate, iscurrent)
  VALUES (sk_customerid, customerid, taxid, status, lastname, firstname, middleinitial, gender, tier, dob, addressline1, addressline2, postalcode, city, stateprov, country, phone1, phone2, phone3, email1, email2, nationaltaxratedesc, nationaltaxrate, localtaxratedesc, localtaxrate, effectivedate, enddate, iscurrent)
""")
