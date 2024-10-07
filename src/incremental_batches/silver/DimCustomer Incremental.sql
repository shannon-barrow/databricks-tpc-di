-- Databricks notebook source
WITH customerincrementalraw AS (
  SELECT
    customerid,
    nullif(taxid, '') taxid,
    decode(status, 
      'ACTV',	'Active',
      'CMPT','Completed',
      'CNCL','Canceled',
      'PNDG','Pending',
      'SBMT','Submitted',
      'INAC','Inactive') status,
    nullif(lastname, '') lastname,
    nullif(firstname, '') firstname,
    nullif(middleinitial, '') middleinitial,
    nullif(gender, '') gender,
    tier,
    dob,
    nullif(addressline1, '') addressline1,
    nullif(addressline2, '') addressline2,
    nullif(postalcode, '') postalcode,
    nullif(city, '') city,
    nullif(stateprov, '') stateprov,
    country,
    nvl2(
      nullif(c_local_1, ''),
      concat(
        nvl2(nullif(c_ctry_1, ''), '+' || c_ctry_1 || ' ', ''),
        nvl2(nullif(c_area_1, ''), '(' || c_area_1 || ') ', ''),
        c_local_1,
        nvl(c_ext_1, '')),
      try_cast(null as string)) phone1,
    nvl2(
      nullif(c_local_2, ''),
      concat(
        nvl2(nullif(c_ctry_2, ''), '+' || c_ctry_2 || ' ', ''),
        nvl2(nullif(c_area_2, ''), '(' || c_area_2 || ') ', ''),
        c_local_2,
        nvl(c_ext_2, '')),
      try_cast(null as string)) phone2,
    nvl2(
      nullif(c_local_3, ''),
      concat(
        nvl2(nullif(c_ctry_3, ''), '+' || c_ctry_3 || ' ', ''),
        nvl2(nullif(c_area_3, ''), '(' || c_area_3 || ') ', ''),
        c_local_3,
        nvl(c_ext_3, '')),
      try_cast(null as string)) phone3,
    nullif(email1, '') email1,
    nullif(email2, '') email2,
    nullif(lcl_tx_id, '') lcl_tx_id,
    nullif(nat_tx_id, '') nat_tx_id,
    int(${batch_id}) batchid
  FROM read_files(
    "${tpcdi_directory}sf=${scale_factor}/Batch${batch_id}",
    format => "csv",
    inferSchema => False,
    header => False,
    sep => "|",
    fileNamePattern => "Customer.txt",
    schema => "cdc_flag STRING, cdc_dsn BIGINT, customerid BIGINT, taxid STRING, status STRING, lastname STRING, firstname STRING, middleinitial STRING, gender STRING, tier TINYINT, dob DATE, addressline1 STRING, addressline2 STRING, postalcode STRING, city STRING, stateprov STRING, country STRING, c_ctry_1 STRING, c_area_1 STRING, c_local_1 STRING, c_ext_1 STRING, c_ctry_2 STRING, c_area_2 STRING, c_local_2 STRING, c_ext_2 STRING, c_ctry_3 STRING, c_area_3 STRING, c_local_3 STRING, c_ext_3 STRING, email1 STRING, email2 STRING, lcl_tx_id STRING, nat_tx_id STRING"
  )
),
prospect as (
  select 
    agencyid, 
    creditrating, 
    networth, 
    marketingnameplate,
    lastname,
    firstname,
    addressline1,
    addressline2,
    postalcode
  from ${catalog}.${wh_db}_${scale_factor}_stage.ProspectIncremental p 
  where 
    batchid = int(${batch_id})
    or (recordbatchid = int(${batch_id}) - 1 and batchid < int(${batch_id}))
  qualify(row_number() over(partition by agencyid order by batchid desc) = 1)
),
prospect_updates as (
  select
    c.* except(agencyid, creditrating, networth, marketingnameplate, batchid),
    if(batchid = int(${batch_id}), p.agencyid, null) agencyid,
    if(batchid = int(${batch_id}), p.creditrating, null) creditrating,
    if(batchid = int(${batch_id}), p.networth, null) networth,
    if(batchid = int(${batch_id}), p.marketingnameplate, null) marketingnameplate
  FROM ${catalog}.${wh_db}_${scale_factor}.DimCustomer c
  JOIN prospect p 
    ON
      upper(p.lastname) = upper(c.lastname)
      and upper(p.firstname) = upper(c.firstname)
      and upper(p.addressline1) = upper(c.addressline1)
      and upper(nvl(p.addressline2, '')) = upper(nvl(c.addressline2, ''))
      and upper(p.postalcode) = upper(c.postalcode)
  WHERE c.iscurrent
),
CustomerIncremental as (
  SELECT
    bigint(concat(date_format(bd.batchdate, 'yyyyMMdd'), c.customerid)) sk_customerid,
    c.customerid,
    c.taxid,
    c.status,
    c.lastname,
    c.firstname,
    c.middleinitial,
    if(upper(c.gender) IN ('M', 'F'), upper(c.gender), 'U') gender,
    c.tier,
    c.dob,
    c.addressline1,
    c.addressline2,
    c.postalcode,
    c.city,
    c.stateprov,
    c.country,
    c.phone1,
    c.phone2,
    c.phone3,
    c.email1,
    c.email2,
    r_nat.tx_name as nationaltaxratedesc,
    r_nat.tx_rate as nationaltaxrate,
    r_lcl.tx_name as localtaxratedesc,
    r_lcl.tx_rate as localtaxrate,
    p.agencyid,
    p.creditrating,
    p.networth,
    p.marketingnameplate,
    true iscurrent,
    c.batchid,
    bd.batchdate effectivedate,
    date('9999-12-31') enddate
  FROM customerincrementalraw c
  JOIN ${catalog}.${wh_db}_${scale_factor}.BatchDate bd ON c.batchid = bd.batchid
  JOIN ${catalog}.${wh_db}_${scale_factor}.TaxRate r_lcl 
    ON c.lcl_tx_id = r_lcl.TX_ID
  JOIN ${catalog}.${wh_db}_${scale_factor}.TaxRate r_nat 
    ON c.nat_tx_id = r_nat.TX_ID
  LEFT JOIN ${catalog}.${wh_db}_${scale_factor}_stage.ProspectIncremental p 
    ON
      p.batchid <= int(${batch_id})
      and p.recordbatchid >= int(${batch_id})
      and upper(p.lastname) = upper(c.lastname)
      and upper(p.firstname) = upper(c.firstname)
      and upper(p.addressline1) = upper(c.addressline1)
      and upper(nvl(p.addressline2, '')) = upper(nvl(c.addressline2, ''))
      and upper(p.postalcode) = upper(c.postalcode)
),
all_updates as(
  SELECT
    c.sk_customerid,
    nvl(c.customerid, p.customerid) customerid,
    c.taxid,
    c.status,
    c.lastname,
    c.firstname,
    c.middleinitial,
    c.gender,
    c.tier,
    c.dob,
    c.addressline1,
    c.addressline2,
    c.postalcode,
    c.city,
    c.stateprov,
    c.country,
    c.phone1,
    c.phone2,
    c.phone3,
    c.email1,
    c.email2,
    c.nationaltaxratedesc,
    c.nationaltaxrate,
    c.localtaxratedesc,
    c.localtaxrate,
    nvl(c.agencyid, p.agencyid) agencyid,
    nvl(c.creditrating, p.creditrating) creditrating,
    nvl(c.networth, p.networth) networth,
    nvl(c.marketingnameplate, p.marketingnameplate) marketingnameplate,
    c.iscurrent,
    c.batchid,
    c.effectivedate,
    c.enddate
  FROM 
    prospect_updates p
  FULL OUTER JOIN 
    CustomerIncremental c
    ON p.customerid = c.customerid
)
MERGE INTO ${catalog}.${wh_db}_${scale_factor}.DimCustomer t USING (
  SELECT
    s.customerid AS mergeKey,
    s.*
  FROM all_updates s
  JOIN ${catalog}.${wh_db}_${scale_factor}.DimCustomer t
    ON s.customerid = t.customerid
  WHERE t.iscurrent
  UNION ALL
  SELECT
    cast(null as bigint) AS mergeKey,
    *
  FROM CustomerIncremental
) s 
  ON 
    t.customerid = s.mergeKey
    AND t.iscurrent
-- These are this batch's customer updates (and potentially some Prospect updates) so apply SCD Type 2
WHEN MATCHED AND s.sk_customerid is not null THEN UPDATE SET
  t.iscurrent = false,
  t.enddate = s.effectivedate
-- These are ONLY Prospect updates, no new customer records this batch
WHEN MATCHED AND s.sk_customerid is null THEN UPDATE SET
  t.agencyid = s.agencyid,
  t.creditrating = s.creditrating,
  t.networth = s.networth,
  t.marketingnameplate = s.marketingnameplate
WHEN NOT MATCHED THEN INSERT (sk_customerid, customerid, taxid, status, lastname, firstname, middleinitial, gender, tier, dob, addressline1, addressline2, postalcode, city, stateprov, country, phone1, phone2, phone3, email1, email2, nationaltaxratedesc, nationaltaxrate, localtaxratedesc, localtaxrate, agencyid, creditrating, networth, marketingnameplate, iscurrent, batchid, effectivedate, enddate)
VALUES (sk_customerid, customerid, taxid, status, lastname, firstname, middleinitial, gender, tier, dob, addressline1, addressline2, postalcode, city, stateprov, country, phone1, phone2, phone3, email1, email2, nationaltaxratedesc, nationaltaxrate, localtaxratedesc, localtaxrate, agencyid, creditrating, networth, marketingnameplate, iscurrent, batchid, effectivedate, enddate);
