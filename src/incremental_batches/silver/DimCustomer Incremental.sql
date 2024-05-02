-- Databricks notebook source
with prospect_updates as (
  select
    c.* except(agencyid, creditrating, networth, marketingnameplate),
    p.agencyid, 
    p.creditrating, 
    p.networth, 
    p.marketingnameplate
  FROM
    ${catalog}.${wh_db}_${scale_factor}.DimCustomer c
  JOIN ${catalog}.${wh_db}_${scale_factor}_stage.ProspectIncremental p 
    ON
      p.batchid = ${batch_id}
      and c.iscurrent
      and upper(p.lastname) = upper(c.lastname)
      and upper(p.firstname) = upper(c.firstname)
      and upper(p.addressline1) = upper(c.addressline1)
      and upper(nvl(p.addressline2, '')) = upper(nvl(c.addressline2, ''))
      and upper(p.postalcode) = upper(c.postalcode)
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
  FROM
    ${catalog}.${wh_db}_${scale_factor}_stage.CustomerIncremental c
  JOIN ${catalog}.${wh_db}_${scale_factor}.BatchDate bd ON c.batchid = bd.batchid
  JOIN ${catalog}.${wh_db}_${scale_factor}.TaxRate r_lcl 
    ON c.lcl_tx_id = r_lcl.TX_ID
  JOIN ${catalog}.${wh_db}_${scale_factor}.TaxRate r_nat 
    ON c.nat_tx_id = r_nat.TX_ID
  LEFT JOIN ${catalog}.${wh_db}_${scale_factor}_stage.ProspectIncremental p 
    ON
      p.batchid <= ${batch_id}
      and p.recordbatchid >= ${batch_id}
      and upper(p.lastname) = upper(c.lastname)
      and upper(p.firstname) = upper(c.firstname)
      and upper(p.addressline1) = upper(c.addressline1)
      and upper(nvl(p.addressline2, '')) = upper(nvl(c.addressline2, ''))
      and upper(p.postalcode) = upper(c.postalcode)
  WHERE c.batchid = cast(${batch_id} as int)
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
    nvl(p.agencyid, c.agencyid) agencyid,
    nvl(p.creditrating, c.creditrating) creditrating,
    nvl(p.networth, c.networth) networth,
    nvl(p.marketingnameplate, c.marketingnameplate) marketingnameplate,
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
WHEN MATCHED AND s.sk_customerid is not null THEN UPDATE SET -- These are this batch's customer updates (and potentially some Prospect updates) so apply SCD Type 2
  t.iscurrent = false,
  t.enddate = s.effectivedate
WHEN MATCHED AND s.sk_customerid is null THEN UPDATE SET -- These are ONLY Prospect updates, no new customer records this batch
  t.agencyid = s.agencyid,
  t.creditrating = s.creditrating,
  t.networth = s.networth,
  t.marketingnameplate = s.marketingnameplate
WHEN NOT MATCHED THEN INSERT (sk_customerid, customerid, taxid, status, lastname, firstname, middleinitial, gender, tier, dob, addressline1, addressline2, postalcode, city, stateprov, country, phone1, phone2, phone3, email1, email2, nationaltaxratedesc, nationaltaxrate, localtaxratedesc, localtaxrate, agencyid, creditrating, networth, marketingnameplate, iscurrent, batchid, effectivedate, enddate)
VALUES (sk_customerid, customerid, taxid, status, lastname, firstname, middleinitial, gender, tier, dob, addressline1, addressline2, postalcode, city, stateprov, country, phone1, phone2, phone3, email1, email2, nationaltaxratedesc, nationaltaxrate, localtaxratedesc, localtaxrate, agencyid, creditrating, networth, marketingnameplate, iscurrent, batchid, effectivedate, enddate);
