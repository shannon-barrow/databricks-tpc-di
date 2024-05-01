-- Databricks notebook source
-- DBTITLE 1,In a "real" job this would prob be a merge but here we are only dealing with 3 total days
INSERT OVERWRITE ${catalog}.${wh_db}_${scale_factor}.Prospect
with p as (
  SELECT 
    * except(recordbatchid),
    least(recordbatchid, cast(${batch_id} as int)) recordbatchid
  FROM ${catalog}.${wh_db}_${scale_factor}_stage.ProspectIncremental
  WHERE batchid <= cast(${batch_id} as int)
  QUALIFY ROW_NUMBER() OVER (PARTITION BY agencyid ORDER BY batchid DESC) = 1
)
SELECT 
  agencyid,
  bigint(date_format(recdate.batchdate, 'yyyyMMdd')) sk_recorddateid,
  bigint(date_format(origdate.batchdate, 'yyyyMMdd')) sk_updatedateid,
  p.batchid,
  nvl2(c.customerid, True, False) iscustomer, 
  p.lastname,
  p.firstname,
  p.middleinitial,
  p.gender,
  p.addressline1,
  p.addressline2,
  p.postalcode,
  city,
  state,
  country,
  phone,
  income,
  numbercars,
  numberchildren,
  maritalstatus,
  age,
  creditrating,
  ownorrentflag,
  employer,
  numbercreditcards,
  networth,
  marketingnameplate
FROM p
JOIN ${catalog}.${wh_db}_${scale_factor}.BatchDate recdate
  ON p.recordbatchid = recdate.batchid
JOIN ${catalog}.${wh_db}_${scale_factor}.BatchDate origdate
  ON p.batchid = origdate.batchid
LEFT JOIN (
  SELECT 
    customerid,
    lastname,
    firstname,
    addressline1,
    addressline2,
    postalcode
  FROM ${catalog}.${wh_db}_${scale_factor}.DimCustomer
  WHERE iscurrent) c
  ON 
    upper(p.LastName) = upper(c.lastname)
    and upper(p.FirstName) = upper(c.firstname)
    and upper(p.AddressLine1) = upper(c.addressline1)
    and upper(nvl(p.addressline2, '')) = upper(nvl(c.addressline2, ''))
    and upper(p.PostalCode) = upper(c.postalcode)

-- COMMAND ----------

MERGE INTO ${catalog}.${wh_db}_${scale_factor}.DimCustomer t USING (
  SELECT 
    lastname,
    firstname,
    addressline1,
    addressline2,
    postalcode,
    p.agencyid,
    p.creditrating,
    p.networth,
    p.marketingnameplate
  FROM ${catalog}.${wh_db}_${scale_factor}_stage.ProspectIncremental p
  WHERE batchid = cast(${batch_id} as int)
) s 
  ON 
    t.iscurrent
    and upper(s.lastname) = upper(t.lastname)
    and upper(s.firstname) = upper(t.firstname)
    and upper(s.addressline1) = upper(t.addressline1)
    and upper(nvl(s.addressline2, '')) = upper(nvl(t.addressline2, ''))
    and upper(s.postalcode) = upper(t.postalcode)
WHEN MATCHED THEN UPDATE SET
  t.agencyid = s.agencyid,
  t.creditrating = s.creditrating,
  t.networth = s.networth,
  t.marketingnameplate = s.marketingnameplate
