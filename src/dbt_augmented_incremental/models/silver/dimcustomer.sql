{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'scd2',
    unique_key = 'customerid',
    on_schema_change = 'ignore',
    file_format = 'delta',
    full_refresh = false,
  )
}}

{# Per-batch SCD2 build for DimCustomer. Reads new bronzecustomer rows
   since the last successful run, joins to TaxRate, emits one row per
   customer event. The scd2 incremental strategy macro generates the
   close-and-insert MERGE. #}

with new_events as (
  select * from {{ ref('bronzecustomer') }}
  {% if is_incremental() %}
  where update_dt > coalesce(
        (select max(effectivedate) from {{ this }}),
        cast('1900-01-01' as date)
      )
  {% endif %}
)

select
  cast(concat(date_format(c.update_dt, 'yyyyMMdd'), c.customerid) as bigint) as sk_customerid,
  c.customerid,
  c.taxid,
  decode(c.status,
    'ACTV', 'Active',
    'CMPT', 'Completed',
    'CNCL', 'Canceled',
    'PNDG', 'Pending',
    'SBMT', 'Submitted',
    'INAC', 'Inactive') as status,
  c.lastname,
  c.firstname,
  c.middleinitial,
  if(upper(c.gender) in ('M', 'F'), upper(c.gender), 'U') as gender,
  c.tier,
  c.dob,
  c.addressline1,
  c.addressline2,
  c.postalcode,
  c.city,
  c.stateprov,
  c.country,
  nvl2(c.c_local_1,
    concat(
      nvl2(c.c_ctry_1, '+' || c.c_ctry_1 || ' ', ''),
      nvl2(c.c_area_1, '(' || c.c_area_1 || ') ', ''),
      c.c_local_1,
      nvl(c.c_ext_1, '')),
    c.c_local_1) as phone1,
  nvl2(c.c_local_2,
    concat(
      nvl2(c.c_ctry_2, '+' || c.c_ctry_2 || ' ', ''),
      nvl2(c.c_area_2, '(' || c.c_area_2 || ') ', ''),
      c.c_local_2,
      nvl(c.c_ext_2, '')),
    c.c_local_2) as phone2,
  nvl2(c.c_local_3,
    concat(
      nvl2(c.c_ctry_3, '+' || c.c_ctry_3 || ' ', ''),
      nvl2(c.c_area_3, '(' || c.c_area_3 || ') ', ''),
      c.c_local_3,
      nvl(c.c_ext_3, '')),
    c.c_local_3) as phone3,
  c.email1,
  c.email2,
  r_nat.tx_name as nationaltaxratedesc,
  r_nat.tx_rate as nationaltaxrate,
  r_lcl.tx_name as localtaxratedesc,
  r_lcl.tx_rate as localtaxrate,
  c.update_dt as effectivedate,
  cast('9999-12-31' as date) as enddate,
  true as iscurrent
from new_events c
join {{ source('run_schema', 'taxrate') }} r_lcl on c.lcl_tx_id = r_lcl.tx_id
join {{ source('run_schema', 'taxrate') }} r_nat on c.nat_tx_id = r_nat.tx_id
