{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'sk_customerid',
    merge_update_columns = ['iscurrent', 'enddate'],
    on_schema_change = 'ignore',
    full_refresh = false,
  )
}}

{# BQ variant — SCD2 via dbt-bigquery merge.

   Translations from Snowflake:
     - to_char(d, 'YYYYMMDD')  -> FORMAT_DATE('%Y%m%d', d)
     - c.customerid::string    -> CAST(c.customerid AS STRING)
     - ::number(38,0)          -> CAST(... AS INT64)
     - decode(x, a, b, ...)    -> CASE x WHEN a THEN b ... END
     - iff(cond, t, f)         -> IF(cond, t, f)
     - nvl(a, b)               -> COALESCE(a, b)
     - nvl2(x, t, f)           -> IF(x IS NOT NULL, t, f)
     - to_date('9999-12-31')   -> DATE '9999-12-31'
     - cast('xxx' as date)     -> DATE 'xxx'
     - ||                      -> CONCAT() (|| also works in BQ but CONCAT
                                  is clearer for multi-arg)  #}

with new_events as (
  select * from {{ ref('bronzecustomer') }}
  where update_dt = DATE '{{ var("batch_date") }}'
),

new_rows as (
  select
    CAST(CONCAT(FORMAT_DATE('%Y%m%d', c.update_dt), CAST(c.customerid AS STRING)) AS INT64) as sk_customerid,
    c.customerid,
    c.taxid,
    CASE c.status
      WHEN 'ACTV' THEN 'Active'
      WHEN 'CMPT' THEN 'Completed'
      WHEN 'CNCL' THEN 'Canceled'
      WHEN 'PNDG' THEN 'Pending'
      WHEN 'SBMT' THEN 'Submitted'
      WHEN 'INAC' THEN 'Inactive'
    END as status,
    c.lastname,
    c.firstname,
    c.middleinitial,
    IF(UPPER(c.gender) IN ('M', 'F'), UPPER(c.gender), 'U') as gender,
    c.tier,
    c.dob,
    c.addressline1,
    c.addressline2,
    c.postalcode,
    c.city,
    c.stateprov,
    c.country,
    IF(c.c_local_1 IS NOT NULL,
      CONCAT(
        IF(c.c_ctry_1 IS NOT NULL, CONCAT('+', c.c_ctry_1, ' '), ''),
        IF(c.c_area_1 IS NOT NULL, CONCAT('(', c.c_area_1, ') '), ''),
        c.c_local_1,
        COALESCE(c.c_ext_1, '')),
      c.c_local_1) as phone1,
    IF(c.c_local_2 IS NOT NULL,
      CONCAT(
        IF(c.c_ctry_2 IS NOT NULL, CONCAT('+', c.c_ctry_2, ' '), ''),
        IF(c.c_area_2 IS NOT NULL, CONCAT('(', c.c_area_2, ') '), ''),
        c.c_local_2,
        COALESCE(c.c_ext_2, '')),
      c.c_local_2) as phone2,
    IF(c.c_local_3 IS NOT NULL,
      CONCAT(
        IF(c.c_ctry_3 IS NOT NULL, CONCAT('+', c.c_ctry_3, ' '), ''),
        IF(c.c_area_3 IS NOT NULL, CONCAT('(', c.c_area_3, ') '), ''),
        c.c_local_3,
        COALESCE(c.c_ext_3, '')),
      c.c_local_3) as phone3,
    c.email1,
    c.email2,
    r_nat.tx_name as nationaltaxratedesc,
    r_nat.tx_rate as nationaltaxrate,
    r_lcl.tx_name as localtaxratedesc,
    r_lcl.tx_rate as localtaxrate,
    c.update_dt as effectivedate,
    DATE '9999-12-31' as enddate,
    true as iscurrent
  from new_events c
  join {{ source('run_schema', 'taxrate') }} r_lcl on c.lcl_tx_id = r_lcl.tx_id
  join {{ source('run_schema', 'taxrate') }} r_nat on c.nat_tx_id = r_nat.tx_id
)

{% if is_incremental() %},

close_rows as (
  select
    t.sk_customerid,
    t.customerid, t.taxid, t.status,
    t.lastname, t.firstname, t.middleinitial, t.gender,
    t.tier, t.dob,
    t.addressline1, t.addressline2, t.postalcode, t.city, t.stateprov, t.country,
    t.phone1, t.phone2, t.phone3, t.email1, t.email2,
    t.nationaltaxratedesc, t.nationaltaxrate,
    t.localtaxratedesc, t.localtaxrate,
    t.effectivedate,
    n.effectivedate as enddate,
    false as iscurrent
  from {{ this }} t
  join new_rows n on t.customerid = n.customerid
  where t.iscurrent
)

select * from new_rows
union all
select * from close_rows
{% else %}
select * from new_rows
{% endif %}
