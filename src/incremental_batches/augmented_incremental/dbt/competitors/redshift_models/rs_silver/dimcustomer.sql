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

{# Redshift variant — SCD2 via dbt-redshift merge.

   Translations from BQ:
     - FORMAT_DATE('%Y%m%d', d)    -> TO_CHAR(d, 'YYYYMMDD')
     - CAST(x AS STRING)           -> CAST(x AS VARCHAR(MAX))
     - CAST(... AS INT64)          -> CAST(... AS BIGINT)
     - IF(cond, t, f)              -> CASE WHEN cond THEN t ELSE f END
     - DATE '9999-12-31'           -> CAST('9999-12-31' AS DATE)
     - CONCAT(a, b, ...)           -> a || b || ...   (Redshift CONCAT is
                                       only 2-arg; the || operator handles
                                       multi-arg cleanly)
     - COALESCE(a, b)              -> COALESCE(a, b)  (same)
#}

with new_events as (
  select * from {{ ref('bronzecustomer') }}
  where update_dt = CAST('{{ var("batch_date") }}' AS DATE)
),

new_rows as (
  select
    CAST(TO_CHAR(c.update_dt, 'YYYYMMDD') || CAST(c.customerid AS VARCHAR(MAX)) AS BIGINT) as sk_customerid,
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
    CASE WHEN UPPER(c.gender) IN ('M', 'F') THEN UPPER(c.gender) ELSE 'U' END as gender,
    c.tier,
    c.dob,
    c.addressline1,
    c.addressline2,
    c.postalcode,
    c.city,
    c.stateprov,
    c.country,
    CASE WHEN c.c_local_1 IS NOT NULL THEN
      CASE WHEN c.c_ctry_1 IS NOT NULL THEN '+' || c.c_ctry_1 || ' ' ELSE '' END
      || CASE WHEN c.c_area_1 IS NOT NULL THEN '(' || c.c_area_1 || ') ' ELSE '' END
      || c.c_local_1
      || COALESCE(c.c_ext_1, '')
    ELSE c.c_local_1 END as phone1,
    CASE WHEN c.c_local_2 IS NOT NULL THEN
      CASE WHEN c.c_ctry_2 IS NOT NULL THEN '+' || c.c_ctry_2 || ' ' ELSE '' END
      || CASE WHEN c.c_area_2 IS NOT NULL THEN '(' || c.c_area_2 || ') ' ELSE '' END
      || c.c_local_2
      || COALESCE(c.c_ext_2, '')
    ELSE c.c_local_2 END as phone2,
    CASE WHEN c.c_local_3 IS NOT NULL THEN
      CASE WHEN c.c_ctry_3 IS NOT NULL THEN '+' || c.c_ctry_3 || ' ' ELSE '' END
      || CASE WHEN c.c_area_3 IS NOT NULL THEN '(' || c.c_area_3 || ') ' ELSE '' END
      || c.c_local_3
      || COALESCE(c.c_ext_3, '')
    ELSE c.c_local_3 END as phone3,
    c.email1,
    c.email2,
    r_nat.tx_name as nationaltaxratedesc,
    r_nat.tx_rate as nationaltaxrate,
    r_lcl.tx_name as localtaxratedesc,
    r_lcl.tx_rate as localtaxrate,
    c.update_dt as effectivedate,
    CAST('9999-12-31' AS DATE) as enddate,
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
