{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'sk_customerid',
    merge_update_columns = ['iscurrent', 'enddate'],
    on_schema_change = 'ignore',
    file_format = 'delta',
    full_refresh = false,
  )
}}

{# SCD2 via stock dbt-databricks merge strategy (no custom macro).
   Source emits two rows per changed customer:
     - new_rows: today's event with iscurrent=true, fresh sk_customerid
     - close_rows: matching existing iscurrent row, with iscurrent=false
       and enddate=new effectivedate; preserves the existing sk_customerid
   merge_update_columns=['iscurrent','enddate'] limits what gets touched
   on MATCH (so the close-row UPDATE only flips those two fields). The
   new row's sk_customerid is unique per (customerid, effectivedate) so
   it never matches an existing target row → INSERT. #}

with new_events as (
  select * from {{ ref('bronzecustomer') }}
  where update_dt = cast('{{ var("batch_date") }}' as date)
),

new_rows as (
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
