{{
  config(
    materialized = 'incremental',
    on_schema_change = 'ignore',
    full_refresh = false,
    incremental_strategy = 'merge',
    unique_key = ['sk_securityid', 'sk_dateid'],
  )
}}

{# Snowflake variant — same logical shape as the Databricks version:
   compute per-symbol min_by/max_by(low, high) over the 365-day window
   ending at batch_date, then join today's new bronzedailymarket rows
   to the aggregate + dimsecurity (effective-date) + companyyeareps.

   Translations from Databricks:
     - struct(dm_low, dm_date)   -> object_construct('dm_low', dm_low, 'dm_date', dm_date)
                                     (min_by/max_by accept any comparable type;
                                      we project the date field out via :)
     - date_format(d, 'yyyyMMdd') -> to_char(d, 'YYYYMMDD')
     - date_sub(d, n)             -> dateadd(day, -n, d)
     - try_divide(...)            -> Snowflake has try_divide natively
     - cast as bigint             -> NUMBER (Snowflake uses NUMBER for bigint)
     - group by all               -> Snowflake supports GROUP BY ALL since 2024
   Target table is pre-created in the engine-specific setup with a CLUSTER
   BY (sk_dateid) clause; no clustering declaration here. #}

with new_dm as (
  -- Only today's bronze rows. The factmarkethistory target is keyed by
  -- sk_dateid, so the merge writes a fresh set of (sk_securityid, sk_dateid)
  -- rows for today.
  select * from {{ ref('bronzedailymarket') }}
  where dm_date = cast('{{ var("batch_date") }}' as date)
),

sym_min_max as (
  select
    dm_s_symb,
    min_by(object_construct('dm_low',  dm_low,  'dm_date', dm_date), dm_low)  as fiftytwoweeklow,
    max_by(object_construct('dm_high', dm_high, 'dm_date', dm_date), dm_high) as fiftytwoweekhigh
  from {{ ref('bronzedailymarket') }}
  where dm_date > dateadd(day, -365, cast('{{ var("batch_date") }}' as date))
  group by all
)

select
  s.sk_securityid,
  s.sk_companyid,
  to_char(dm.dm_date, 'YYYYMMDD')::number                          as sk_dateid,
  try_divide(dm.dm_close, f.prev_year_basic_eps)                    as peratio,
  try_divide(s.dividend, dm.dm_close) / 100                          as yield,
  agg.fiftytwoweekhigh:dm_high::float                                as fiftytwoweekhigh,
  to_char(agg.fiftytwoweekhigh:dm_date::date, 'YYYYMMDD')::number    as sk_fiftytwoweekhighdate,
  agg.fiftytwoweeklow:dm_low::float                                  as fiftytwoweeklow,
  to_char(agg.fiftytwoweeklow:dm_date::date,  'YYYYMMDD')::number    as sk_fiftytwoweeklowdate,
  dm.dm_close                                                         as closeprice,
  dm.dm_high                                                          as dayhigh,
  dm.dm_low                                                           as daylow,
  dm.dm_vol                                                           as volume
from new_dm dm
join sym_min_max agg on dm.dm_s_symb = agg.dm_s_symb
join {{ source('run_schema', 'dimsecurity') }} s
  on s.symbol = dm.dm_s_symb
 and dm.dm_date >= s.effectivedate
 and dm.dm_date <  s.enddate
left join {{ source('run_schema', 'companyyeareps') }} f
  on f.sk_companyid = s.sk_companyid
 and quarter(dm.dm_date) = quarter(f.qtr_start_date)
 and year(dm.dm_date)    = year(f.qtr_start_date)
