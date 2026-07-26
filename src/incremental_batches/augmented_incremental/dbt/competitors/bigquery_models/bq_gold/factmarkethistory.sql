{{
  config(
    materialized = 'incremental',
    on_schema_change = 'ignore',
    full_refresh = false,
    incremental_strategy = 'merge',
    unique_key = ['sk_securityid', 'sk_dateid'],
  )
}}

{# BQ variant of factmarkethistory.

   Translations from Snowflake:
     - object_construct('a', x, 'b', y) -> STRUCT(x AS a, y AS b)
     - obj:field::type                  -> CAST(obj.field AS type)
     - to_char(d, 'YYYYMMDD')           -> FORMAT_DATE('%Y%m%d', d)
     - dateadd(day, -365, d)            -> DATE_SUB(d, INTERVAL 365 DAY)
     - div0(a, b)                       -> SAFE_DIVIDE(a, b)
     - quarter(d) / year(d)             -> EXTRACT(QUARTER FROM d) / EXTRACT(YEAR FROM d)
     - ::number                         -> CAST(... AS INT64)
     - ::float                          -> CAST(... AS FLOAT64)
     - ::date                           -> CAST(... AS DATE)
     - min_by(struct, key) / max_by     -> MIN_BY / MAX_BY (BQ supports both)
     - group by all                     -> BQ supports
#}

with new_dm as (
  select * from {{ ref('bronzedailymarket') }}
  where dm_date = DATE '{{ var("batch_date") }}'
),

sym_min_max as (
  select
    dm_s_symb,
    MIN_BY(STRUCT(dm_low  AS dm_low,  dm_date AS dm_date), dm_low)  as fiftytwoweeklow,
    MAX_BY(STRUCT(dm_high AS dm_high, dm_date AS dm_date), dm_high) as fiftytwoweekhigh
  from {{ ref('bronzedailymarket') }}
  where dm_date > DATE_SUB(DATE '{{ var("batch_date") }}', INTERVAL 365 DAY)
  group by all
)

select
  s.sk_securityid,
  s.sk_companyid,
  CAST(FORMAT_DATE('%Y%m%d', dm.dm_date) AS INT64)                  as sk_dateid,
  SAFE_DIVIDE(dm.dm_close, f.prev_year_basic_eps)                   as peratio,
  SAFE_DIVIDE(s.dividend, dm.dm_close) / 100                         as yield,
  CAST(agg.fiftytwoweekhigh.dm_high AS FLOAT64)                      as fiftytwoweekhigh,
  CAST(FORMAT_DATE('%Y%m%d', CAST(agg.fiftytwoweekhigh.dm_date AS DATE)) AS INT64) as sk_fiftytwoweekhighdate,
  CAST(agg.fiftytwoweeklow.dm_low  AS FLOAT64)                       as fiftytwoweeklow,
  CAST(FORMAT_DATE('%Y%m%d', CAST(agg.fiftytwoweeklow.dm_date AS DATE)) AS INT64)  as sk_fiftytwoweeklowdate,
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
 and EXTRACT(QUARTER FROM dm.dm_date) = EXTRACT(QUARTER FROM f.qtr_start_date)
 and EXTRACT(YEAR    FROM dm.dm_date) = EXTRACT(YEAR    FROM f.qtr_start_date)
