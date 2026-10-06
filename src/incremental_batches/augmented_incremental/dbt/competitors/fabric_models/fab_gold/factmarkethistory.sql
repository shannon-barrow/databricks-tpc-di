{{
  config(
    materialized = 'incremental',
    on_schema_change = 'ignore',
    full_refresh = false,
    incremental_strategy = 'merge',
    unique_key = ['sk_securityid', 'sk_dateid'],
  )
}}

{# Redshift -> Fabric T-SQL — daily market data with rolling 365-day high/low.
   Redshift and Fabric DW both lack MIN_BY/MAX_BY/STRUCT, so the two-ROW_NUMBER
   rewrite (rn_low over dm_low ASC, rn_high over dm_high DESC) + MIN(CASE WHEN
   rn=1 ...) pivot to flat scalars carries over unchanged.
     - TO_CHAR(d, 'YYYYMMDD')      -> CONVERT(CHAR(8), d, 112)
     - DOUBLE PRECISION            -> FLOAT
     - EXTRACT(QUARTER FROM d)     -> DATEPART(QUARTER, d)
     - EXTRACT(YEAR FROM d)        -> YEAR(d)
     - DATEADD(day, -365, d)       -> unchanged (identical in T-SQL)
     - a / NULLIF(b, 0)            -> unchanged
#}

with new_dm as (
  select * from {{ ref('bronzedailymarket') }}
  where dm_date = CAST('{{ var("batch_date") }}' AS DATE)
),

window_rows as (
  select
    dm_s_symb,
    dm_low,
    dm_high,
    dm_date,
    row_number() over (partition by dm_s_symb order by dm_low  asc)  as rn_low,
    row_number() over (partition by dm_s_symb order by dm_high desc) as rn_high
  from {{ ref('bronzedailymarket') }}
  where dm_date > DATEADD(day, -365, CAST('{{ var("batch_date") }}' AS DATE))
),

sym_min_max as (
  select
    dm_s_symb,
    MIN(CASE WHEN rn_low  = 1 THEN dm_low   END) as fiftytwoweeklow_val,
    MIN(CASE WHEN rn_low  = 1 THEN dm_date  END) as fiftytwoweeklow_date,
    MIN(CASE WHEN rn_high = 1 THEN dm_high  END) as fiftytwoweekhigh_val,
    MIN(CASE WHEN rn_high = 1 THEN dm_date  END) as fiftytwoweekhigh_date
  from window_rows
  where rn_low = 1 or rn_high = 1
  group by dm_s_symb
)

select
  s.sk_securityid,
  s.sk_companyid,
  CAST(CONVERT(CHAR(8), dm.dm_date, 112) AS BIGINT)                               as sk_dateid,
  dm.dm_close / NULLIF(f.prev_year_basic_eps, 0)                                   as peratio,
  (s.dividend / NULLIF(dm.dm_close, 0)) / 100                                      as yield,
  CAST(agg.fiftytwoweekhigh_val AS FLOAT)                                          as fiftytwoweekhigh,
  CAST(CONVERT(CHAR(8), CAST(agg.fiftytwoweekhigh_date AS DATE), 112) AS BIGINT)   as sk_fiftytwoweekhighdate,
  CAST(agg.fiftytwoweeklow_val  AS FLOAT)                                          as fiftytwoweeklow,
  CAST(CONVERT(CHAR(8), CAST(agg.fiftytwoweeklow_date  AS DATE), 112) AS BIGINT)   as sk_fiftytwoweeklowdate,
  dm.dm_close                                                                       as closeprice,
  dm.dm_high                                                                        as dayhigh,
  dm.dm_low                                                                         as daylow,
  dm.dm_vol                                                                         as volume
from new_dm dm
join sym_min_max agg on dm.dm_s_symb = agg.dm_s_symb
join {{ source('run_schema', 'dimsecurity') }} s
  on s.symbol = dm.dm_s_symb
 and dm.dm_date >= s.effectivedate
 and dm.dm_date <  s.enddate
left join {{ source('run_schema', 'companyyeareps') }} f
  on f.sk_companyid = s.sk_companyid
 and DATEPART(QUARTER, dm.dm_date) = DATEPART(QUARTER, f.qtr_start_date)
 and YEAR(dm.dm_date)              = YEAR(f.qtr_start_date)
