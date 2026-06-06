{{
  config(
    materialized = 'incremental',
    on_schema_change = 'ignore',
    full_refresh = false,
    incremental_strategy = 'merge',
    unique_key = ['sk_securityid', 'sk_dateid'],
  )
}}

{# Redshift variant of factmarkethistory.

   Translations from BQ:
     - MIN_BY(STRUCT(dm_low, dm_date), dm_low) -> rewrite with
                                                  ROW_NUMBER() OVER
                                                  (PARTITION BY dm_s_symb
                                                  ORDER BY dm_low ASC) = 1
                                                  in a subquery, then
                                                  select the dm_low/dm_date
                                                  scalars directly. Same for
                                                  MAX_BY (dm_high DESC).
                                                  Redshift has neither
                                                  MAX_BY/MIN_BY nor STRUCT.
     - STRUCT field access (agg.x.y)            -> flattened to scalar columns
                                                  fiftytwoweeklow_val /
                                                  fiftytwoweeklow_date etc.
     - FORMAT_DATE('%Y%m%d', d)                 -> TO_CHAR(d, 'YYYYMMDD')
     - DATE_SUB(d, INTERVAL n DAY)              -> DATEADD(day, -n, d)
     - SAFE_DIVIDE(a, b)                        -> a / NULLIF(b, 0)
                                                  (NULL on /0, matches
                                                  SAFE_DIVIDE semantics)
     - EXTRACT(QUARTER FROM d) /
       EXTRACT(YEAR FROM d)                     -> same (Redshift supports
                                                  EXTRACT)
     - CAST(... AS INT64)                       -> CAST(... AS BIGINT)
     - CAST(... AS FLOAT64)                     -> CAST(... AS DOUBLE PRECISION)
     - CAST(... AS DATE)                        -> same
     - GROUP BY ALL                             -> spell columns out
#}

with new_dm as (
  -- Only today's bronze rows.
  select * from {{ ref('bronzedailymarket') }}
  where dm_date = CAST('{{ var("batch_date") }}' AS DATE)
),

-- Per-symbol low/high tracking over the 365-day window.
-- Redshift can't return struct-valued aggregates, so we use ROW_NUMBER
-- twice: once over (dm_low ASC) to find the min-low row, once over
-- (dm_high DESC) to find the max-high row.
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
  -- Collapse the two rn=1 rows per symbol into one row of scalars.
  -- MIN/MAX in CASE acts as a NULL-skipping pivot — exactly one row per
  -- (dm_s_symb, rn=1 axis) is non-null, the others are NULL.
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
  CAST(TO_CHAR(dm.dm_date, 'YYYYMMDD') AS BIGINT)                              as sk_dateid,
  dm.dm_close / NULLIF(f.prev_year_basic_eps, 0)                               as peratio,
  (s.dividend / NULLIF(dm.dm_close, 0)) / 100                                  as yield,
  CAST(agg.fiftytwoweekhigh_val AS DOUBLE PRECISION)                           as fiftytwoweekhigh,
  CAST(TO_CHAR(CAST(agg.fiftytwoweekhigh_date AS DATE), 'YYYYMMDD') AS BIGINT) as sk_fiftytwoweekhighdate,
  CAST(agg.fiftytwoweeklow_val  AS DOUBLE PRECISION)                           as fiftytwoweeklow,
  CAST(TO_CHAR(CAST(agg.fiftytwoweeklow_date  AS DATE), 'YYYYMMDD') AS BIGINT) as sk_fiftytwoweeklowdate,
  dm.dm_close                                                                   as closeprice,
  dm.dm_high                                                                    as dayhigh,
  dm.dm_low                                                                     as daylow,
  dm.dm_vol                                                                     as volume
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
