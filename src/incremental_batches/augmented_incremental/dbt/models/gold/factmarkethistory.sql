{{
  config(
    materialized = 'incremental',
    on_schema_change = 'ignore',
    file_format = 'delta',
    full_refresh = false,
  )
}}

{# `merge` (not `insert_overwrite`) so the model isn't tied to partition
   replacement semantics — Liquid tables have no partitions. Unique key
   is composite (sk_securityid, sk_dateid) — one row per (security, date).
   No `liquid_clustered_by` here on purpose — the table is pre-created in
   setup_dbt.py with CLUSTER BY (sk_dateid). Declaring it in dbt config
   would force per-batch ALTER TABLE CLUSTER BY. #}
{{ config(
    incremental_strategy='merge',
    unique_key=['sk_securityid','sk_dateid'],
) }}

{# Daily market history with rolling 365-day high/low. Each batch:
   1. Compute per-symbol min_by/max_by(low, high) over the 365 days
      ending at batch_date. (The min_by/max_by aggregate is computed
      against ALL of bronzedailymarket — we cannot incrementalize the
      365-day window cheaply, so we recompute it each batch.)
   2. Join the day's new bronzedailymarket rows to the aggregate +
      dimsecurity (effective-date join) + companyyeareps (left).
   3. MERGE on composite key (sk_securityid, sk_dateid) inserts new
      (security, date) rows; existing rows for prior days are untouched. #}

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
    min_by(struct(dm_low, dm_date), dm_low) as fiftytwoweeklow,
    max_by(struct(dm_high, dm_date), dm_high) as fiftytwoweekhigh
  from {{ ref('bronzedailymarket') }}
  where dm_date > date_sub(cast('{{ var("batch_date") }}' as date), 365)
  group by all
)

select
  s.sk_securityid,
  s.sk_companyid,
  cast(date_format(dm.dm_date, 'yyyyMMdd') as bigint) as sk_dateid,
  try_divide(dm.dm_close, f.prev_year_basic_eps) as peratio,
  try_divide(s.dividend, dm.dm_close) / 100 as yield,
  agg.fiftytwoweekhigh.dm_high as fiftytwoweekhigh,
  cast(date_format(agg.fiftytwoweekhigh.dm_date, 'yyyyMMdd') as bigint) as sk_fiftytwoweekhighdate,
  agg.fiftytwoweeklow.dm_low as fiftytwoweeklow,
  cast(date_format(agg.fiftytwoweeklow.dm_date, 'yyyyMMdd') as bigint) as sk_fiftytwoweeklowdate,
  dm.dm_close as closeprice,
  dm.dm_high as dayhigh,
  dm.dm_low as daylow,
  dm.dm_vol as volume
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
