{{
  config(
    materialized = "table"
  )
}}

SELECT * EXCLUDE(Value) FROM {{source('tpcdi', 'DimDateRaw') }}