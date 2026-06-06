{{
  config(
    materialized = 'view',
  )
}}

select * from {{ source('rs_landing', 'bronzeholdings') }}
