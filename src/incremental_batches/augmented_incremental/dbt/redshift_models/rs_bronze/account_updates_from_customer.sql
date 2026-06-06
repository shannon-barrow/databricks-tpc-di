{{
  config(
    materialized = 'view',
  )
}}

select * from {{ source('rs_landing', 'account_updates_from_customer') }}
