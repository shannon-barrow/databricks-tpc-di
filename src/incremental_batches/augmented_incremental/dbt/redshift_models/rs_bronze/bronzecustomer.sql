{{
  config(
    materialized = 'view',
  )
}}

{# Redshift variant — the COPY landing table in the {wh_db}_{sf}_bronze
   schema is populated by load_bronze_rs.py BEFORE dbt runs each batch.
   This view is just a pass-through so the dbt DAG retains bronze→silver→gold
   lineage with zero write amplification. #}

select * from {{ source('rs_landing', 'bronzecustomer') }}
