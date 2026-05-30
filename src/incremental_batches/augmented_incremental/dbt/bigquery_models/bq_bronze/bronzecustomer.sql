{# BQ variant — reads the day's CSV via the wildcard BQ external table set
   up by setup_bq.py. External-table columns are already typed (see
   DATASET_SCHEMAS in setup_bq.py), so no positional cast layer is needed.
   Same `since_last_load` re-run guard as the SF + Databricks variants. #}

select * from {{ source('csv', 'Customer') }}
