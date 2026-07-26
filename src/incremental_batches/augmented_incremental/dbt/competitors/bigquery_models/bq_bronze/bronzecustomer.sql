{{
  config(
    partition_by = {
      'field': 'update_dt',
      'data_type': 'date',
      'copy_partitions': true,
    },
  )
}}

{# BQ variant — reads the day's CSV via the wildcard BQ external table set
   up by setup_bq.py. External-table columns are already typed (see
   DATASET_SCHEMAS in setup_bq.py), so no positional cast layer is needed.
   insert_overwrite + partition_by update_dt + copy_partitions does a
   metadata-only swap of today's partition — same write-amp as SF/DBX
   append. #}

select * from {{ source('csv', 'Customer') }}
