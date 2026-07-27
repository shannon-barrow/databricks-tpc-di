{{
  config(
    partition_by = {
      'field': 'event_dt',
      'data_type': 'date',
      'copy_partitions': true,
    },
  )
}}

select * from {{ source('csv', 'Trade') }}
