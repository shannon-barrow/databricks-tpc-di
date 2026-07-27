{{
  config(
    partition_by = {
      'field': 'update_dt',
      'data_type': 'date',
      'copy_partitions': true,
    },
  )
}}

select * from {{ source('csv', 'Account') }}
