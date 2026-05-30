select * from {{ source('csv', 'CashTransaction') }}
