select * from {{ source('csv', 'Account') }}
