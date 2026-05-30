select * from {{ source('csv', 'WatchHistory') }}
