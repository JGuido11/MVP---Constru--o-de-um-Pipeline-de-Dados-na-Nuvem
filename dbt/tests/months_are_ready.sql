select * from {{ source('ops', 'month_status') }} where status <> 'SUCCESS' or status is null
