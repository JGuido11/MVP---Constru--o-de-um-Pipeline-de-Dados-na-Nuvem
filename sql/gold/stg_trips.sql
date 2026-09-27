select
    *,
    concat(source_month, ':', cast(pickup_zone_id as string)) as pickup_zone_key,
    concat(source_month, ':', cast(dropoff_zone_id as string)) as dropoff_zone_key,
    cast(pickup_at as date) as pickup_date,
    date_format(pickup_at, 'yyyy-MM') as pickup_month,
    hour(pickup_at) as pickup_hour,
    size(quality_warnings) > 0 as has_quality_warning
from {silver}.trips
