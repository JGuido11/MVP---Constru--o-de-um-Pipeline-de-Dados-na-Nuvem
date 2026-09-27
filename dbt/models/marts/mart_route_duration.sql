select
    source_month, pickup_zone_id, dropoff_zone_id,
    pickup_zone_name, dropoff_zone_name,
    count(*) as trip_count,
    avg(duration_minutes) as average_duration_minutes,
    percentile_approx(duration_minutes, 0.5, 10000) as median_duration_minutes,
    sum(case when has_quality_warning then 1 else 0 end) as unusual_trip_count
from {{ ref('fct_trips') }}
group by source_month, pickup_zone_id, dropoff_zone_id, pickup_zone_name, dropoff_zone_name
