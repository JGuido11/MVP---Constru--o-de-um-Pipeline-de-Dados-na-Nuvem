select
    source_month, pickup_zone_id, pickup_zone_name, pickup_hour,
    count(*) as trip_count,
    coalesce(sum(fare_amount), 0) as recorded_fare_amount,
    sum(case when has_quality_warning then 1 else 0 end) as unusual_trip_count
from {gold}.fct_trips
group by source_month, pickup_zone_id, pickup_zone_name, pickup_hour
