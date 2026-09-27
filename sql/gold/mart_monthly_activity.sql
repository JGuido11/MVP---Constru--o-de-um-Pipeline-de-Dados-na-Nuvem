-- Calendar month of pickup, not the source file's reporting month.
select
    pickup_month,
    count(*) as trip_count,
    coalesce(sum(fare_amount), 0) as recorded_fare_amount,
    coalesce(sum(total_amount), 0) as recorded_total_amount,
    sum(case when fare_amount is null then 1 else 0 end) as missing_fare_count,
    sum(case when has_quality_warning then 1 else 0 end) as unusual_trip_count
from {gold}.fct_trips
group by pickup_month
