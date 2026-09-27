-- Run these separately in Databricks SQL after replacing the ref expressions
-- with the compiled relation names (dbt compile writes target/compiled/...).
select pickup_zone_name, pickup_hour, sum(trip_count) as completed_trips
from {{ ref('mart_pickup_activity') }}
group by pickup_zone_name, pickup_hour
order by completed_trips desc limit 20;

select *, trip_count - lag(trip_count) over (order by pickup_month) as trip_count_change
from {{ ref('mart_monthly_activity') }}
where pickup_month between '2026-01' and '2026-03'
order by pickup_month;

select * from {{ ref('mart_route_duration') }}
where trip_count >= 100
order by median_duration_minutes desc limit 20;
