with expected as (
    select count(*) as trips, coalesce(sum(fare_amount), 0) as fares from {{ source('silver', 'trips') }}
), actual as (
    select 'fact' as model, count(*) as trips, coalesce(sum(fare_amount), 0) as fares from {{ ref('fct_trips') }}
    union all
    select 'pickup', coalesce(sum(trip_count), 0), coalesce(sum(recorded_fare_amount), 0) from {{ ref('mart_pickup_activity') }}
    union all
    select 'monthly', coalesce(sum(trip_count), 0), coalesce(sum(recorded_fare_amount), 0) from {{ ref('mart_monthly_activity') }}
    union all
    select 'route', coalesce(sum(trip_count), 0), (select fares from expected) from {{ ref('mart_route_duration') }}
)
select actual.* from actual cross join expected
where actual.trips <> expected.trips or actual.fares <> expected.fares
