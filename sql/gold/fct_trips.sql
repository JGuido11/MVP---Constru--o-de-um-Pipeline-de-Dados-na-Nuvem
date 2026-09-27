select
    t.*,
    p.zone_name as pickup_zone_name,
    p.borough as pickup_borough,
    d.zone_name as dropoff_zone_name,
    d.borough as dropoff_borough
from {gold}.stg_trips t
left join {gold}.dim_zones p on t.pickup_zone_key = p.zone_key
left join {gold}.dim_zones d on t.dropoff_zone_key = d.zone_key
