SELECT t.pickup_zone_key FROM {gold}.fct_trips t
LEFT JOIN {gold}.dim_zones p ON t.pickup_zone_key = p.zone_key
LEFT JOIN {gold}.dim_zones d ON t.dropoff_zone_key = d.zone_key
WHERE p.zone_key IS NULL OR d.zone_key IS NULL
