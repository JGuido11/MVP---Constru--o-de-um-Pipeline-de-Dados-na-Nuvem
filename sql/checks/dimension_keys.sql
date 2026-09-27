SELECT zone_key FROM {gold}.dim_zones
GROUP BY zone_key HAVING zone_key IS NULL OR count(*) <> 1
