select
    concat(source_month, ':', cast(zone_id as string)) as zone_key,
    source_month, zone_id, borough, zone_name, service_zone
from {{ source('silver', 'taxi_zones') }}
