with raw as (
    select source_month, count(*) as n from {{ source('bronze', 'yellow_trips') }} group by source_month
), accepted as (
    select source_month, count(*) as n from {{ source('silver', 'trips') }} group by source_month
), rejected as (
    select source_month, count(*) as n from {{ source('silver', 'rejected_trips') }} group by source_month
), months as (
    select source_month from raw union select source_month from accepted
    union select source_month from rejected union select source_month from {{ source('ops', 'month_status') }}
)
select m.source_month
from months m
left join raw r using (source_month)
left join accepted a using (source_month)
left join rejected q using (source_month)
left join {{ source('ops', 'month_status') }} s using (source_month)
where r.n is null or s.source_month is null or s.status <> 'SUCCESS'
   or coalesce(r.n, 0) <> coalesce(a.n, 0) + coalesce(q.n, 0)
   or not (s.input_count <=> r.n)
   or not (s.accepted_count <=> coalesce(a.n, 0))
   or not (s.rejected_count <=> coalesce(q.n, 0))
