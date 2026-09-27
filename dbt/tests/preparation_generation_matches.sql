select t.source_month
from {{ source('silver', 'trips') }} t
left join {{ source('ops', 'month_status') }} s using (source_month)
where not (t.preparation_run_id <=> s.run_id)
group by t.source_month
