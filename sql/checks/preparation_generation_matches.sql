select t.source_month
from {silver}.trips t
left join {ops}.month_status s using (source_month)
where not (t.preparation_run_id <=> s.run_id)
group by t.source_month
