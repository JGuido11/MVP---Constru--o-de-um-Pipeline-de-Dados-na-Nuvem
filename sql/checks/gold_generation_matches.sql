SELECT t.source_month
FROM {gold}.fct_trips t
LEFT JOIN {ops}.month_status s USING (source_month)
WHERE NOT (t.preparation_run_id <=> s.run_id)
GROUP BY t.source_month
