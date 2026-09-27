select * from {gold}.fct_trips
where pickup_at is null or dropoff_at is null or duration_minutes < 0 or size(rejection_reasons) <> 0
