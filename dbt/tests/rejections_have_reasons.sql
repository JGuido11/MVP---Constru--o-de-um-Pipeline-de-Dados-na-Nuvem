select * from {{ source('silver', 'rejected_trips') }}
where rejection_reasons is null or size(rejection_reasons) = 0
