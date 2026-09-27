{{ config(tags=['smoke']) }}
select probe_id from {{ source('ops', 'connectivity_probe') }}
