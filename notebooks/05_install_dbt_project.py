# Databricks notebook source
# Gerado por scripts/build_dbt_workspace_notebook.py a partir do projeto dbt local.
# DBTITLE 1,Destino dos arquivos dbt no Workspace
dbutils.widgets.text("project_root", "/Workspace/Users/joaopgher@gmail.com/urban_mobility")
dbutils.widgets.dropdown("overwrite", "false", ["false", "true"], "Substituir arquivos diferentes?")

# COMMAND ----------
    
# DBTITLE 1,Arquivos do projeto dbt (SQL e YAML)
PROJECT_FILES = {
    "analyses/business_questions.sql": "-- Run these separately in Databricks SQL after replacing the ref expressions\n-- with the compiled relation names (dbt compile writes target/compiled/...).\nselect pickup_zone_name, pickup_hour, sum(trip_count) as completed_trips\nfrom {{ ref('mart_pickup_activity') }}\ngroup by pickup_zone_name, pickup_hour\norder by completed_trips desc limit 20;\n\nselect *, trip_count - lag(trip_count) over (order by pickup_month) as trip_count_change\nfrom {{ ref('mart_monthly_activity') }}\nwhere pickup_month between '2026-01' and '2026-03'\norder by pickup_month;\n\nselect * from {{ ref('mart_route_duration') }}\nwhere trip_count >= 100\norder by median_duration_minutes desc limit 20;\n",
    "dbt_project.yml": "name: urban_mobility\nversion: '1.0.0'\nconfig-version: 2\nprofile: urban_mobility\nmodel-paths: [models]\ntest-paths: [tests]\nmacro-paths: [macros]\nanalysis-paths: [analyses]\nclean-targets: [target, dbt_packages]\nmodels:\n  urban_mobility:\n    +materialized: table\n    +file_format: delta\n    staging:\n      +materialized: view\n",
    "macros/assert_ready.sql": "{% macro assert_ready(requested_month) %}\n  {% if not modules.re.fullmatch('[0-9]{4}-[0-9]{2}', requested_month) %}\n    {{ exceptions.raise_compiler_error('requested_month must be YYYY-MM') }}\n  {% endif %}\n  {% if execute %}\n    {% set result = run_query(\"select source_month, status from \" ~ source('ops', 'month_status')) %}\n    {% set requested = namespace(found=false) %}\n    {% for row in result.rows %}\n      {% if row[0] == requested_month %}{% set requested.found = true %}{% endif %}\n      {% if row[1] != 'SUCCESS' %}\n        {{ exceptions.raise_compiler_error('Month ' ~ row[0] ~ ' is not ready: ' ~ row[1]) }}\n      {% endif %}\n    {% endfor %}\n    {% if not requested.found %}\n      {{ exceptions.raise_compiler_error('Requested month has not been prepared: ' ~ requested_month) }}\n    {% endif %}\n  {% endif %}\n{% endmacro %}\n",
    "models/marts/dim_zones.sql": "select\n    concat(source_month, ':', cast(zone_id as string)) as zone_key,\n    source_month, zone_id, borough, zone_name, service_zone\nfrom {{ source('silver', 'taxi_zones') }}\n",
    "models/marts/fct_trips.sql": "select\n    t.*,\n    p.zone_name as pickup_zone_name,\n    p.borough as pickup_borough,\n    d.zone_name as dropoff_zone_name,\n    d.borough as dropoff_borough\nfrom {{ ref('stg_trips') }} t\nleft join {{ ref('dim_zones') }} p on t.pickup_zone_key = p.zone_key\nleft join {{ ref('dim_zones') }} d on t.dropoff_zone_key = d.zone_key\n",
    "models/marts/mart_monthly_activity.sql": "-- Calendar month of pickup, not the source file's reporting month.\nselect\n    pickup_month,\n    count(*) as trip_count,\n    coalesce(sum(fare_amount), 0) as recorded_fare_amount,\n    coalesce(sum(total_amount), 0) as recorded_total_amount,\n    sum(case when fare_amount is null then 1 else 0 end) as missing_fare_count,\n    sum(case when has_quality_warning then 1 else 0 end) as unusual_trip_count\nfrom {{ ref('fct_trips') }}\ngroup by pickup_month\n",
    "models/marts/mart_pickup_activity.sql": "select\n    source_month, pickup_zone_id, pickup_zone_name, pickup_hour,\n    count(*) as trip_count,\n    coalesce(sum(fare_amount), 0) as recorded_fare_amount,\n    sum(case when has_quality_warning then 1 else 0 end) as unusual_trip_count\nfrom {{ ref('fct_trips') }}\ngroup by source_month, pickup_zone_id, pickup_zone_name, pickup_hour\n",
    "models/marts/mart_route_duration.sql": "select\n    source_month, pickup_zone_id, dropoff_zone_id,\n    pickup_zone_name, dropoff_zone_name,\n    count(*) as trip_count,\n    avg(duration_minutes) as average_duration_minutes,\n    percentile_approx(duration_minutes, 0.5, 10000) as median_duration_minutes,\n    sum(case when has_quality_warning then 1 else 0 end) as unusual_trip_count\nfrom {{ ref('fct_trips') }}\ngroup by source_month, pickup_zone_id, dropoff_zone_id, pickup_zone_name, dropoff_zone_name\n",
    "models/schema.yml": "version: 2\nmodels:\n  - name: connectivity_check\n    description: End-to-end SQL connectivity gate, run before downloading taxi data.\n    columns:\n      - name: probe_id\n        data_tests: [not_null, unique]\n  - name: dim_zones\n    description: Month-specific zone snapshots; one row per source month and location ID.\n    columns:\n      - name: zone_key\n        data_tests: [not_null, unique]\n      - name: zone_id\n        data_tests: [not_null]\n  - name: stg_trips\n    description: Accepted trips with calendar and composite zone keys.\n    columns:\n      - name: source_month\n        data_tests: [not_null]\n      - name: pickup_at\n        data_tests: [not_null]\n      - name: dropoff_at\n        data_tests: [not_null]\n  - name: fct_trips\n    description: One row per accepted source record. TLC does not supply a reliable unique trip identifier.\n    columns:\n      - name: pickup_zone_key\n        data_tests:\n          - not_null\n          - relationships:\n              arguments:\n                to: ref('dim_zones')\n                field: zone_key\n      - name: dropoff_zone_key\n        data_tests:\n          - not_null\n          - relationships:\n              arguments:\n                to: ref('dim_zones')\n                field: zone_key\n  - name: mart_pickup_activity\n    description: Completed trips and recorded fares by source month, pickup zone, and local hour.\n  - name: mart_monthly_activity\n    description: Activity by actual pickup month, including separately visible out-of-period records.\n    columns:\n      - name: pickup_month\n        data_tests: [unique, not_null]\n  - name: mart_route_duration\n    description: Typical route durations; use at least 100 trips when ranking routes.\n",
    "models/smoke/connectivity_check.sql": "{{ config(tags=['smoke']) }}\nselect probe_id from {{ source('ops', 'connectivity_probe') }}\n",
    "models/sources.yml": "version: 2\nsources:\n  - name: silver\n    database: \"{{ env_var('DATABRICKS_CATALOG', 'workspace') }}\"\n    schema: \"{{ env_var('MOBILITY_SCHEMA_PREFIX', 'mobility') }}_silver\"\n    tables:\n      - name: trips\n        description: Accepted trip records; genuine duplicate records are preserved.\n      - name: rejected_trips\n        description: Invalid records with rejection reason arrays.\n      - name: taxi_zones\n        description: Taxi zone lookup snapshots, keyed by source month and zone ID.\n  - name: bronze\n    database: \"{{ env_var('DATABRICKS_CATALOG', 'workspace') }}\"\n    schema: \"{{ env_var('MOBILITY_SCHEMA_PREFIX', 'mobility') }}_bronze\"\n    tables:\n      - name: yellow_trips\n  - name: ops\n    database: \"{{ env_var('DATABRICKS_CATALOG', 'workspace') }}\"\n    schema: \"{{ env_var('MOBILITY_SCHEMA_PREFIX', 'mobility') }}_ops\"\n    tables:\n      - name: month_status\n      - name: connectivity_probe\n",
    "models/staging/stg_trips.sql": "select\n    *,\n    concat(source_month, ':', cast(pickup_zone_id as string)) as pickup_zone_key,\n    concat(source_month, ':', cast(dropoff_zone_id as string)) as dropoff_zone_key,\n    cast(pickup_at as date) as pickup_date,\n    date_format(pickup_at, 'yyyy-MM') as pickup_month,\n    hour(pickup_at) as pickup_hour,\n    size(quality_warnings) > 0 as has_quality_warning\nfrom {{ source('silver', 'trips') }}\n",
    "tests/aggregate_totals_reconcile.sql": "with expected as (\n    select count(*) as trips, coalesce(sum(fare_amount), 0) as fares from {{ source('silver', 'trips') }}\n), actual as (\n    select 'fact' as model, count(*) as trips, coalesce(sum(fare_amount), 0) as fares from {{ ref('fct_trips') }}\n    union all\n    select 'pickup', coalesce(sum(trip_count), 0), coalesce(sum(recorded_fare_amount), 0) from {{ ref('mart_pickup_activity') }}\n    union all\n    select 'monthly', coalesce(sum(trip_count), 0), coalesce(sum(recorded_fare_amount), 0) from {{ ref('mart_monthly_activity') }}\n    union all\n    select 'route', coalesce(sum(trip_count), 0), (select fares from expected) from {{ ref('mart_route_duration') }}\n)\nselect actual.* from actual cross join expected\nwhere actual.trips <> expected.trips or actual.fares <> expected.fares\n",
    "tests/months_are_ready.sql": "select * from {{ source('ops', 'month_status') }} where status <> 'SUCCESS' or status is null\n",
    "tests/preparation_generation_matches.sql": "select t.source_month\nfrom {{ source('silver', 'trips') }} t\nleft join {{ source('ops', 'month_status') }} s using (source_month)\nwhere not (t.preparation_run_id <=> s.run_id)\ngroup by t.source_month\n",
    "tests/rejections_have_reasons.sql": "select * from {{ source('silver', 'rejected_trips') }}\nwhere rejection_reasons is null or size(rejection_reasons) = 0\n",
    "tests/source_counts_reconcile.sql": "with raw as (\n    select source_month, count(*) as n from {{ source('bronze', 'yellow_trips') }} group by source_month\n), accepted as (\n    select source_month, count(*) as n from {{ source('silver', 'trips') }} group by source_month\n), rejected as (\n    select source_month, count(*) as n from {{ source('silver', 'rejected_trips') }} group by source_month\n), months as (\n    select source_month from raw union select source_month from accepted\n    union select source_month from rejected union select source_month from {{ source('ops', 'month_status') }}\n)\nselect m.source_month\nfrom months m\nleft join raw r using (source_month)\nleft join accepted a using (source_month)\nleft join rejected q using (source_month)\nleft join {{ source('ops', 'month_status') }} s using (source_month)\nwhere r.n is null or s.source_month is null or s.status <> 'SUCCESS'\n   or coalesce(r.n, 0) <> coalesce(a.n, 0) + coalesce(q.n, 0)\n   or not (s.input_count <=> r.n)\n   or not (s.accepted_count <=> coalesce(a.n, 0))\n   or not (s.rejected_count <=> coalesce(q.n, 0))\n",
    "tests/valid_journeys.sql": "select * from {{ ref('fct_trips') }}\nwhere pickup_at is null or dropoff_at is null or duration_minutes < 0 or size(rejection_reasons) <> 0\n"
}

# COMMAND ----------

# DBTITLE 1,Instalar os arquivos no Workspace
from pathlib import Path

project_root = Path(dbutils.widgets.get("project_root").strip())
if not str(project_root).startswith("/Workspace/") or ".." in project_root.parts:
    raise ValueError("project_root deve ser uma pasta do projeto dentro de /Workspace/.")
destination = project_root / "dbt"
overwrite = dbutils.widgets.get("overwrite") == "true"

# A tarefa dbt nativa gera o perfil a partir do SQL Warehouse selecionado.
# O perfil usado pelo runner local não deve ser instalado por este notebook.
if (destination / "profiles.yml").exists():
    raise ValueError(
        "Já existe dbt/profiles.yml. Renomeie esse perfil local para profiles.yml.local "
        "antes de usar a configuração automática do SQL Warehouse."
    )

conflicts = []
for relative, content in PROJECT_FILES.items():
    relative_path = Path(relative)
    if relative_path.is_absolute() or ".." in relative_path.parts:
        raise ValueError(f"Caminho inválido no pacote: {relative}")
    path = destination / relative_path
    if path.exists() and path.read_text(encoding="utf-8") != content:
        conflicts.append(relative)
if conflicts and not overwrite:
    raise ValueError(
        "Existem arquivos diferentes: " + ", ".join(conflicts)
        + ". Confira suas alterações; para substituir, ajuste overwrite=true."
    )

written = 0
for relative, content in PROJECT_FILES.items():
    path = destination / relative
    if not path.exists() or path.read_text(encoding="utf-8") != content:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")
        written += 1

print(f"Projeto dbt: {destination}")
print(f"Arquivos no pacote: {len(PROJECT_FILES)}; escritos nesta execução: {written}")
print("\n".join(sorted(PROJECT_FILES)))
print("Próximo passo: criar uma tarefa dbt com Source=Workspace e selecionar esta pasta.")
