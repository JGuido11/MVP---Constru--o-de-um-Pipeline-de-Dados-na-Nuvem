{% macro assert_ready(requested_month) %}
  {% if not modules.re.fullmatch('[0-9]{4}-[0-9]{2}', requested_month) %}
    {{ exceptions.raise_compiler_error('requested_month must be YYYY-MM') }}
  {% endif %}
  {% if execute %}
    {% set result = run_query("select source_month, status from " ~ source('ops', 'month_status')) %}
    {% set requested = namespace(found=false) %}
    {% for row in result.rows %}
      {% if row[0] == requested_month %}{% set requested.found = true %}{% endif %}
      {% if row[1] != 'SUCCESS' %}
        {{ exceptions.raise_compiler_error('Month ' ~ row[0] ~ ' is not ready: ' ~ row[1]) }}
      {% endif %}
    {% endfor %}
    {% if not requested.found %}
      {{ exceptions.raise_compiler_error('Requested month has not been prepared: ' ~ requested_month) }}
    {% endif %}
  {% endif %}
{% endmacro %}
