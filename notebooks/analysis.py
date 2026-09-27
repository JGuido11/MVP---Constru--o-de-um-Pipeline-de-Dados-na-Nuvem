# Databricks notebook source
import sys
dbutils.widgets.text("project_root", "")
dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema_prefix", "mobility")
sys.path.insert(0, dbutils.widgets.get("project_root") + "/src")
from mobility.config import PipelineConfig
config = PipelineConfig(dbutils.widgets.get("catalog"), dbutils.widgets.get("schema_prefix"))
from mobility.analytics import GoldBuilder, require_ready
states = [r.asDict() for r in spark.table(config.table('ops', 'month_status')).collect()]
require_ready(states, ['2026-01', '2026-02', '2026-03'])
builder = GoldBuilder(spark, config, dbutils.widgets.get('project_root'))
for check in ('gold_generation_matches', 'aggregate_totals_reconcile'):
    builder.check(check)

# COMMAND ----------
# P1: zonas e horários com mais registros aceitos nos arquivos carregados.
display(spark.sql(f"""
SELECT pickup_zone_id, pickup_zone_name, pickup_hour, SUM(trip_count) AS completed_trips
FROM {config.table('gold', 'mart_pickup_activity')}
GROUP BY pickup_zone_id, pickup_zone_name, pickup_hour ORDER BY completed_trips DESC LIMIT 20
"""))

# COMMAND ----------
# P2: comparação mensal; registros fora do trimestre são excluídos explicitamente.
display(spark.sql(f"""
SELECT *, trip_count - LAG(trip_count) OVER (ORDER BY pickup_month) AS trip_count_change
FROM {config.table('gold', 'mart_monthly_activity')}
WHERE pickup_month BETWEEN '2026-01' AND '2026-03' ORDER BY pickup_month
"""))

# COMMAND ----------
# P3: mediana por rota/mês, com pelo menos 100 registros aceitos.
display(spark.sql(f"""
SELECT * FROM {config.table('gold', 'mart_route_duration')}
WHERE trip_count >= 100 ORDER BY median_duration_minutes DESC LIMIT 20
"""))

# COMMAND ----------
display(spark.table(config.table("ops", "month_status")))
display(spark.sql(f"""
SELECT source_month, reason, COUNT(*) AS rejected_records_with_reason
FROM {config.table('silver', 'rejected_trips')}
LATERAL VIEW explode(rejection_reasons) e AS reason
GROUP BY source_month, reason ORDER BY source_month, rejected_records_with_reason DESC
"""))
display(spark.sql(f"""
SELECT source_month, warning, COUNT(*) AS accepted_records_with_warning
FROM {config.table('silver', 'trips')}
LATERAL VIEW explode(quality_warnings) e AS warning
GROUP BY source_month, warning ORDER BY source_month, accepted_records_with_warning DESC
"""))
