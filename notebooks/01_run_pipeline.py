# Databricks notebook source
# Fluxo principal: execute pelo Job único ou manualmente, sem sobrepor execuções.
import json
import sys
from uuid import uuid4

dbutils.widgets.text('project_root', '')
dbutils.widgets.text('catalog', 'workspace')
dbutils.widgets.text('schema_prefix', 'mobility')
dbutils.widgets.text('source_months', '2026-01,2026-02,2026-03')
dbutils.widgets.text('source_mode', 'volume')
dbutils.widgets.text('run_id', '')
root = dbutils.widgets.get('project_root').strip().rstrip('/')
if not root:
    raise ValueError('Preencha project_root com o caminho da pasta Git no Workspace.')
sys.path.insert(0, root + '/src')
from mobility.config import PipelineConfig
from mobility.pipeline import parse_months, run_pipeline
config = PipelineConfig(dbutils.widgets.get('catalog'), dbutils.widgets.get('schema_prefix'))
spark.conf.set('spark.sql.session.timeZone', 'America/New_York')
result = run_pipeline(
    spark, config, root, parse_months(dbutils.widgets.get('source_months')),
    dbutils.widgets.get('run_id').strip() or f'manual-{uuid4()}',
    dbutils.widgets.get('source_mode'),
)
# COMMAND ----------
display(spark.table(config.table('ops', 'month_status')))
print(json.dumps(result, indent=2))
dbutils.notebook.exit(json.dumps(result))
