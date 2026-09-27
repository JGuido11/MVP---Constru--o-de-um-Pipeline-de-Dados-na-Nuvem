# Databricks notebook source
# Uso manual após preparar todos os meses; o fluxo principal já executa esta lógica.
import json
import sys

dbutils.widgets.text('project_root', '')
dbutils.widgets.text('catalog', 'workspace')
dbutils.widgets.text('schema_prefix', 'mobility')
dbutils.widgets.text('source_months', '2026-01,2026-02,2026-03')
root = dbutils.widgets.get('project_root').strip().rstrip('/')
if not root:
    raise ValueError('Preencha project_root.')
sys.path.insert(0, root + '/src')
from mobility.config import PipelineConfig
from mobility.pipeline import parse_months
from mobility.analytics import GoldBuilder
from mobility.catalog import annotate_catalog
config = PipelineConfig(dbutils.widgets.get('catalog'), dbutils.widgets.get('schema_prefix'))
spark.conf.set('spark.sql.session.timeZone', 'America/New_York')
result = GoldBuilder(spark, config, root).build(parse_months(dbutils.widgets.get('source_months')))
annotate_catalog(spark, config)
print(json.dumps(result, indent=2))
