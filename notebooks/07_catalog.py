# Databricks notebook source
import sys
dbutils.widgets.text('project_root', '')
dbutils.widgets.text('catalog', 'workspace')
dbutils.widgets.text('schema_prefix', 'mobility')
sys.path.insert(0, dbutils.widgets.get('project_root').rstrip('/') + '/src')
from mobility.config import PipelineConfig
from mobility.catalog import definitions, field_definition
config = PipelineConfig(dbutils.widgets.get('catalog'), dbutils.widgets.get('schema_prefix'))
metadata = definitions()
rows = []
for qualified in metadata['tables']:
    layer, name = qualified.split('.')
    full = config.table(layer, name)
    if spark.catalog.tableExists(full):
        for field in spark.table(full).schema.fields:
            info = field_definition(metadata, field.name) or {}
            rows.append((full, field.name, field.dataType.simpleString(), field.nullable,
                         info.get('description', 'PENDENTE: coluna nova'),
                         info.get('domain', 'PENDENTE'), info.get('lineage', 'PENDENTE')))
if rows:
    display(spark.createDataFrame(rows, 'tabela string, coluna string, tipo string, nullable boolean, descricao string, dominio string, linhagem string'))
else:
    raise ValueError('Execute o pipeline antes de consultar o catálogo.')
