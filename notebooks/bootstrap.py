# Databricks notebook source
import sys
dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema_prefix", "mobility")
dbutils.widgets.text("project_root", "")
sys.path.insert(0, dbutils.widgets.get("project_root") + "/src")

from mobility.config import PipelineConfig
from mobility.storage import DeltaStore

config = PipelineConfig(dbutils.widgets.get("catalog"), dbutils.widgets.get("schema_prefix"))
DeltaStore(spark, config).bootstrap()
display(spark.table(config.table("ops", "connectivity_probe")))
dbutils.notebook.exit('{"status":"SUCCESS","service":"bootstrap"}')
