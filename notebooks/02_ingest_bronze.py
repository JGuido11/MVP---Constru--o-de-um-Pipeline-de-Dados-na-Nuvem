# Databricks notebook source
# DBTITLE 1,Parâmetros da ingestão
dbutils.widgets.text("source_month", "2026-01", "Mês de origem (YYYY-MM)")
dbutils.widgets.text("catalog", "workspace", "Catálogo")
dbutils.widgets.text("schema_prefix", "mobility", "Prefixo dos schemas")
dbutils.widgets.text(
    "project_root",
    "/Workspace/Users/joaopgher@gmail.com/urban_mobility",
    "Pasta do projeto no Workspace",
)
dbutils.widgets.text("run_id", "", "ID da execução (vazio gera UUID)")
dbutils.widgets.text("source_mode", "volume", "Origem: volume ou download")

# COMMAND ----------

# DBTITLE 1,Configuração e classes
import json
import sys
from uuid import uuid4

from pyspark.sql import functions as F

project_root = dbutils.widgets.get("project_root").strip().rstrip("/")
if not project_root:
    raise ValueError("Preencha project_root com a pasta do projeto no Workspace.")
package_path = f"{project_root}/src"
if package_path not in sys.path:
    sys.path.insert(0, package_path)

from mobility.config import PipelineConfig, source_month as validate_source_month
from mobility.jobs import TripIngestionJob

source_month = validate_source_month(dbutils.widgets.get("source_month").strip())
run_id = dbutils.widgets.get("run_id").strip() or f"manual-ingestion-{uuid4()}"
source_mode = dbutils.widgets.get("source_mode").strip()
if source_mode not in {"volume", "download"}:
    raise ValueError("source_mode deve ser volume ou download.")
config = PipelineConfig(
    catalog=dbutils.widgets.get("catalog").strip(),
    prefix=dbutils.widgets.get("schema_prefix").strip(),
)
spark.conf.set("spark.sql.session.timeZone", "America/New_York")

# COMMAND ----------

# DBTITLE 1,Carregar somente a camada bronze
result = TripIngestionJob(spark, config).run(
    month=source_month,
    run_id=run_id,
    source_mode=source_mode,
)
print(json.dumps(result, indent=2))

# COMMAND ----------

# DBTITLE 1,Conferir somente o mês processado
for name in ("yellow_trips", "taxi_zones"):
    display(
        spark.table(config.table("bronze", name))
        .where(F.col("source_month") == source_month)
        .limit(10)
    )
display(
    spark.table(config.table("ops", "month_status"))
    .where(F.col("source_month") == source_month)
)

# COMMAND ----------

# DBTITLE 1,Retorno para o job
dbutils.notebook.exit(json.dumps(result))
