# Databricks notebook source
# DBTITLE 1,Parâmetros da preparação silver
dbutils.widgets.text("source_month", "2026-01", "Mês de origem (YYYY-MM)")
dbutils.widgets.text("catalog", "workspace", "Catálogo")
dbutils.widgets.text("schema_prefix", "mobility", "Prefixo dos schemas")
dbutils.widgets.text(
    "project_root",
    "/Workspace/Users/joaopgher@gmail.com/urban_mobility",
    "Pasta do projeto no Workspace",
)
dbutils.widgets.text("run_id", "", "ID da execução (vazio gera UUID)")

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
from mobility.jobs import TripPreparationJob

source_month = validate_source_month(dbutils.widgets.get("source_month").strip())
run_id = dbutils.widgets.get("run_id").strip() or f"manual-preparation-{uuid4()}"
config = PipelineConfig(
    catalog=dbutils.widgets.get("catalog").strip(),
    prefix=dbutils.widgets.get("schema_prefix").strip(),
)
spark.conf.set("spark.sql.session.timeZone", "America/New_York")

# COMMAND ----------

# DBTITLE 1,Preparar somente a camada silver
# A bronze do mesmo source_month deve ter sido carregada com sucesso antes.
result = TripPreparationJob(spark, config).run(
    month=source_month,
    run_id=run_id,
)
print(json.dumps(result, indent=2))

# COMMAND ----------

# DBTITLE 1,Conferir aceitos, rejeitados e reconciliação
display(
    spark.table(config.table("silver", "trips"))
    .where(F.col("source_month") == source_month)
    .limit(10)
)
display(
    spark.table(config.table("silver", "rejected_trips"))
    .where(F.col("source_month") == source_month)
    .select("source_month", "rejection_reasons", "pickup_at", "dropoff_at")
    .limit(20)
)
display(
    spark.table(config.table("ops", "month_status"))
    .where(F.col("source_month") == source_month)
    .withColumn(
        "reconciliado",
        F.col("input_count") == F.col("accepted_count") + F.col("rejected_count"),
    )
)

# COMMAND ----------

# DBTITLE 1,Retorno para o job
dbutils.notebook.exit(json.dumps(result))
