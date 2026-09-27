# Databricks notebook source
# DBTITLE 1,Parâmetros do teste de reprocessamento
# Execute este teste isoladamente, sem outro job escrevendo nas mesmas tabelas.
dbutils.widgets.text("source_month", "2026-01")
dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema_prefix", "mobility")
dbutils.widgets.text("project_root", "/Workspace/Users/joaopgher@gmail.com/urban_mobility")

# COMMAND ----------

# DBTITLE 1,Configuração
import json
import sys
from uuid import uuid4

from pyspark.sql import functions as F

project_root = dbutils.widgets.get("project_root").strip().rstrip("/")
if not project_root:
    raise ValueError("Preencha project_root.")
sys.path.insert(0, f"{project_root}/src")
from mobility.config import PipelineConfig, source_month as validate_source_month
from mobility.jobs import TripIngestionJob, TripPreparationJob

config = PipelineConfig(dbutils.widgets.get("catalog"), dbutils.widgets.get("schema_prefix"))
source_month = validate_source_month(dbutils.widgets.get("source_month").strip())
test_run_id = f"replay-{uuid4()}"
spark.conf.set("spark.sql.session.timeZone", "America/New_York")

# COMMAND ----------

# DBTITLE 1,Capturar e persistir a situação anterior
state = (spark.table(config.table("ops", "month_status"))
         .where(F.col("source_month") == source_month).collect())
if len(state) != 1 or state[0].status != "SUCCESS":
    raise ValueError("Execute bronze e silver com sucesso para este mês antes do teste.")


def snapshot():
    """Contagens de todos os meses e totais monetários, sem metadados de execução."""
    result = {}
    for layer, name in (
        ("bronze", "yellow_trips"), ("bronze", "taxi_zones"),
        ("silver", "trips"), ("silver", "rejected_trips"), ("silver", "taxi_zones"),
    ):
        frame = spark.table(config.table(layer, name))
        aggregates = [F.count("*").alias("row_count")]
        if name in {"yellow_trips", "trips", "rejected_trips"}:
            for column in ("fare_amount", "total_amount"):
                value = F.expr(f"try_cast({column} AS DECIMAL(18,2))")
                aggregates.extend([
                    F.coalesce(F.sum(value), F.lit(0)).alias(f"sum_{column}"),
                    F.count(value).alias(f"populated_{column}"),
                ])
        rows = frame.groupBy("source_month").agg(*aggregates).collect()
        result[f"{layer}.{name}"] = {
            row.source_month: {key: str(value) for key, value in row.asDict().items()
                               if key != "source_month"}
            for row in rows
        }
    return result


def save_evidence(stage, evidence):
    frame = spark.range(1).select(
        F.lit(test_run_id).alias("test_run_id"),
        F.lit(source_month).alias("source_month"),
        F.lit(stage).alias("stage"),
        F.current_timestamp().alias("recorded_at"),
        F.lit(json.dumps(evidence, sort_keys=True)).alias("evidence_json"),
    )
    frame.write.format("delta").mode("append").saveAsTable(
        config.table("ops", "reprocessing_evidence")
    )


before = snapshot()
save_evidence("BEFORE", before)
print("Teste:", test_run_id)
print(json.dumps(before, indent=2, sort_keys=True))

# COMMAND ----------

# DBTITLE 1,Reprocessar e comparar
# Os arquivos no volume devem permanecer iguais. Este teste não baixa novas versões.
try:
    ingestion = TripIngestionJob(spark, config).run(
        month=source_month, run_id=test_run_id, source_mode="volume",
    )
    preparation = TripPreparationJob(spark, config).run(
        month=source_month, run_id=test_run_id,
    )
    after = snapshot()
    save_evidence("AFTER", after)
    changed = [name for name in before if before[name] != after[name]]
    if changed:
        raise AssertionError(f"Contagens ou totais mudaram nas tabelas: {changed}")
    result = {
        "status": "PASS", "test_run_id": test_run_id, "source_month": source_month,
        "checks": ["row_counts_all_months", "fare_and_total_amount_sums", "populated_amount_counts"],
        "preparation": preparation,
    }
    save_evidence("PASS", result)
except Exception as error:
    save_evidence("FAIL", {"error_type": type(error).__name__, "message": str(error)[:1000]})
    raise

print(json.dumps(result, indent=2))
display(
    spark.table(config.table("ops", "reprocessing_evidence"))
    .where(F.col("test_run_id") == test_run_id)
    .orderBy("recorded_at")
)

# COMMAND ----------

# DBTITLE 1,Resultado do teste
# Este teste comprova as métricas comparadas; não é uma comparação de todas as células.
dbutils.notebook.exit(json.dumps(result))
