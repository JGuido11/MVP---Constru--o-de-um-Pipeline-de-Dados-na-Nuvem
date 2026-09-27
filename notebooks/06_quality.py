# Databricks notebook source
# Perfil por atributo bruto, sem alterar ou descartar dados.
import sys
from pyspark.sql import functions as F
from pyspark.sql.types import NumericType, StringType

dbutils.widgets.text('project_root', '')
dbutils.widgets.text('catalog', 'workspace')
dbutils.widgets.text('schema_prefix', 'mobility')
sys.path.insert(0, dbutils.widgets.get('project_root').rstrip('/') + '/src')
from mobility.config import PipelineConfig
config = PipelineConfig(dbutils.widgets.get('catalog'), dbutils.widgets.get('schema_prefix'))
# COMMAND ----------
for name in ('yellow_trips', 'taxi_zones'):
    raw = spark.table(config.table('bronze', name))
    source_columns = [f for f in raw.schema.fields if f.name not in
                      {'source_month', 'source_filename', 'ingested_at', 'ingestion_run_id'}]
    # Perfil por mês e coluna. Coleta apenas agregados; não leva viagens ao driver.
    rows = []
    for field in source_columns:
        col = F.col(field.name)
        missing = col.isNull()
        if isinstance(field.dataType, StringType):
            missing = missing | (F.trim(col) == '')
        if isinstance(field.dataType, NumericType):
            missing = missing | F.isnan(col.cast('double'))
        numeric = isinstance(field.dataType, NumericType)
        metrics = [F.count('*').alias('total'),
                   F.sum(F.when(missing, 1).otherwise(0)).alias('missing'),
                   F.approx_count_distinct(col).alias('distinct_approx'),
                   F.min(col).cast('string').alias('min'),
                   F.max(col).cast('string').alias('max')]
        if numeric:
            metrics.append(F.percentile_approx(F.when(~missing, col), [0.25, 0.5, 0.75], 10000).alias('quartiles'))
        for result in raw.groupBy('source_month').agg(*metrics).collect():
            d = result.asDict()
            rows.append((name, d['source_month'], field.name, field.dataType.simpleString(),
                         d['total'], d['missing'], 100.0*d['missing']/d['total'],
                         d['distinct_approx'], d['min'], d['max'], str(d.get('quartiles', 'N/A'))))
    display(spark.createDataFrame(rows, 'tabela string, mes string, coluna string, tipo string, total long, ausentes long, percentual_ausentes double, distintos_aproximados long, minimo string, maximo string, quartis string'))
    duplicates = raw.groupBy('source_month', *[f.name for f in source_columns]).count().where('count > 1')
    display(duplicates.groupBy('source_month').agg(F.count('*').alias('grupos_identicos'),
            F.sum(F.col('count')-1).alias('linhas_excedentes_identicas')))
    # Categorias efetivas do lookup e dos atributos categóricos da fonte.
    categorical = ('VendorID','RatecodeID','payment_type','store_and_fwd_flag','Borough','service_zone')
    for column in categorical:
        if column in raw.columns:
            display(raw.groupBy('source_month', column).count().orderBy('source_month', column))
# COMMAND ----------
display(spark.sql(f'''SELECT *,
    100.0 * rejected_count / nullif(input_count,0) AS rejected_pct,
    100.0 * unusual_count / nullif(accepted_count,0) AS accepted_warning_pct
FROM {config.table('ops','month_status')} ORDER BY source_month'''))
# COMMAND ----------
# Sensibilidade: valores monetários com/sem alertas monetários e duração extrema.
display(spark.sql(f'''SELECT source_month, count(*) AS accepted,
    sum(fare_amount) AS fare_all,
    sum(CASE WHEN NOT array_contains(quality_warnings,'unusual_fare') THEN fare_amount END) AS fare_without_fare_warning,
    sum(CASE WHEN duration_minutes = 0 THEN 1 ELSE 0 END) AS zero_duration,
    sum(CASE WHEN duration_minutes > 180 THEN 1 ELSE 0 END) AS over_180_minutes,
    percentile_approx(duration_minutes,array(0.5,0.95,0.99)) AS duration_percentiles
FROM {config.table('silver','trips')} GROUP BY source_month'''))
# 180 minutos é triagem exploratória, não exclusão automática nem fraude comprovada.
