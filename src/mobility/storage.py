"""Databricks-managed Delta writes and per-month publication state."""
from pyspark.sql import functions as F

from mobility.config import PipelineConfig, source_month


class DeltaStore:
    def __init__(self, spark, config: PipelineConfig):
        self.spark = spark
        self.config = config

    def bootstrap(self):
        for layer in ("bronze", "silver", "gold", "ops"):
            self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.config.schema(layer)}")
        self.spark.sql(f"CREATE VOLUME IF NOT EXISTS {self.config.schema('bronze')}.landing")
        self.spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.config.table('ops', 'run_events')} (
            source_month STRING, run_id STRING, service STRING, status STRING,
            event_time TIMESTAMP, error_type STRING
        ) USING DELTA""")
        self.spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.config.table('ops', 'month_status')} (
            source_month STRING, run_id STRING, status STRING,
            input_count BIGINT, accepted_count BIGINT, rejected_count BIGINT,
            unusual_count BIGINT, updated_at TIMESTAMP
        ) USING DELTA PARTITIONED BY (source_month)""")
        self.spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.config.table('ops', 'connectivity_probe')}
            USING DELTA AS SELECT 1 AS probe_id""")

    def replace_month(self, frame, layer: str, name: str, month: str):
        month = source_month(month)
        (frame.write.format("delta").mode("overwrite")
         .option("replaceWhere", f"source_month = '{month}'")
         .partitionBy("source_month").saveAsTable(self.config.table(layer, name)))

    def read_month(self, layer: str, name: str, month: str):
        return self.spark.table(self.config.table(layer, name)).where(
            F.col("source_month") == source_month(month)
        )

    def event(self, month: str, run_id: str, service: str, status: str, error_type=None):
        frame = self.spark.range(1).select(
            F.lit(source_month(month)).alias("source_month"), F.lit(run_id).alias("run_id"),
            F.lit(service).alias("service"), F.lit(status).alias("status"),
            F.current_timestamp().alias("event_time"),
            F.lit(error_type).cast("string").alias("error_type"),
        )
        frame.write.format("delta").mode("append").saveAsTable(self.config.table('ops', 'run_events'))

    def status(self, month: str, run_id: str, status: str, counts=None):
        counts = counts or {}
        frame = self.spark.range(1).select(
            F.lit(source_month(month)).alias("source_month"), F.lit(run_id).alias("run_id"),
            F.lit(status).alias("status"),
            *[F.lit(counts.get(key)).cast("long").alias(key) for key in
              ("input_count", "accepted_count", "rejected_count", "unusual_count")],
            F.current_timestamp().alias("updated_at"),
        )
        self.replace_month(frame, "ops", "month_status", month)
