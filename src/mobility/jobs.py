"""Separate ingestion and preparation entry points with recoverable month writes."""
import logging
from pathlib import Path

from pyspark.sql import functions as F

from mobility.config import PipelineConfig, source_month
from mobility.source import TripSourceReader
from mobility.storage import DeltaStore
from mobility.validation import REQUIRED_COLUMNS, TripValidator

LOG = logging.getLogger(__name__)


class BatchJob:
    service = "base"

    def __init__(self, spark, config: PipelineConfig, store=None):
        self.spark = spark
        self.config = config
        self.store = store if store is not None else DeltaStore(spark, config)

    def run(self, month: str, run_id: str, **kwargs):
        source_month(month)
        if not run_id or len(run_id) > 256:
            raise ValueError("run_id must contain 1–256 characters")
        # Invalidate publication before touching bronze or silver. Failed attempts
        # never leave a month marked ready for gold.
        self.store.status(month, run_id, "RUNNING")
        try:
            self.store.event(month, run_id, self.service, "STARTED")
            result = self.execute(month, run_id, **kwargs)
            self.store.event(month, run_id, self.service, "SUCCESS")
            return result
        except Exception as error:
            try:
                self.store.status(month, run_id, "FAILED")
                self.store.event(month, run_id, self.service, "FAILED", type(error).__name__)
            except Exception:
                LOG.exception("Could not record failure; inspect the Databricks task log")
            raise


class TripIngestionJob(BatchJob):
    service = "ingestion"

    def execute(self, month: str, run_id: str, source_mode="volume"):
        directory = Path(self.config.volume) / month
        reader = TripSourceReader()
        if source_mode == "download":
            reader.download_month(month, directory)
        elif source_mode != "volume":
            raise ValueError("source_mode must be volume or download")
        trip_path = directory / reader.trip_filename(month)
        zone_path = directory / "taxi_zone_lookup.csv"
        raw = self.spark.read.parquet(str(trip_path))
        missing = REQUIRED_COLUMNS - set(raw.columns)
        if missing:
            raise ValueError(f"Source schema missing: {sorted(missing)}")
        if raw.limit(1).count() == 0:
            raise ValueError("Empty trip dataset")
        raw = (raw.withColumn("source_month", F.lit(month))
               .withColumn("source_filename", F.lit(trip_path.name))
               .withColumn("ingested_at", F.current_timestamp())
               .withColumn("ingestion_run_id", F.lit(run_id)))
        # A lookup snapshot per source month keeps replay independent of later downloads.
        raw_zones = self.spark.read.option("header", True).option("mode", "FAILFAST").csv(str(zone_path))
        zones = raw_zones.select(
            F.expr("try_cast(LocationID AS INT)").alias("zone_id"),
            F.col("Borough").alias("borough"), F.col("Zone").alias("zone_name"),
            F.col("service_zone"), F.lit(month).alias("source_month"),
        )
        if zones.limit(1).count() == 0:
            raise ValueError("Empty zone lookup")
        if zones.where("zone_id IS NULL").limit(1).count() or zones.groupBy("zone_id").count().where("count > 1").limit(1).count():
            raise ValueError("Zone IDs must be populated and unique")
        raw_zones = (raw_zones.withColumn("source_month", F.lit(month))
                     .withColumn("source_filename", F.lit(zone_path.name))
                     .withColumn("ingested_at", F.current_timestamp())
                     .withColumn("ingestion_run_id", F.lit(run_id)))
        self.store.replace_month(raw_zones, "bronze", "taxi_zones", month)
        self.store.replace_month(raw, "bronze", "yellow_trips", month)
        count = self.store.read_month("bronze", "yellow_trips", month).count()
        self.store.status(month, run_id, "INGESTED", {"input_count": count})
        return {"source_month": month, "input_count": count, "status": "INGESTED"}


class TripPreparationJob(BatchJob):
    service = "preparation"

    def execute(self, month: str, run_id: str):
        raw = self.store.read_month("bronze", "yellow_trips", month)
        zones = self.store.read_month("bronze", "taxi_zones", month).select(
            F.col("LocationID").cast("int").alias("zone_id"),
            F.col("Borough").alias("borough"), F.col("Zone").alias("zone_name"),
            "service_zone", "source_month",
        )
        if raw.limit(1).count() == 0:
            raise ValueError("No raw data for requested month")
        validated = TripValidator().validate(raw, zones).withColumn("preparation_run_id", F.lit(run_id))
        accepted = validated.where(F.size("rejection_reasons") == 0)
        rejected = validated.where(F.size("rejection_reasons") > 0)
        self.store.replace_month(accepted, "silver", "trips", month)
        self.store.replace_month(rejected, "silver", "rejected_trips", month)
        self.store.replace_month(zones, "silver", "taxi_zones", month)
        # Count persisted outputs, so SUCCESS describes the tables readers actually use.
        persisted = self.store.read_month("silver", "trips", month)
        counts = {
            "input_count": raw.count(), "accepted_count": persisted.count(),
            "rejected_count": self.store.read_month("silver", "rejected_trips", month).count(),
            "unusual_count": persisted.where(F.size("quality_warnings") > 0).count(),
        }
        if counts["input_count"] != counts["accepted_count"] + counts["rejected_count"]:
            raise RuntimeError("Raw/accepted/rejected counts do not reconcile")
        if counts["accepted_count"] == 0:
            raise ValueError("No accepted trips: refusing to publish an empty analytical month")
        self.store.status(month, run_id, "SUCCESS", counts)
        return {"source_month": month, "status": "SUCCESS", **counts}
