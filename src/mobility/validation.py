"""Native Spark expressions: no Python UDFs or driver-side row processing."""
from pyspark.sql import functions as F

# Preserve the complete input file in the volume and its native columns in bronze.
REQUIRED_COLUMNS = {
    "tpep_pickup_datetime", "tpep_dropoff_datetime", "PULocationID", "DOLocationID",
    "trip_distance", "fare_amount", "total_amount",
}


class TripValidator:
    def normalize(self, raw):
        missing = REQUIRED_COLUMNS - set(raw.columns)
        if missing:
            raise ValueError(f"Missing required source columns: {sorted(missing)}")
        return raw.select(
            F.expr("try_cast(tpep_pickup_datetime AS TIMESTAMP_NTZ)").alias("pickup_at"),
            F.expr("try_cast(tpep_dropoff_datetime AS TIMESTAMP_NTZ)").alias("dropoff_at"),
            F.expr("try_cast(PULocationID AS INT)").alias("pickup_zone_id"),
            F.expr("try_cast(DOLocationID AS INT)").alias("dropoff_zone_id"),
            F.expr("try_cast(trip_distance AS DOUBLE)").alias("trip_distance_miles"),
            F.expr("try_cast(fare_amount AS DECIMAL(18,2))").alias("fare_amount"),
            F.expr("try_cast(total_amount AS DECIMAL(18,2))").alias("total_amount"),
            "source_month", "source_filename", "ingested_at", "ingestion_run_id",
        )

    def validate(self, raw, zones):
        trips = self.normalize(raw)
        pickup = zones.select(F.col("zone_id").alias("known_pickup")).distinct()
        dropoff = zones.select(F.col("zone_id").alias("known_dropoff")).distinct()
        joined = (trips.join(F.broadcast(pickup), trips.pickup_zone_id == pickup.known_pickup, "left")
                  .join(F.broadcast(dropoff), trips.dropoff_zone_id == dropoff.known_dropoff, "left"))

        def reasons(rules):
            return F.filter(F.array(*[F.when(condition, F.lit(reason))
                                      for condition, reason in rules]), lambda x: x.isNotNull())

        missing_time = F.col("pickup_at").isNull() | F.col("dropoff_at").isNull()
        result = joined.withColumn("rejection_reasons", reasons([
            (missing_time, "missing_or_invalid_timestamp"),
            (F.col("dropoff_at") < F.col("pickup_at"), "reversed_journey"),
            (F.col("known_pickup").isNull(), "invalid_pickup_zone"),
            (F.col("known_dropoff").isNull(), "invalid_dropoff_zone"),
        ])).withColumn("quality_warnings", reasons([
            (F.col("trip_distance_miles").isNull() | F.isnan("trip_distance_miles") |
             (F.col("trip_distance_miles") <= 0) | (F.col("trip_distance_miles") > 100),
             "unusual_distance"),
            (F.col("fare_amount").isNull() | (F.col("fare_amount") < 0) |
             (F.col("fare_amount") > 500), "unusual_fare"),
            (F.col("total_amount").isNull() | (F.col("total_amount") < 0), "unusual_total"),
            (F.date_format("pickup_at", "yyyy-MM") != F.col("source_month"), "pickup_outside_source_month"),
        ])).withColumn("duration_minutes", F.expr(
            "timestampdiff(SECOND, pickup_at, dropoff_at) / 60.0"
        )).drop("known_pickup", "known_dropoff")
        return result
