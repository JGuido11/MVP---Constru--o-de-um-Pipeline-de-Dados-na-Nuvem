# Databricks notebook source
# Uses isolated managed schemas; does not modify real mobility_* project tables.
import json
import sys
dbutils.widgets.text("project_root", "")
dbutils.widgets.text("catalog", "workspace")
sys.path.insert(0, dbutils.widgets.get("project_root") + "/src")

from mobility.config import PipelineConfig, source_month
from mobility.jobs import TripPreparationJob
from mobility.storage import DeltaStore

config = PipelineConfig(dbutils.widgets.get("catalog"), "mobility_acceptance")
store = DeltaStore(spark, config)
store.bootstrap()
job = TripPreparationJob(spark, config, store)

# COMMAND ----------
def seed(month):
    source_month(month)
    raw = spark.sql(f"""
        SELECT cast(pickup AS TIMESTAMP_NTZ) AS tpep_pickup_datetime,
               cast(dropoff AS TIMESTAMP_NTZ) AS tpep_dropoff_datetime,
               pu AS PULocationID, dest AS DOLocationID,
               cast(distance AS DOUBLE) AS trip_distance,
               cast(fare AS DECIMAL(18,2)) AS fare_amount,
               cast(fare AS DECIMAL(18,2)) AS total_amount,
               '{month}' AS source_month, 'fixture.parquet' AS source_filename,
               current_timestamp() AS ingested_at, 'fixture-ingestion' AS ingestion_run_id
        FROM VALUES
          ('2026-01-10 10:00:00', '2026-01-10 10:10:00', 1, 2, 2.0, 15.0),
          ('2026-01-10 10:00:00', '2026-01-10 10:10:00', 1, 2, 2.0, 15.0),
          (NULL, '2026-01-10 10:10:00', 1, 2, 2.0, 15.0),
          ('2026-01-10 10:10:00', '2026-01-10 10:00:00', 1, 2, 2.0, 15.0),
          ('2026-01-10 10:00:00', '2026-01-10 10:10:00', 999, 2, 2.0, 15.0),
          ('2026-01-10 10:00:00', '2026-01-10 10:10:00', 1, 2, -1.0, -5.0),
          ('2025-12-31 23:50:00', '2026-01-01 00:10:00', 1, 2, 2.0, 15.0)
        AS fixture(pickup, dropoff, pu, dest, distance, fare)
    """)
    zones = spark.sql(f"""SELECT * FROM VALUES
        ('1', 'Test borough', 'Zone A', 'Yellow', '{month}'),
        ('2', 'Test borough', 'Zone B', 'Yellow', '{month}')
        AS zones(LocationID, Borough, Zone, service_zone, source_month)""")
    store.replace_month(raw, "bronze", "yellow_trips", month)
    store.replace_month(zones, "bronze", "taxi_zones", month)


def totals(month):
    return store.read_month("silver", "trips", month).selectExpr(
        "count(*) AS n", "sum(fare_amount) AS fare"
    ).first().asDict()


seed("2026-01")
first = job.run("2026-01", "acceptance-first")
assert (first["input_count"], first["accepted_count"], first["rejected_count"]) == (7, 4, 3)
assert first["unusual_count"] == 2
before = totals("2026-01")
assert before["n"] == 4 and float(before["fare"]) == 40.0  # identical valid rows retained
reason_rows = store.read_month("silver", "rejected_trips", "2026-01").select("rejection_reasons").collect()
reasons = {reason for row in reason_rows for reason in row.rejection_reasons}
assert reasons == {"missing_or_invalid_timestamp", "reversed_journey", "invalid_pickup_zone"}

# COMMAND ----------
seed("2026-02")
job.run("2026-02", "acceptance-other-month")
february_before = totals("2026-02")
job.run("2026-01", "acceptance-replay")
assert totals("2026-01") == before
assert totals("2026-02") == february_before

# Rebuild January with no invalid rows: old rejected rows must disappear.
raw = store.read_month("bronze", "yellow_trips", "2026-01")
# Write via a separate staging table to avoid reading and overwriting the same Delta table.
clean = raw.where("tpep_pickup_datetime IS NOT NULL AND tpep_dropoff_datetime >= tpep_pickup_datetime AND PULocationID = 1")
clean.write.format("delta").mode("overwrite").saveAsTable(config.table("ops", "clean_fixture"))
store.replace_month(spark.table(config.table("ops", "clean_fixture")), "bronze", "yellow_trips", "2026-01")
result = job.run("2026-01", "acceptance-no-rejections")
assert result["rejected_count"] == 0

# A preparation failure must be visible and recoverable.
broken = spark.sql("SELECT '2026-01' AS source_month, 1 AS unexpected_column")
# Use a separate month/table schema overwrite is intentionally avoided: exercise
# the validator failure through a store wrapper without changing the bronze schema.
class BrokenInputStore(DeltaStore):
    def read_month(self, layer, name, month):
        if layer == "bronze" and name == "yellow_trips":
            return broken
        return super().read_month(layer, name, month)

try:
    TripPreparationJob(spark, config, BrokenInputStore(spark, config)).run("2026-01", "acceptance-failure")
except ValueError:
    pass
else:
    raise AssertionError("Malformed source schema unexpectedly succeeded")
status = store.read_month("ops", "month_status", "2026-01").first()
assert status.status == "FAILED"
seed("2026-01")
job.run("2026-01", "acceptance-recovery")
assert totals("2026-01") == before
print(json.dumps({"status": "PASS", "checks": ["quarantine", "warning_retention", "duplicate_retention",
    "month_replay", "other_month_unchanged", "empty_rejection_replacement", "failure_state", "recovery"]}))
