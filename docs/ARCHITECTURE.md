# Architecture and data dictionary

## Services and interfaces

The three processing services are ingestion, preparation and analytics. They have
separate entry points, input/output tables and retry boundaries. They are batch jobs,
not independently hosted HTTP microservices.

All monthly services accept `source_month` (`YYYY-MM`) and `run_id` (nonempty, at most
256 characters). Airflow limits its user-selected months to January–March 2026. The
underlying services accept other valid months for future extension.

Airflow is the control plane. Databricks holds source files and Delta tables and runs
PySpark. dbt owns analytical SQL transformations and tests. Local Python runs only
orchestration and file transfer, not trip-data transformations.

## Tables

Default catalog: `workspace`. Configurable schema prefix: `mobility`.

| Table | Grain and principal fields |
| --- | --- |
| `mobility_bronze.yellow_trips` | One original trip row; all source columns, `source_month`, `source_filename`, `ingested_at`, `ingestion_run_id` |
| `mobility_bronze.taxi_zones` | One original lookup row per snapshot month, plus the same metadata |
| `mobility_silver.trips` | One accepted source record, normalized fields and validation arrays |
| `mobility_silver.rejected_trips` | One invalid source record, normalized fields and rejection reason array; the original file remains in bronze |
| `mobility_silver.taxi_zones` | One zone per source month: `zone_id`, `borough`, `zone_name`, `service_zone` |
| `mobility_ops.month_status` | One current publication state per source month and its row counts |
| `mobility_ops.run_events` | Append-only service attempt events: month, run, service, status, event time, error class |
| `mobility_gold.dim_zones` | One source month + zone ID; `zone_key` is `YYYY-MM:ID` |
| `mobility_gold.fct_trips` | One accepted record with calendar fields and zone names |
| `mobility_gold.mart_pickup_activity` | Source month + pickup zone + local pickup hour |
| `mobility_gold.mart_monthly_activity` | Actual calendar month of pickup |
| `mobility_gold.mart_route_duration` | Source month + pickup zone + drop-off zone |

### Normalized trip fields

| Field | Type / interpretation |
| --- | --- |
| `pickup_at`, `dropoff_at` | Timestamp without timezone; NYC local wall-clock time |
| `pickup_zone_id`, `dropoff_zone_id` | Integer TLC location IDs |
| `trip_distance_miles` | Double, distance in miles |
| `fare_amount`, `total_amount` | Decimal(18,2), recorded USD amounts |
| `duration_minutes` | Seconds between timestamps / 60.0; wall-clock duration |
| `rejection_reasons` | Array of reason strings; empty for accepted rows |
| `quality_warnings` | Array of nonblocking warning strings |
| `preparation_run_id` | Identifier of the preparation attempt that wrote this row |

## Validation policy

Reject missing/unparseable timestamps, drop-off before pickup, and pickup/drop-off
IDs absent from the monthly lookup. Zero-duration trips remain accepted.

Flag, but retain, distances missing/NaN/nonpositive/over 100 miles; fares missing,
negative or over USD 500; totals missing or negative; and pickup dates outside the
source month. Thresholds are screening heuristics, not fraud classifications.
An accepted row with multiple warnings is counted once in `unusual_count`.

Bronze preserves source schema. Missing required columns or incompatible schema
changes fail ingestion instead of silently discarding fields. TLC's 2025 congestion
fee field stays in bronze. Source schema evolution must be reviewed before a future
period is added. Lookup IDs 264/265, if present in the official lookup, remain valid
but represent geographically nonspecific locations.

The fact table has no invented unique trip ID. Identical source records are retained.
Monetary NULLs remain visible; summaries coalesce an all-NULL sum to zero and monthly
outputs include `missing_fare_count` so that zero is not mistaken for complete data.

## Replay and publication

Delta `replaceWhere` overwrites only the requested source-month partition. Even an
empty rejection set replaces the previous rejection partition. Other months remain
unchanged. Bronze and silver writes are individually atomic; the whole pipeline is not
a cross-table transaction. Month state becomes RUNNING before writes and SUCCESS only
after preparation reconciles persisted outputs.

Analytics checks that every tracked month is SUCCESS and the requested month exists.
dbt also verifies required values, dimensional relationships, row-count reconciliation,
preparation generation, and aggregate totals. Gold models are rebuilt from all accepted
months: three months is a deliberately small scope, avoiding a second incremental-write
mechanism. A failed dbt build can leave some rebuilt gold tables; consumers should use
only a fully successful Airflow run as the publication signal.

The project assumes a single writer coordinated by Airflow. Do not overlap independent
manual runs or edit bronze/silver tables manually. Original files in the landing volume
are replaced on re-download; this preserves the current source, not a permanent archive
of every publisher revision. Monthly zone snapshots preserve the downloaded mapping,
not a verified historical January 2026 mapping.

## Analytical limits

Completed trips measure observed activity, not unmet demand. Recorded amounts are not
profit and do not include a cost model. Route medians use approximate percentiles and
rankings require at least 100 trips per month/route. Out-of-period pickups remain visible
in monthly summaries; the report filters the target calendar quarter explicitly.
Naive local timestamps cannot resolve all daylight-saving transitions; wall-clock
durations near those transitions require caution. No predictions or causal claims are made.
