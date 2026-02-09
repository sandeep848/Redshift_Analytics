# Kafka topics and contracts

## Topics
- `query_metrics_raw` — optional raw dataset stream.
- `query_metrics_processed` — cleaned/enriched query metrics (primary stream).

## Message schema
Messages on `query_metrics_processed` conform to `QueryMetricsEvent` (see `src/common/schema.py`).

Fields include:
- `query_id`
- `deployment_type`
- `instance_id`
- `arrival_timestamp`
- `execution_start_time`
- `execution_end_time`
- `queue_duration_ms`
- `compile_duration_ms`
- `execution_duration_ms`
- `scanned_mb`
- `spilled_mb`
- `duration_seconds`
- `spill_pressure`
- `queued`

## Consumer groups
- `duckdb_writer` — persists processed events to DuckDB and updates rollups.
- `ui_stream` — live UI streaming.
- `redshift` — batches events to S3 and loads into Redshift.
