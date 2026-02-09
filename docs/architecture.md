# Architecture overview

The system ingests Redshift query metrics, enriches them, and writes rollups for analytics.

## Data flow
1. **Replay producer** reads parquet files, cleans + enriches records, and publishes to Kafka.
2. **DuckDB consumer** subscribes to the processed topic and writes events + rollups to DuckDB.
3. **Redshift loader** (optional) batches processed events, uploads parquet files to S3, and COPYs into Redshift.
4. **Streamlit UI** queries DuckDB or consumes the Kafka stream for live dashboards.

## Storage
- **Kafka** — streaming transport for raw and processed events.
- **DuckDB** — local analytics + rollup storage.
- **Redshift** — optional warehouse for long-term storage.
- **S3** — staging bucket for Redshift COPY.

## Observability
Logging is configured via `configs/logging.yaml`. The CLI initializes logging at startup and all components emit structured logs with event metadata.
