-- Processed events table: cleaned + enriched canonical events consumed from Kafka
CREATE TABLE IF NOT EXISTS query_metrics_processed (
    query_id TEXT NOT NULL,
    deployment_type TEXT NOT NULL,
    instance_id TEXT,
    arrival_timestamp TIMESTAMPTZ NOT NULL,
    execution_start_time TIMESTAMPTZ NOT NULL,
    execution_end_time TIMESTAMPTZ NOT NULL,

    queue_duration_ms INTEGER NOT NULL,
    compile_duration_ms INTEGER NOT NULL,
    execution_duration_ms INTEGER NOT NULL,

    scanned_mb DOUBLE NOT NULL,
    spilled_mb DOUBLE NOT NULL,

    duration_seconds DOUBLE NOT NULL,
    spill_pressure DOUBLE NOT NULL,
    queued BOOLEAN NOT NULL,
    anomaly_tags TEXT
);

-- Helpful index for time-based queries
CREATE INDEX IF NOT EXISTS idx_query_metrics_processed_arrival
ON query_metrics_processed(arrival_timestamp);

CREATE INDEX IF NOT EXISTS idx_query_metrics_processed_deployment
ON query_metrics_processed(deployment_type);

-- Rollups table: minute window aggregates for dashboard charts
CREATE TABLE IF NOT EXISTS query_metrics_rollups (
    window_start TIMESTAMPTZ NOT NULL,
    deployment_type TEXT NOT NULL,

    query_count BIGINT NOT NULL,
    avg_duration_seconds DOUBLE NOT NULL,
    avg_spill_pressure DOUBLE NOT NULL,
    queued_ratio DOUBLE NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_query_metrics_rollups_window
ON query_metrics_rollups(window_start);