# Configuration reference

`configs/app.yaml` is the source of truth. Environment variables may override values using `${VAR}` or `${VAR:default}` placeholders. `.env.example` shows the recommended variables.

## Application
| Key | Description |
| --- | --- |
| `app.name` | Application name for logging and metadata. |
| `app.environment` | Environment label (local, staging, prod). |

## Kafka
| Key | Description |
| --- | --- |
| `kafka.bootstrap_servers` | Kafka bootstrap list (e.g., `localhost:9092`). |
| `kafka.topics.raw_query_metrics` | Raw input topic name (optional). |
| `kafka.topics.processed_query_metrics` | Cleaned/enriched topic name. |
| `kafka.consumer_groups.duckdb_writer` | Consumer group id for DuckDB. |
| `kafka.consumer_groups.ui_stream` | Consumer group id for Streamlit UI. |
| `kafka.consumer_groups.redshift` | Consumer group id for Redshift loader. |

## Dataset / replay
| Key | Description |
| --- | --- |
| `dataset.source_url` | Parquet source URL or local path. |
| `dataset.event_time_column` | Event timestamp field. |
| `replay.time_scale_factor` | Time scale multiplier for replay speed. |
| `replay.producer_batch_size` | Max number of Kafka sends before flush. |
| `replay.max_events` | Optional cap on event count. |

## Processing
Controls deduplication, missing values, and enrichment logic.

## Storage
| Key | Description |
| --- | --- |
| `storage.duckdb.path` | DuckDB file path. |
| `storage.duckdb.tables.*` | Table names for processed + rollups. |

## Redshift + S3
| Key | Description |
| --- | --- |
| `s3.bucket` | Bucket used for staging parquet files. |
| `s3.prefix` | Prefix used for parquet uploads. |
| `s3.region` | AWS region. |
| `redshift.host` | Redshift endpoint. |
| `redshift.port` | Redshift port (default 5439). |
| `redshift.database` | Redshift database name. |
| `redshift.user` | Username. |
| `redshift.password` | Password. |
| `redshift.schema_name` | Schema to load data into. |
| `redshift.events_table` | Target table name. |
| `redshift.iam_role` | Optional IAM role for COPY. |

## Batching
| Key | Description |
| --- | --- |
| `batching.max_rows` | Max events per batch to Redshift. |
| `batching.max_seconds` | Max time (seconds) before flushing. |
