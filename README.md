# Redshift Streaming Analytics

Streaming analytics pipeline for Redshift query metrics with Kafka ingestion, DuckDB rollups, and a Streamlit UI.

## Features
- Kafka replay producer that cleans, enriches, and de-duplicates query metrics.
- DuckDB consumer for local analytics and rollups.
- Optional Redshift loader that stages batches to S3 and COPYs into Redshift.
- Streamlit dashboards for live and historical insights.
- Typed configuration with environment-variable overrides.
- Structured logging and health-friendly CLI entrypoints.

## Project layout
```
configs/          # App + logging configs and SQL schemas
scripts/          # Bootstrap helpers
src/              # Application source
  common/         # Settings, logging, schema utilities
  consumers/      # Kafka consumers (DuckDB, Redshift)
  producer/       # Kafka replay producer
  storage/        # DuckDB, S3, Redshift clients
  ui/             # Streamlit UI
```

## Requirements
- Python 3.11+
- Docker (optional, for Kafka + Zookeeper)
- AWS credentials (only if using the Redshift loader)

## Setup
```bash
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

Copy the sample environment file and edit values as needed:
```bash
cp .env.example .env
```

## Development
Start Kafka locally:
```bash
make up
```

Create Kafka topics (once):
```bash
make topics
```

Bootstrap DuckDB schema:
```bash
make bootstrap
```

Run the replay producer:
```bash
make producer
```

Run the DuckDB consumer:
```bash
make consumer-db
```

Start the UI:
```bash
make run-ui
```

## CLI usage
The project exposes a single CLI entrypoint:
```bash
python -m src.main --help
```

Subcommands:
- `producer` — replay query metrics from a parquet file/URL into Kafka.
- `consumer-duckdb` — write processed events into DuckDB and maintain rollups.
- `consumer-redshift` — batch events to S3 and load into Redshift.
- `bootstrap-duckdb` — initialize DuckDB schema.
- `ui` — launch the Streamlit UI.

## Configuration
All configuration lives in `configs/app.yaml` and can be overridden with `.env` variables. See `docs/configuration.md` for details.

## Testing
```bash
make test
```

## Deployment
A production-ready Docker image can be built with:
```bash
docker build -t redshift-streaming-analytics .
```

Use `docker-compose.yml` for Kafka. The application expects secrets via environment variables (see `.env.example`).

## Known limitations / future improvements
- The Redshift loader requires valid AWS credentials and an existing Redshift cluster.
- Streamlit pages assume a running Kafka pipeline for live updates.
- Add managed secrets integration (e.g., AWS Secrets Manager) for production.

## Documentation
- Configuration reference: `docs/configuration.md`
- Pipeline & storage overview: `docs/architecture.md`
- Kafka topics and data contracts: `docs/kafka.md`
- API notes: `docs/api.md`
