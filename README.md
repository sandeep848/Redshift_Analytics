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

Create and activate your virtual environment:

### Windows (PowerShell / CMD)
```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1   # PowerShell
# or .venv\Scripts\activate.bat for CMD

pip install -e ".[dev]"
copy .env.example .env
```

### macOS / Linux
```bash
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
cp .env.example .env
```

---

## Development

First, make sure Docker Desktop or your Docker daemon is running.

### 1. Start Kafka Stack
Starts Kafka and Zookeeper in the background:
```bash
docker compose up -d
# Or: make up
```

### 2. Create Kafka Topics (Once)
Create the required `query_metrics_raw` and `query_metrics_processed` topics inside the container:

**Windows (PowerShell/CMD):**
```powershell
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic query_metrics_raw --partitions 3 --replication-factor 1
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic query_metrics_processed --partitions 3 --replication-factor 1
```

**macOS / Linux:**
```bash
make topics
```

### 3. Bootstrap DuckDB Database
Initializes the DuckDB database file (`data/analytics.duckdb`) and schemas:

**Windows (PowerShell):**
```powershell
$env:PYTHONUTF8=1
python -m scripts.bootstrap_duckdb
```

**Windows (CMD):**
```cmd
set PYTHONUTF8=1
python -m scripts.bootstrap_duckdb
```

**macOS / Linux:**
```bash
make bootstrap
```

### 4. Run the Pipeline Components
Run these in separate terminals (ensure the virtual environment is activated in each):

* **DuckDB Consumer**:
  ```bash
  python -m src.main consumer-duckdb
  # Or: make consumer-db
  ```
* **Replay Producer**:
  ```bash
  python -m src.main producer
  # Or: make producer
  ```
* **Analytics Dashboard UI**:
  ```bash
  python -m src.main ui
  # Or: make run-ui
  ```

### 5. Running Tests & Sanity Checks
* **Running Pytest**:
  ```bash
  pytest
  # Or: make test
  ```
* **End-to-End Sanity Check (Windows)**:
  ```powershell
  $env:PYTHONUTF8=1
  python -m scripts.run_sanity_checks
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
