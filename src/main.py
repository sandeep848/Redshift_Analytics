from __future__ import annotations

import argparse
import sys
import socket
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.pipeline import Settings, run_replay_producer, run_duckdb_consumer

def _producer_command(args: argparse.Namespace) -> None:
    settings = Settings.load()
    source = args.source_url or args.input or settings.dataset.source_url
    run_replay_producer(source, settings)

def _duckdb_command(_: argparse.Namespace) -> None:
    settings = Settings.load()
    run_duckdb_consumer(settings)

def _bootstrap_duckdb_command(args: argparse.Namespace) -> None:
    settings = Settings.load()
    import duckdb
    db_path = args.db_path or settings.storage.duckdb.path
    print(f"Bootstrapping DuckDB at: {db_path}")
    con = duckdb.connect(db_path)
    try:
        from src.pipeline import PROCESSED_SCHEMA_SQL, ROLLUPS_SCHEMA_SQL
        con.execute(PROCESSED_SCHEMA_SQL.format(processed_table=settings.storage.duckdb.tables.processed))
        con.execute(ROLLUPS_SCHEMA_SQL.format(rollups_table=settings.storage.duckdb.tables.rollups))
        con.execute(f"CREATE OR REPLACE VIEW v_query_metrics_processed_latest AS SELECT * FROM {settings.storage.duckdb.tables.processed} ORDER BY arrival_timestamp DESC LIMIT 500;")
        con.execute(f"CREATE OR REPLACE VIEW v_pipeline_status AS SELECT (SELECT COUNT(*) FROM {settings.storage.duckdb.tables.processed}) AS processed_rows, (SELECT MIN(arrival_timestamp) FROM {settings.storage.duckdb.tables.processed}) AS earliest_arrival_ts, (SELECT MAX(arrival_timestamp) FROM {settings.storage.duckdb.tables.processed}) AS latest_arrival_ts;")
        print("✅ DuckDB bootstrapped successfully.")
    except Exception as e:
        print(f"❌ Error bootstrapping: {e}")
    finally:
        con.close()

def _ui_command(args: argparse.Namespace) -> None:
    import uvicorn
    host = args.host
    port = args.port
    orig_port = port
    while True:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(0.2)
            if s.connect_ex((host if host != "0.0.0.0" else "127.0.0.1", port)) == 0:
                port += 1
            else:
                break
    if port != orig_port:
        print(f"Port {orig_port} is busy. Switched to: {port}")
    print(f"Starting Dashboard Server: {host}:{port}")
    uvicorn.run("src.server:app", host=host, port=port, reload=False)

def cli() -> None:
    parser = argparse.ArgumentParser(description="Redshift streaming analytics CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    producer = subparsers.add_parser("producer", help="Replay parquet data to Kafka")
    producer.add_argument("--input", type=str, default=None)
    producer.add_argument("--source-url", type=str, default=None)
    producer.set_defaults(func=_producer_command)

    duckdb_cmd = subparsers.add_parser("consumer-duckdb", help="Consume Kafka into DuckDB")
    duckdb_cmd.set_defaults(func=_duckdb_command)

    bootstrap = subparsers.add_parser("bootstrap-duckdb", help="Initialize DuckDB schema")
    bootstrap.add_argument("--db-path", type=str, default=None)
    bootstrap.set_defaults(func=_bootstrap_duckdb_command)

    ui = subparsers.add_parser("ui", help="Start Dashboard Server")
    ui.add_argument("--host", type=str, default="0.0.0.0")
    ui.add_argument("--port", type=int, default=8501)
    ui.set_defaults(func=_ui_command)

    args = parser.parse_args()
    args.func(args)

if __name__ == "__main__":
    cli()
