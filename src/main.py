from __future__ import annotations

import argparse
import subprocess
from pathlib import Path

from src.common.logging import setup_logging
from src.common.settings import Settings
from src.consumers.duckdb_consumer import run_duckdb_consumer
from src.consumers.redshift_loader import run_redshift_consumer
from src.producer.replay_producer import run_replay_producer


def _load_settings() -> Settings:
    settings = Settings.load()
    setup_logging(settings.logging)
    return settings


def _producer_command(args: argparse.Namespace) -> None:
    settings = _load_settings()
    source = args.source_url or args.input or settings.dataset.source_url
    run_replay_producer(source, settings)


def _duckdb_command(_: argparse.Namespace) -> None:
    settings = _load_settings()
    run_duckdb_consumer(settings)


def _redshift_command(_: argparse.Namespace) -> None:
    settings = _load_settings()
    run_redshift_consumer(settings)


def _bootstrap_duckdb_command(args: argparse.Namespace) -> None:
    from scripts.bootstrap_duckdb import bootstrap

    settings = _load_settings()
    db_path = Path(args.db_path or settings.storage.duckdb.path)
    bootstrap(db_path, skip_rollups=args.skip_rollups, skip_views=args.skip_views)


def _ui_command(args: argparse.Namespace) -> None:
    cmd = [
        "streamlit",
        "run",
        "src/ui/app.py",
        "--server.address",
        args.host,
        "--server.port",
        str(args.port),
    ]
    subprocess.run(cmd, check=True)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Redshift streaming analytics CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    producer = subparsers.add_parser("producer", help="Replay parquet data to Kafka")
    producer.add_argument(
        "--input",
        type=str,
        default=None,
        help="Path or URL to parquet dataset (defaults to dataset.source_url)",
    )
    producer.add_argument(
        "--source-url",
        type=str,
        default=None,
        help="Explicit parquet URL to replay (overrides --input)",
    )
    producer.set_defaults(func=_producer_command)

    duckdb = subparsers.add_parser("consumer-duckdb", help="Consume Kafka into DuckDB")
    duckdb.set_defaults(func=_duckdb_command)

    redshift = subparsers.add_parser("consumer-redshift", help="Consume Kafka into Redshift")
    redshift.set_defaults(func=_redshift_command)

    bootstrap = subparsers.add_parser("bootstrap-duckdb", help="Initialize DuckDB schema")
    bootstrap.add_argument("--db-path", type=str, default=None, help="Path to DuckDB file")
    bootstrap.add_argument(
        "--skip-rollups",
        action="store_true",
        help="Skip rollup SQL files",
    )
    bootstrap.add_argument(
        "--skip-views",
        action="store_true",
        help="Skip view SQL files",
    )
    bootstrap.set_defaults(func=_bootstrap_duckdb_command)

    ui = subparsers.add_parser("ui", help="Start the Streamlit UI")
    ui.add_argument("--host", type=str, default="0.0.0.0", help="Host to bind")
    ui.add_argument("--port", type=int, default=8501, help="Port to bind")
    ui.set_defaults(func=_ui_command)

    return parser


def cli() -> None:
    parser = _build_parser()
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    cli()
