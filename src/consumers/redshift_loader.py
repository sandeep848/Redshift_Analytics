from __future__ import annotations

import json
import logging
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import List

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from kafka import KafkaConsumer

from src.common.schema import QueryMetricsEvent
from src.common.settings import Settings
from src.storage.s3_client import S3Client
from src.storage.redshift_client import RedshiftClient


logger = logging.getLogger(__name__)


def _validate_redshift_settings(settings: Settings) -> None:
    missing = []
    if not settings.s3.bucket:
        missing.append("s3.bucket")
    if not settings.redshift.host:
        missing.append("redshift.host")
    if not settings.redshift.user:
        missing.append("redshift.user")
    if not settings.redshift.password:
        missing.append("redshift.password")

    if missing:
        raise ValueError(
            "Missing required Redshift/S3 configuration values: "
            + ", ".join(missing)
            + ". Update configs/app.yaml or .env."
        )


def _parse_event(raw: bytes) -> QueryMetricsEvent:
    payload = json.loads(raw.decode("utf-8"))
    return QueryMetricsEvent.model_validate(payload)


def _events_to_dataframe(events: List[QueryMetricsEvent]) -> pd.DataFrame:
    rows = []
    for e in events:
        rows.append(
            {
                "query_id": e.query_id,
                "deployment_type": e.deployment_type,
                "instance_id": e.instance_id,
                "arrival_timestamp": e.arrival_timestamp,
                "queue_duration_ms": e.queue_duration_ms,
                "compile_duration_ms": e.compile_duration_ms,
                "execution_duration_ms": e.execution_duration_ms,
                "scanned_mb": e.scanned_mb,
                "spilled_mb": e.spilled_mb,
                "execution_start_time": e.execution_start_time,
                "execution_end_time": e.execution_end_time,
            }
        )
    return pd.DataFrame(rows)


def _write_parquet(df: pd.DataFrame, path: Path) -> None:
    table = pa.Table.from_pandas(df)
    pq.write_table(table, path)


def run_redshift_consumer(settings: Settings) -> None:
    """
    Consume Kafka events, batch into Parquet, upload to S3, COPY into Redshift.
    """
    logger.info("Starting Redshift loader")

    _validate_redshift_settings(settings)

    consumer = KafkaConsumer(
        settings.kafka.topics.processed_query_metrics,
        bootstrap_servers=settings.kafka.bootstrap_servers,
        group_id=settings.kafka.consumer_groups.get("redshift"),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda v: v,
    )

    s3 = S3Client.from_settings(settings)
    rs = RedshiftClient.from_settings(settings)

    batch: List[QueryMetricsEvent] = []
    max_rows = settings.batching.max_rows
    max_seconds = settings.batching.max_seconds

    last_flush = datetime.now(timezone.utc)

    for msg in consumer:
        try:
            evt = _parse_event(msg.value)
            batch.append(evt)

            now = datetime.now(timezone.utc)
            age_s = (now - last_flush).total_seconds()

            if len(batch) >= max_rows or age_s >= max_seconds:
                _flush_batch(batch, s3, rs, settings)
                batch.clear()
                last_flush = now

        except Exception:
            logger.exception("Failed to process Kafka message")

    # final flush (normally unreachable)
    if batch:
        _flush_batch(batch, s3, rs, settings)


def _flush_batch(
    batch: List[QueryMetricsEvent],
    s3: S3Client,
    rs: RedshiftClient,
    settings: Settings,
) -> None:
    if not batch:
        return

    logger.info("Flushing %d events to Redshift", len(batch))

    df = _events_to_dataframe(batch)

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)
        fname = f"query_metrics_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%f')}.parquet"
        parquet_path = tmp_path / fname

        _write_parquet(df, parquet_path)

        s3_key = s3.upload_file(parquet_path)

        rs.copy_parquet_from_s3(
            s3_bucket=settings.s3.bucket,
            s3_key=s3_key,
            schema=settings.redshift.schema_name,
            table=settings.redshift.events_table,
            region=settings.s3.region,
            iam_role=settings.redshift.iam_role or None,
        )

    logger.info("Batch loaded successfully")
