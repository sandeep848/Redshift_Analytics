from __future__ import annotations

import os
import sys
import time
import json
import socket
import random
import yaml
from pathlib import Path
from datetime import datetime, date, timezone, timedelta
from typing import Any, Optional, Dict, List, Iterable
from dotenv import load_dotenv
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
import duckdb
from kafka import KafkaProducer, KafkaConsumer

ROOT = Path(__file__).resolve().parents[1]
APP_CONFIG_PATH = ROOT / "configs" / "app.yaml"

class KafkaTopicsConfig(BaseModel):
    raw_query_metrics: str = "query_metrics_raw"
    processed_query_metrics: str = "query_metrics_processed"
    dead_letter_query_metrics: str = "query_metrics_dlq"

class KafkaConfig(BaseModel):
    bootstrap_servers: str
    topics: KafkaTopicsConfig = Field(default_factory=KafkaTopicsConfig)
    consumer_groups: Dict[str, str] = Field(default_factory=dict)

class ReplayConfig(BaseModel):
    time_scale_factor: int = 50
    producer_batch_size: int = 500
    max_events: Optional[int] = None

class DatasetConfig(BaseModel):
    source_url: str
    format: str = "parquet"
    event_time_column: str = "arrival_timestamp"
    batch_read_size: int = 10000
    enforce_event_time_order: bool = True

class MissingValuesConfig(BaseModel):
    numeric_fill_value: float = 0.0
    string_empty_as_null: bool = True
    normalize_strings: bool = True
    timestamp_invalid_action: str = "drop"

class DuplicatesConfig(BaseModel):
    enabled: bool = True
    key_columns: List[str] = Field(default_factory=lambda: ["query_id", "arrival_timestamp"])
    ttl_seconds: int = 3600

class InconsistenciesConfig(BaseModel):
    clip_negative_durations: bool = True
    clip_negative_metrics: bool = True
    enforce_end_after_start: bool = True
    allowed_deployment_types: List[str] = Field(default_factory=lambda: ["provisioned", "serverless", "unknown"])

class EnrichmentConfig(BaseModel):
    compute_spill_pressure: bool = True
    compute_queue_flag: bool = True
    compute_duration_seconds: bool = True

class ProcessingConfig(BaseModel):
    missing_values: MissingValuesConfig = Field(default_factory=MissingValuesConfig)
    duplicates: DuplicatesConfig = Field(default_factory=DuplicatesConfig)
    inconsistencies: InconsistenciesConfig = Field(default_factory=InconsistenciesConfig)
    enrichment: EnrichmentConfig = Field(default_factory=EnrichmentConfig)

class DuckDBTablesConfig(BaseModel):
    processed: str = "query_metrics_processed"
    rollups: str = "query_metrics_rollups"

class DuckDBConfig(BaseModel):
    path: str
    tables: DuckDBTablesConfig = Field(default_factory=DuckDBTablesConfig)

class StorageConfig(BaseModel):
    duckdb: DuckDBConfig

class Settings(BaseModel):
    kafka: KafkaConfig
    replay: ReplayConfig = Field(default_factory=ReplayConfig)
    dataset: DatasetConfig
    processing: ProcessingConfig = Field(default_factory=ProcessingConfig)
    storage: StorageConfig

    @classmethod
    def load(cls) -> Settings:
        load_dotenv()
        raw = {}
        if APP_CONFIG_PATH.exists():
            with open(APP_CONFIG_PATH, "r", encoding="utf-8") as f:
                raw = yaml.safe_load(f) or {}
        resolved = cls._resolve_env(raw)
        return cls.model_validate(resolved)

    @staticmethod
    def _resolve_env(obj: Any) -> Any:
        if isinstance(obj, dict):
            return {k: Settings._resolve_env(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [Settings._resolve_env(v) for v in obj]
        if isinstance(obj, str):
            return Settings._resolve_str(obj)
        return obj

    @staticmethod
    def _resolve_str(value: str) -> Any:
        value = value.strip()
        if not (value.startswith("${") and value.endswith("}")):
            return value
        inner = value[2:-1]
        if ":" in inner:
            key, default = inner.split(":", 1)
            val = os.getenv(key, default)
            try:
                if val.isdigit(): return int(val)
                float_val = float(val)
                return int(float_val) if float_val.is_integer() else float_val
            except Exception:
                return val
        env_val = os.getenv(inner)
        return env_val if env_val is not None else ""

class TimeScaler:
    def __init__(self, factor: int, anchor_event_time: datetime, anchor_wall_time: datetime):
        self.factor = factor
        self.anchor_event_time = anchor_event_time
        self.anchor_wall_time = anchor_wall_time

    def to_wall_time(self, event_time: datetime) -> datetime:
        delta = event_time - self.anchor_event_time
        scaled = timedelta(seconds=delta.total_seconds() / self.factor)
        return self.anchor_wall_time + scaled

class QueryMetricsEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")
    query_id: str = Field(min_length=1)
    deployment_type: str
    instance_id: Optional[str] = None
    arrival_timestamp: datetime
    queue_duration_ms: int = Field(ge=0)
    compile_duration_ms: int = Field(ge=0)
    execution_duration_ms: int = Field(ge=0)
    scanned_mb: float = Field(ge=0)
    spilled_mb: float = Field(ge=0)
    execution_start_time: datetime
    execution_end_time: datetime
    duration_seconds: float = Field(ge=0)
    spill_pressure: float = Field(ge=0)
    queued: bool
    anomaly_tags: str = ""

    @field_validator("deployment_type")
    @classmethod
    def _validate_deployment_type(cls, v: str) -> str:
        vv = (v or "").strip().lower()
        return vv if vv in {"provisioned", "serverless"} else "unknown"

    @field_validator("arrival_timestamp", "execution_start_time", "execution_end_time", mode="before")
    @classmethod
    def _coerce_dt_to_utc(cls, v):
        if isinstance(v, datetime):
            if v.tzinfo is None:
                return v.replace(tzinfo=timezone.utc)
            return v.astimezone(timezone.utc)
        return v

    @model_validator(mode="after")
    def _validate_time_order(self) -> QueryMetricsEvent:
        if self.execution_end_time < self.execution_start_time:
            self.execution_end_time = self.execution_start_time
        if self.arrival_timestamp > self.execution_end_time:
            self.arrival_timestamp = self.execution_end_time
        return self

class DuckDBClient:
    def __init__(self, db_path: str, read_only: bool = False):
        self.db_path = db_path
        self.read_only = read_only

    @classmethod
    def from_settings(cls, settings: Settings) -> "DuckDBClient":
        return cls(db_path=settings.storage.duckdb.path)

    def as_read_only(self, busy_timeout_ms: int = 1000) -> "DuckDBClient":
        """Return a new DuckDBClient configured for read-only access."""
        client = DuckDBClient(db_path=self.db_path, read_only=True)
        client._busy_timeout_ms = busy_timeout_ms
        return client

    def connect(self) -> duckdb.DuckDBPyConnection:
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        con = duckdb.connect(self.db_path, read_only=self.read_only)
        timeout = getattr(self, "_busy_timeout_ms", 30000)
        try:
            con.execute(f"PRAGMA busy_timeout={timeout};")
        except Exception:
            pass
        return con

    def fetchdf(self, query: str):
        """Execute a query and return results as a pandas DataFrame."""
        import pandas as pd
        with self.connect() as con:
            return con.execute(query).df()

def _pick_first(row: dict, *keys: str) -> Any:
    for k in keys:
        if k in row: return row[k]
    return None

def map_clean_enrich_row(row: Dict[str, Any], processing: ProcessingConfig) -> QueryMetricsEvent:
    mv = processing.missing_values
    inc = processing.inconsistencies
    enr = processing.enrichment

    query_id = str(_pick_first(row, "query_id", "queryid", "query") or "").strip()
    if not query_id:
        raise ValueError("Missing query_id")

    deployment_raw = str(_pick_first(row, "deployment_type", "deployment", "cluster_type") or "").strip().lower()
    deployment_type = deployment_raw if deployment_raw in {"provisioned", "serverless"} else "unknown"
    instance_id = str(_pick_first(row, "instance_id", "cluster_identifier", "cluster_id") or "").strip() or None

    def parse_ts(val):
        if not val: return None
        if isinstance(val, datetime): return val
        try: return datetime.fromisoformat(str(val))
        except Exception: return None

    arrival_ts = parse_ts(_pick_first(row, "arrival_timestamp", "arrival_time", "arrived_at"))
    if not arrival_ts:
        raise ValueError("Missing or invalid arrival_timestamp")
    if arrival_ts.tzinfo is None:
        arrival_ts = arrival_ts.replace(tzinfo=timezone.utc)

    start_ts = parse_ts(_pick_first(row, "execution_start_time", "start_time", "exec_start_time"))
    end_ts = parse_ts(_pick_first(row, "execution_end_time", "end_time", "exec_end_time"))

    def parse_num(val, default):
        if val is None: return default
        try: return float(val)
        except Exception: return default

    queue_ms = int(parse_num(_pick_first(row, "queue_duration_ms", "queue_time_ms", "queue_ms"), mv.numeric_fill_value))
    compile_ms = int(parse_num(_pick_first(row, "compile_duration_ms", "compile_time_ms", "compile_ms"), mv.numeric_fill_value))
    exec_ms = int(parse_num(_pick_first(row, "execution_duration_ms", "execution_time_ms", "exec_ms"), mv.numeric_fill_value))

    if inc.clip_negative_durations:
        queue_ms = max(0, queue_ms)
        compile_ms = max(0, compile_ms)
        exec_ms = max(0, exec_ms)

    scanned_mb = parse_num(_pick_first(row, "scanned_mb", "scan_mb", "scanned_megabytes"), mv.numeric_fill_value)
    spilled_mb = parse_num(_pick_first(row, "spilled_mb", "spill_mb", "spilled_megabytes"), mv.numeric_fill_value)

    if inc.clip_negative_metrics:
        scanned_mb = max(0.0, scanned_mb)
        spilled_mb = max(0.0, spilled_mb)

    if not start_ts:
        start_ts = arrival_ts + timedelta(milliseconds=queue_ms)
    if not end_ts:
        end_ts = start_ts + timedelta(milliseconds=(compile_ms + exec_ms))

    if start_ts.tzinfo is None: start_ts = start_ts.replace(tzinfo=timezone.utc)
    if end_ts.tzinfo is None: end_ts = end_ts.replace(tzinfo=timezone.utc)

    if inc.enforce_end_after_start and end_ts < start_ts:
        end_ts = start_ts
    if arrival_ts > end_ts:
        arrival_ts = start_ts

    duration_seconds = max(0.0, (end_ts - start_ts).total_seconds()) if enr.compute_duration_seconds else 0.0
    spill_pressure = max(0.0, spilled_mb / max(scanned_mb, 1.0)) if enr.compute_spill_pressure else 0.0
    queued = queue_ms > 0 if enr.compute_queue_flag else False

    tags = []
    if spill_pressure > 0.15: tags.append("Heavy Spiller")
    if queue_ms > 2000: tags.append("Queue Bound")
    if compile_ms > 500: tags.append("Compile Bound")
    if duration_seconds > 8.0: tags.append("Execution Spike")
    anomaly_tags = ", ".join(tags)

    return QueryMetricsEvent(
        query_id=query_id,
        deployment_type=deployment_type,
        instance_id=instance_id,
        arrival_timestamp=arrival_ts,
        queue_duration_ms=queue_ms,
        compile_duration_ms=compile_ms,
        execution_duration_ms=exec_ms,
        scanned_mb=scanned_mb,
        spilled_mb=spilled_mb,
        execution_start_time=start_ts,
        execution_end_time=end_ts,
        duration_seconds=duration_seconds,
        spill_pressure=spill_pressure,
        queued=queued,
        anomaly_tags=anomaly_tags
    )

PROCESSED_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS {processed_table} (
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
"""

ROLLUPS_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS {rollups_table} (
    window_start TIMESTAMPTZ NOT NULL,
    deployment_type TEXT NOT NULL,
    query_count BIGINT NOT NULL,
    avg_duration_seconds DOUBLE NOT NULL,
    avg_spill_pressure DOUBLE NOT NULL,
    queued_ratio DOUBLE NOT NULL
);
"""

ROLLUP_AGG_SQL = """
INSERT INTO {rollups_table}
SELECT
    date_trunc('minute', arrival_timestamp) AS window_start,
    deployment_type,
    COUNT(*) AS query_count,
    AVG(duration_seconds) AS avg_duration_seconds,
    AVG(spill_pressure) AS avg_spill_pressure,
    AVG(CASE WHEN queued THEN 1 ELSE 0 END) AS queued_ratio
FROM {processed_table}
WHERE arrival_timestamp >= now() - INTERVAL '5 minutes'
GROUP BY 1, 2;
"""

def run_replay_producer(source: str, settings: Settings) -> None:
    raw_topic = settings.kafka.topics.raw_query_metrics
    processed_topic = settings.kafka.topics.processed_query_metrics
    replay_factor = settings.replay.time_scale_factor
    batch_send = settings.replay.producer_batch_size
    max_events = settings.replay.max_events

    producer = KafkaProducer(
        bootstrap_servers=settings.kafka.bootstrap_servers,
        value_serializer=lambda v: json.dumps(v, default=lambda o: o.isoformat() if isinstance(o, (datetime, date)) else o).encode("utf-8")
    )
    sent = 0
    scaler = None

    try:
        import pyarrow.parquet as pq
        table = pq.read_table(source)
        df = table.to_pandas()
        if settings.dataset.enforce_event_time_order:
            df = df.sort_values(settings.dataset.event_time_column)
            
        for _, row_series in df.iterrows():
            raw = row_series.to_dict()
            try:
                evt = map_clean_enrich_row(raw, settings.processing)
            except Exception as e:
                dlq_topic = settings.kafka.topics.dead_letter_query_metrics
                dlq_payload = {
                    "raw_payload": raw,
                    "error_message": str(e),
                    "failed_at": datetime.now(timezone.utc).isoformat()
                }
                try: producer.send(dlq_topic, value=dlq_payload)
                except Exception: pass
                continue

            ev_time = evt.arrival_timestamp.replace(tzinfo=timezone.utc)
            if scaler is None:
                scaler = TimeScaler(
                    factor=replay_factor,
                    anchor_event_time=ev_time,
                    anchor_wall_time=datetime.now(timezone.utc)
                )
            target_wall = scaler.to_wall_time(ev_time)
            sleep_s = (target_wall - datetime.now(timezone.utc)).total_seconds()
            if sleep_s > 0.05:
                time.sleep(sleep_s)

            producer.send(processed_topic, value=evt.model_dump())
            producer.send(raw_topic, value=raw)
            sent += 1
            if sent % batch_send == 0:
                producer.flush()
            if max_events is not None and sent >= max_events:
                break
    finally:
        producer.close()

def run_duckdb_consumer(settings: Settings) -> None:
    consumer = KafkaConsumer(
        settings.kafka.topics.processed_query_metrics,
        bootstrap_servers=settings.kafka.bootstrap_servers,
        group_id=settings.kafka.consumer_groups.get("duckdb_writer"),
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=False
    )
    db_client = DuckDBClient.from_settings(settings)
    con = db_client.connect()
    try:
        con.execute(PROCESSED_SCHEMA_SQL.format(processed_table=settings.storage.duckdb.tables.processed))
        con.execute(ROLLUPS_SCHEMA_SQL.format(rollups_table=settings.storage.duckdb.tables.rollups))
    finally:
        con.close()

    while True:
        records = consumer.poll(timeout_ms=1000)
        if not records: continue
        events = []
        for tp, msgs in records.items():
            for m in msgs:
                try: events.append(QueryMetricsEvent.model_validate(m.value))
                except Exception: pass
        if not events: continue

        con = db_client.connect()
        try:
            con.execute("BEGIN;")
            rows = [
                (
                    e.query_id, e.deployment_type, e.instance_id, e.arrival_timestamp,
                    e.execution_start_time, e.execution_end_time, e.queue_duration_ms,
                    e.compile_duration_ms, e.execution_duration_ms, e.scanned_mb,
                    e.spilled_mb, e.duration_seconds, e.spill_pressure, e.queued, e.anomaly_tags
                )
                for e in events
            ]
            con.executemany(
                f"INSERT INTO {settings.storage.duckdb.tables.processed} VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                rows
            )
            con.execute(
                ROLLUP_AGG_SQL.format(
                    processed_table=settings.storage.duckdb.tables.processed,
                    rollups_table=settings.storage.duckdb.tables.rollups
                )
            )
            con.execute("COMMIT;")
            consumer.commit()
        except Exception:
            con.execute("ROLLBACK;")
        finally:
            con.close()
