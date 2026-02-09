from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml
from dotenv import load_dotenv
from pydantic import BaseModel, Field, model_validator

ROOT = Path(__file__).resolve().parents[2]
CONFIG_DIR = ROOT / "configs"
APP_CONFIG_PATH = CONFIG_DIR / "app.yaml"
LOGGING_CONFIG_PATH = CONFIG_DIR / "logging.yaml"


# ---------------------------------------------------------
# Typed config models
# ---------------------------------------------------------

class KafkaTopicsConfig(BaseModel):
    raw_query_metrics: str = "query_metrics_raw"
    processed_query_metrics: str = "query_metrics_processed"


class KafkaConfig(BaseModel):
    bootstrap_servers: str
    topics: KafkaTopicsConfig = Field(default_factory=KafkaTopicsConfig)
    consumer_groups: Dict[str, str] = Field(default_factory=dict)

    # Backward-compatible alias used by older code paths
    topic_query_metrics: Optional[str] = None

    @model_validator(mode="before")
    @classmethod
    def _coerce_legacy_shape(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data

        # Legacy: kafka.topic_query_metrics
        legacy_topic = data.get("topic_query_metrics")
        if legacy_topic and "topics" not in data:
            data = dict(data)
            data["topics"] = {"processed_query_metrics": legacy_topic}

        # Ensure alias field present for code that still references it
        topics = data.get("topics") or {}
        if isinstance(topics, dict):
            processed = topics.get("processed_query_metrics") or legacy_topic
            if processed:
                data = dict(data)
                data["topic_query_metrics"] = processed

        return data


class ReplayConfig(BaseModel):
    time_scale_factor: int = Field(default=50, gt=0)
    producer_batch_size: int = Field(default=500, gt=0)
    max_events: Optional[int] = Field(default=None, gt=0)


class DatasetConfig(BaseModel):
    source_url: str
    format: str = Field(default="parquet", pattern=r"^parquet$")
    event_time_column: str = "arrival_timestamp"
    batch_read_size: int = Field(default=10000, gt=0)
    enforce_event_time_order: bool = True


class MissingValuesConfig(BaseModel):
    numeric_fill_value: float = 0.0
    string_empty_as_null: bool = True
    normalize_strings: bool = True
    # drop | coerce_to_null
    timestamp_invalid_action: str = Field(default="drop", pattern=r"^(drop|coerce_to_null)$")


class DuplicatesConfig(BaseModel):
    enabled: bool = True
    key_columns: List[str] = Field(default_factory=lambda: ["query_id", "arrival_timestamp"])
    ttl_seconds: int = Field(default=3600, gt=0)


class InconsistenciesConfig(BaseModel):
    clip_negative_durations: bool = True
    clip_negative_metrics: bool = True
    enforce_end_after_start: bool = True
    allowed_deployment_types: List[str] = Field(
        default_factory=lambda: ["provisioned", "serverless", "unknown"]
    )


class EnrichmentConfig(BaseModel):
    compute_spill_pressure: bool = True
    compute_queue_flag: bool = True
    compute_duration_seconds: bool = True


class ProcessingConfig(BaseModel):
    missing_values: MissingValuesConfig = Field(default_factory=MissingValuesConfig)
    duplicates: DuplicatesConfig = Field(default_factory=DuplicatesConfig)
    inconsistencies: InconsistenciesConfig = Field(default_factory=InconsistenciesConfig)
    enrichment: EnrichmentConfig = Field(default_factory=EnrichmentConfig)


class DuckDBStorageConfig(BaseModel):
    path: str = "./data/analytics.duckdb"
    tables: Dict[str, str] = Field(
        default_factory=lambda: {
            "processed": "query_metrics_processed",
            "rollups": "query_metrics_rollups",
        }
    )


class StorageConfig(BaseModel):
    duckdb: DuckDBStorageConfig = Field(default_factory=DuckDBStorageConfig)


class S3Config(BaseModel):
    bucket: str = ""
    prefix: str = "redshift-streaming-analytics"
    region: str = "us-east-1"


class RedshiftConfig(BaseModel):
    host: str = ""
    port: int = 5439
    database: str = "dev"
    user: str = ""
    password: str = ""
    schema_name: str = "public"
    events_table: str = "query_metrics"
    iam_role: Optional[str] = None


class BatchingConfig(BaseModel):
    max_rows: int = Field(default=5000, gt=0)
    max_seconds: int = Field(default=30, gt=0)


class UiStreamConfig(BaseModel):
    enabled: bool = True
    topic: str = "query_metrics_processed"
    max_buffer_size: int = Field(default=1000, gt=0)


class UiConfig(BaseModel):
    refresh_interval_seconds: int = Field(default=1, gt=0)
    stream: UiStreamConfig = Field(default_factory=UiStreamConfig)


class Settings(BaseModel):
    app: Dict[str, Any]
    kafka: KafkaConfig
    replay: ReplayConfig = Field(default_factory=ReplayConfig)
    dataset: DatasetConfig
    processing: ProcessingConfig = Field(default_factory=ProcessingConfig)
    storage: StorageConfig = Field(default_factory=StorageConfig)
    ui: UiConfig = Field(default_factory=UiConfig)
    s3: S3Config = Field(default_factory=S3Config)
    redshift: RedshiftConfig = Field(default_factory=RedshiftConfig)
    batching: BatchingConfig = Field(default_factory=BatchingConfig)
    logging: Dict[str, Any]

    # -----------------------------------------------------
    # Loader
    # -----------------------------------------------------
    @classmethod
    def load(cls) -> "Settings":
        # 1) Load env first (so YAML ${VAR} resolves)
        load_dotenv()

        # 2) Load YAML configs
        with open(APP_CONFIG_PATH, "r", encoding="utf-8") as f:
            raw_app = yaml.safe_load(f) or {}

        with open(LOGGING_CONFIG_PATH, "r", encoding="utf-8") as f:
            logging_cfg = yaml.safe_load(f) or {}

        # 3) Resolve ${ENV_VAR} or ${ENV_VAR:default} patterns
        resolved = cls._resolve_env(raw_app)

        # 4) Attach logging config
        resolved["logging"] = logging_cfg

        # 5) Validate + return
        return cls.model_validate(resolved)

    # -----------------------------------------------------
    # Helpers
    # -----------------------------------------------------
    @staticmethod
    def _resolve_env(obj: Any) -> Any:
        """Recursively resolve ${VAR} or ${VAR:default} patterns."""
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
            return os.getenv(key, default)

        env_val = os.getenv(inner)
        # If not found, keep empty string rather than None to avoid surprising type errors.
        return env_val if env_val is not None else ""
