from __future__ import annotations

import json
import logging
import time
from collections import deque
from dataclasses import dataclass
from typing import Deque, Dict, Iterable, List, Optional

from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

from src.common.schema import QueryMetricsEvent, UiQueryMetricsEvent
from src.common.settings import Settings

logger = logging.getLogger(__name__)


@dataclass
class StreamBuffer:
    max_size: int
    _buf: Deque[Dict] = None  # type: ignore

    def __post_init__(self) -> None:
        if self.max_size <= 0:
            raise ValueError("max_size must be > 0")
        self._buf = deque(maxlen=self.max_size)

    def append(self, item: Dict) -> None:
        self._buf.append(item)

    def extend(self, items: Iterable[Dict]) -> None:
        for it in items:
            self._buf.append(it)

    def snapshot(self) -> List[Dict]:
        return list(self._buf)


def make_ui_stream_consumer(settings: Settings) -> Optional[KafkaConsumer]:
    """
    Creates a Kafka consumer for the UI stream.

    Notes for Streamlit:
    - keep poll timeouts short
    - don't auto-commit aggressively; UI can tolerate replay on refresh
    """
    topic = settings.ui.stream.topic or settings.kafka.topics.processed_query_metrics
    group_id = settings.kafka.consumer_groups.get("ui_stream") or "ui-stream-group"

    try:
        return KafkaConsumer(
            topic,
            bootstrap_servers=settings.kafka.bootstrap_servers,
            group_id=group_id,
            enable_auto_commit=True,
            auto_offset_reset="latest",
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            consumer_timeout_ms=250,
            max_poll_records=500,
        )
    except NoBrokersAvailable:
        logger.warning("Kafka brokers unavailable for UI stream.")
        return None


def poll_stream_into_buffer(
    consumer: KafkaConsumer,
    buffer: StreamBuffer,
    *,
    max_poll_seconds: float = 0.5,
) -> int:
    """
    Poll Kafka for up to max_poll_seconds and append events into buffer.
    Returns number of appended events.
    """
    deadline = time.time() + max(0.0, max_poll_seconds)
    appended = 0

    while time.time() < deadline:
        records = consumer.poll(timeout_ms=200)
        if not records:
            break

        for _tp, msgs in records.items():
            for m in msgs:
                try:
                    evt = QueryMetricsEvent.model_validate(m.value)
                    ui_evt = UiQueryMetricsEvent.from_canonical(evt).model_dump()
                    buffer.append(ui_evt)
                    appended += 1
                except Exception as e:
                    logger.debug("UI stream dropped invalid msg: %s", e)

    return appended
