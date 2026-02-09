from __future__ import annotations

from datetime import datetime, timezone

from src.common.settings import ProcessingConfig
from src.producer.metrics_calc import map_clean_enrich_row


def test_map_clean_enrich_row_clips_negative_values() -> None:
    processing = ProcessingConfig()
    row = {
        "query_id": "q-1",
        "deployment_type": "Provisioned",
        "arrival_timestamp": datetime(2024, 1, 1, tzinfo=timezone.utc),
        "queue_duration_ms": -10,
        "compile_duration_ms": -5,
        "execution_duration_ms": -1,
        "scanned_mb": -25.0,
        "spilled_mb": -1.0,
    }

    event = map_clean_enrich_row(row, processing)

    assert event.queue_duration_ms == 0
    assert event.compile_duration_ms == 0
    assert event.execution_duration_ms == 0
    assert event.scanned_mb == 0.0
    assert event.spilled_mb == 0.0
    assert event.deployment_type == "provisioned"
    assert event.duration_seconds >= 0
