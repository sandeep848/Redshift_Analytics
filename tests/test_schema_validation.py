from __future__ import annotations

from datetime import datetime, timezone

import pytest

from src.common.schema import QueryMetricsEvent


def test_query_metrics_event_rejects_future_arrival_after_end() -> None:
    with pytest.raises(ValueError):
        QueryMetricsEvent(
            query_id="q-1",
            deployment_type="provisioned",
            instance_id=None,
            arrival_timestamp=datetime(2024, 1, 1, 0, 5, tzinfo=timezone.utc),
            execution_start_time=datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc),
            execution_end_time=datetime(2024, 1, 1, 0, 1, tzinfo=timezone.utc),
            queue_duration_ms=0,
            compile_duration_ms=0,
            execution_duration_ms=0,
            scanned_mb=1.0,
            spilled_mb=0.0,
            duration_seconds=1.0,
            spill_pressure=0.0,
            queued=False,
        )
