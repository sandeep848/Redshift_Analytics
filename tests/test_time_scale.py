from __future__ import annotations

from datetime import datetime, timezone

from src.common.time_scale import TimeScaler


def test_time_scaler_maps_event_to_wall_time() -> None:
    anchor_event = datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc)
    anchor_wall = datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc)
    scaler = TimeScaler(factor=2, anchor_event_time=anchor_event, anchor_wall_time=anchor_wall)

    event_time = datetime(2024, 1, 1, 0, 2, tzinfo=timezone.utc)
    expected_wall = datetime(2024, 1, 1, 12, 1, tzinfo=timezone.utc)

    assert scaler.to_wall_time(event_time) == expected_wall
