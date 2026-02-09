from __future__ import annotations

import sys
import time
from pathlib import Path

import pandas as pd
import streamlit as st

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.common.settings import Settings
from src.storage.duckdb_client import DuckDBClient
from src.ui.components.queries import (
    fetch_distinct_deployment_types,
    fetch_recent_processed,
    fetch_rollups_last_minutes,
)
from src.ui.components.streaming import StreamBuffer, make_ui_stream_consumer, poll_stream_into_buffer

st.set_page_config(page_title="Live Dashboard", layout="wide")


def _get_settings() -> Settings:
    if "settings" not in st.session_state:
        st.session_state["settings"] = Settings.load()
    return st.session_state["settings"]


def _get_duckdb_ro(settings: Settings) -> DuckDBClient:
    """
    IMPORTANT (Windows): do NOT keep an open DuckDB connection in session_state.
    Keep a lightweight client and open connections only for the duration of each query.
    """
    if "duckdb_ro" not in st.session_state:
        st.session_state["duckdb_ro"] = DuckDBClient.from_settings(settings).as_read_only(
            busy_timeout_ms=30_000
        )
    return st.session_state["duckdb_ro"]


def _ensure_stream_objects(settings: Settings) -> None:
    if "stream_buffer" not in st.session_state:
        st.session_state["stream_buffer"] = StreamBuffer(max_size=settings.ui.stream.max_buffer_size)

    if "stream_consumer" not in st.session_state and settings.ui.stream.enabled:
        st.session_state["stream_consumer"] = make_ui_stream_consumer(settings)

    if "stream_enabled" not in st.session_state:
        st.session_state["stream_enabled"] = bool(settings.ui.stream.enabled)


def _render_stream_panel(settings: Settings) -> None:
    st.subheader("Live Stream (Kafka → UI)")

    col_a, col_b, col_c = st.columns([1, 1, 2])

    with col_a:
        if st.button("Start streaming", width="stretch"):
            st.session_state["stream_enabled"] = True

    with col_b:
        if st.button("Stop streaming", width="stretch"):
            st.session_state["stream_enabled"] = False

    with col_c:
        st.caption(
            f"Topic: `{settings.ui.stream.topic}` · Buffer: last {settings.ui.stream.max_buffer_size} events"
        )

    if not st.session_state.get("stream_enabled", False):
        st.info("Streaming paused.")
        return

    consumer = st.session_state.get("stream_consumer")
    buffer = st.session_state.get("stream_buffer")

    if consumer is None or buffer is None:
        st.warning(
            "Streaming is not available. Ensure Kafka is running and the UI can reach the broker."
        )
        return

    appended = poll_stream_into_buffer(consumer, buffer, max_poll_seconds=0.5)
    snapshot = buffer.snapshot()

    if snapshot:
        df = pd.DataFrame(snapshot)

        k1, k2, k3, k4 = st.columns(4)
        with k1:
            st.metric("Buffered events", len(snapshot))
        with k2:
            st.metric("New since last refresh", appended)
        with k3:
            st.metric("Queued ratio (buffer)", float(df["queued"].mean()) if "queued" in df else 0.0)
        with k4:
            st.metric(
                "Avg duration (s)",
                float(df["duration_seconds"].mean()) if "duration_seconds" in df else 0.0,
            )

        st.dataframe(
            df.sort_values("arrival_timestamp", ascending=False).head(200),
            width="stretch",
            hide_index=True,
        )
    else:
        st.write("No streamed events yet. Start the producer + consumer.")

    time.sleep(settings.ui.refresh_interval_seconds)
    st.rerun()


def _render_analytics_panel(settings: Settings) -> None:
    st.subheader("Stored Analytics (DuckDB)")

    db = _get_duckdb_ro(settings)
    tables = settings.storage.duckdb.tables
    processed_table = tables["processed"]
    rollups_table = tables["rollups"]

    deployment_types = ["all"]
    try:
        # Each call opens a short-lived read-only connection internally
        deployment_types += fetch_distinct_deployment_types(
            db.connect(),
            processed_table=processed_table,
        )
    except Exception:
        pass

    deployment_filter = st.selectbox("Deployment type", deployment_types, index=0)

    left, right = st.columns(2)

    with left:
        st.markdown("**Recent processed events (DuckDB)**")
        try:
            # open/close per query to avoid Windows locks
            with db.connect() as con:
                rel = fetch_recent_processed(
                    con,
                    processed_table=processed_table,
                    limit=500,
                    deployment_type=deployment_filter,
                )
                df = rel.df()
            st.dataframe(df, width="stretch", hide_index=True)
        except Exception as e:
            st.info(f"No DuckDB data yet: {e}")

    with right:
        st.markdown("**Rollups (last 60 minutes)**")
        try:
            with db.connect() as con:
                rel = fetch_rollups_last_minutes(
                    con,
                    rollups_table=rollups_table,
                    minutes=60,
                    deployment_type=deployment_filter,
                )
                rdf = rel.df()

            if not rdf.empty:
                st.line_chart(
                    rdf.pivot(index="window_start", columns="deployment_type", values="avg_duration_seconds")
                )
            st.dataframe(rdf, width="stretch", hide_index=True)
        except Exception as e:
            st.info(f"No rollups yet: {e}")


def main() -> None:
    settings = _get_settings()
    _ensure_stream_objects(settings)

    st.title("Redshift Streaming Analytics — Live Dashboard")

    stream_col, analytics_col = st.columns([1, 1])

    with stream_col:
        _render_stream_panel(settings)

    with analytics_col:
        _render_analytics_panel(settings)


if __name__ == "__main__":
    main()
