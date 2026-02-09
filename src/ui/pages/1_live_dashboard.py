from __future__ import annotations

import sys
import time
from datetime import datetime, timezone
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


def _inject_styles() -> None:
    st.markdown(
        """
        <style>
        :root {
          --bg: #0b111f;
          --panel: #111a2b;
          --panel-soft: #162238;
          --accent: #5dd0ff;
          --accent-strong: #7c5cff;
          --muted: #8da2c0;
          --success: #23d18b;
          --warning: #ffb347;
        }
        .stApp {
          background: radial-gradient(1200px 600px at 10% -20%, #1b2a4b 0%, transparent 60%),
            radial-gradient(900px 700px at 100% 0%, #201833 0%, transparent 55%),
            var(--bg);
          color: #e8eef9;
        }
        .hero {
          background: linear-gradient(120deg, rgba(93,208,255,0.12), rgba(124,92,255,0.08));
          border: 1px solid rgba(124, 92, 255, 0.25);
          border-radius: 18px;
          padding: 22px 28px;
          margin-bottom: 18px;
          box-shadow: 0 10px 40px rgba(5, 10, 20, 0.4);
        }
        .hero h1 {
          font-size: 2.1rem;
          margin-bottom: 0.25rem;
        }
        .hero p {
          color: var(--muted);
          margin: 0.1rem 0 0;
        }
        .panel {
          background: linear-gradient(160deg, rgba(22, 34, 56, 0.9), rgba(12, 18, 32, 0.9));
          border: 1px solid rgba(93, 208, 255, 0.15);
          border-radius: 16px;
          padding: 16px 18px;
          box-shadow: 0 8px 24px rgba(2, 7, 15, 0.45);
        }
        .panel h3 {
          margin: 0 0 0.4rem;
        }
        .live-pill {
          display: inline-flex;
          align-items: center;
          gap: 8px;
          background: rgba(35, 209, 139, 0.12);
          color: var(--success);
          padding: 6px 10px;
          border-radius: 999px;
          font-size: 0.8rem;
          border: 1px solid rgba(35, 209, 139, 0.4);
        }
        .live-dot {
          height: 8px;
          width: 8px;
          background: var(--success);
          border-radius: 50%;
          box-shadow: 0 0 0 rgba(35, 209, 139, 0.7);
          animation: pulse 1.6s infinite;
        }
        @keyframes pulse {
          0% { box-shadow: 0 0 0 0 rgba(35, 209, 139, 0.7); }
          70% { box-shadow: 0 0 0 8px rgba(35, 209, 139, 0); }
          100% { box-shadow: 0 0 0 0 rgba(35, 209, 139, 0); }
        }
        .muted {
          color: var(--muted);
        }
        .metric-card {
          background: rgba(17, 26, 43, 0.9);
          border-radius: 14px;
          border: 1px solid rgba(124, 92, 255, 0.25);
          padding: 12px 14px;
        }
        .metric-card span {
          display: block;
          font-size: 0.8rem;
          color: var(--muted);
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


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


def _format_timestamp(ts: object) -> str:
    if isinstance(ts, datetime):
        return ts.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    return "—"


def _sidebar_controls(settings: Settings) -> dict:
    st.sidebar.markdown("## Control Deck")
    st.sidebar.caption("Tune the live stream, filters, and visual preferences.")

    stream_toggle = st.sidebar.toggle(
        "Live streaming",
        value=st.session_state.get("stream_enabled", True),
        help="Pause or resume the real-time Kafka stream.",
    )
    st.session_state["stream_enabled"] = stream_toggle

    refresh_rate = st.sidebar.slider(
        "Refresh rate (seconds)",
        min_value=1,
        max_value=15,
        value=int(settings.ui.refresh_interval_seconds),
        help="Controls how often the dashboard refreshes.",
    )
    st.sidebar.markdown("---")
    st.sidebar.markdown("### Data filters")
    deployment_filter = st.sidebar.selectbox(
        "Deployment type",
        ["all", "provisioned", "serverless"],
        help="Filter the live stream and analytics by deployment type.",
    )
    view_mode = st.sidebar.radio(
        "View density",
        ["Compact", "Comfortable"],
        index=1,
        help="Adjust spacing for charts and tables.",
    )
    return {
        "refresh_rate": refresh_rate,
        "deployment_filter": deployment_filter,
        "view_mode": view_mode,
    }


def _render_stream_panel(
    settings: Settings,
    *,
    deployment_filter: str,
    refresh_rate: int,
) -> None:
    st.markdown("### Live Stream Command Center")
    st.caption("Kafka → UI stream with real-time signal health and data previews.")

    header = st.container()
    with header:
        col_a, col_b, col_c, col_d = st.columns([1.1, 1.1, 1.4, 1.4])
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
        with col_d:
            if st.session_state.get("stream_enabled", False):
                st.markdown(
                    "<span class='live-pill'><span class='live-dot'></span>Live feed active</span>",
                    unsafe_allow_html=True,
                )
            else:
                st.markdown("<span class='live-pill'>Paused</span>", unsafe_allow_html=True)

    if not st.session_state.get("stream_enabled", False):
        st.info("Streaming paused. Use the toggle or Start button to resume.")
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
        df["arrival_timestamp"] = pd.to_datetime(df["arrival_timestamp"], utc=True, errors="coerce")

        if deployment_filter != "all" and "deployment_type" in df.columns:
            df = df[df["deployment_type"] == deployment_filter]

        last_update = df["arrival_timestamp"].max() if not df.empty else None

        k1, k2, k3, k4, k5 = st.columns(5)
        with k1:
            st.metric("Buffered events", len(df))
        with k2:
            st.metric("New since refresh", appended)
        with k3:
            queued_ratio = float(df["queued"].mean()) if "queued" in df else 0.0
            st.metric("Queued ratio", f"{queued_ratio * 100:.1f}%")
        with k4:
            avg_duration = float(df["duration_seconds"].mean()) if "duration_seconds" in df else 0.0
            st.metric("Avg duration (s)", f"{avg_duration:.2f}")
        with k5:
            st.metric("Last update", _format_timestamp(last_update))

        st.progress(min(max(queued_ratio, 0.0), 1.0))
        st.caption("Queued ratio gauge (live buffer).")

        chart_cols = st.columns(2)
        with chart_cols[0]:
            st.markdown("**Duration trend**")
            st.line_chart(
                df.sort_values("arrival_timestamp")
                .set_index("arrival_timestamp")["duration_seconds"],
                height=240,
            )
        with chart_cols[1]:
            st.markdown("**Queued events over time**")
            queue_series = (
                df.assign(queued_int=df["queued"].astype(int))
                .sort_values("arrival_timestamp")
                .set_index("arrival_timestamp")["queued_int"]
            )
            st.area_chart(queue_series, height=240)

        st.markdown("**Live event stream**")
        st.dataframe(
            df.sort_values("arrival_timestamp", ascending=False).head(200),
            width="stretch",
            hide_index=True,
        )

        st.download_button(
            "Export live stream CSV",
            df.to_csv(index=False).encode("utf-8"),
            file_name="live_stream.csv",
            mime="text/csv",
        )
    else:
        st.info("No streamed events yet. Start the producer + consumer.")

    time.sleep(refresh_rate)
    st.rerun()


def _render_analytics_panel(settings: Settings, *, deployment_filter: str) -> None:
    st.subheader("Stored Analytics (DuckDB)")
    st.caption("Historic rollups with filters, drill-down tables, and trend views.")

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

    if deployment_filter not in deployment_types:
        deployment_filter = "all"
    deployment_filter = st.selectbox(
        "Deployment type",
        deployment_types,
        index=deployment_types.index(deployment_filter),
    )

    left, right = st.columns([1.1, 1])

    with left:
        st.markdown("**Recent processed events**")
        try:
            with db.connect() as con:
                rel = fetch_recent_processed(
                    con,
                    processed_table=processed_table,
                    limit=500,
                    deployment_type=deployment_filter,
                )
                df = rel.df()
            if not df.empty:
                df["arrival_timestamp"] = pd.to_datetime(df["arrival_timestamp"], utc=True)
                st.dataframe(df, width="stretch", hide_index=True)
            else:
                st.info("No processed events for the selected filter yet.")
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
                    rdf.pivot(index="window_start", columns="deployment_type", values="avg_duration_seconds"),
                    height=220,
                )
                st.bar_chart(
                    rdf.pivot(index="window_start", columns="deployment_type", values="queued_ratio"),
                    height=220,
                )
            st.dataframe(rdf, width="stretch", hide_index=True)
        except Exception as e:
            st.info(f"No rollups yet: {e}")


def main() -> None:
    settings = _get_settings()
    _ensure_stream_objects(settings)

    _inject_styles()
    ui_state = _sidebar_controls(settings)

    st.markdown(
        """
        <section class="hero">
          <h1>🚀 Redshift Streaming Analytics</h1>
          <p>Live command center for streaming query health, queue pressure, and performance trends.</p>
        </section>
        """,
        unsafe_allow_html=True,
    )

    top_cols = st.columns([1.2, 1.2, 1])
    with top_cols[0]:
        st.markdown(
            """
            <div class="panel">
              <h3>Live Status</h3>
              <p class="muted">Streaming starts automatically on load. Use the sidebar to pause or resume.</p>
              <p class="muted">Refresh rate: <strong>{refresh_rate}s</strong></p>
            </div>
            """.format(
                refresh_rate=ui_state["refresh_rate"]
            ),
            unsafe_allow_html=True,
        )
    with top_cols[1]:
        st.markdown(
            """
            <div class="panel">
              <h3>Deployment Focus</h3>
              <p class="muted">Currently viewing: <strong>{deployment}</strong></p>
              <p class="muted">View mode: <strong>{view_mode}</strong></p>
            </div>
            """.format(
                deployment=ui_state["deployment_filter"].title(),
                view_mode=ui_state["view_mode"],
            ),
            unsafe_allow_html=True,
        )
    with top_cols[2]:
        now = datetime.now(tz=timezone.utc)
        st.markdown(
            """
            <div class="panel">
              <h3>System Time</h3>
              <p class="muted">{timestamp}</p>
              <p class="muted">All metrics shown in UTC.</p>
            </div>
            """.format(timestamp=_format_timestamp(now)),
            unsafe_allow_html=True,
        )

    stream_col, analytics_col = st.columns([1.1, 1])

    with stream_col:
        _render_stream_panel(
            settings,
            deployment_filter=ui_state["deployment_filter"],
            refresh_rate=ui_state["refresh_rate"],
        )

    with analytics_col:
        _render_analytics_panel(settings, deployment_filter=ui_state["deployment_filter"])


if __name__ == "__main__":
    main()
