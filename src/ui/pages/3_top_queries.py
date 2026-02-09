from __future__ import annotations

import sys
from datetime import timedelta
from pathlib import Path

import streamlit as st

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.common.settings import Settings
from src.storage.duckdb_client import DuckDBClient
from src.ui.components.filters import deployment_filter, time_window_filter
from src.ui.components.queries import SQL_TOP_QUERIES

st.set_page_config(page_title="Top Queries", layout="wide")


def main() -> None:
    settings = Settings.load()
    db = DuckDBClient.from_settings(settings).as_read_only(busy_timeout_ms=30_000)

    st.header("🔎 Top Queries")
    st.caption("Recent heavy queries (scan/spill/queue/compile) from DuckDB processed events")

    with st.sidebar:
        st.subheader("Filters")

        deployment = deployment_filter()
        window_minutes = time_window_filter(default_minutes=60)

        metric = st.selectbox(
            "Rank by",
            options=["scanned_mb", "spilled_mb", "queue_duration_ms", "execution_duration_ms", "compile_duration_ms"],
            index=0,
        )
        limit = st.slider("Top N", 10, 200, 50)

    # Use processed table (consumer writes there)
    processed_table = settings.storage.duckdb.tables["processed"]

    # Anchor window using latest processed arrival_timestamp
    latest = db.fetchall(f"SELECT max(arrival_timestamp) FROM {processed_table}")[0][0]
    if latest is None:
        st.info("No data yet. Run the producer + consumer first.")
        return

    start_ts = latest - timedelta(minutes=window_minutes)

    # If your SQL_TOP_QUERIES references query_metrics_raw, update it to use processed_table,
    # or inject the table name here if SQL_TOP_QUERIES is a template.
    sql = SQL_TOP_QUERIES.format(table=processed_table, metric=metric)

    df = db.fetchdf(
        sql,
        params=[start_ts, latest, deployment, deployment, limit],
    )

    if df.empty:
        st.info("No queries in selected window.")
        return

    st.subheader(f"Top {limit} queries by `{metric}`")
    st.dataframe(df, width="stretch", hide_index=True)

    c1, c2, c3 = st.columns(3)
    c1.metric("Count", f"{len(df):,}")
    if "scanned_mb" in df:
        c2.metric("Total scanned (MB)", f"{df['scanned_mb'].sum():,.1f}")
    if "spilled_mb" in df:
        c3.metric("Total spilled (MB)", f"{df['spilled_mb'].sum():,.1f}")


if __name__ == "__main__":
    main()
