from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.common.settings import Settings  # noqa: E402

st.set_page_config(page_title="Redshift Streaming Analytics", layout="wide")


@st.cache_resource
def _settings() -> Settings:
    return Settings.load()


def main() -> None:
    settings = _settings()

    st.title("Redshift Streaming Analytics")

    with st.expander("System status", expanded=True):
        st.markdown(f"**Kafka bootstrap:** `{settings.kafka.bootstrap_servers}`")
        st.markdown(f"**Processed topic:** `{settings.kafka.topics.processed_query_metrics}`")
        st.markdown(f"**UI stream topic:** `{settings.ui.stream.topic}`")
        st.markdown(f"**DuckDB path:** `{settings.storage.duckdb.path}`")

    st.info("Use the sidebar to open the Live Dashboard and other pages.")


if __name__ == "__main__":
    main()
