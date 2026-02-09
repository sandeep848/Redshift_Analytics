from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

import pandas as pd
import streamlit as st

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.common.settings import Settings
from src.storage.redshift_client import RedshiftClient
from src.ui.components.charts import line_chart_mb
from src.ui.components.filters import deployment_filter
from src.ui.components.queries import SQL_HISTORICAL_DEPLOYMENT


st.set_page_config(page_title="Historical Dashboard", layout="wide")


def main() -> None:
    settings = Settings.load()

    st.header("🕰️ Historical Dashboard")
    st.caption("Long-range analytics from Redshift")

    # --------------------------------------------------
    # Filters
    # --------------------------------------------------
    with st.sidebar:
        st.subheader("Filters")

        deployment = deployment_filter()

        start_date = st.date_input(
            "Start date",
            value=date.today().replace(day=1),
        )
        end_date = st.date_input(
            "End date",
            value=date.today(),
        )

    if start_date > end_date:
        st.error("Start date must be before end date")
        return

    # --------------------------------------------------
    # Connect to Redshift (graceful if not available)
    # --------------------------------------------------
    try:
        rs = RedshiftClient.from_settings(settings).as_read_only()
        with rs._connect() as _:
            pass
    except Exception as e:
        st.warning("Redshift not available. Configure Redshift connection or skip this page.")
        st.caption(str(e))
        return

    # --------------------------------------------------
    # Query Redshift
    # --------------------------------------------------
    with st.spinner("Querying Redshift…"):
        with rs._connect() as conn:
            with conn.cursor() as cur:
                # NOTE: SQL expects 4 params due to (%s='all' OR deployment_type=%s)
                cur.execute(
                    SQL_HISTORICAL_DEPLOYMENT,
                    (start_date, end_date, deployment, deployment),
                )
                cols = [c.name for c in cur.description]
                rows = cur.fetchall()

    if not rows:
        st.info("No data for selected range")
        return

    df = pd.DataFrame(rows, columns=cols)

    # --------------------------------------------------
    # KPIs
    # --------------------------------------------------
    c1, c2 = st.columns(2)
    c1.metric("Total Scanned (MB)", f"{df['scanned_mb'].sum():,.0f}")
    c2.metric("Total Spilled (MB)", f"{df['spilled_mb'].sum():,.0f}")

    # --------------------------------------------------
    # Charts
    # --------------------------------------------------
    st.subheader("Daily Throughput")
    line_chart_mb(
        df,
        x="bucket_start",
        y=["scanned_mb", "spilled_mb"],
    )

    # --------------------------------------------------
    # Raw
    # --------------------------------------------------
    with st.expander("Raw data"):
        st.dataframe(df, use_container_width=True)


if __name__ == "__main__":
    main()

