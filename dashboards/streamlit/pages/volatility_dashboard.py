import sys
from pathlib import Path

import streamlit as st

_PAGES_ROOT = Path(__file__).resolve().parent
_STREAMLIT_ROOT = _PAGES_ROOT.parent
if str(_STREAMLIT_ROOT) not in sys.path:
    sys.path.insert(0, str(_STREAMLIT_ROOT))

from db import db_exists, query_dataframe  # noqa: E402

st.title("📈 Market Volatility & Anomalies Tracker")
st.markdown(
    "Monitor rolling pricing standard deviations, spot price-drop velocities, "
    "and detected financial anomalies — computed once in dbt "
    "(`transform_dbt/models/marts/fact_market_anomalies.sql`) rather than "
    "recomputed here, so the dashboard and the warehouse always agree on "
    "the same numbers."
)

# Sourced from the dbt mart (not recomputed in pandas) on purpose: an
# earlier version of this page re-derived its own rolling mean/stddev
# here, duplicating the same window-function logic already computed in
# int_volatility.sql / fact_market_anomalies.sql. That meant two places
# to update if the volatility formula ever changed. Querying the mart
# directly removes that second source of truth.
df = query_dataframe("""
    SELECT
        fact_timestamp,
        asset_ticker,
        asset_price AS spot_price,
        traded_volume AS trading_volume,
        local_volatility_score AS rolling_volatility,
        is_structural_anomaly
    FROM fact_market_anomalies
    ORDER BY fact_timestamp DESC
    LIMIT 200
""")

if not df.empty:
    selected_asset = st.selectbox("Select Asset to Analyze:", df["asset_ticker"].unique())
    filtered_df = df[df["asset_ticker"] == selected_asset].sort_values("fact_timestamp")

    col1, col2 = st.columns(2)
    with col1:
        st.metric("Latest Spot Price", f"${filtered_df['spot_price'].iloc[-1]:,.4f}")
    with col2:
        current_vol = filtered_df["rolling_volatility"].iloc[-1]
        st.metric("Current 5-Tick Volatility Dev", f"{current_vol:.6f}")

    st.markdown("### Price Vector vs. Volatility Deviation")
    st.markdown("**Spot Price Trend**")
    st.line_chart(data=filtered_df, x="fact_timestamp", y="spot_price", color="#1f77b4")

    st.markdown("**Rolling Volatility Dev Deviation Area**")
    st.area_chart(data=filtered_df, x="fact_timestamp", y="rolling_volatility", color="#d62728")

    st.markdown("### 🚨 Detected Price Swings (dbt-flagged structural anomalies)")
    anomaly_df = filtered_df[filtered_df["is_structural_anomaly"]].sort_values(
        "rolling_volatility", ascending=False
    )

    if not anomaly_df.empty:
        st.dataframe(
            anomaly_df[["fact_timestamp", "spot_price", "rolling_volatility", "trading_volume"]],
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.info("No structural anomalies flagged in the recent stream window block.")
elif db_exists():
    st.info("⏳ Price tables are temporarily locked while dbt refreshes. Retrying on the next load...")
else:
    st.error(
        "❌ Analytical Database not compiled yet. Please run your `dbt run` command step via your "
        "terminal to generate tables."
    )
