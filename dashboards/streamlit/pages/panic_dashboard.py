import sys
from pathlib import Path

import streamlit as st

_PAGES_ROOT = Path(__file__).resolve().parent
_STREAMLIT_ROOT = _PAGES_ROOT.parent
if str(_STREAMLIT_ROOT) not in sys.path:
    sys.path.insert(0, str(_STREAMLIT_ROOT))

from db import db_exists, query_dataframe  # noqa: E402

st.title("🚨 Live Market Panic Index Tracker")
st.markdown(
    "Combines price-drop ratio and rolling sentiment score (from a template-based "
    "simulator, not a trained NLP model) to compute a bounded panic index."
)

# Ordered by fact_timestamp (event time), not ROWID: fact_market_panic_index
# is a `+materialized: table` that dbt fully rebuilds on every run, so row
# insertion order (and therefore ROWID) isn't guaranteed to match
# chronological order.
df = query_dataframe("""
    SELECT fact_timestamp, asset_ticker, asset_price, market_panic_index
    FROM fact_market_panic_index
    ORDER BY fact_timestamp DESC
    LIMIT 200
""")

if not df.empty:
    selected_asset = st.selectbox("Filter Target Asset:", df["asset_ticker"].unique())
    filtered_df = df[df["asset_ticker"] == selected_asset].sort_values("fact_timestamp")

    st.subheader(f"Rolling 5-Minute Panic Index Trend: {selected_asset}")
    st.line_chart(data=filtered_df, x="fact_timestamp", y="market_panic_index", color="#FF4B4B")

    col1, col2 = st.columns(2)
    with col1:
        st.write("📊 Raw Analytical Mart View", filtered_df.tail(10))
    with col2:
        st.write(
            "💡 **How to interpret this index:** Scores above `0.7` indicate severe price drops "
            "accompanied by highly negative conversation patterns (Panic Selling). Scores below "
            "`0.3` flag quiet accumulation trends."
        )
elif db_exists():
    st.info("⏳ Waiting for a clean database snapshot tick while dbt or ingestion workers refresh data...")
else:
    st.error(
        "❌ Analytical Database not compiled yet. Please execute your `dbt run` steps via your "
        "terminal window to create your tables."
    )
