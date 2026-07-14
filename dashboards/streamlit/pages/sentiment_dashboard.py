import sys
from pathlib import Path

import streamlit as st

_PAGES_ROOT = Path(__file__).resolve().parent
_STREAMLIT_ROOT = _PAGES_ROOT.parent
if str(_STREAMLIT_ROOT) not in sys.path:
    sys.path.insert(0, str(_STREAMLIT_ROOT))

from db import db_exists, query_dataframe  # noqa: E402

st.title("💬 Social Media Sentiment Deep-Dive")
st.markdown(
    "Explore the simulated sentiment feed (template posts with a jittered score, "
    "not a trained NLP model - see README) across tracked crypto assets."
)

df = query_dataframe("""
    SELECT fact_timestamp, asset_ticker, raw_text, simulated_sentiment_score, analytics_source
    FROM stg_sentiment
    ORDER BY fact_timestamp DESC
    LIMIT 100
""")

if not df.empty:
    selected_asset = st.selectbox("Select Asset to Audit:", df["asset_ticker"].unique())
    filtered_df = df[df["asset_ticker"] == selected_asset].sort_values("fact_timestamp")

    st.subheader(f"Current Narrative Pulse: {selected_asset}")
    avg_score = filtered_df["simulated_sentiment_score"].mean()

    if avg_score > 0.2:
        st.success(f"🟢 Overall Sentiment is Bullish ({round(avg_score, 2)})")
    elif avg_score < -0.2:
        st.error(f"🔴 Overall Sentiment is Bearish ({round(avg_score, 2)})")
    else:
        st.warning(f"🟡 Overall Sentiment is Neutral ({round(avg_score, 2)})")

    st.markdown("### Rolling Text Sentiment Spectrum")
    st.bar_chart(data=filtered_df, x="fact_timestamp", y="simulated_sentiment_score", color="#29B5E8")

    st.markdown("### 📋 Live Ingested Social Stream Logs")
    st.dataframe(
        filtered_df[["fact_timestamp", "raw_text", "simulated_sentiment_score"]],
        use_container_width=True,
        hide_index=True,
    )
elif db_exists():
    st.info("⏳ Sentiment tables are temporarily locked while dbt refreshes. Retrying on the next load...")
else:
    st.error(
        "❌ Analytical Database not compiled yet. Please run your `dbt run` command step via your "
        "terminal to generate tables."
    )
