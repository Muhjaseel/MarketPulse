import sys
from pathlib import Path

import duckdb
import streamlit as st
from streamlit_autorefresh import st_autorefresh

_STREAMLIT_ROOT = Path(__file__).resolve().parent
if str(_STREAMLIT_ROOT) not in sys.path:
    sys.path.insert(0, str(_STREAMLIT_ROOT))

# Reuse the same DB helpers as pages/*.py (db.py re-exports from
# src/utils/duckdb_client.py) instead of re-resolving the DuckDB path here.
# An earlier version of this file had its own inline path-resolution copy,
# which meant two places to keep in sync with any future change to how
# the DB path is discovered (env var > dbt profiles.yml > project-relative
# default — see resolve_db_path() in src/utils/duckdb_client.py).
from db import db_exists, query_dataframe, resolve_db_path  # noqa: E402

# Configure professional dashboard page layout
st.set_page_config(
    page_title="MarketPulse Analytics Command Center",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Poll DuckDB every 2 seconds without blocking widget interactions
st_autorefresh(interval=2000, limit=None, key="marketpulse_dashboard_refresh")


def _db_health() -> tuple[bool, str]:
    """Check the one thing this dashboard actually depends on: the DuckDB
    file dbt writes to. Returns (is_healthy, human-readable detail) instead
    of a hardcoded status string."""
    if not db_exists():
        return False, "not found - run `dbt seed && dbt run` in transform_dbt/"

    try:
        conn = duckdb.connect(database=resolve_db_path(), read_only=True)
        tables = conn.execute("SHOW TABLES").fetchall()
        conn.close()
        return True, f"{len(tables)} table(s) found at {resolve_db_path()}"
    except Exception as e:
        return False, f"exists but could not be opened ({e})"


# Main Landing Page Copy
st.title("📊 MarketPulse Data Platform Command Center")
st.markdown("---")
st.markdown("""
### Welcome to your MarketPulse Data Platform.
This dashboard reads directly from the **DuckDB** warehouse file that **dbt**
builds. Redpanda handles the streaming ingestion; the simulators and the
DuckDB sink populate the landing tables that dbt transforms into the marts
queried below.
""")

st.subheader("🧠 Data Warehouse Status")
db_healthy, db_detail = _db_health()
if db_healthy:
    st.success(f"DuckDB warehouse: {db_detail}")
else:
    st.warning(f"DuckDB warehouse: {db_detail}")

# High-level summary scorecard metrics block
st.subheader("📈 Core Tracked Asset Coverage Map")

# Deduplicates by asset_ticker to show each ticker's newest entry, ordered
# by fact_timestamp (the actual event time) rather than DuckDB's ROWID —
# `fact_market_panic_index` is a `+materialized: table` that dbt fully
# rebuilds on every run, so row insertion order (and therefore ROWID) has
# no guaranteed relationship to which row is chronologically newest.
query = """
    SELECT * FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY asset_ticker ORDER BY fact_timestamp DESC) as rn
        FROM fact_market_panic_index
    ) WHERE rn = 1
    ORDER BY asset_ticker ASC
"""
asset_df = query_dataframe(query)

if not asset_df.empty:
    cols = st.columns(len(asset_df))
    for idx, row in asset_df.iterrows():
        with cols[idx]:
            try:
                price_val = f"${float(row['asset_price']):,}"
            except (ValueError, TypeError):
                price_val = f"${row['asset_price']}"
                
            st.metric(
                label=f"Asset: {row['asset_ticker']}", 
                value=price_val, 
                delta=f"Panic Score: {row['market_panic_index']}"
            )
else:
    st.info("💡 Ingestion pipelines are starting or waiting for your first `dbt run` compilation step. Data marts metrics will display here momentarily.")

st.sidebar.success("Select a detailed analytical page above to explore deeper metrics.")