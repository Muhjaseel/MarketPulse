"""Streamlit-facing DuckDB helpers (re-export from src.utils.duckdb_client)."""

from __future__ import annotations

import sys
from pathlib import Path

_STREAMLIT_ROOT = Path(__file__).resolve().parent
_PROJECT_ROOT = _STREAMLIT_ROOT.parents[1]

if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from src.utils.duckdb_client import (  # noqa: E402
    db_exists,
    query_dataframe,
    resolve_db_path,
)

__all__ = ["db_exists", "query_dataframe", "resolve_db_path"]
