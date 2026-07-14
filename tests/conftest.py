"""Shared pytest fixtures for the MarketPulse test suite.

Tests that need a DuckDB file use the `temp_db_path` fixture, which points
`MARKETPULSE_DB_PATH` at a throwaway file for the duration of the test so
nothing here touches the real transform_dbt/marketpulse_local.db used by
`dbt run` / the dashboard.
"""
import sys
from pathlib import Path

import pytest

# Make `src` importable when pytest is run from the repo root.
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


@pytest.fixture
def temp_db_path(tmp_path, monkeypatch):
    """Point MARKETPULSE_DB_PATH at an empty DuckDB file for one test."""
    import duckdb

    db_file = tmp_path / "test_marketpulse.db"
    conn = duckdb.connect(str(db_file))
    conn.close()

    monkeypatch.setenv("MARKETPULSE_DB_PATH", str(db_file))
    yield str(db_file)
