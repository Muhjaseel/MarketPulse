"""Unit tests for src/utils/duckdb_client.py."""
import os

import pandas as pd

from src.utils.duckdb_client import (
    db_exists,
    ensure_landing_tables,
    execute_write,
    is_lock_error,
    query_dataframe,
    resolve_db_path,
)


def test_resolve_db_path_respects_env_override(temp_db_path):
    assert resolve_db_path() == os.path.normpath(temp_db_path)


def test_db_exists_true_for_created_file(temp_db_path):
    assert db_exists() is True


def test_db_exists_false_for_missing_file(tmp_path, monkeypatch):
    missing = tmp_path / "does_not_exist.db"
    monkeypatch.setenv("MARKETPULSE_DB_PATH", str(missing))
    assert db_exists() is False


def test_is_lock_error_matches_duckdb_lock_messages():
    assert is_lock_error(Exception("Could not set lock on file")) is True
    assert is_lock_error(Exception("Conflicting lock is held by process 123")) is True
    assert is_lock_error(Exception("table not found")) is False


def test_query_dataframe_returns_empty_df_when_db_missing(tmp_path, monkeypatch):
    missing = tmp_path / "does_not_exist.db"
    monkeypatch.setenv("MARKETPULSE_DB_PATH", str(missing))
    df = query_dataframe("SELECT 1")
    assert isinstance(df, pd.DataFrame)
    assert df.empty


def test_ensure_landing_tables_and_write_roundtrip(temp_db_path):
    ensure_landing_tables()

    written = execute_write(
        "INSERT INTO raw_market_prices (timestamp, ticker, price, volume) VALUES (?, ?, ?, ?)",
        ["2026-01-01T00:00:00", "BTCUSDT", 65000.0, 1.5],
    )
    assert written is True

    df = query_dataframe("SELECT * FROM raw_market_prices")
    assert len(df) == 1
    assert df.iloc[0]["ticker"] == "BTCUSDT"
    assert df.iloc[0]["price"] == 65000.0


def test_execute_write_returns_false_for_missing_db(tmp_path, monkeypatch):
    missing = tmp_path / "does_not_exist.db"
    monkeypatch.setenv("MARKETPULSE_DB_PATH", str(missing))
    result = execute_write("INSERT INTO raw_market_prices VALUES (1)")
    assert result is False
