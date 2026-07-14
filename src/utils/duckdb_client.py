"""Unified DuckDB connection helpers for MarketPulse read/write workloads."""

from __future__ import annotations

import os
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator, Optional

import duckdb
import pandas as pd

_PROJECT_ROOT = Path(__file__).resolve().parents[2]
_TRANSFORM_DBT_DIR = _PROJECT_ROOT / "transform_dbt"

# Computed relative to this file's location — works identically on any
# machine or container, unlike a hardcoded absolute path.
_DEFAULT_DB_PATH = str((_TRANSFORM_DBT_DIR / "marketpulse_local.db").resolve())


def _read_profiles_path() -> Optional[str]:
    """Parse the `path:` entry out of transform_dbt/profiles.yml, resolved
    relative to the transform_dbt directory (dbt itself treats that path
    as relative to the project dir, not the caller's CWD)."""
    profiles_file = _TRANSFORM_DBT_DIR / "profiles.yml"
    if not profiles_file.is_file():
        return None

    for line in profiles_file.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if stripped.startswith("path:"):
            value = stripped.split(":", 1)[1].strip().strip("'\"")
            candidate = Path(value)
            if not candidate.is_absolute():
                candidate = _TRANSFORM_DBT_DIR / candidate
            return os.path.normpath(str(candidate))
    return None


def _discover_project_db() -> Optional[str]:
    candidates = [
        _TRANSFORM_DBT_DIR / "marketpulse_local.db",
        Path.cwd() / "transform_dbt" / "marketpulse_local.db",
    ]
    for candidate in candidates:
        if candidate.is_file():
            return str(candidate.resolve())
    return None


def resolve_db_path() -> str:
    """Return the active DuckDB file path, aligned with dbt profiles when
    possible. Precedence: explicit env override > dbt profiles.yml >
    project-relative discovery > computed default (never a machine-specific
    hardcoded path)."""
    for candidate in (
        os.environ.get("MARKETPULSE_DB_PATH"),
        _read_profiles_path(),
        _discover_project_db(),
    ):
        if candidate and os.path.isfile(candidate):
            return os.path.normpath(candidate)

    # Fall back to the computed project-relative target even if the file
    # has not been created yet (e.g. before the first `dbt seed` run).
    return os.path.normpath(
        os.environ.get("MARKETPULSE_DB_PATH") or _DEFAULT_DB_PATH
    )


def db_exists() -> bool:
    return os.path.isfile(resolve_db_path())


def is_lock_error(exc: BaseException) -> bool:
    message = str(exc).lower()
    return "could not set lock" in message or "conflicting lock is held" in message


@contextmanager
def duckdb_connection(*, read_only: bool = True) -> Iterator[duckdb.DuckDBPyConnection]:
    """Open a short-lived DuckDB connection and always close it after use.

    `read_only=True` lets Streamlit/Airflow read the file concurrently with
    a writer holding it open elsewhere; only writers need the exclusive
    lock, and those go through `execute_write` below with retry-on-lock.
    """
    db_path = resolve_db_path()
    conn = duckdb.connect(database=db_path, read_only=read_only)
    try:
        yield conn
    finally:
        conn.close()


def query_dataframe(
    query: str,
    *,
    parameters: Optional[list[Any]] = None,
) -> pd.DataFrame:
    """
    Run a read-only query and return an in-memory DataFrame.
    Returns an empty DataFrame when the database is missing or temporarily
    locked, so callers (dashboard, DAG branch checks) degrade gracefully
    instead of crashing on transient contention.
    """
    if not db_exists():
        return pd.DataFrame()

    try:
        with duckdb_connection(read_only=True) as conn:
            if parameters is None:
                result = conn.execute(query)
            else:
                result = conn.execute(query, parameters)
            return result.df().copy()
    except duckdb.IOException as exc:
        if is_lock_error(exc):
            return pd.DataFrame()
        raise
    except Exception:
        return pd.DataFrame()


def execute_write(
    query: str,
    parameters: Optional[list[Any]] = None,
    *,
    max_retries: int = 3,
    retry_delay_seconds: float = 0.15,
) -> bool:
    """
    Execute a write statement inside a short-lived connection block.
    Retries briefly when another process (dbt/Airflow/Streamlit) holds the
    file lock, releasing it immediately after each attempt.
    """
    if not db_exists():
        return False

    for attempt in range(max_retries):
        try:
            with duckdb_connection(read_only=False) as conn:
                if parameters is None:
                    conn.execute(query)
                else:
                    conn.execute(query, parameters)
            return True
        except duckdb.IOException as exc:
            if is_lock_error(exc) and attempt < max_retries - 1:
                time.sleep(retry_delay_seconds * (attempt + 1))
                continue
            return False
        except Exception:
            return False

    return False


def ensure_landing_tables() -> None:
    """Create append-only landing tables used by the ingestion consumer if needed."""
    if not db_exists():
        return

    ddl_statements = [
        """
        CREATE TABLE IF NOT EXISTS raw_market_prices (
            timestamp TIMESTAMP,
            ticker VARCHAR,
            price DOUBLE,
            volume DOUBLE
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS raw_sentiment (
            timestamp TIMESTAMP,
            asset_tag VARCHAR,
            text_payload VARCHAR,
            sentiment_score DOUBLE,
            source VARCHAR
        )
        """,
    ]

    for statement in ddl_statements:
        execute_write(statement)
