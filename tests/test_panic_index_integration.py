"""Integration test for the panic-index math.

The panic index formula lives in SQL, in
transform_dbt/models/marts/fact_market_panic_index.sql - there is no
Python `calculate_windowed_panic_index` function (an earlier, fabricated
CI spec referenced one that never existed; see README > Known
Limitations). This test builds the real dbt project against the seed data
with `dbt seed && dbt run`, then queries the compiled DuckDB file to check
the two properties the audit called out:

  1. The index is bounded to [0.0, 1.0] for every row.
  2. BTCUSDT, whose seed data has the steepest last-tick price decline
     plus negative rolling sentiment, ends up with a materially higher
     panic score than the other two tickers at the same timestamp.

Skips (rather than fails) if `dbt` isn't on PATH, so `pytest tests/` still
runs cleanly for someone who hasn't installed the dbt-duckdb adapter.
"""
import shutil
import subprocess
from pathlib import Path

import duckdb
import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
DBT_PROJECT_DIR = REPO_ROOT / "transform_dbt"

pytestmark = pytest.mark.skipif(
    shutil.which("dbt") is None,
    reason="dbt is not installed; run `pip install dbt-duckdb` to enable this test",
)


@pytest.fixture(scope="module")
def built_warehouse():
    """Run `dbt seed && dbt run` once for this module and return the db path."""
    db_path = DBT_PROJECT_DIR / "marketpulse_local.db"
    db_path.unlink(missing_ok=True)

    env_args = ["--profiles-dir", str(DBT_PROJECT_DIR)]
    subprocess.run(["dbt", "seed", *env_args], cwd=DBT_PROJECT_DIR, check=True, capture_output=True)
    subprocess.run(["dbt", "run", *env_args], cwd=DBT_PROJECT_DIR, check=True, capture_output=True)

    yield db_path


def test_dbt_build_succeeds(built_warehouse):
    assert built_warehouse.exists()


def test_panic_index_is_bounded(built_warehouse):
    conn = duckdb.connect(str(built_warehouse), read_only=True)
    row = conn.execute(
        "SELECT min(market_panic_index), max(market_panic_index) FROM fact_market_panic_index"
    ).fetchone()
    conn.close()

    min_val, max_val = row
    assert min_val >= 0.0
    assert max_val <= 1.0


def test_price_drop_produces_elevated_panic_score(built_warehouse):
    """The formula is a *tick-over-tick* drop ratio, not a drop from the
    start of the series, so scores stay small in absolute terms for this
    seed data. What should hold is the relative ordering: BTCUSDT has the
    steepest last-tick decline in the seed data (65000 -> 62800 across 5
    ticks) plus negative rolling sentiment, so it should end with a
    strictly higher panic score than the other two tickers, which are
    flat-to-mildly-positive over the same window."""
    conn = duckdb.connect(str(built_warehouse), read_only=True)
    df = conn.execute(
        """
        select asset_ticker, market_panic_index
        from fact_market_panic_index
        qualify row_number() over (
            partition by asset_ticker order by fact_timestamp desc
        ) = 1
        """
    ).df()
    conn.close()

    scores = df.set_index("asset_ticker")["market_panic_index"]
    assert scores["BTCUSDT"] > scores["ETHUSDT"]
    assert scores["BTCUSDT"] > scores["SOLUSDT"]
    assert scores["BTCUSDT"] > 0.0


def test_dbt_tests_pass(built_warehouse):
    """Runs the real `dbt test` suite (schema tests + the singular test in
    transform_dbt/tests/) against the just-built warehouse."""
    result = subprocess.run(
        ["dbt", "test", "--profiles-dir", str(DBT_PROJECT_DIR)],
        cwd=DBT_PROJECT_DIR,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stdout[-3000:]
