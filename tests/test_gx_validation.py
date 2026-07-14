"""Unit tests for src/quality/gx_validation.py.

These exercise the actual Great Expectations Core API calls the DAG relies
on (via `_assert_data_contracts` in dags/marketpulse_master_pipeline.py),
with both a passing batch and a batch engineered to fail each rule.
"""
import pandas as pd
import pytest

from src.quality.gx_validation import dq_gate


@pytest.fixture
def good_staging_prices():
    return pd.DataFrame({
        "fact_timestamp": pd.to_datetime(["2026-01-01T00:00:00", "2026-01-01T00:01:00"]),
        "asset_ticker": ["BTCUSDT", "ETHUSDT"],
        "asset_price": [65000.0, 3500.0],
        "traded_volume": [1.2, 3.4],
    })


@pytest.fixture
def good_staging_sentiment():
    return pd.DataFrame({
        "fact_timestamp": pd.to_datetime(["2026-01-01T00:00:00"]),
        "asset_ticker": ["BTCUSDT"],
        "raw_text": ["Bitcoin is pumping today"],
        "simulated_sentiment_score": [0.6],
        "analytics_source": ["sentiment_simulator"],
    })


def test_empty_batch_bypasses_validation():
    ok, details = dq_gate.validate_staging_market_prices(pd.DataFrame())
    assert ok is True
    assert "message" in details


def test_valid_staging_market_prices_passes(good_staging_prices):
    ok, details = dq_gate.validate_staging_market_prices(good_staging_prices)
    assert ok is True
    assert details["passed"] == details["total_checks"]


def test_negative_price_fails_validation(good_staging_prices):
    bad = good_staging_prices.copy()
    bad.loc[0, "asset_price"] = -500.0
    ok, details = dq_gate.validate_staging_market_prices(bad)
    assert ok is False
    assert details["passed"] < details["total_checks"]


def test_unknown_ticker_fails_validation(good_staging_prices):
    bad = good_staging_prices.copy()
    bad.loc[0, "asset_ticker"] = "DOGEUSDT"
    ok, _ = dq_gate.validate_staging_market_prices(bad)
    assert ok is False


def test_valid_staging_sentiment_passes(good_staging_sentiment):
    ok, details = dq_gate.validate_staging_sentiment(good_staging_sentiment)
    assert ok is True
    assert details["passed"] == details["total_checks"]


def test_sentiment_score_out_of_range_fails(good_staging_sentiment):
    bad = good_staging_sentiment.copy()
    bad.loc[0, "simulated_sentiment_score"] = 5.0
    ok, _ = dq_gate.validate_staging_sentiment(bad)
    assert ok is False


def test_short_text_payload_fails(good_staging_sentiment):
    bad = good_staging_sentiment.copy()
    bad.loc[0, "raw_text"] = "hi"
    ok, _ = dq_gate.validate_staging_sentiment(bad)
    assert ok is False
