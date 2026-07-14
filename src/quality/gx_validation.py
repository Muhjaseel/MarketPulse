import pandas as pd
import great_expectations as gx
from typing import Dict, Any, Tuple
from src.utils.logger import get_logger

logger = get_logger(__name__)

class StreamDataQualityGate:
    """
    Leverages Great Expectations (GX Core API) to programmatically run
    in-memory data quality assertions against raw ingestion payloads and
    against the compiled dbt staging views.
    """
    def __init__(self):
        self.context = gx.get_context()
        self.datasource_name = "in_memory_stream_source"

        try:
            self.datasource = self.context.data_sources.add_pandas(name=self.datasource_name)
        except Exception:
            self.datasource = self.context.data_sources.get(self.datasource_name)

    def _validate_dataframe(self, batch_df: pd.DataFrame, asset_name: str, batch_def_name: str, checks_factory):
        """Shared batch/asset plumbing for the per-schema validators below."""
        try:
            asset = self.datasource.add_dataframe_asset(name=asset_name)
        except Exception:
            asset = self.datasource.get_asset(name=asset_name)

        batch_definition = asset.add_batch_definition_whole_dataframe(batch_def_name)
        batch = batch_definition.get_batch(batch_parameters={"dataframe": batch_df})

        checks = checks_factory(batch)
        success = all(result.success for result in checks)

        asset.delete_batch_definition(batch_def_name)

        return success, {"total_checks": len(checks), "passed": sum(1 for c in checks if c.success)}

    # ------------------------------------------------------------------
    # Raw / landing-table schema validators (consumer-side, pre-dbt)
    # ------------------------------------------------------------------
    def validate_market_prices(self, batch_df: pd.DataFrame) -> Tuple[bool, Dict[str, Any]]:
        """Validates incoming streaming tickers for structural and numeric accuracy."""
        if batch_df.empty:
            return True, {"message": "Empty batch bypassed validation"}

        from great_expectations import expectations as e

        def checks_factory(batch):
            return [
                batch.validate(e.ExpectColumnValuesToNotBeNull(column="timestamp")),
                batch.validate(e.ExpectColumnValuesToNotBeNull(column="ticker")),
                batch.validate(e.ExpectColumnValuesToBeInSet(column="ticker", value_set=["BTCUSDT", "ETHUSDT", "SOLUSDT"])),
                batch.validate(e.ExpectColumnValuesToNotBeNull(column="price")),
                batch.validate(e.ExpectColumnValuesToBeBetween(column="price", min_value=0.0, strict_min=True)),
                batch.validate(e.ExpectColumnValuesToBeBetween(column="volume", min_value=0.0)),
            ]

        return self._validate_dataframe(batch_df, "market_prices_batch", "market_prices_def", checks_factory)

    def validate_social_sentiment(self, batch_df: pd.DataFrame) -> Tuple[bool, Dict[str, Any]]:
        """Validates streaming text strings for structural accuracy and NLP metrics ranges."""
        if batch_df.empty:
            return True, {"message": "Empty batch bypassed validation"}

        from great_expectations import expectations as e

        def checks_factory(batch):
            return [
                batch.validate(e.ExpectColumnValuesToNotBeNull(column="timestamp")),
                batch.validate(e.ExpectColumnValuesToNotBeNull(column="asset_tag")),
                batch.validate(e.ExpectColumnValuesToNotBeNull(column="sentiment_score")),
                batch.validate(e.ExpectColumnValueLengthsToBeBetween(column="text_payload", min_value=3)),
                batch.validate(e.ExpectColumnValuesToBeBetween(column="sentiment_score", min_value=-1.0, max_value=1.0)),
            ]

        return self._validate_dataframe(batch_df, "social_sentiment_batch", "sentiment_def", checks_factory)

    # ------------------------------------------------------------------
    # dbt staging-view validators (post-transform, used by the Airflow
    # data-contract gate). Column names here match the actual `select`
    # aliases in transform_dbt/models/staging/*.sql — NOT the raw landing
    # table columns above, which is what the DAG needs to check.
    # ------------------------------------------------------------------
    def validate_staging_market_prices(self, batch_df: pd.DataFrame) -> Tuple[bool, Dict[str, Any]]:
        """Validates stg_market_prices (fact_timestamp, asset_ticker, asset_price, traded_volume)."""
        if batch_df.empty:
            return True, {"message": "Empty batch bypassed validation"}

        from great_expectations import expectations as e

        def checks_factory(batch):
            return [
                batch.validate(e.ExpectColumnValuesToNotBeNull(column="fact_timestamp")),
                batch.validate(e.ExpectColumnValuesToNotBeNull(column="asset_ticker")),
                batch.validate(e.ExpectColumnValuesToBeInSet(column="asset_ticker", value_set=["BTCUSDT", "ETHUSDT", "SOLUSDT"])),
                batch.validate(e.ExpectColumnValuesToNotBeNull(column="asset_price")),
                batch.validate(e.ExpectColumnValuesToBeBetween(column="asset_price", min_value=0.0, strict_min=True)),
                batch.validate(e.ExpectColumnValuesToBeBetween(column="traded_volume", min_value=0.0)),
            ]

        return self._validate_dataframe(batch_df, "stg_market_prices_batch", "stg_market_prices_def", checks_factory)

    def validate_staging_sentiment(self, batch_df: pd.DataFrame) -> Tuple[bool, Dict[str, Any]]:
        """Validates stg_sentiment (fact_timestamp, asset_ticker, raw_text, simulated_sentiment_score, analytics_source)."""
        if batch_df.empty:
            return True, {"message": "Empty batch bypassed validation"}

        from great_expectations import expectations as e

        def checks_factory(batch):
            return [
                batch.validate(e.ExpectColumnValuesToNotBeNull(column="fact_timestamp")),
                batch.validate(e.ExpectColumnValuesToNotBeNull(column="asset_ticker")),
                batch.validate(e.ExpectColumnValuesToNotBeNull(column="simulated_sentiment_score")),
                batch.validate(e.ExpectColumnValueLengthsToBeBetween(column="raw_text", min_value=3)),
                batch.validate(e.ExpectColumnValuesToBeBetween(column="simulated_sentiment_score", min_value=-1.0, max_value=1.0)),
            ]

        return self._validate_dataframe(batch_df, "stg_sentiment_batch", "stg_sentiment_def", checks_factory)


class _LazyDQGate:
    """Defers `gx.get_context()` (and the datasource setup that follows it)
    until the gate is actually used, instead of paying that cost — and any
    risk of it failing in an environment where GX isn't configured — the
    moment this module is imported. `dags/marketpulse_master_pipeline.py`
    and the test suite both just do `from ... import dq_gate` and call a
    method on it, so this stays a drop-in replacement for the old
    module-level instance."""

    def __init__(self) -> None:
        self._instance: StreamDataQualityGate | None = None

    def _get(self) -> StreamDataQualityGate:
        if self._instance is None:
            self._instance = StreamDataQualityGate()
        return self._instance

    def __getattr__(self, name: str):
        return getattr(self._get(), name)


# Singleton runtime gate instance for application consumption. Built lazily
# on first attribute access (see _LazyDQGate above) rather than at import
# time.
dq_gate = _LazyDQGate()
