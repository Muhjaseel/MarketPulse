"""Kafka/Redpanda consumer that writes raw streaming payloads into DuckDB.

This is the landing-zone sink referenced by the README's "ingestion" stage:
it reads from the two Redpanda topics and appends rows into the
`raw_market_prices` / `raw_sentiment` tables that the dbt staging models
build on top of. It writes to a local DuckDB file, not to Apache Iceberg or
any object store — see README > Known Limitations.
"""
import json

from kafka import KafkaConsumer

from src.utils.config_loader import load_yaml_config
from src.utils.constants import MARKET_TOPIC, SENTIMENT_TOPIC
from src.utils.duckdb_client import ensure_landing_tables, execute_write
from src.utils.logger import get_logger

logger = get_logger(__name__)


def _persist_market_price(payload: dict) -> bool:
    ticker = payload.get("ticker") or payload.get("asset_tag")
    if ticker is None:
        return False

    return execute_write(
        """
        INSERT INTO raw_market_prices (timestamp, ticker, price, volume)
        VALUES (?, ?, ?, ?)
        """,
        [
            payload.get("timestamp"),
            ticker,
            payload.get("price"),
            payload.get("volume"),
        ],
    )


def _persist_sentiment(payload: dict) -> bool:
    return execute_write(
        """
        INSERT INTO raw_sentiment (timestamp, asset_tag, text_payload, sentiment_score, source)
        VALUES (?, ?, ?, ?, ?)
        """,
        [
            payload.get("timestamp"),
            payload.get("asset_tag"),
            payload.get("text_payload"),
            payload.get("sentiment_score"),
            payload.get("source"),
        ],
    )


def start_duckdb_sink() -> None:
    """Consume both topics from Redpanda and append each message into DuckDB."""
    try:
        app_cfg = load_yaml_config("app_settings.yaml")["kafka"]
        brokers = app_cfg["brokers"]
    except Exception:
        brokers = ["localhost:19092"]

    logger.info(f"Connecting DuckDB sink consumer to brokers: {brokers}")
    ensure_landing_tables()

    try:
        consumer = KafkaConsumer(
            MARKET_TOPIC,
            SENTIMENT_TOPIC,
            bootstrap_servers=brokers,
            group_id="marketpulse_duckdb_sink_group",
            value_deserializer=lambda x: json.loads(x.decode("utf-8")),
            auto_offset_reset="latest",
            enable_auto_commit=True,
        )

        logger.info(f"Listening on topics: ['{MARKET_TOPIC}', '{SENTIMENT_TOPIC}']")

        for message in consumer:
            topic = message.topic
            payload = message.value
            key = message.key.decode("utf-8") if message.key else "UNKNOWN"

            if topic == MARKET_TOPIC:
                stored = _persist_market_price(payload)
                status = "stored" if stored else "skipped (db lock or write failure)"
                logger.info(
                    f"[DUCKDB SINK] {status} price -> {key}: "
                    f"${payload.get('price')} | vol {payload.get('volume')}"
                )
            elif topic == SENTIMENT_TOPIC:
                stored = _persist_sentiment(payload)
                status = "stored" if stored else "skipped (db lock or write failure)"
                logger.info(
                    f"[DUCKDB SINK] {status} sentiment -> {key}: "
                    f"score {payload.get('sentiment_score')}"
                )

    except KeyboardInterrupt:
        logger.warning("DuckDB sink consumer stopped by user request.")
    except Exception as e:
        logger.critical(f"Consumer loop failed: {e}")


if __name__ == "__main__":
    start_duckdb_sink()
