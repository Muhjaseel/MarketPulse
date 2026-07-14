"""Sentiment stream simulator.

Picks from a small fixed set of template social-media posts and streams
them to Redpanda with a jittered sentiment score. This is a simulator for
exercising the rest of the pipeline (ingestion -> DuckDB -> dbt -> panic
index) end to end - it does not run any NLP model. See README > Known
Limitations for what a real version of this would need.
"""
import time
import random
from datetime import datetime, timezone
from src.producers.producer_utils import create_resilient_producer
from src.utils.config_loader import load_yaml_config
from src.utils.logger import get_logger
from src.utils.constants import SENTIMENT_TOPIC

logger = get_logger(__name__)

# Fixed template posts with a hand-assigned base sentiment score each.
# stream_social_sentiment() below jitters the score to simulate variety;
# it does not compute sentiment from the text.
TEMPLATE_POSTS = [
    ("BTCUSDT", "Wow, Bitcoin is absolutely pumping right now! To the moon!", 0.85),
    ("BTCUSDT", "Massive whale liquidation detected. This looks like a classic dump, get out.", -0.72),
    ("ETHUSDT", "Ethereum network gas fees are low and transaction volume is stable today.", 0.25),
    ("ETHUSDT", "Vitalik just moved millions of ETH. Panic sell incoming!!", -0.65),
    ("SOLUSDT", "Solana DEX volume just flipped Ethereum again. Bullish market structure!", 0.78),
    ("SOLUSDT", "Another congestion issue on Solana? Transactions are dropping. Frustrating.", -0.45),
    ("BTCUSDT", "Just holding my crypto bags and watching the sideways price action.", 0.00),
    ("SOLUSDT", "Solana speed is unmatched. Incredibly cheap to mint NFTs right now.", 0.60),
]


def stream_social_sentiment():
    """Continuously stream simulated sentiment payloads to Redpanda."""
    app_cfg = load_yaml_config("app_settings.yaml")["sentiment"]
    producer = create_resilient_producer()

    logger.info(f"Sentiment simulator active. Keywords tracked: {app_cfg['keywords']}")

    try:
        while True:
            ticker, raw_text, base_sentiment = random.choice(TEMPLATE_POSTS)

            # Jitter the fixed base score to simulate variety across repeats
            # of the same template - not a real sentiment computation.
            sentiment_score = max(-1.0, min(1.0, round(base_sentiment + random.uniform(-0.1, 0.1), 2)))

            payload = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "asset_tag": ticker,
                "text_payload": raw_text,
                "sentiment_score": sentiment_score,
                "source": "sentiment_simulator",
            }

            producer.send(SENTIMENT_TOPIC, key=ticker.encode('utf-8'), value=payload)
            producer.flush()

            time.sleep(1.0 / app_cfg["mock_stream_rate_per_second"])

    except KeyboardInterrupt:
        logger.warning("Sentiment simulator stopped by user request.")
    finally:
        producer.close()


if __name__ == "__main__":
    stream_social_sentiment()
