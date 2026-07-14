import time
import random
from datetime import datetime, timezone
from src.producers.producer_utils import create_resilient_producer
from src.utils.config_loader import load_yaml_config
from src.utils.logger import get_logger
from src.utils.constants import MARKET_TOPIC

logger = get_logger(__name__)

# Base asset prices to start the simulated random walk from
INITIAL_PRICES = {
    "BTCUSDT": 65000.0,
    "ETHUSDT": 35000.0,
    "SOLUSDT": 150.0
}

def stream_market_prices():
    """Simulates market price ticks (random walk) and streams them to Redpanda."""
    try:
        app_cfg = load_yaml_config("app_settings.yaml")["market"]
        stream_rate = app_cfg.get("mock_stream_rate_per_second", 1)
    except Exception:
        stream_rate = 1  # 1 payload per second baseline fallback

    producer = create_resilient_producer()
    current_prices = INITIAL_PRICES.copy()

    logger.info("Market price simulator active.")

    try:
        while True:
            for ticker in current_prices.keys():
                # Apply a small random percentage volatility swing (-0.5% to +0.5%)
                pct_change = random.uniform(-0.005, 0.005)
                current_prices[ticker] = round(current_prices[ticker] * (1 + pct_change), 2)

                payload = {
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "asset_tag": ticker,
                    "price": current_prices[ticker],
                    "volume": round(random.uniform(0.1, 5.0), 4),
                    "source": "market_price_simulator"
                }

                # Push streaming pricing metrics using the ticker symbol as a partition key
                producer.send(MARKET_TOPIC, key=ticker.encode('utf-8'), value=payload)
                logger.info(f"Sent price tick -> {ticker}: ${current_prices[ticker]:,}")

            producer.flush()
            time.sleep(1.0 / stream_rate)

    except KeyboardInterrupt:
        logger.warning("Market price simulator stopped by user request.")
    finally:
        producer.close()

if __name__ == "__main__":
    stream_market_prices()