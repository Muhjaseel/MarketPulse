import json
import time
from kafka import KafkaProducer
from src.utils.config_loader import load_yaml_config
from src.utils.logger import get_logger

logger = get_logger(__name__)

def create_resilient_producer(max_retries=10, retry_backoff=2.0):
    """
    Create a Kafka/Redpanda producer with retry logic, so producers
    started before the broker is fully up don't crash immediately.
    """
    try:
        app_cfg = load_yaml_config("app_settings.yaml")["kafka"]
        brokers = app_cfg["brokers"]
    except Exception:
        brokers = ["localhost:19092"]

    logger.info(f"Connecting producer to brokers: {brokers}")

    for attempt in range(1, max_retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=brokers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks='all',              # wait for full ISR ack before considering the write durable
                retries=5,               # internal client-level retry on transient send failures
                request_timeout_ms=30000
            )
            logger.info("Producer connected.")
            return producer
        except Exception as e:
            logger.warning(
                f"Broker connection attempt {attempt}/{max_retries} failed, "
                f"retrying in {retry_backoff}s... ({e})"
            )
            time.sleep(retry_backoff)

    logger.critical("Could not connect to any broker after all retries.")
    raise ConnectionError("Unable to connect to the Kafka/Redpanda broker.")
