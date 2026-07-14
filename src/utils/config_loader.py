import os
import yaml
from src.utils.logger import get_logger

logger = get_logger(__name__)

def load_yaml_config(config_name="app_settings.yaml"):
    """
    Load a YAML config file, checking the project root and a couple of
    likely subdirectories so this works whether it's invoked from the
    repo root, from transform_dbt/, or from inside a container.
    """
    possible_paths = [
        config_name,
        os.path.join("..", config_name),
        os.path.join("docker", config_name),
        os.path.join(os.path.dirname(__file__), "..", "..", config_name)
    ]

    for path in possible_paths:
        if os.path.exists(path):
            try:
                with open(path, "r") as f:
                    config_data = yaml.safe_load(f)
                logger.info(f"Loaded config from: {path}")
                return config_data
            except Exception as e:
                logger.error(f"Failed to parse {path}: {e}")
                raise e

    logger.warning(f"Config file '{config_name}' not found in any known path. Using defaults.")
    return {
        "kafka": {"brokers": ["localhost:19092"]},
        "sentiment": {"keywords": ["BTC", "ETH", "SOL"], "mock_stream_rate_per_second": 1}
    }