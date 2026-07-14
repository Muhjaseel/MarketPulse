"""Unit tests for src/utils/config_loader.py."""
import os

from src.utils.config_loader import load_yaml_config


def test_load_yaml_config_finds_real_app_settings():
    """app_settings.yaml exists at the repo root, so this should load real values,
    not the hardcoded fallback."""
    cwd = os.getcwd()
    try:
        os.chdir(os.path.join(os.path.dirname(__file__), ".."))
        config = load_yaml_config("app_settings.yaml")
    finally:
        os.chdir(cwd)

    assert "kafka" in config
    assert "brokers" in config["kafka"]
    assert "sentiment" in config
    assert "keywords" in config["sentiment"]


def test_load_yaml_config_falls_back_when_file_missing():
    config = load_yaml_config("this_file_does_not_exist.yaml")

    assert config["kafka"]["brokers"] == ["localhost:19092"]
    assert config["sentiment"]["mock_stream_rate_per_second"] == 1
