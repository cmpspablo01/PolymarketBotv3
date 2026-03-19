"""
Tests for src/config_loader.py

Coverage:
  - Valid config loads cleanly and returns correct types
  - Missing config file raises ConfigurationError
  - Missing required section raises ConfigurationError
  - Invalid field values raise ConfigurationError (env, log level)
  - Empty config file raises ConfigurationError
  - Malformed YAML raises ConfigurationError
  - Field types are as expected (int, str, bool)
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml

from src.config_loader import ConfigurationError, Settings, load_config

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _make_valid_raw() -> dict[str, Any]:
    """Return a minimal valid raw config dict."""
    return {
        "project": {"name": "test-project", "env": "development"},
        "logging": {
            "level": "INFO",
            "log_dir": "logs",
            "log_file": "test.log",
            "console": False,
            "json_to_file": False,
        },
        "storage": {
            "data_dir": "data",
            "market_snapshots_dir": "data/markets",
            "price_data_dir": "data/prices",
            "orderbook_data_dir": "data/orderbooks",
        },
        "runner": {"heartbeat_interval_seconds": 60},
        "polymarket": {
            "base_url": "https://clob.polymarket.com",
            "gamma_base_url": "https://gamma-api.polymarket.com",
        },
    }


@pytest.fixture()
def valid_config_file(tmp_path: Path) -> Path:
    path = tmp_path / "settings.yaml"
    path.write_text(yaml.dump(_make_valid_raw()), encoding="utf-8")
    return path


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------

def test_valid_config_returns_settings(valid_config_file: Path) -> None:
    settings = load_config(valid_config_file)
    assert isinstance(settings, Settings)


def test_project_fields(valid_config_file: Path) -> None:
    settings = load_config(valid_config_file)
    assert settings.project.name == "test-project"
    assert settings.project.env == "development"


def test_logging_level_uppercased(valid_config_file: Path, tmp_path: Path) -> None:
    raw = _make_valid_raw()
    raw["logging"]["level"] = "debug"  # lowercase input
    path = tmp_path / "lower.yaml"
    path.write_text(yaml.dump(raw), encoding="utf-8")
    settings = load_config(path)
    assert settings.logging.level == "DEBUG"


def test_storage_fields(valid_config_file: Path) -> None:
    settings = load_config(valid_config_file)
    assert settings.storage.data_dir == "data"
    assert settings.storage.market_snapshots_dir == "data/markets"
    assert settings.storage.price_data_dir == "data/prices"
    assert settings.storage.orderbook_data_dir == "data/orderbooks"


def test_heartbeat_interval_is_int(valid_config_file: Path) -> None:
    settings = load_config(valid_config_file)
    assert isinstance(settings.runner.heartbeat_interval_seconds, int)
    assert settings.runner.heartbeat_interval_seconds == 60


def test_production_env_accepted(tmp_path: Path) -> None:
    raw = _make_valid_raw()
    raw["project"]["env"] = "production"
    path = tmp_path / "prod.yaml"
    path.write_text(yaml.dump(raw), encoding="utf-8")
    settings = load_config(path)
    assert settings.project.env == "production"


# ---------------------------------------------------------------------------
# Failure cases — all must raise ConfigurationError
# ---------------------------------------------------------------------------

def test_missing_config_file_raises(tmp_path: Path) -> None:
    with pytest.raises(ConfigurationError, match="not found"):
        load_config(tmp_path / "nonexistent.yaml")


def test_missing_logging_section_raises(tmp_path: Path) -> None:
    raw = _make_valid_raw()
    del raw["logging"]
    path = tmp_path / "no_logging.yaml"
    path.write_text(yaml.dump(raw), encoding="utf-8")
    with pytest.raises(ConfigurationError, match="Invalid configuration"):
        load_config(path)


def test_missing_storage_section_raises(tmp_path: Path) -> None:
    raw = _make_valid_raw()
    del raw["storage"]
    path = tmp_path / "no_storage.yaml"
    path.write_text(yaml.dump(raw), encoding="utf-8")
    with pytest.raises(ConfigurationError, match="Invalid configuration"):
        load_config(path)


def test_missing_runner_section_raises(tmp_path: Path) -> None:
    raw = _make_valid_raw()
    del raw["runner"]
    path = tmp_path / "no_runner.yaml"
    path.write_text(yaml.dump(raw), encoding="utf-8")
    with pytest.raises(ConfigurationError, match="Invalid configuration"):
        load_config(path)


def test_invalid_env_value_raises(tmp_path: Path) -> None:
    raw = _make_valid_raw()
    raw["project"]["env"] = "local"  # not in allowed set
    path = tmp_path / "bad_env.yaml"
    path.write_text(yaml.dump(raw), encoding="utf-8")
    with pytest.raises(ConfigurationError, match="Invalid configuration"):
        load_config(path)


def test_invalid_log_level_raises(tmp_path: Path) -> None:
    raw = _make_valid_raw()
    raw["logging"]["level"] = "VERBOSE"  # not a valid stdlib level
    path = tmp_path / "bad_level.yaml"
    path.write_text(yaml.dump(raw), encoding="utf-8")
    with pytest.raises(ConfigurationError, match="Invalid configuration"):
        load_config(path)


def test_empty_config_file_raises(tmp_path: Path) -> None:
    path = tmp_path / "empty.yaml"
    path.write_text("", encoding="utf-8")
    with pytest.raises(ConfigurationError, match="Invalid configuration"):
        load_config(path)


def test_malformed_yaml_raises(tmp_path: Path) -> None:
    path = tmp_path / "bad.yaml"
    path.write_text("project: {name: [bad: yaml: here", encoding="utf-8")
    with pytest.raises(ConfigurationError, match="Failed to parse"):
        load_config(path)
