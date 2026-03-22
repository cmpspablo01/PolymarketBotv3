"""
Configuration loader for the Polymarket BTC 15m research framework.

Loads and validates settings from config/settings.yaml.
Loads secrets from .env (API keys — never stored in settings.yaml).
Raises ConfigurationError on any missing required field or invalid value.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml
from dotenv import load_dotenv
from pydantic import BaseModel, ValidationError, field_validator


class ConfigurationError(Exception):
    """Raised when configuration loading or validation fails."""

DEFAULT_CONFIG_PATH = Path("config/settings.yaml")


class ProjectConfig(BaseModel):
    name: str
    env: str

    @field_validator("env")
    @classmethod
    def env_must_be_valid(cls, v: str) -> str:
        allowed = {"development", "production", "staging"}
        if v not in allowed:
            raise ValueError(f"project.env must be one of {allowed}, got '{v}'")
        return v


class LoggingConfig(BaseModel):
    level: str
    log_dir: str
    log_file: str
    console: bool
    json_to_file: bool

    @field_validator("level")
    @classmethod
    def level_must_be_valid(cls, v: str) -> str:
        allowed = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
        if v.upper() not in allowed:
            raise ValueError(f"logging.level must be one of {allowed}, got '{v}'")
        return v.upper()


class StorageConfig(BaseModel):
    data_dir: str
    market_snapshots_dir: str
    price_data_dir: str
    orderbook_data_dir: str
    binance_spot_data_dir: str
    reference_price_data_dir: str


class PolymarketConfig(BaseModel):
    base_url: str
    gamma_base_url: str


class RunnerConfig(BaseModel):
    heartbeat_interval_seconds: int
    mode: str = "loop"

    @field_validator("mode")
    @classmethod
    def mode_must_be_valid(cls, v: str) -> str:
        allowed = {"once", "loop"}
        if v not in allowed:
            raise ValueError(f"runner.mode must be one of {allowed}, got '{v}'")
        return v


class Settings(BaseModel):
    project: ProjectConfig
    logging: LoggingConfig
    storage: StorageConfig
    runner: RunnerConfig
    polymarket: PolymarketConfig


def load_config(config_path: Path = DEFAULT_CONFIG_PATH) -> Settings:
    """
    Load and validate settings from a YAML config file.

    Also loads .env for secrets (API keys etc.).
    Raises ConfigurationError with a descriptive message on any failure.

    Args:
        config_path: Path to the YAML settings file. Defaults to config/settings.yaml.

    Returns:
        A fully validated Settings instance.

    Raises:
        ConfigurationError: If the config file is missing, malformed, or invalid.
    """
    load_dotenv()

    if not config_path.exists():
        raise ConfigurationError(f"Config file not found: {config_path}")

    try:
        raw: dict[str, Any] = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    except yaml.YAMLError as exc:
        raise ConfigurationError(f"Failed to parse config file '{config_path}': {exc}") from exc

    if not isinstance(raw, dict):
        raise ConfigurationError(
            f"Config file '{config_path}' must contain a YAML mapping at the top level."
        )

    try:
        return Settings(**raw)
    except ValidationError as exc:
        raise ConfigurationError(
            f"Invalid configuration in '{config_path}':\n{exc}"
        ) from exc
