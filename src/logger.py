"""
Structured logger factory for the Polymarket BTC 15m research framework.

Configures structlog with two output targets:
  - Console: human-readable, colored output (via structlog ConsoleRenderer)
  - File:    machine-readable JSON lines, one record per line (grep/jq friendly)

Usage:
    from src.logger import setup_logging, get_logger

    setup_logging(config.logging)          # call once at startup, after dirs exist
    log = get_logger(__name__)             # call per module
    log.info("event_name", key="value")   # structured key=value logging
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path
from typing import Any

import structlog

from src.config_loader import LoggingConfig


def setup_logging(config: LoggingConfig) -> None:
    """
    Configure stdlib logging and structlog based on LoggingConfig.

    Must be called exactly once at application startup, after required
    directories have been created (run.py handles that ordering).

    Args:
        config: Validated LoggingConfig from settings.yaml.
    """
    log_level = getattr(logging, config.level, logging.INFO)

    # Processors applied to every log record before rendering.
    # Order matters: contextvars first, then metadata, then exception info.
    shared_processors: list[Any] = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
    ]

    structlog.configure(
        processors=shared_processors
        + [structlog.stdlib.ProcessorFormatter.wrap_for_formatter],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

    console_formatter = structlog.stdlib.ProcessorFormatter(
        processors=[
            structlog.stdlib.ProcessorFormatter.remove_processors_meta,
            structlog.dev.ConsoleRenderer(),
        ],
        foreign_pre_chain=shared_processors,
    )

    json_formatter = structlog.stdlib.ProcessorFormatter(
        processors=[
            structlog.stdlib.ProcessorFormatter.remove_processors_meta,
            structlog.processors.JSONRenderer(),
        ],
        foreign_pre_chain=shared_processors,
    )

    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)
    # Clear any handlers added by previous setup_logging calls (e.g. in tests).
    root_logger.handlers.clear()

    if config.console:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(console_formatter)
        root_logger.addHandler(console_handler)

    if config.json_to_file:
        log_path = Path(config.log_dir) / config.log_file
        file_handler = logging.FileHandler(log_path, encoding="utf-8")
        file_handler.setFormatter(json_formatter)
        root_logger.addHandler(file_handler)


def get_logger(name: str) -> Any:
    """
    Return a named structured logger.

    setup_logging() must be called before this is used in production.
    In tests, call setup_logging() with a tmp_path-backed LoggingConfig first.

    Returns structlog.stdlib.BoundLogger; typed as Any to avoid mypy noise
    from structlog's internal proxy types.
    """
    return structlog.get_logger(name)
