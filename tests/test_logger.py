"""
Tests for src/logger.py

Coverage:
  - setup_logging() does not raise under valid config
  - File handler is registered when json_to_file=True
  - No file handler when json_to_file=False
  - Console handler registered when console=True
  - No console handler when console=False
  - get_logger() returns a usable logger after setup
  - Logger writes to file and file contains the event name
  - Log level is respected (DEBUG vs INFO)
  - setup_logging() can be called multiple times without accumulating handlers
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

import pytest
import structlog

from src.config_loader import LoggingConfig
from src.logger import get_logger, setup_logging


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _make_log_config(
    tmp_path: Path,
    *,
    level: str = "DEBUG",
    console: bool = False,
    json_to_file: bool = True,
) -> LoggingConfig:
    """Return a LoggingConfig pointing at tmp_path."""
    return LoggingConfig(
        level=level,
        log_dir=str(tmp_path),
        log_file="test.log",
        console=console,
        json_to_file=json_to_file,
    )


@pytest.fixture(autouse=True)
def reset_logging_state() -> None:  # type: ignore[return]
    """
    Reset stdlib root logger handlers and structlog defaults after each test.
    autouse=True ensures isolation without needing explicit fixture use.
    """
    yield
    root = logging.getLogger()
    for handler in root.handlers[:]:
        handler.flush()
        handler.close()
        root.removeHandler(handler)
    structlog.reset_defaults()


# ---------------------------------------------------------------------------
# setup_logging — handler registration
# ---------------------------------------------------------------------------

def test_setup_logging_does_not_raise(tmp_path: Path) -> None:
    config = _make_log_config(tmp_path)
    setup_logging(config)  # must not raise


def test_file_handler_registered_when_enabled(tmp_path: Path) -> None:
    config = _make_log_config(tmp_path, json_to_file=True)
    setup_logging(config)
    handlers = logging.getLogger().handlers
    assert any(isinstance(h, logging.FileHandler) for h in handlers)


def test_no_file_handler_when_disabled(tmp_path: Path) -> None:
    config = _make_log_config(tmp_path, json_to_file=False, console=True)
    setup_logging(config)
    handlers = logging.getLogger().handlers
    assert not any(isinstance(h, logging.FileHandler) for h in handlers)


def test_console_handler_registered_when_enabled(tmp_path: Path) -> None:
    config = _make_log_config(tmp_path, console=True, json_to_file=False)
    setup_logging(config)
    handlers = logging.getLogger().handlers
    assert any(isinstance(h, logging.StreamHandler) for h in handlers)


def test_no_console_handler_when_disabled(tmp_path: Path) -> None:
    config = _make_log_config(tmp_path, console=False, json_to_file=False)
    setup_logging(config)
    handlers = logging.getLogger().handlers
    # FileHandler IS a StreamHandler subclass, so we check more specifically
    stream_only = [
        h for h in handlers
        if type(h) is logging.StreamHandler  # exact type, not subclass
    ]
    assert len(stream_only) == 0


def test_repeated_setup_does_not_accumulate_handlers(tmp_path: Path) -> None:
    config = _make_log_config(tmp_path)
    setup_logging(config)
    setup_logging(config)
    handlers = logging.getLogger().handlers
    file_handlers = [h for h in handlers if isinstance(h, logging.FileHandler)]
    assert len(file_handlers) == 1, "Handlers must not accumulate across repeated setup calls"


# ---------------------------------------------------------------------------
# setup_logging — log level
# ---------------------------------------------------------------------------

def test_root_logger_level_set_correctly(tmp_path: Path) -> None:
    config = _make_log_config(tmp_path, level="WARNING")
    setup_logging(config)
    assert logging.getLogger().level == logging.WARNING


def test_debug_level_accepted(tmp_path: Path) -> None:
    config = _make_log_config(tmp_path, level="DEBUG")
    setup_logging(config)
    assert logging.getLogger().level == logging.DEBUG


# ---------------------------------------------------------------------------
# get_logger — usability
# ---------------------------------------------------------------------------

def test_get_logger_returns_non_none(tmp_path: Path) -> None:
    config = _make_log_config(tmp_path)
    setup_logging(config)
    log = get_logger("test.module")
    assert log is not None


def test_get_logger_emits_without_raising(tmp_path: Path) -> None:
    config = _make_log_config(tmp_path)
    setup_logging(config)
    log = get_logger("test.emit")
    log.info("test_event", key="value")  # must not raise


# ---------------------------------------------------------------------------
# File output content
# ---------------------------------------------------------------------------

def _flush_handlers() -> None:
    for handler in logging.getLogger().handlers:
        handler.flush()


def test_log_file_is_created(tmp_path: Path) -> None:
    config = _make_log_config(tmp_path)
    setup_logging(config)
    log = get_logger("test.file_created")
    log.info("creation_check")
    _flush_handlers()
    assert (tmp_path / "test.log").exists()


def test_log_file_contains_event_name(tmp_path: Path) -> None:
    config = _make_log_config(tmp_path)
    setup_logging(config)
    log = get_logger("test.content")
    log.info("unique_event_name_xyz", payload="abc")
    _flush_handlers()
    content = (tmp_path / "test.log").read_text(encoding="utf-8")
    assert "unique_event_name_xyz" in content


def test_log_file_is_valid_json_lines(tmp_path: Path) -> None:
    config = _make_log_config(tmp_path)
    setup_logging(config)
    log = get_logger("test.json")
    log.info("json_line_event", some_key="some_value")
    _flush_handlers()
    lines = (tmp_path / "test.log").read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) >= 1
    for line in lines:
        parsed = json.loads(line)  # raises if not valid JSON
        assert isinstance(parsed, dict)


def test_log_file_json_contains_log_level(tmp_path: Path) -> None:
    config = _make_log_config(tmp_path)
    setup_logging(config)
    log = get_logger("test.level_in_json")
    log.warning("level_check_event")
    _flush_handlers()
    lines = (tmp_path / "test.log").read_text(encoding="utf-8").strip().splitlines()
    last = json.loads(lines[-1])
    assert "level" in last
    assert last["level"].lower() == "warning"
