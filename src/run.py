"""
Entry point for the Polymarket BTC 15m research framework.

Startup sequence (order is intentional):
  1. Load and validate config         — fails loudly before anything else
  2. Create required directories      — before logging opens file handles
  3. Initialize structured logging    — all subsequent output is structured
  4. Register shutdown signal handlers
  5. Emit startup log
  6. Run heartbeat loop               — Phase 1 stub; fetcher will plug in here

Phase 1 status: No market fetching. Heartbeat loop is a placeholder.
"""

from __future__ import annotations

import signal
import sys
import threading
from pathlib import Path
from types import FrameType

from src.config_loader import ConfigurationError, Settings, load_config
from src.logger import get_logger, setup_logging

# Event set by signal handlers — .wait(timeout=N) returns immediately when set.
# Replaces time.sleep() which blocks for the full duration and delays shutdown.
_shutdown_event = threading.Event()


def _handle_signal(signum: int, frame: FrameType | None) -> None:
    """Set the shutdown event; the heartbeat loop exits instantly."""
    _shutdown_event.set()


def _ensure_directories(settings: Settings) -> None:
    """Create all required runtime directories if they do not already exist."""
    dirs = [
        settings.logging.log_dir,
        settings.storage.data_dir,
        settings.storage.market_snapshots_dir,
        settings.storage.price_data_dir,
    ]
    for dir_path in dirs:
        Path(dir_path).mkdir(parents=True, exist_ok=True)


def main() -> None:
    # 1. Load config — raises ConfigurationError on any failure
    try:
        settings = load_config()
    except ConfigurationError as exc:
        print(f"[FATAL] {exc}", file=sys.stderr)
        sys.exit(1)

    # 2. Create directories before logging opens a file handle
    _ensure_directories(settings)

    # 3. Initialize structured logging
    setup_logging(settings.logging)
    log = get_logger(__name__)

    # 4. Register shutdown handlers for SIGINT and SIGTERM
    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    # 5. Emit startup event
    log.info(
        "startup",
        project=settings.project.name,
        env=settings.project.env,
    )

    # TODO: Phase 1 — instantiate market discovery (src/polymarket/markets.py)
    # TODO: Phase 1 — instantiate price fetcher (src/data/fetcher.py)
    # TODO: Phase 1 — instantiate storage writer (src/data/storage.py)

    # 6. Heartbeat loop — placeholder until Phase 1 fetcher is wired in
    # Uses threading.Event.wait(timeout=N) instead of time.sleep(N).
    # Shutdown is instant: signal handler calls _shutdown_event.set(),
    # and .wait() returns immediately instead of blocking for the full interval.
    interval = settings.runner.heartbeat_interval_seconds
    log.info("heartbeat_loop_started", interval_seconds=interval)

    try:
        while not _shutdown_event.is_set():
            log.info("heartbeat")

            # TODO: Phase 1 — call fetcher.run_once() here on each heartbeat

            _shutdown_event.wait(timeout=interval)
    except KeyboardInterrupt:
        # Fallback: handles KeyboardInterrupt if signal handler was not reached
        pass
    finally:
        log.info("shutdown")


if __name__ == "__main__":
    main()
