"""
Entry point for the Polymarket BTC 15m research framework.

Startup sequence (order is intentional):
  1. Load and validate config         — fails loudly before anything else
  2. Create required directories      — before logging opens file handles
  3. Initialize structured logging    — all subsequent output is structured
  4. Register shutdown signal handlers
  5. Build data pipeline components   — explicit construction, no DI framework
  6. Run collection cycle(s)          — once or loop per runner.mode

Phase 1 status: End-to-end data collection wired.
"""

from __future__ import annotations

import signal
import sys
import threading
from pathlib import Path
from types import FrameType
from typing import Any

from src.config_loader import ConfigurationError, Settings, load_config
from src.data.fetcher import DataFetcher, FetchCycleResult
from src.data.storage import DataStorage
from src.logger import get_logger, setup_logging
from src.polymarket.http_client import PolymarketHTTPClient
from src.polymarket.markets import MarketDiscovery
from src.polymarket.prices import PriceFetcher

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


# ---------------------------------------------------------------------------
# Cycle helpers — extracted for testability
# ---------------------------------------------------------------------------


def _run_once(fetcher: DataFetcher, log: Any) -> FetchCycleResult:
    """Execute a single fetch cycle and log the result."""
    log.info("cycle_start", mode="once")
    result = fetcher.run_cycle()
    log.info(
        "cycle_complete",
        markets_found=result.markets_found,
        prices_stored=result.prices_stored,
        orderbooks_stored=result.orderbooks_stored,
        errors=result.errors,
    )
    return result


def _run_loop(
    fetcher: DataFetcher,
    interval: int,
    shutdown_event: threading.Event,
    log: Any,
) -> None:
    """
    Run fetch cycles in a loop until *shutdown_event* is set.

    Uses ``threading.Event.wait(timeout=N)`` instead of ``time.sleep(N)``
    so that shutdown is instant when the signal handler fires.
    Per-cycle exceptions are caught and logged; the loop continues.
    """
    log.info("loop_started", interval_seconds=interval)
    while not shutdown_event.is_set():
        log.info("cycle_start")
        try:
            result = fetcher.run_cycle()
            log.info(
                "cycle_complete",
                markets_found=result.markets_found,
                prices_stored=result.prices_stored,
                orderbooks_stored=result.orderbooks_stored,
                errors=result.errors,
            )
        except Exception as exc:
            log.error("cycle_failed", error=str(exc))
        shutdown_event.wait(timeout=interval)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


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
        mode=settings.runner.mode,
    )

    # 6. Build components explicitly — no DI framework
    client = PolymarketHTTPClient(base_url=settings.polymarket.base_url)
    try:
        discovery = MarketDiscovery(client)
        price_fetcher = PriceFetcher(client)
        storage = DataStorage(base_dir=Path(settings.storage.data_dir))
        fetcher = DataFetcher(discovery, price_fetcher, storage)

        log.info("components_ready")

        # 7. Run
        if settings.runner.mode == "once":
            _run_once(fetcher, log)
        else:
            _run_loop(
                fetcher,
                settings.runner.heartbeat_interval_seconds,
                _shutdown_event,
                log,
            )
    except KeyboardInterrupt:
        # Fallback: handles KeyboardInterrupt if signal handler was not reached
        pass
    finally:
        client.close()
        log.info("shutdown")


if __name__ == "__main__":
    main()
