"""
Tests for src/run.py

Coverage:
  - _run_once     : single cycle execution, result returned, logging,
                    exception propagation
  - _run_loop     : shutdown exit, cycle execution, cycle error survival,
                    logging of loop_started / cycle_complete
  - main          : config failure exit, once-mode wiring + client close,
                    loop-mode shutdown + client close

All dependencies are mocked — no live API calls, no live filesystem
(main tests use tmp_path-backed settings).
"""

from __future__ import annotations

import threading
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from src.config_loader import (
    LoggingConfig,
    PolymarketConfig,
    ProjectConfig,
    RunnerConfig,
    Settings,
    StorageConfig,
)
from src.data.fetcher import DataFetcher, FetchCycleResult
from src.run import _run_loop, _run_once

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _mock_fetcher(result: FetchCycleResult | None = None) -> MagicMock:
    """Return a MagicMock spec'd to DataFetcher with a default run_cycle result."""
    fetcher = MagicMock(spec=DataFetcher)
    fetcher.run_cycle.return_value = result or FetchCycleResult()
    return fetcher


def _result(**kwargs: int) -> FetchCycleResult:
    return FetchCycleResult(**kwargs)


def _test_settings(tmp_path: Path, mode: str = "once") -> Settings:
    """Build a Settings instance backed by tmp_path (no real dirs touched)."""
    return Settings(
        project=ProjectConfig(name="test", env="development"),
        logging=LoggingConfig(
            level="INFO",
            log_dir=str(tmp_path / "logs"),
            log_file="test.log",
            console=False,
            json_to_file=False,
        ),
        storage=StorageConfig(
            data_dir=str(tmp_path / "data"),
            market_snapshots_dir=str(tmp_path / "data" / "markets"),
            price_data_dir=str(tmp_path / "data" / "prices"),
            orderbook_data_dir=str(tmp_path / "data" / "orderbooks"),
        ),
        runner=RunnerConfig(heartbeat_interval_seconds=1, mode=mode),
        polymarket=PolymarketConfig(
            base_url="https://clob.polymarket.com",
            gamma_base_url="https://gamma-api.polymarket.com",
        ),
    )


@pytest.fixture(autouse=True)
def _reset_shutdown_event() -> None:
    """Clear the module-level shutdown event before every test."""
    import src.run

    src.run._shutdown_event.clear()


# ---------------------------------------------------------------------------
# _run_once
# ---------------------------------------------------------------------------


def test_run_once_calls_run_cycle() -> None:
    fetcher = _mock_fetcher(
        _result(markets_found=2, prices_stored=4, orderbooks_stored=4),
    )
    log = MagicMock()

    result = _run_once(fetcher, log)

    fetcher.run_cycle.assert_called_once()
    assert result.markets_found == 2
    assert result.prices_stored == 4


def test_run_once_logs_start_and_complete() -> None:
    fetcher = _mock_fetcher()
    log = MagicMock()

    _run_once(fetcher, log)

    events = [c.args[0] for c in log.info.call_args_list]
    assert "cycle_start" in events
    assert "cycle_complete" in events


def test_run_once_propagates_exception() -> None:
    fetcher = MagicMock(spec=DataFetcher)
    fetcher.run_cycle.side_effect = RuntimeError("boom")
    log = MagicMock()

    with pytest.raises(RuntimeError, match="boom"):
        _run_once(fetcher, log)


# ---------------------------------------------------------------------------
# _run_loop
# ---------------------------------------------------------------------------


def test_run_loop_exits_immediately_when_shutdown_set() -> None:
    """Loop body never executes if shutdown event is already set."""
    fetcher = _mock_fetcher()
    log = MagicMock()
    event = threading.Event()
    event.set()

    _run_loop(fetcher, interval=60, shutdown_event=event, log=log)

    fetcher.run_cycle.assert_not_called()


def test_run_loop_runs_one_cycle_then_exits() -> None:
    event = threading.Event()

    def cycle_then_stop() -> FetchCycleResult:
        event.set()
        return _result(markets_found=1)

    fetcher = MagicMock(spec=DataFetcher)
    fetcher.run_cycle.side_effect = cycle_then_stop
    log = MagicMock()

    _run_loop(fetcher, interval=0, shutdown_event=event, log=log)

    fetcher.run_cycle.assert_called_once()


def test_run_loop_logs_loop_started_and_cycle_complete() -> None:
    event = threading.Event()

    def cycle_then_stop() -> FetchCycleResult:
        event.set()
        return _result(markets_found=3, prices_stored=6, orderbooks_stored=6)

    fetcher = MagicMock(spec=DataFetcher)
    fetcher.run_cycle.side_effect = cycle_then_stop
    log = MagicMock()

    _run_loop(fetcher, interval=0, shutdown_event=event, log=log)

    events = [c.args[0] for c in log.info.call_args_list]
    assert "loop_started" in events
    assert "cycle_start" in events
    assert "cycle_complete" in events


def test_run_loop_survives_cycle_exception() -> None:
    """A cycle exception is logged and the loop continues."""
    event = threading.Event()
    call_count = 0

    def failing_then_ok() -> FetchCycleResult:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise RuntimeError("transient")
        event.set()
        return _result()

    fetcher = MagicMock(spec=DataFetcher)
    fetcher.run_cycle.side_effect = failing_then_ok
    log = MagicMock()

    _run_loop(fetcher, interval=0, shutdown_event=event, log=log)

    assert fetcher.run_cycle.call_count == 2
    log.error.assert_called_once()


# ---------------------------------------------------------------------------
# main — integration tests with heavy mocking
# ---------------------------------------------------------------------------


def test_main_config_failure_exits() -> None:
    from src.config_loader import ConfigurationError

    with patch("src.run.load_config", side_effect=ConfigurationError("bad")):
        with pytest.raises(SystemExit) as exc_info:
            from src.run import main

            main()
        assert exc_info.value.code == 1


def test_main_once_mode_wires_and_closes_clients(tmp_path: Path) -> None:
    """Verify: builds all components with dual clients, runs one cycle, closes both."""
    settings = _test_settings(tmp_path, mode="once")

    mock_gamma_client = MagicMock(name="gamma_client")
    mock_clob_client = MagicMock(name="clob_client")
    mock_fetcher_inst = MagicMock(spec=DataFetcher)
    mock_fetcher_inst.run_cycle.return_value = FetchCycleResult()

    # PolymarketHTTPClient is called twice: first for gamma, then for clob
    with (
        patch("src.run.load_config", return_value=settings),
        patch("src.run.setup_logging"),
        patch(
            "src.run.PolymarketHTTPClient",
            side_effect=[mock_gamma_client, mock_clob_client],
        ) as mock_client_cls,
        patch("src.run.MarketDiscovery") as mock_disc_cls,
        patch("src.run.PriceFetcher") as mock_pf_cls,
        patch("src.run.DataStorage") as mock_storage_cls,
        patch(
            "src.run.DataFetcher", return_value=mock_fetcher_inst,
        ) as mock_fetcher_cls,
    ):
        from src.run import main

        main()

    # Wiring: two clients created
    assert mock_client_cls.call_count == 2
    mock_client_cls.assert_any_call(base_url="https://gamma-api.polymarket.com")
    mock_client_cls.assert_any_call(base_url="https://clob.polymarket.com")

    # Discovery uses gamma, PriceFetcher uses clob
    mock_disc_cls.assert_called_once_with(mock_gamma_client)
    mock_pf_cls.assert_called_once_with(mock_clob_client)
    mock_storage_cls.assert_called_once()
    mock_fetcher_cls.assert_called_once()

    # Cycle ran exactly once
    mock_fetcher_inst.run_cycle.assert_called_once()

    # Both clients closed on exit
    mock_clob_client.close.assert_called_once()
    mock_gamma_client.close.assert_called_once()


def test_main_loop_mode_exits_on_shutdown(tmp_path: Path) -> None:
    """Loop mode with pre-set shutdown event exits immediately; both clients closed."""
    import src.run

    src.run._shutdown_event.set()

    settings = _test_settings(tmp_path, mode="loop")

    mock_gamma_client = MagicMock(name="gamma_client")
    mock_clob_client = MagicMock(name="clob_client")
    mock_fetcher_inst = MagicMock(spec=DataFetcher)
    mock_fetcher_inst.run_cycle.return_value = FetchCycleResult()

    with (
        patch("src.run.load_config", return_value=settings),
        patch("src.run.setup_logging"),
        patch(
            "src.run.PolymarketHTTPClient",
            side_effect=[mock_gamma_client, mock_clob_client],
        ),
        patch("src.run.MarketDiscovery"),
        patch("src.run.PriceFetcher"),
        patch("src.run.DataStorage"),
        patch("src.run.DataFetcher", return_value=mock_fetcher_inst),
    ):
        from src.run import main

        main()

    # Loop exited immediately — no cycles ran
    mock_fetcher_inst.run_cycle.assert_not_called()

    # Both clients still closed
    mock_clob_client.close.assert_called_once()
    mock_gamma_client.close.assert_called_once()
