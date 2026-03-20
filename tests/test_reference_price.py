"""
Tests for src/external/reference_price.py

Coverage:
  - Happy path: valid proxy tick with correct metadata
  - is_proxy always True for current implementation
  - pair is always "BTC/USD"
  - source is "binance_spot_proxy"
  - proxy_description is non-empty
  - Source timestamp propagated from upstream Binance tick
  - Three-clock ordering maintained
  - raw_payload from upstream preserved
  - Upstream failure wrapped as ReferencePriceError (source-agnostic)
  - Underlying cause preserved on wrapped errors
  - Custom BinanceSpotFetcher injection

All upstream calls are mocked — no live API requests.
"""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from src.external.binance_spot import BinanceAPIError, BinanceSpotFetcher
from src.external.models import BinanceSpotTick, ReferencePriceTick
from src.external.reference_price import (
    PROXY_DESCRIPTION,
    PROXY_SOURCE,
    REFERENCE_PAIR,
    ReferencePriceError,
    ReferencePriceFetcher,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

TS_EXCHANGE = datetime(2026, 3, 19, 3, 0, 0, tzinfo=timezone.utc)
TS_RECEIVE = datetime(2026, 3, 19, 3, 0, 0, 200_000, tzinfo=timezone.utc)
TS_PROCESSED_UPSTREAM = datetime(2026, 3, 19, 3, 0, 0, 400_000, tzinfo=timezone.utc)

RAW_TRADE = {
    "id": 123456789,
    "price": "84273.49000000",
    "time": 1774004400000,
}


def _upstream_tick(**overrides: object) -> BinanceSpotTick:
    defaults: dict = {
        "symbol": "BTCUSDT",
        "price": 84273.49,
        "exchange_timestamp": TS_EXCHANGE,
        "local_receive_timestamp": TS_RECEIVE,
        "processed_timestamp": TS_PROCESSED_UPSTREAM,
        "raw_payload": RAW_TRADE,
    }
    defaults.update(overrides)
    return BinanceSpotTick(**defaults)


def _build_fetcher(
    upstream_tick: BinanceSpotTick | None = None,
    upstream_error: Exception | None = None,
) -> ReferencePriceFetcher:
    """Build a ReferencePriceFetcher with a mocked BinanceSpotFetcher."""
    mock_binance = MagicMock(spec=BinanceSpotFetcher)
    if upstream_error is not None:
        mock_binance.fetch_latest_trade.side_effect = upstream_error
    else:
        mock_binance.fetch_latest_trade.return_value = upstream_tick or _upstream_tick()
    return ReferencePriceFetcher(binance_fetcher=mock_binance)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestReferencePriceFetcher:
    def test_happy_path_returns_reference_tick(self) -> None:
        fetcher = _build_fetcher()
        tick = fetcher.fetch_reference_price()

        assert isinstance(tick, ReferencePriceTick)
        assert tick.price == 84273.49

    def test_pair_is_btc_usd(self) -> None:
        fetcher = _build_fetcher()
        tick = fetcher.fetch_reference_price()

        assert tick.pair == REFERENCE_PAIR
        assert tick.pair == "BTC/USD"

    def test_is_proxy_always_true(self) -> None:
        fetcher = _build_fetcher()
        tick = fetcher.fetch_reference_price()

        assert tick.is_proxy is True

    def test_source_is_binance_spot_proxy(self) -> None:
        fetcher = _build_fetcher()
        tick = fetcher.fetch_reference_price()

        assert tick.source == PROXY_SOURCE
        assert tick.source == "binance_spot_proxy"

    def test_proxy_description_is_non_empty(self) -> None:
        fetcher = _build_fetcher()
        tick = fetcher.fetch_reference_price()

        assert tick.proxy_description is not None
        assert len(tick.proxy_description) > 0
        assert tick.proxy_description == PROXY_DESCRIPTION

    def test_source_timestamp_from_upstream(self) -> None:
        """source_timestamp should be the upstream exchange_timestamp."""
        fetcher = _build_fetcher()
        tick = fetcher.fetch_reference_price()

        assert tick.source_timestamp == TS_EXCHANGE

    def test_local_receive_timestamp_from_upstream(self) -> None:
        """local_receive_timestamp should be preserved from upstream."""
        fetcher = _build_fetcher()
        tick = fetcher.fetch_reference_price()

        assert tick.local_receive_timestamp == TS_RECEIVE

    def test_processed_timestamp_is_fresh(self) -> None:
        """processed_timestamp should be >= upstream processed_timestamp."""
        fetcher = _build_fetcher()
        tick = fetcher.fetch_reference_price()

        assert tick.processed_timestamp >= TS_PROCESSED_UPSTREAM

    def test_three_clock_ordering(self) -> None:
        """source <= receive <= processed."""
        fetcher = _build_fetcher()
        tick = fetcher.fetch_reference_price()

        assert tick.source_timestamp <= tick.local_receive_timestamp
        assert tick.local_receive_timestamp <= tick.processed_timestamp

    def test_timestamps_are_timezone_aware(self) -> None:
        fetcher = _build_fetcher()
        tick = fetcher.fetch_reference_price()

        assert tick.source_timestamp.tzinfo is not None
        assert tick.local_receive_timestamp.tzinfo is not None
        assert tick.processed_timestamp.tzinfo is not None

    def test_raw_payload_from_upstream(self) -> None:
        fetcher = _build_fetcher()
        tick = fetcher.fetch_reference_price()

        assert tick.raw_payload is not None
        assert tick.raw_payload == RAW_TRADE

    def test_raw_payload_none_when_upstream_has_none(self) -> None:
        upstream = _upstream_tick(raw_payload=None)
        fetcher = _build_fetcher(upstream_tick=upstream)
        tick = fetcher.fetch_reference_price()

        assert tick.raw_payload is None

    def test_upstream_error_wrapped_as_reference_price_error(self) -> None:
        fetcher = _build_fetcher(
            upstream_error=BinanceAPIError("HTTP 500: server error")
        )
        with pytest.raises(ReferencePriceError, match="Reference price fetch failed"):
            fetcher.fetch_reference_price()

    def test_upstream_network_error_wrapped(self) -> None:
        fetcher = _build_fetcher(
            upstream_error=BinanceAPIError("Network error: connection refused")
        )
        with pytest.raises(ReferencePriceError, match="Reference price fetch failed"):
            fetcher.fetch_reference_price()

    def test_wrapped_error_preserves_cause(self) -> None:
        """The original BinanceAPIError is preserved as __cause__."""
        original = BinanceAPIError("HTTP 500: server error")
        fetcher = _build_fetcher(upstream_error=original)

        with pytest.raises(ReferencePriceError) as exc_info:
            fetcher.fetch_reference_price()

        assert exc_info.value.__cause__ is original
        assert isinstance(exc_info.value.__cause__, BinanceAPIError)

    def test_non_binance_error_also_wrapped(self) -> None:
        """Any upstream exception type is wrapped, not just BinanceAPIError."""
        fetcher = _build_fetcher(
            upstream_error=RuntimeError("unexpected failure")
        )
        with pytest.raises(ReferencePriceError, match="unexpected failure"):
            fetcher.fetch_reference_price()

    def test_delegates_to_binance_fetcher(self) -> None:
        """Verify that fetch_latest_trade is called exactly once."""
        mock_binance = MagicMock(spec=BinanceSpotFetcher)
        mock_binance.fetch_latest_trade.return_value = _upstream_tick()

        fetcher = ReferencePriceFetcher(binance_fetcher=mock_binance)
        fetcher.fetch_reference_price()

        mock_binance.fetch_latest_trade.assert_called_once()

    def test_default_binance_fetcher_created(self) -> None:
        """When no fetcher is injected, a default BinanceSpotFetcher is created."""
        fetcher = ReferencePriceFetcher()
        assert fetcher._binance is not None

    def test_price_propagated_from_upstream(self) -> None:
        upstream = _upstream_tick(price=99999.99)
        fetcher = _build_fetcher(upstream_tick=upstream)
        tick = fetcher.fetch_reference_price()

        assert tick.price == 99999.99
