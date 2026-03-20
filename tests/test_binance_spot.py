"""
Tests for src/external/binance_spot.py

Coverage:
  - Happy path: valid trade response → correct BinanceSpotTick
  - Timestamp provenance: exchange_timestamp parsed from ms unix,
    local_receive <= processed
  - Correct URL and params passed to HTTP layer
  - Empty trades list → BinanceAPIError
  - Malformed trade (missing 'price') → BinanceAPIError
  - Malformed trade (missing 'time') → BinanceAPIError
  - Non-numeric price string → BinanceAPIError
  - Non-dict trade element → BinanceAPIError
  - Non-list response → BinanceAPIError
  - HTTP error status → BinanceAPIError
  - Network error → BinanceAPIError

All HTTP calls are mocked — no live API requests.
"""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from src.external.binance_spot import (
    BTCUSDT_SYMBOL,
    BinanceAPIError,
    BinanceSpotFetcher,
    _parse_trade,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

# 2026-03-19 03:00:00.000 UTC in milliseconds
TRADE_TIME_MS = 1774004400000

VALID_TRADE = {
    "id": 123456789,
    "price": "84273.49000000",
    "qty": "0.00100000",
    "quoteQty": "84.27349000",
    "time": TRADE_TIME_MS,
    "isBuyerMaker": False,
    "isBestMatch": True,
}


def _mock_response(status_code: int = 200, json_data: object = None) -> MagicMock:
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = json_data
    resp.text = "error body"
    return resp


# ---------------------------------------------------------------------------
# _parse_trade unit tests
# ---------------------------------------------------------------------------


class TestParseTrade:
    def test_valid_trade(self) -> None:
        receive_ts = datetime(2026, 3, 19, 3, 0, 0, 500_000, tzinfo=timezone.utc)
        tick = _parse_trade([VALID_TRADE], receive_ts)

        assert tick.symbol == BTCUSDT_SYMBOL
        assert tick.price == 84273.49
        assert tick.source == "binance_spot"
        assert tick.raw_payload == VALID_TRADE

    def test_exchange_timestamp_parsed_from_ms(self) -> None:
        receive_ts = datetime(2026, 3, 19, 3, 0, 1, tzinfo=timezone.utc)
        tick = _parse_trade([VALID_TRADE], receive_ts)

        expected = datetime.fromtimestamp(TRADE_TIME_MS / 1000.0, tz=timezone.utc)
        assert tick.exchange_timestamp == expected

    def test_local_receive_timestamp_preserved(self) -> None:
        receive_ts = datetime(2026, 3, 19, 3, 0, 0, 500_000, tzinfo=timezone.utc)
        tick = _parse_trade([VALID_TRADE], receive_ts)

        assert tick.local_receive_timestamp == receive_ts

    def test_processed_after_receive(self) -> None:
        receive_ts = datetime(2026, 3, 19, 3, 0, 0, tzinfo=timezone.utc)
        tick = _parse_trade([VALID_TRADE], receive_ts)

        assert tick.processed_timestamp >= tick.local_receive_timestamp

    def test_empty_list_raises(self) -> None:
        receive_ts = datetime(2026, 3, 19, 3, 0, 0, tzinfo=timezone.utc)
        with pytest.raises(BinanceAPIError, match="non-empty list"):
            _parse_trade([], receive_ts)

    def test_non_list_raises(self) -> None:
        receive_ts = datetime(2026, 3, 19, 3, 0, 0, tzinfo=timezone.utc)
        with pytest.raises(BinanceAPIError, match="non-empty list"):
            _parse_trade({"price": "100"}, receive_ts)

    def test_non_dict_trade_raises(self) -> None:
        receive_ts = datetime(2026, 3, 19, 3, 0, 0, tzinfo=timezone.utc)
        with pytest.raises(BinanceAPIError, match="trade dict"):
            _parse_trade(["not_a_dict"], receive_ts)

    def test_missing_price_raises(self) -> None:
        receive_ts = datetime(2026, 3, 19, 3, 0, 0, tzinfo=timezone.utc)
        trade = {**VALID_TRADE}
        del trade["price"]
        with pytest.raises(BinanceAPIError, match="'price'"):
            _parse_trade([trade], receive_ts)

    def test_missing_time_raises(self) -> None:
        receive_ts = datetime(2026, 3, 19, 3, 0, 0, tzinfo=timezone.utc)
        trade = {**VALID_TRADE}
        del trade["time"]
        with pytest.raises(BinanceAPIError, match="'time'"):
            _parse_trade([trade], receive_ts)

    def test_non_numeric_price_raises(self) -> None:
        receive_ts = datetime(2026, 3, 19, 3, 0, 0, tzinfo=timezone.utc)
        trade = {**VALID_TRADE, "price": "not_a_number"}
        with pytest.raises(BinanceAPIError, match="Cannot parse trade price"):
            _parse_trade([trade], receive_ts)

    def test_invalid_time_raises(self) -> None:
        receive_ts = datetime(2026, 3, 19, 3, 0, 0, tzinfo=timezone.utc)
        trade = {**VALID_TRADE, "time": "not_a_timestamp"}
        with pytest.raises(BinanceAPIError, match="Cannot parse trade time"):
            _parse_trade([trade], receive_ts)


# ---------------------------------------------------------------------------
# BinanceSpotFetcher integration tests (mocked HTTP)
# ---------------------------------------------------------------------------


class TestBinanceSpotFetcher:
    @patch("src.external.binance_spot.requests.get")
    def test_fetch_latest_trade_success(self, mock_get: MagicMock) -> None:
        mock_get.return_value = _mock_response(200, [VALID_TRADE])

        fetcher = BinanceSpotFetcher()
        tick = fetcher.fetch_latest_trade()

        assert tick.symbol == BTCUSDT_SYMBOL
        assert tick.price == 84273.49
        assert tick.source == "binance_spot"

    @patch("src.external.binance_spot.requests.get")
    def test_fetch_passes_correct_params(self, mock_get: MagicMock) -> None:
        mock_get.return_value = _mock_response(200, [VALID_TRADE])

        fetcher = BinanceSpotFetcher(base_url="https://test.example.com")
        fetcher.fetch_latest_trade()

        mock_get.assert_called_once()
        call_args = mock_get.call_args
        assert "https://test.example.com/api/v3/trades" == call_args[1].get("url", call_args[0][0])
        assert call_args[1]["params"]["symbol"] == BTCUSDT_SYMBOL
        assert call_args[1]["params"]["limit"] == "1"

    @patch("src.external.binance_spot.requests.get")
    def test_fetch_http_error_raises(self, mock_get: MagicMock) -> None:
        mock_get.return_value = _mock_response(500, None)

        fetcher = BinanceSpotFetcher()
        with pytest.raises(BinanceAPIError, match="HTTP 500"):
            fetcher.fetch_latest_trade()

    @patch("src.external.binance_spot.requests.get")
    def test_fetch_network_error_raises(self, mock_get: MagicMock) -> None:
        import requests

        mock_get.side_effect = requests.exceptions.ConnectionError("refused")

        fetcher = BinanceSpotFetcher()
        with pytest.raises(BinanceAPIError, match="Network error"):
            fetcher.fetch_latest_trade()

    @patch("src.external.binance_spot.requests.get")
    def test_fetch_invalid_json_raises(self, mock_get: MagicMock) -> None:
        resp = _mock_response(200)
        resp.json.side_effect = ValueError("bad json")

        mock_get.return_value = resp

        fetcher = BinanceSpotFetcher()
        with pytest.raises(BinanceAPIError, match="Invalid JSON"):
            fetcher.fetch_latest_trade()

    @patch("src.external.binance_spot.requests.get")
    def test_fetch_timestamp_provenance(self, mock_get: MagicMock) -> None:
        """All three timestamps are present and correctly ordered."""
        mock_get.return_value = _mock_response(200, [VALID_TRADE])

        fetcher = BinanceSpotFetcher()
        tick = fetcher.fetch_latest_trade()

        assert tick.exchange_timestamp.tzinfo is not None
        assert tick.local_receive_timestamp.tzinfo is not None
        assert tick.processed_timestamp.tzinfo is not None
        # processed >= receive (both captured during the call)
        assert tick.processed_timestamp >= tick.local_receive_timestamp

    @patch("src.external.binance_spot.requests.get")
    def test_fetch_raw_payload_preserved(self, mock_get: MagicMock) -> None:
        mock_get.return_value = _mock_response(200, [VALID_TRADE])

        fetcher = BinanceSpotFetcher()
        tick = fetcher.fetch_latest_trade()

        assert tick.raw_payload is not None
        assert tick.raw_payload["id"] == 123456789

    @patch("src.external.binance_spot.requests.get")
    def test_custom_base_url(self, mock_get: MagicMock) -> None:
        mock_get.return_value = _mock_response(200, [VALID_TRADE])

        fetcher = BinanceSpotFetcher(base_url="https://custom.binance.com/")
        fetcher.fetch_latest_trade()

        url_called = mock_get.call_args[0][0]
        assert url_called.startswith("https://custom.binance.com/")
