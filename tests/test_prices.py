"""
Tests for src/polymarket/prices.py

Coverage:
  - fetch_price         : calls /midpoint, returns TokenPrice, handles
                          string/numeric mid values, UTC timestamp
  - fetch_orderbook     : returns Orderbook, handles string values, empty book
  - _parse_midpoint     : raises ValueError on malformed input
  - _parse_orderbook    : non-dict fallback
  - timestamp policy    : /midpoint always stamps UTC; /book uses API timestamp
                          when present, UTC fallback otherwise
  - params forwarding   : verify token_id reaches the client on correct endpoint

All HTTP calls are mocked — no live network calls.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from unittest.mock import MagicMock

import pytest

from src.polymarket.http_client import PolymarketHTTPClient
from src.polymarket.models import Orderbook, TokenPrice
from src.polymarket.models import OrderbookLevel
from src.polymarket.prices import (
    PriceFetcher,
    _parse_midpoint,
    _parse_orderbook,
    midpoint_from_book,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_fetcher(return_value: Any) -> PriceFetcher:
    """Return a PriceFetcher with a mocked client."""
    client = MagicMock(spec=PolymarketHTTPClient)
    client.get.return_value = return_value
    return PriceFetcher(client)


# ---------------------------------------------------------------------------
# fetch_price (now via /midpoint)
# ---------------------------------------------------------------------------


def test_fetch_price_returns_token_price() -> None:
    fetcher = _make_fetcher({"mid": "0.72"})

    result = fetcher.fetch_price("tok_yes")

    assert isinstance(result, TokenPrice)
    assert result.token_id == "tok_yes"
    assert result.price == pytest.approx(0.72)


def test_fetch_price_handles_numeric_value() -> None:
    fetcher = _make_fetcher({"mid": 0.65})

    result = fetcher.fetch_price("tok_yes")

    assert result.price == pytest.approx(0.65)


def test_fetch_price_timestamp_is_utc() -> None:
    """The /midpoint endpoint has no timestamp; we always stamp UTC."""
    fetcher = _make_fetcher({"mid": "0.50"})

    result = fetcher.fetch_price("tok_yes")

    assert result.timestamp is not None
    assert result.timestamp.tzinfo is not None
    assert result.timestamp.tzinfo == timezone.utc


def test_fetch_price_passes_token_id_to_midpoint_endpoint() -> None:
    client = MagicMock(spec=PolymarketHTTPClient)
    client.get.return_value = {"mid": "0.50"}
    fetcher = PriceFetcher(client)

    fetcher.fetch_price("tok_abc")

    client.get.assert_called_once_with("/midpoint", params={"token_id": "tok_abc"})


# ---------------------------------------------------------------------------
# fetch_orderbook
# ---------------------------------------------------------------------------


def test_fetch_orderbook_returns_orderbook() -> None:
    raw = {
        "bids": [{"price": "0.64", "size": "50.0"}],
        "asks": [{"price": "0.66", "size": "30.0"}],
    }
    fetcher = _make_fetcher(raw)

    result = fetcher.fetch_orderbook("tok_yes")

    assert isinstance(result, Orderbook)
    assert result.token_id == "tok_yes"
    assert len(result.bids) == 1
    assert len(result.asks) == 1
    assert result.bids[0].price == pytest.approx(0.64)
    assert result.bids[0].size == pytest.approx(50.0)


def test_fetch_orderbook_handles_multiple_levels() -> None:
    raw = {
        "bids": [
            {"price": "0.64", "size": "50.0"},
            {"price": "0.63", "size": "100.0"},
        ],
        "asks": [
            {"price": "0.66", "size": "30.0"},
            {"price": "0.67", "size": "75.0"},
        ],
    }
    fetcher = _make_fetcher(raw)

    result = fetcher.fetch_orderbook("tok_yes")

    assert len(result.bids) == 2
    assert len(result.asks) == 2


def test_fetch_orderbook_empty_book() -> None:
    fetcher = _make_fetcher({"bids": [], "asks": []})

    result = fetcher.fetch_orderbook("tok_yes")

    assert result.bids == []
    assert result.asks == []


def test_fetch_orderbook_passes_token_id_param() -> None:
    client = MagicMock(spec=PolymarketHTTPClient)
    client.get.return_value = {"bids": [], "asks": []}
    fetcher = PriceFetcher(client)

    fetcher.fetch_orderbook("tok_abc")

    client.get.assert_called_once_with("/book", params={"token_id": "tok_abc"})


# ---------------------------------------------------------------------------
# Edge cases — malformed responses
# ---------------------------------------------------------------------------


def test_parse_midpoint_non_dict_raises() -> None:
    with pytest.raises(ValueError, match="Expected dict"):
        _parse_midpoint("tok_x", "not a dict")


def test_parse_midpoint_missing_key_raises() -> None:
    with pytest.raises(ValueError, match="Missing 'mid'"):
        _parse_midpoint("tok_x", {})


def test_parse_midpoint_unparseable_value_raises() -> None:
    with pytest.raises(ValueError, match="Cannot convert"):
        _parse_midpoint("tok_x", {"mid": "not_a_number"})


def test_parse_orderbook_non_dict_returns_empty() -> None:
    result = _parse_orderbook("tok_x", [1, 2, 3])

    assert result.token_id == "tok_x"
    assert result.bids == []
    assert result.asks == []


def test_parse_orderbook_missing_sides_returns_empty() -> None:
    result = _parse_orderbook("tok_x", {})

    assert result.bids == []
    assert result.asks == []


# ---------------------------------------------------------------------------
# Timestamp policy
# ---------------------------------------------------------------------------


def test_fetch_price_always_stamps_utc() -> None:
    """/midpoint never returns a timestamp; verify we always stamp UTC."""
    fetcher = _make_fetcher({"mid": "0.72"})

    result = fetcher.fetch_price("tok_yes")

    assert result.timestamp is not None
    assert result.timestamp.tzinfo == timezone.utc


def test_fetch_orderbook_uses_api_timestamp() -> None:
    raw = {
        "bids": [{"price": "0.64", "size": "50.0"}],
        "asks": [{"price": "0.66", "size": "30.0"}],
        "timestamp": "2025-03-18T16:00:00Z",
    }
    fetcher = _make_fetcher(raw)

    result = fetcher.fetch_orderbook("tok_yes")

    assert isinstance(result.timestamp, datetime)
    assert result.timestamp.year == 2025
    assert result.timestamp.month == 3
    assert result.timestamp.day == 18


def test_fetch_orderbook_falls_back_to_utc_when_no_timestamp() -> None:
    raw = {
        "bids": [{"price": "0.64", "size": "50.0"}],
        "asks": [],
    }
    fetcher = _make_fetcher(raw)

    result = fetcher.fetch_orderbook("tok_yes")

    assert result.timestamp is not None
    assert result.timestamp.tzinfo == timezone.utc


# ---------------------------------------------------------------------------
# midpoint_from_book
# ---------------------------------------------------------------------------


def test_midpoint_from_book_basic() -> None:
    book = Orderbook(
        token_id="tok_up",
        bids=[OrderbookLevel(price=0.50, size=100.0)],
        asks=[OrderbookLevel(price=0.52, size=80.0)],
    )
    result = midpoint_from_book(book)

    assert result is not None
    assert result.token_id == "tok_up"
    assert result.price == pytest.approx(0.51)


def test_midpoint_from_book_uses_best_bid_ask() -> None:
    """Best bid is max(bids), best ask is min(asks)."""
    book = Orderbook(
        token_id="tok_up",
        bids=[
            OrderbookLevel(price=0.48, size=100.0),
            OrderbookLevel(price=0.50, size=50.0),
        ],
        asks=[
            OrderbookLevel(price=0.52, size=80.0),
            OrderbookLevel(price=0.55, size=40.0),
        ],
    )
    result = midpoint_from_book(book)

    assert result is not None
    # midpoint = (0.50 + 0.52) / 2 = 0.51
    assert result.price == pytest.approx(0.51)


def test_midpoint_from_book_returns_none_when_no_bids() -> None:
    book = Orderbook(
        token_id="tok_up",
        bids=[],
        asks=[OrderbookLevel(price=0.52, size=80.0)],
    )
    assert midpoint_from_book(book) is None


def test_midpoint_from_book_returns_none_when_no_asks() -> None:
    book = Orderbook(
        token_id="tok_up",
        bids=[OrderbookLevel(price=0.50, size=100.0)],
        asks=[],
    )
    assert midpoint_from_book(book) is None


def test_midpoint_from_book_preserves_timestamp() -> None:
    ts = datetime(2025, 3, 19, 16, 0, 0, tzinfo=timezone.utc)
    book = Orderbook(
        token_id="tok_up",
        bids=[OrderbookLevel(price=0.50, size=100.0)],
        asks=[OrderbookLevel(price=0.52, size=80.0)],
        timestamp=ts,
    )
    result = midpoint_from_book(book)

    assert result is not None
    assert result.timestamp == ts
