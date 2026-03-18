"""
Tests for src/polymarket/markets.py

Coverage:
  - fetch_markets       : list response, paginated response, empty, unparseable item
  - discover_btc_15m    : filters correctly from a mixed set
  - _is_btc_15m         : keyword + time pattern matching, inactive/closed rejection
  - _parse_single_market: end_date_iso → end_date mapping, group_id/category parsed
  - params forwarding   : verify query params reach the client

All HTTP calls are mocked — no live network calls.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any
from unittest.mock import MagicMock

from src.polymarket.http_client import PolymarketHTTPClient
from src.polymarket.markets import (
    MarketDiscovery,
    _is_btc_15m,
    _parse_single_market,
)
from src.polymarket.models import Market

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _raw_market(
    condition_id: str = "cond_001",
    question: str = "Will BTC be above $100k at 12:00 PM ET?",
    active: bool = True,
    closed: bool = False,
    **extra: Any,
) -> dict[str, Any]:
    """Build a minimal raw market dict matching the expected API shape."""
    base: dict[str, Any] = {
        "condition_id": condition_id,
        "question": question,
        "tokens": [
            {"token_id": "tok_yes", "outcome": "Yes"},
            {"token_id": "tok_no", "outcome": "No"},
        ],
        "active": active,
        "closed": closed,
    }
    base.update(extra)
    return base


def _make_discovery(return_value: Any) -> MarketDiscovery:
    """Return a MarketDiscovery with a mocked client."""
    client = MagicMock(spec=PolymarketHTTPClient)
    client.get.return_value = return_value
    return MarketDiscovery(client)


# ---------------------------------------------------------------------------
# fetch_markets
# ---------------------------------------------------------------------------


def test_fetch_markets_parses_list_response() -> None:
    discovery = _make_discovery([_raw_market(), _raw_market(condition_id="cond_002")])

    markets = discovery.fetch_markets()

    assert len(markets) == 2
    assert all(isinstance(m, Market) for m in markets)


def test_fetch_markets_parses_paginated_response() -> None:
    discovery = _make_discovery({"data": [_raw_market()], "next_cursor": "abc"})

    markets = discovery.fetch_markets()

    assert len(markets) == 1
    assert markets[0].condition_id == "cond_001"


def test_fetch_markets_returns_empty_on_empty_list() -> None:
    discovery = _make_discovery([])

    assert discovery.fetch_markets() == []


def test_fetch_markets_returns_empty_on_unexpected_type() -> None:
    discovery = _make_discovery("not a list or dict")

    assert discovery.fetch_markets() == []


def test_fetch_markets_skips_unparseable_market() -> None:
    good = _raw_market(condition_id="good")
    bad = {"question": "missing condition_id"}  # will cause KeyError
    discovery = _make_discovery([good, bad])

    markets = discovery.fetch_markets()

    assert len(markets) == 1
    assert markets[0].condition_id == "good"


def test_fetch_markets_forwards_params() -> None:
    client = MagicMock(spec=PolymarketHTTPClient)
    client.get.return_value = []
    discovery = MarketDiscovery(client)

    discovery.fetch_markets(active="true", next_cursor="xyz")

    client.get.assert_called_once_with("/markets", params={"active": "true", "next_cursor": "xyz"})


# ---------------------------------------------------------------------------
# discover_btc_15m
# ---------------------------------------------------------------------------


def test_discover_btc_15m_filters_correctly() -> None:
    raw = [
        _raw_market(condition_id="btc_1", question="Will BTC be above $100k at 12:00 PM ET?"),
        _raw_market(condition_id="eth_1", question="Will ETH hit $5k at 1:00 PM?"),
        _raw_market(condition_id="btc_2", question="Bitcoin above $90k at 3:15 PM ET?"),
        _raw_market(condition_id="btc_closed", question="BTC above $80k at 4:00 PM?", closed=True),
        _raw_market(condition_id="btc_long", question="Will BTC reach $200k by end of 2025?"),
    ]
    discovery = _make_discovery(raw)

    result = discovery.discover_btc_15m()

    ids = [m.condition_id for m in result]
    assert "btc_1" in ids
    assert "btc_2" in ids
    assert "eth_1" not in ids
    assert "btc_closed" not in ids
    assert "btc_long" not in ids


# ---------------------------------------------------------------------------
# _is_btc_15m filter
# ---------------------------------------------------------------------------


def test_is_btc_15m_matches_btc_with_time() -> None:
    m = Market(
        condition_id="c1",
        question="Will BTC exceed $100k at 12:00 PM ET?",
        tokens=[], active=True, closed=False,
    )
    assert _is_btc_15m(m) is True


def test_is_btc_15m_matches_bitcoin_with_time() -> None:
    m = Market(
        condition_id="c1",
        question="Bitcoin above $90k at 3:15 PM ET?",
        tokens=[], active=True, closed=False,
    )
    assert _is_btc_15m(m) is True


def test_is_btc_15m_case_insensitive() -> None:
    m = Market(
        condition_id="c1",
        question="Will btc be above $95k at 9:45 am?",
        tokens=[], active=True, closed=False,
    )
    assert _is_btc_15m(m) is True


def test_is_btc_15m_rejects_inactive() -> None:
    m = Market(
        condition_id="c1",
        question="Will BTC hit $100k at 12:00 PM?",
        tokens=[], active=False, closed=False,
    )
    assert _is_btc_15m(m) is False


def test_is_btc_15m_rejects_closed() -> None:
    m = Market(
        condition_id="c1",
        question="Will BTC hit $100k at 12:00 PM?",
        tokens=[], active=True, closed=True,
    )
    assert _is_btc_15m(m) is False


def test_is_btc_15m_rejects_non_btc() -> None:
    m = Market(
        condition_id="c1",
        question="Will ETH hit $5k at 1:00 PM?",
        tokens=[], active=True, closed=False,
    )
    assert _is_btc_15m(m) is False


def test_is_btc_15m_rejects_long_term_btc() -> None:
    """BTC market without a time-of-day pattern must be rejected."""
    m = Market(
        condition_id="c1",
        question="Will BTC reach $200k by end of 2025?",
        tokens=[], active=True, closed=False,
    )
    assert _is_btc_15m(m) is False


def test_is_btc_15m_rejects_btc_daily_no_time() -> None:
    """BTC market with a date but no H:MM AM/PM pattern."""
    m = Market(
        condition_id="c1",
        question="Will Bitcoin close above $100k on March 18?",
        tokens=[], active=True, closed=False,
    )
    assert _is_btc_15m(m) is False


# ---------------------------------------------------------------------------
# _parse_single_market field mapping
# ---------------------------------------------------------------------------


def test_parse_single_market_maps_end_date_iso() -> None:
    raw = _raw_market(end_date_iso="2025-03-31T23:59:59Z")

    market = _parse_single_market(raw)

    assert isinstance(market.end_date, datetime)


def test_parse_single_market_end_date_none_when_absent() -> None:
    raw = _raw_market()

    market = _parse_single_market(raw)

    assert market.end_date is None


def test_parse_single_market_group_id_and_category() -> None:
    raw = _raw_market(group_id="grp_btc", category="Crypto")

    market = _parse_single_market(raw)

    assert market.group_id == "grp_btc"
    assert market.category == "Crypto"
