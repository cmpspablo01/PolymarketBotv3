"""
Tests for src/polymarket/models.py

Coverage:
  - Token              : valid construction, missing required fields
  - Market             : valid construction, optional end_date_iso, missing required
  - TokenPrice         : valid construction, boundary prices (0.0 and 1.0),
                         out-of-range prices (negative, > 1.0), optional timestamp
  - OrderbookLevel     : valid construction, missing required fields
  - Orderbook          : valid construction with nested levels, empty sides,
                         optional timestamp
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from src.polymarket.models import Market, Orderbook, OrderbookLevel, Token, TokenPrice

# ---------------------------------------------------------------------------
# Token
# ---------------------------------------------------------------------------


def test_token_valid() -> None:
    token = Token(token_id="abc123", outcome="YES")
    assert token.token_id == "abc123"
    assert token.outcome == "YES"


def test_token_missing_token_id_raises() -> None:
    with pytest.raises(ValidationError):
        Token(outcome="YES")  # type: ignore[call-arg]


def test_token_missing_outcome_raises() -> None:
    with pytest.raises(ValidationError):
        Token(token_id="abc123")  # type: ignore[call-arg]


# ---------------------------------------------------------------------------
# Market
# ---------------------------------------------------------------------------


def _make_tokens() -> list[dict[str, str]]:
    return [
        {"token_id": "tok_yes", "outcome": "YES"},
        {"token_id": "tok_no", "outcome": "NO"},
    ]


def test_market_valid() -> None:
    market = Market(
        condition_id="cond_001",
        question="Will BTC exceed $100k by end of Q1?",
        tokens=_make_tokens(),
        active=True,
        closed=False,
    )
    assert market.condition_id == "cond_001"
    assert len(market.tokens) == 2
    assert isinstance(market.tokens[0], Token)


def test_market_end_date_optional() -> None:
    market = Market(
        condition_id="cond_002",
        question="BTC > 90k?",
        tokens=_make_tokens(),
        active=True,
        closed=False,
    )
    assert market.end_date is None


def test_market_end_date_set() -> None:
    market = Market(
        condition_id="cond_003",
        question="BTC > 90k?",
        tokens=_make_tokens(),
        active=True,
        closed=False,
        end_date=datetime(2025, 3, 31, 23, 59, 59, tzinfo=timezone.utc),
    )
    assert market.end_date == datetime(2025, 3, 31, 23, 59, 59, tzinfo=timezone.utc)


def test_market_end_date_parsed_from_iso_string() -> None:
    market = Market(
        condition_id="cond_003b",
        question="BTC > 90k?",
        tokens=_make_tokens(),
        active=True,
        closed=False,
        end_date="2025-03-31T23:59:59Z",  # type: ignore[arg-type]
    )
    assert isinstance(market.end_date, datetime)


def test_market_group_id_optional() -> None:
    market = Market(
        condition_id="cond_004",
        question="BTC > 90k?",
        tokens=_make_tokens(),
        active=True,
        closed=False,
    )
    assert market.group_id is None


def test_market_category_optional() -> None:
    market = Market(
        condition_id="cond_005",
        question="BTC > 90k?",
        tokens=_make_tokens(),
        active=True,
        closed=False,
    )
    assert market.category is None


def test_market_group_id_and_category_set() -> None:
    market = Market(
        condition_id="cond_006",
        question="BTC > 90k?",
        tokens=_make_tokens(),
        active=True,
        closed=False,
        group_id="grp_btc",
        category="Crypto",
    )
    assert market.group_id == "grp_btc"
    assert market.category == "Crypto"


def test_market_enriched_fields_optional() -> None:
    """New enriched fields all default to None."""
    market = Market(
        condition_id="cond_010",
        question="BTC > 90k?",
        tokens=_make_tokens(),
        active=True,
        closed=False,
    )
    assert market.slug is None
    assert market.market_id is None
    assert market.event_id is None
    assert market.event_slug is None
    assert market.description is None
    assert market.start_date is None
    assert market.event_start_time is None


def test_market_enriched_fields_set() -> None:
    market = Market(
        condition_id="cond_011",
        question="Bitcoin Up or Down - March 19, 4:00PM-4:15PM ET",
        tokens=_make_tokens(),
        active=True,
        closed=False,
        slug="btc-updown-15m-123",
        market_id="55555",
        event_id="99001",
        event_slug="ev-slug",
        description="Resolves Up if BTC...",
        start_date=datetime(2025, 3, 18, 15, 45, tzinfo=timezone.utc),
        event_start_time=datetime(2025, 3, 19, 16, 0, tzinfo=timezone.utc),
    )
    assert market.slug == "btc-updown-15m-123"
    assert market.market_id == "55555"
    assert market.event_id == "99001"
    assert market.event_slug == "ev-slug"
    assert market.description == "Resolves Up if BTC..."
    assert market.start_date == datetime(2025, 3, 18, 15, 45, tzinfo=timezone.utc)
    assert market.event_start_time == datetime(2025, 3, 19, 16, 0, tzinfo=timezone.utc)


def test_market_event_start_time_parsed_from_iso_string() -> None:
    market = Market(
        condition_id="cond_012",
        question="BTC > 90k?",
        tokens=_make_tokens(),
        active=True,
        closed=False,
        event_start_time="2026-03-19T00:45:00Z",  # type: ignore[arg-type]
    )
    assert isinstance(market.event_start_time, datetime)


def test_market_missing_condition_id_raises() -> None:
    with pytest.raises(ValidationError):
        Market(  # type: ignore[call-arg]
            question="BTC > 90k?",
            tokens=_make_tokens(),
            active=True,
            closed=False,
        )


def test_market_missing_tokens_raises() -> None:
    with pytest.raises(ValidationError):
        Market(  # type: ignore[call-arg]
            condition_id="cond_001",
            question="BTC > 90k?",
            active=True,
            closed=False,
        )


def test_market_empty_tokens_accepted() -> None:
    market = Market(
        condition_id="cond_004",
        question="BTC > 90k?",
        tokens=[],
        active=False,
        closed=True,
    )
    assert market.tokens == []


# ---------------------------------------------------------------------------
# TokenPrice
# ---------------------------------------------------------------------------


def test_token_price_valid() -> None:
    tp = TokenPrice(token_id="tok_yes", price=0.72)
    assert tp.token_id == "tok_yes"
    assert tp.price == 0.72


def test_token_price_boundary_zero() -> None:
    tp = TokenPrice(token_id="tok_yes", price=0.0)
    assert tp.price == 0.0


def test_token_price_boundary_one() -> None:
    tp = TokenPrice(token_id="tok_yes", price=1.0)
    assert tp.price == 1.0


def test_token_price_negative_raises() -> None:
    with pytest.raises(ValidationError, match="0.0 and 1.0"):
        TokenPrice(token_id="tok_yes", price=-0.01)


def test_token_price_above_one_raises() -> None:
    with pytest.raises(ValidationError, match="0.0 and 1.0"):
        TokenPrice(token_id="tok_yes", price=1.01)


def test_token_price_timestamp_optional() -> None:
    tp = TokenPrice(token_id="tok_yes", price=0.5)
    assert tp.timestamp is None


def test_token_price_timestamp_set() -> None:
    now = datetime.now(tz=timezone.utc)
    tp = TokenPrice(token_id="tok_yes", price=0.5, timestamp=now)
    assert tp.timestamp == now


def test_token_price_missing_token_id_raises() -> None:
    with pytest.raises(ValidationError):
        TokenPrice(price=0.5)  # type: ignore[call-arg]


# ---------------------------------------------------------------------------
# OrderbookLevel
# ---------------------------------------------------------------------------


def test_orderbook_level_valid() -> None:
    level = OrderbookLevel(price=0.65, size=100.0)
    assert level.price == 0.65
    assert level.size == 100.0


def test_orderbook_level_missing_price_raises() -> None:
    with pytest.raises(ValidationError):
        OrderbookLevel(size=100.0)  # type: ignore[call-arg]


def test_orderbook_level_missing_size_raises() -> None:
    with pytest.raises(ValidationError):
        OrderbookLevel(price=0.65)  # type: ignore[call-arg]


# ---------------------------------------------------------------------------
# Orderbook
# ---------------------------------------------------------------------------


def test_orderbook_valid() -> None:
    ob = Orderbook(
        token_id="tok_yes",
        bids=[OrderbookLevel(price=0.64, size=50.0)],
        asks=[OrderbookLevel(price=0.66, size=30.0)],
    )
    assert ob.token_id == "tok_yes"
    assert len(ob.bids) == 1
    assert len(ob.asks) == 1
    assert isinstance(ob.bids[0], OrderbookLevel)


def test_orderbook_empty_sides_accepted() -> None:
    ob = Orderbook(token_id="tok_yes", bids=[], asks=[])
    assert ob.bids == []
    assert ob.asks == []


def test_orderbook_timestamp_optional() -> None:
    ob = Orderbook(token_id="tok_yes", bids=[], asks=[])
    assert ob.timestamp is None


def test_orderbook_timestamp_set() -> None:
    ts = datetime(2025, 3, 18, 16, 0, 0, tzinfo=timezone.utc)
    ob = Orderbook(token_id="tok_yes", bids=[], asks=[], timestamp=ts)
    assert ob.timestamp == ts


def test_orderbook_timestamp_parsed_from_iso_string() -> None:
    ob = Orderbook(
        token_id="tok_yes", bids=[], asks=[],
        timestamp="2025-03-18T16:00:00Z",  # type: ignore[arg-type]
    )
    assert isinstance(ob.timestamp, datetime)


def test_orderbook_missing_token_id_raises() -> None:
    with pytest.raises(ValidationError):
        Orderbook(  # type: ignore[call-arg]
            bids=[],
            asks=[],
        )
