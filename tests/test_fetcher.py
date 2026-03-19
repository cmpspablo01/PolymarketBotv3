"""
Tests for src/data/fetcher.py

Coverage:
  - happy path          : single/multiple markets, correct storage call counts
  - call order          : discovery → snapshot → orderbooks/prices
  - empty markets       : no price fetches, no snapshot saved
  - partial failures    : price failure (with midpoint fallback), orderbook failure,
                          discovery failure, snapshot storage failure
  - price fallback      : /price fails → midpoint derived from orderbook
  - isolation           : orderbook failure does not block price attempt
  - traceability        : context dict passed to storage with condition_id, slug, etc.
  - price source        : prices_direct vs prices_midpoint counters

All dependencies are mocked — no live API calls.
"""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock

from src.data.fetcher import DataFetcher
from src.data.storage import DataStorage
from src.polymarket.markets import MarketDiscovery
from src.polymarket.models import (
    Market,
    Orderbook,
    OrderbookLevel,
    Token,
    TokenPrice,
)
from src.polymarket.prices import PriceFetcher

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _market(
    condition_id: str = "cond_001", n_tokens: int = 2,
) -> Market:
    tokens = [
        Token(
            token_id=f"tok_{condition_id}_{i}",
            outcome="Up" if i == 0 else "Down",
        )
        for i in range(n_tokens)
    ]
    return Market(
        condition_id=condition_id,
        question="Bitcoin Up or Down - March 19, 4:00PM-4:15PM ET",
        tokens=tokens,
        active=True,
        closed=False,
        slug=f"btc-updown-15m-{condition_id}",
        event_id="ev_001",
        event_slug=f"btc-updown-15m-ev-{condition_id}",
        event_start_time=datetime(2025, 3, 19, 16, 0, 0, tzinfo=timezone.utc),
    )


def _price(token_id: str) -> TokenPrice:
    return TokenPrice(
        token_id=token_id,
        price=0.72,
        timestamp=datetime(2025, 3, 18, 16, 0, 0, tzinfo=timezone.utc),
    )


def _orderbook(token_id: str) -> Orderbook:
    return Orderbook(
        token_id=token_id,
        bids=[OrderbookLevel(price=0.50, size=50.0)],
        asks=[OrderbookLevel(price=0.52, size=30.0)],
    )


def _build(
    markets: list[Market] | Exception | None = None,
    price_effect: object = None,
    book_effect: object = None,
) -> tuple[DataFetcher, MagicMock, MagicMock, MagicMock]:
    """
    Build a DataFetcher with mocked dependencies.

    Returns ``(fetcher, discovery_mock, price_fetcher_mock, storage_mock)``.
    """
    discovery = MagicMock(spec=MarketDiscovery)
    if isinstance(markets, Exception):
        discovery.discover_btc_15m.side_effect = markets
    else:
        discovery.discover_btc_15m.return_value = (
            markets if markets is not None else []
        )

    pf = MagicMock(spec=PriceFetcher)
    if price_effect is not None:
        pf.fetch_price.side_effect = price_effect
    else:
        pf.fetch_price.side_effect = lambda tid: _price(tid)

    if book_effect is not None:
        pf.fetch_orderbook.side_effect = book_effect
    else:
        pf.fetch_orderbook.side_effect = lambda tid: _orderbook(tid)

    storage = MagicMock(spec=DataStorage)

    fetcher = DataFetcher(discovery, pf, storage)
    return fetcher, discovery, pf, storage


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


def test_happy_path_single_market() -> None:
    fetcher, _, _, _ = _build(markets=[_market("c1", n_tokens=2)])

    result = fetcher.run_cycle()

    assert result.markets_found == 1
    assert result.prices_stored == 2
    assert result.prices_direct == 2
    assert result.prices_midpoint == 0
    assert result.orderbooks_stored == 2
    assert result.errors == 0


def test_happy_path_multiple_markets() -> None:
    fetcher, _, _, _ = _build(
        markets=[_market("c1"), _market("c2")],
    )

    result = fetcher.run_cycle()

    assert result.markets_found == 2
    assert result.prices_stored == 4   # 2 markets × 2 tokens
    assert result.orderbooks_stored == 4
    assert result.errors == 0


def test_storage_calls_correct_counts() -> None:
    fetcher, disc, _, storage = _build(
        markets=[_market("c1", n_tokens=1)],
    )

    fetcher.run_cycle()

    disc.discover_btc_15m.assert_called_once()
    storage.save_market_snapshot.assert_called_once()
    assert storage.append_price.call_count == 1
    assert storage.append_orderbook.call_count == 1


# ---------------------------------------------------------------------------
# Traceability context
# ---------------------------------------------------------------------------


def test_context_passed_to_append_orderbook() -> None:
    """append_orderbook receives context with condition_id, slug, outcome."""
    m = _market("c1", n_tokens=1)
    fetcher, _, _, storage = _build(markets=[m])

    fetcher.run_cycle()

    _, kwargs = storage.append_orderbook.call_args
    ctx = kwargs["context"]
    assert ctx["condition_id"] == "c1"
    assert ctx["market_slug"] == "btc-updown-15m-c1"
    assert ctx["event_id"] == "ev_001"
    assert ctx["outcome"] == "Up"
    assert ctx["event_start_time"] == "2025-03-19T16:00:00+00:00"


def test_context_passed_to_append_price_direct() -> None:
    """append_price receives context with price_source='direct'."""
    m = _market("c1", n_tokens=1)
    fetcher, _, _, storage = _build(markets=[m])

    fetcher.run_cycle()

    _, kwargs = storage.append_price.call_args
    ctx = kwargs["context"]
    assert ctx["price_source"] == "direct"
    assert ctx["condition_id"] == "c1"
    assert ctx["outcome"] == "Up"


def test_context_passed_to_append_price_midpoint() -> None:
    """When price falls back to midpoint, price_source='midpoint'."""
    m = _market("c1", n_tokens=1)
    fetcher, _, _, storage = _build(
        markets=[m],
        price_effect=ValueError("Invalid side"),
    )

    fetcher.run_cycle()

    _, kwargs = storage.append_price.call_args
    ctx = kwargs["context"]
    assert ctx["price_source"] == "midpoint"


# ---------------------------------------------------------------------------
# Empty markets
# ---------------------------------------------------------------------------


def test_empty_markets_no_further_calls() -> None:
    fetcher, _, pf, storage = _build(markets=[])

    result = fetcher.run_cycle()

    assert result.markets_found == 0
    assert result.prices_stored == 0
    assert result.errors == 0
    pf.fetch_price.assert_not_called()
    storage.save_market_snapshot.assert_not_called()


# ---------------------------------------------------------------------------
# Partial failures
# ---------------------------------------------------------------------------


def test_price_failure_falls_back_to_book_midpoint() -> None:
    """When /price fails, price should be derived from orderbook midpoint."""
    m = _market("c1", n_tokens=1)
    fetcher, _, _, storage = _build(
        markets=[m],
        price_effect=ValueError("Invalid side"),  # neg-risk 400 error
    )

    result = fetcher.run_cycle()

    # Orderbook succeeds, midpoint fallback provides a price
    assert result.orderbooks_stored == 1
    assert result.prices_stored == 1   # derived from book
    assert result.prices_direct == 0
    assert result.prices_midpoint == 1
    assert result.errors == 0
    # append_price called with midpoint = (0.50 + 0.52) / 2 = 0.51
    stored_price = storage.append_price.call_args[0][0]
    assert abs(stored_price.price - 0.51) < 0.001


def test_price_failure_no_fallback_when_book_also_fails() -> None:
    """When both /price and /book fail, errors are counted for both."""
    m = _market("c1", n_tokens=1)
    fetcher, _, _, storage = _build(
        markets=[m],
        price_effect=ValueError("Invalid side"),
        book_effect=ValueError("bad book"),
    )

    result = fetcher.run_cycle()

    assert result.orderbooks_stored == 0
    assert result.prices_stored == 0
    assert result.errors == 2  # book failure + price failure (no fallback)


def test_orderbook_failure_continues_to_next_token() -> None:
    m = _market("c1", n_tokens=2)

    def book_effect(tid: str) -> Orderbook:
        if tid.endswith("_0"):
            raise ValueError("bad book")
        return _orderbook(tid)

    fetcher, _, _, _ = _build(
        markets=[m], book_effect=book_effect,
    )

    result = fetcher.run_cycle()

    assert result.orderbooks_stored == 1
    assert result.errors >= 1


def test_orderbook_failure_does_not_block_price() -> None:
    """If orderbook fails, direct /price is still attempted."""
    m = _market("c1", n_tokens=1)
    fetcher, _, _, storage = _build(
        markets=[m],
        book_effect=ValueError("bad book"),
    )

    result = fetcher.run_cycle()

    assert result.orderbooks_stored == 0
    assert result.prices_stored == 1  # direct /price still works
    assert result.prices_direct == 1
    assert result.errors >= 1
    storage.append_price.assert_called_once()


def test_discovery_failure_returns_early() -> None:
    fetcher, _, _, storage = _build(
        markets=RuntimeError("API down"),
    )

    result = fetcher.run_cycle()

    assert result.markets_found == 0
    assert result.errors == 1
    storage.save_market_snapshot.assert_not_called()


def test_snapshot_storage_failure_continues_to_prices() -> None:
    m = _market("c1", n_tokens=1)
    fetcher, _, _, storage = _build(markets=[m])
    storage.save_market_snapshot.side_effect = OSError("disk full")

    result = fetcher.run_cycle()

    assert result.errors >= 1
    # Prices/orderbooks still attempted despite snapshot failure
    assert result.prices_stored == 1
    assert result.orderbooks_stored == 1
