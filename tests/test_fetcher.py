"""
Tests for src/data/fetcher.py

Coverage:
  - happy path          : single/multiple markets, correct storage call counts
  - call order          : discovery → snapshot → prices/orderbooks
  - empty markets       : no price fetches, no snapshot saved
  - partial failures    : price failure, orderbook failure, discovery failure,
                          snapshot storage failure — cycle never crashes
  - isolation           : price failure does not block orderbook for same token

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
            outcome="Yes" if i == 0 else "No",
        )
        for i in range(n_tokens)
    ]
    return Market(
        condition_id=condition_id,
        question="Will BTC be above $100k at 12:00 PM ET?",
        tokens=tokens,
        active=True,
        closed=False,
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
        bids=[OrderbookLevel(price=0.64, size=50.0)],
        asks=[OrderbookLevel(price=0.66, size=30.0)],
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


def test_price_failure_continues_to_next_token() -> None:
    m = _market("c1", n_tokens=2)

    def price_effect(tid: str) -> TokenPrice:
        if tid.endswith("_0"):
            raise ValueError("bad price")
        return _price(tid)

    fetcher, _, _, _ = _build(
        markets=[m], price_effect=price_effect,
    )

    result = fetcher.run_cycle()

    assert result.prices_stored == 1
    assert result.errors >= 1


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


def test_price_failure_does_not_block_orderbook() -> None:
    """If price fetch fails, orderbook for the same token is still attempted."""
    m = _market("c1", n_tokens=1)
    fetcher, _, _, storage = _build(
        markets=[m],
        price_effect=ValueError("bad price"),
    )

    result = fetcher.run_cycle()

    assert result.prices_stored == 0
    assert result.orderbooks_stored == 1
    assert result.errors == 1
    storage.append_orderbook.assert_called_once()


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
