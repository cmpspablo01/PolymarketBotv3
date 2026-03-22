"""
Tests for src/data/fetcher.py

Coverage:
  - happy path          : single/multiple markets, correct storage call counts
  - call order          : discovery → snapshot → orderbooks/prices
  - empty markets       : no price fetches, no snapshot saved
  - partial failures    : price failure (with midpoint fallback), orderbook failure,
                          discovery failure, snapshot storage failure
  - price fallback      : /midpoint fails → midpoint derived from orderbook
  - isolation           : orderbook failure does not block price attempt
  - traceability        : context dict passed to storage with condition_id, slug, etc.
  - price source        : prices_direct vs prices_midpoint counters
  - multi-source        : Binance spot + reference price integrated into cycle
  - external failures   : external-source failure does not crash cycle

All dependencies are mocked — no live API calls.
"""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock

from src.data.fetcher import DataFetcher
from src.data.storage import DataStorage
from src.external.binance_spot import BinanceSpotFetcher
from src.external.reference_price import ReferencePriceFetcher
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
    binance_fetcher: MagicMock | None = None,
    reference_fetcher: MagicMock | None = None,
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

    fetcher = DataFetcher(
        discovery, pf, storage,
        binance_fetcher=binance_fetcher,
        reference_fetcher=reference_fetcher,
    )
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
    """When /midpoint fails, price should be derived from orderbook midpoint."""
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
    """When both /midpoint and /book fail, errors are counted for both."""
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
    """If orderbook fails, direct /midpoint is still attempted."""
    m = _market("c1", n_tokens=1)
    fetcher, _, _, storage = _build(
        markets=[m],
        book_effect=ValueError("bad book"),
    )

    result = fetcher.run_cycle()

    assert result.orderbooks_stored == 0
    assert result.prices_stored == 1  # direct /midpoint still works
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


# ---------------------------------------------------------------------------
# Multi-source cycle (Binance spot + reference price)
# ---------------------------------------------------------------------------


def _mock_binance() -> MagicMock:
    """Return a MagicMock for BinanceSpotFetcher with a default tick."""
    mock = MagicMock(spec=BinanceSpotFetcher)
    tick = MagicMock()
    tick.price = 84273.49
    mock.fetch_latest_trade.return_value = tick
    return mock


def _mock_reference() -> MagicMock:
    """Return a MagicMock for ReferencePriceFetcher with a default tick."""
    mock = MagicMock(spec=ReferencePriceFetcher)
    tick = MagicMock()
    tick.price = 84273.49
    tick.is_proxy = True
    mock.fetch_reference_price.return_value = tick
    return mock


def test_multi_source_happy_path() -> None:
    """Full cycle: Polymarket + Binance spot + reference price all succeed."""
    binance = _mock_binance()
    reference = _mock_reference()
    fetcher, _, _, storage = _build(
        markets=[_market("c1", n_tokens=1)],
        binance_fetcher=binance,
        reference_fetcher=reference,
    )

    result = fetcher.run_cycle()

    assert result.markets_found == 1
    assert result.prices_stored == 1
    assert result.orderbooks_stored == 1
    assert result.binance_spot_stored == 1
    assert result.reference_price_stored == 1
    assert result.errors == 0
    storage.append_binance_spot_tick.assert_called_once()
    storage.append_reference_price_tick.assert_called_once()


def test_multi_source_run_id_propagated() -> None:
    """Same run_id should appear in all storage calls within one cycle."""
    binance = _mock_binance()
    reference = _mock_reference()
    fetcher, _, _, storage = _build(
        markets=[_market("c1", n_tokens=1)],
        binance_fetcher=binance,
        reference_fetcher=reference,
    )

    fetcher.run_cycle()

    # Extract run_id from each storage call
    snap_run_id = storage.save_market_snapshot.call_args[1]["run_id"]
    price_run_id = storage.append_price.call_args[1]["run_id"]
    book_run_id = storage.append_orderbook.call_args[1]["run_id"]
    binance_run_id = storage.append_binance_spot_tick.call_args[1]["run_id"]
    ref_run_id = storage.append_reference_price_tick.call_args[1]["run_id"]

    assert snap_run_id == price_run_id == book_run_id == binance_run_id == ref_run_id
    assert snap_run_id is not None


def test_binance_failure_does_not_crash_cycle() -> None:
    """Binance spot failure is logged but cycle still returns successfully."""
    binance = MagicMock(spec=BinanceSpotFetcher)
    binance.fetch_latest_trade.side_effect = RuntimeError("Binance down")
    reference = _mock_reference()
    fetcher, _, _, storage = _build(
        markets=[_market("c1", n_tokens=1)],
        binance_fetcher=binance,
        reference_fetcher=reference,
    )

    result = fetcher.run_cycle()

    assert result.binance_spot_stored == 0
    assert result.reference_price_stored == 1  # reference still works
    assert result.prices_stored == 1  # Polymarket still works
    assert result.errors >= 1


def test_reference_failure_does_not_crash_cycle() -> None:
    """Reference price failure is logged but cycle still returns successfully."""
    binance = _mock_binance()
    reference = MagicMock(spec=ReferencePriceFetcher)
    reference.fetch_reference_price.side_effect = RuntimeError("ref down")
    fetcher, _, _, storage = _build(
        markets=[_market("c1", n_tokens=1)],
        binance_fetcher=binance,
        reference_fetcher=reference,
    )

    result = fetcher.run_cycle()

    assert result.reference_price_stored == 0
    assert result.binance_spot_stored == 1  # binance still works
    assert result.prices_stored == 1  # Polymarket still works
    assert result.errors >= 1


def test_both_external_failures_do_not_crash_cycle() -> None:
    """Both external failures are logged; Polymarket part is unaffected."""
    binance = MagicMock(spec=BinanceSpotFetcher)
    binance.fetch_latest_trade.side_effect = RuntimeError("Binance down")
    reference = MagicMock(spec=ReferencePriceFetcher)
    reference.fetch_reference_price.side_effect = RuntimeError("ref down")
    fetcher, _, _, storage = _build(
        markets=[_market("c1", n_tokens=1)],
        binance_fetcher=binance,
        reference_fetcher=reference,
    )

    result = fetcher.run_cycle()

    assert result.binance_spot_stored == 0
    assert result.reference_price_stored == 0
    assert result.prices_stored == 1
    assert result.orderbooks_stored == 1
    assert result.errors == 2  # one per external source


def test_no_external_fetchers_polymarket_only() -> None:
    """When external fetchers are None, only Polymarket data is collected."""
    fetcher, _, _, storage = _build(
        markets=[_market("c1", n_tokens=1)],
        binance_fetcher=None,
        reference_fetcher=None,
    )

    result = fetcher.run_cycle()

    assert result.markets_found == 1
    assert result.prices_stored == 1
    assert result.orderbooks_stored == 1
    assert result.binance_spot_stored == 0
    assert result.reference_price_stored == 0
    assert result.errors == 0
    storage.append_binance_spot_tick.assert_not_called()
    storage.append_reference_price_tick.assert_not_called()
