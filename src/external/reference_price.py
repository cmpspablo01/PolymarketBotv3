"""
BTC/USD reference-price source for Polymarket BTC 15-minute markets.

Background:
  Polymarket's BTC 15m Up/Down markets resolve based on the Chainlink
  BTC/USD oracle price at the window boundaries (eventStartTime and
  endDate).  The "Price to Beat" (PTB) is the oracle price at the start
  of the 15-minute window.

Current implementation status — **PROXY**:
  A true direct Chainlink integration would require:
    - ``web3.py`` (not in project dependencies)
    - An Ethereum/Polygon RPC provider (Infura, Alchemy — needs API key)
    - The specific Chainlink BTC/USD aggregator contract address and ABI
    - Understanding of round-based oracle update semantics

  None of these are in place.  This module therefore provides a **temporary
  proxy** that uses the Binance BTCUSDT spot price as a stand-in for
  BTC/USD.

Known limitations of the proxy:
  1. USDT ≠ USD — Tether's peg can deviate (typically <0.3%%, up to ~5%%
     historically under extreme stress).
  2. Binance is a single CEX; Chainlink aggregates multiple sources with
     its own update cadence and heartbeat.
  3. No Chainlink-specific metadata: round IDs, answeredInRound, on-chain
     updatedAt timestamps are unavailable.
  4. HTTP polling adds latency vs on-chain reads.

The ``is_proxy`` flag on every :class:`ReferencePriceTick` produced by
this module is **always True** until a real oracle integration replaces it.

Design decisions:
  - Wraps :class:`BinanceSpotFetcher` internally — no code duplication.
  - Maps ``BinanceSpotTick`` → ``ReferencePriceTick`` with explicit proxy
    metadata.
  - Preserves the three-clock timestamp provenance from the upstream tick.
  - No async, no business logic, no strategy decisions.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from src.external.binance_spot import BinanceSpotFetcher
from src.external.models import ReferencePriceTick

log = logging.getLogger(__name__)


class ReferencePriceError(Exception):
    """Raised when the reference-price fetch fails, regardless of upstream source.

    Wraps the underlying source-specific error as ``__cause__`` so callers
    can inspect it when needed, but the public contract of
    :class:`ReferencePriceFetcher` never exposes Binance-specific (or any
    other source-specific) exception types directly.
    """

REFERENCE_PAIR = "BTC/USD"
PROXY_SOURCE = "binance_spot_proxy"
PROXY_DESCRIPTION = (
    "Binance BTCUSDT spot last-trade used as temporary proxy for "
    "Chainlink BTC/USD oracle. Limitations: USDT != USD (peg can deviate), "
    "single CEX source (Chainlink aggregates multiple), no oracle round "
    "metadata. This proxy will be replaced by a direct Chainlink "
    "integration in a future phase."
)


class ReferencePriceFetcher:
    """
    Fetches a BTC/USD reference-price tick (currently proxy-based).

    Wraps :class:`BinanceSpotFetcher` and maps the result into a
    :class:`ReferencePriceTick` with ``is_proxy=True``.

    Usage::

        fetcher = ReferencePriceFetcher()
        tick = fetcher.fetch_reference_price()
        assert tick.is_proxy is True
        assert tick.pair == "BTC/USD"

    When a real Chainlink integration is available, this class should be
    replaced or extended to set ``is_proxy=False`` and ``source="chainlink"``.
    """

    def __init__(self, binance_fetcher: BinanceSpotFetcher | None = None) -> None:
        """
        Args:
            binance_fetcher: Optional pre-configured BinanceSpotFetcher.
                If None, a default instance is created.
        """
        self._binance = binance_fetcher or BinanceSpotFetcher()

    def fetch_reference_price(self) -> ReferencePriceTick:
        """
        Fetch the current BTC/USD reference price (proxy-based).

        Delegates to :meth:`BinanceSpotFetcher.fetch_latest_trade` and
        maps the result into a :class:`ReferencePriceTick`.

        Returns:
            A :class:`ReferencePriceTick` with ``is_proxy=True``.

        Raises:
            ReferencePriceError: If the upstream fetch fails.  The
                original source-specific exception is preserved as
                ``__cause__``.
        """
        try:
            upstream = self._binance.fetch_latest_trade()
        except Exception as exc:
            raise ReferencePriceError(
                f"Reference price fetch failed: {exc}"
            ) from exc

        processed_ts = datetime.now(tz=timezone.utc)

        tick = ReferencePriceTick(
            pair=REFERENCE_PAIR,
            price=upstream.price,
            source_timestamp=upstream.exchange_timestamp,
            local_receive_timestamp=upstream.local_receive_timestamp,
            processed_timestamp=processed_ts,
            source=PROXY_SOURCE,
            is_proxy=True,
            proxy_description=PROXY_DESCRIPTION,
            raw_payload=upstream.raw_payload,
        )

        log.info(
            "Reference price (proxy): %.2f %s [source=%s, is_proxy=%s]",
            tick.price,
            tick.pair,
            tick.source,
            tick.is_proxy,
        )

        return tick
