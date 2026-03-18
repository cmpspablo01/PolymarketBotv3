"""
Synchronous data fetcher orchestrating market discovery, price/orderbook
collection, and storage.

Responsibilities:
  - Wire together MarketDiscovery, PriceFetcher, and DataStorage
  - Run one fetch cycle: discover → fetch prices/orderbooks → persist
  - Handle partial failures gracefully (one bad token never crashes the cycle)
  - Log key events for debugging

Does NOT:
  - Contain strategy or execution logic
  - Manage cadence/scheduling (that belongs in run.py)
  - Perform any data analysis or transformation
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone

from src.data.storage import DataStorage
from src.polymarket.markets import MarketDiscovery
from src.polymarket.prices import PriceFetcher

log = logging.getLogger(__name__)


@dataclass
class FetchCycleResult:
    """Summary of a single fetch cycle for logging and debugging."""

    markets_found: int = 0
    prices_stored: int = 0
    orderbooks_stored: int = 0
    errors: int = 0


class DataFetcher:
    """
    Thin synchronous orchestrator for one fetch cycle.

    Usage::

        fetcher = DataFetcher(discovery, price_fetcher, storage)
        result = fetcher.run_cycle()
    """

    def __init__(
        self,
        discovery: MarketDiscovery,
        price_fetcher: PriceFetcher,
        storage: DataStorage,
    ) -> None:
        self._discovery = discovery
        self._price_fetcher = price_fetcher
        self._storage = storage

    def run_cycle(self) -> FetchCycleResult:
        """
        Run one discovery → price/orderbook → storage cycle.

        Partial failures are logged and counted but never crash the cycle.
        Returns a :class:`FetchCycleResult` summarizing what happened.
        """
        result = FetchCycleResult()

        # --- 1. Discover markets ----------------------------------------
        try:
            markets = self._discovery.discover_btc_15m()
        except Exception as exc:
            log.error("Market discovery failed: %s", exc)
            result.errors += 1
            return result

        result.markets_found = len(markets)

        if not markets:
            log.info("No BTC 15m markets found, cycle complete")
            return result

        # --- 2. Save market snapshot ------------------------------------
        snapshot_ts = datetime.now(tz=timezone.utc)
        try:
            self._storage.save_market_snapshot(markets, snapshot_ts)
        except Exception as exc:
            log.error("Market snapshot storage failed: %s", exc)
            result.errors += 1
            # Continue — prices/orderbooks are still valuable.

        # --- 3. Fetch + store prices and orderbooks per token -----------
        for market in markets:
            for token in market.tokens:
                tid = token.token_id

                # Price
                try:
                    price = self._price_fetcher.fetch_price(tid)
                    self._storage.append_price(price)
                    result.prices_stored += 1
                except Exception as exc:
                    log.warning(
                        "Price fetch/store failed for %s: %s",
                        tid, exc,
                    )
                    result.errors += 1

                # Orderbook (always attempted, even if price failed)
                try:
                    book = self._price_fetcher.fetch_orderbook(tid)
                    self._storage.append_orderbook(book)
                    result.orderbooks_stored += 1
                except Exception as exc:
                    log.warning(
                        "Orderbook fetch/store failed for %s: %s",
                        tid, exc,
                    )
                    result.errors += 1

        log.info(
            "Fetch cycle complete: %d markets, %d prices, "
            "%d orderbooks, %d errors",
            result.markets_found,
            result.prices_stored,
            result.orderbooks_stored,
            result.errors,
        )
        return result
