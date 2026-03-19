"""
Synchronous data fetcher orchestrating market discovery, price/orderbook
collection, and storage.

Responsibilities:
  - Wire together MarketDiscovery, PriceFetcher, and DataStorage
  - Run one fetch cycle: discover → fetch orderbooks/prices → persist
  - Pass traceability context (condition_id, outcome, slug, event_id) to storage
  - Track price source (clob_midpoint via /midpoint vs book_midpoint fallback)
  - Handle partial failures gracefully (one bad token never crashes the cycle)
  - Log key events for debugging

Does NOT:
  - Contain strategy or execution logic
  - Manage cadence/scheduling (that belongs in run.py)
  - Perform any data analysis or transformation
"""

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from src.data.storage import DataStorage
from src.polymarket.markets import MarketDiscovery
from src.polymarket.prices import PriceFetcher, midpoint_from_book

log = logging.getLogger(__name__)


@dataclass
class FetchCycleResult:
    """Summary of a single fetch cycle for logging and debugging."""

    markets_found: int = 0
    prices_stored: int = 0
    prices_direct: int = 0
    prices_midpoint: int = 0
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
        Run one discovery → orderbook/price → storage cycle.

        Generates a unique run_id to link all artifacts (snapshot, prices,
        orderbooks) produced during this cycle for traceability.

        Partial failures are logged and counted but never crash the cycle.
        Returns a :class:`FetchCycleResult` summarizing what happened.
        """
        run_id = str(uuid.uuid4())
        run_ts = datetime.now(tz=timezone.utc)
        log.info("Starting cycle run_id=%s", run_id)
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

        # --- 2. Save market snapshot        # Save snapshot
        try:
            path = self._storage.save_market_snapshot(
                markets, run_ts, run_id=run_id
            )
            log.info("Market snapshot written: %s", path.name)
        except Exception as exc:
            log.error("Failed to save market snapshot: %s", exc)
            result.errors += 1
            # Continue — prices/orderbooks are still valuable.

        # --- 3. Fetch + store orderbooks and prices per token -----------
        for market in markets:
            ctx = _build_context(market)

            for token in market.tokens:
                tid = token.token_id
                tok_ctx: dict[str, Any] = {
                    **ctx,
                    "outcome": token.outcome,
                }

                # Orderbook first (reliable — works for neg-risk markets)
                book = None
                try:
                    book = self._price_fetcher.fetch_orderbook(tid)
                    self._storage.append_orderbook(
                        book, run_ts, run_id=run_id, context=tok_ctx
                    )
                    result.orderbooks_stored += 1
                    log.debug("Orderbook stored for %s (%s)", tid[:24], token.outcome)
                except Exception as exc:
                    log.warning(
                        "Orderbook fetch/store failed for %s: %s",
                        tid[:24], exc,
                    )
                    result.errors += 1

                # Price: fetch from /midpoint endpoint, fall back to book midpoint
                try:
                    price = self._price_fetcher.fetch_price(tid)
                    self._storage.append_price(
                        price,
                        run_ts,
                        run_id=run_id,
                        context={**tok_ctx, "price_source": "direct"},
                    )
                    result.prices_stored += 1
                    result.prices_direct += 1
                    log.debug(
                        "Price stored (direct) for %s: %.4f",
                        tid[:24], price.price,
                    )
                except Exception as exc:
                    log.debug("Direct price fetch failed for %s: %s", tid[:24], exc)
                    # Fallback: derive price from orderbook midpoint
                    if book is not None:
                        fallback = midpoint_from_book(book)
                        if fallback is not None:
                            try:
                                self._storage.append_price(
                                    fallback,
                                    run_ts,
                                    run_id=run_id,
                                    context={**tok_ctx, "price_source": "midpoint"},
                                )
                                result.prices_stored += 1
                                result.prices_midpoint += 1
                                log.debug(
                                    "Price stored (midpoint) for %s: %.4f",
                                    tid[:24], fallback.price,
                                )
                                continue
                            except Exception as store_exc:
                                log.warning(
                                    "Book-derived price store failed for %s: %s",
                                    tid[:24], store_exc,
                                )
                    log.warning("Price unavailable for %s", tid[:24])
                    result.errors += 1

        _log_cycle_summary(result)
        return result


def _build_context(market: Any) -> dict[str, Any]:
    """Build a traceability context dict from a Market object."""
    ctx: dict[str, Any] = {
        "condition_id": market.condition_id,
        "market_slug": market.slug,
        "event_id": market.event_id,
        "question": market.question,
    }
    if market.event_start_time is not None:
        ctx["event_start_time"] = market.event_start_time.isoformat()
    return ctx


def _log_cycle_summary(result: FetchCycleResult) -> None:
    """Emit an INFO-level summary of the fetch cycle."""
    log.info(
        "Fetch cycle complete: %d markets, %d orderbooks, "
        "%d prices (direct=%d, midpoint=%d), %d errors",
        result.markets_found,
        result.orderbooks_stored,
        result.prices_stored,
        result.prices_direct,
        result.prices_midpoint,
        result.errors,
    )
