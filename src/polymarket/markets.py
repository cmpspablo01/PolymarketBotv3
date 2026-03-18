"""
Market discovery for BTC 15-minute Polymarket markets.

Responsibilities:
  - Fetch markets from the Polymarket CLOB API via PolymarketHTTPClient
  - Parse raw API responses into typed Market models
  - Filter for active BTC 15-minute markets

Does NOT:
  - Fetch prices or orderbook data (that belongs in prices.py)
  - Contain strategy or execution logic

API assumptions (Phase 1):
  - GET /markets returns either a JSON list or {"data": [...], "next_cursor": "..."}
  - Each market object contains: condition_id, question, tokens, active, closed,
    end_date_iso (optional), group_id (optional), category (optional)
  - Pagination is not chased automatically; caller can pass next_cursor as a param
"""

from __future__ import annotations

import logging
import re
from typing import Any

from src.polymarket.http_client import PolymarketHTTPClient
from src.polymarket.models import Market, Token

log = logging.getLogger(__name__)

MARKETS_ENDPOINT = "/markets"

# Heuristic keywords for BTC market discovery (case-insensitive)
_BTC_KEYWORDS: frozenset[str] = frozenset({"btc", "bitcoin"})

# Matches a time-of-day like "12:00 PM", "3:15 AM", "9:45PM".
# Polymarket BTC 15m markets embed a specific resolution time in the question.
# Long-term BTC markets (e.g. "Will BTC reach $200k by 2026?") lack this pattern.
_TIME_PATTERN: re.Pattern[str] = re.compile(r"\d{1,2}:\d{2}\s*[ap]m", re.IGNORECASE)


class MarketDiscovery:
    """
    Discovers active BTC 15-minute markets on Polymarket.

    Wraps PolymarketHTTPClient to fetch, parse, and filter markets.

    Usage::

        discovery = MarketDiscovery(client)
        btc_markets = discovery.discover_btc_15m()
    """

    def __init__(self, client: PolymarketHTTPClient) -> None:
        self._client = client

    def fetch_markets(self, **params: Any) -> list[Market]:
        """
        Fetch one page of markets from the API and return typed Market models.

        Extra keyword arguments are forwarded as query parameters.
        """
        raw = self._client.get(MARKETS_ENDPOINT, params=params or None)
        return _parse_markets(raw)

    def discover_btc_15m(self, **params: Any) -> list[Market]:
        """
        Fetch markets and filter for active BTC 15-minute candidates.

        Returns only markets where the question mentions BTC/Bitcoin
        and the market is active and not closed.
        """
        markets = self.fetch_markets(**params)
        filtered = [m for m in markets if _is_btc_15m(m)]
        log.info(
            "Discovered %d BTC 15m candidates out of %d markets",
            len(filtered),
            len(markets),
        )
        return filtered


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------


def _parse_markets(raw: Any) -> list[Market]:
    """
    Parse raw API response into a list of Market models.

    Handles both flat list and paginated {"data": [...]} response shapes.
    Skips individual markets that fail to parse (logs a warning, does not crash).
    """
    if isinstance(raw, dict):
        items = raw.get("data", [])
    elif isinstance(raw, list):
        items = raw
    else:
        log.warning("Unexpected markets response type: %s", type(raw).__name__)
        return []

    markets: list[Market] = []
    for item in items:
        try:
            markets.append(_parse_single_market(item))
        except Exception:
            cid = item.get("condition_id", "unknown") if isinstance(item, dict) else "unknown"
            log.warning("Skipping unparseable market: %s", cid)
    return markets


def _parse_single_market(raw: dict[str, Any]) -> Market:
    """
    Parse a single raw market dict into a Market model.

    Maps API field ``end_date_iso`` to model field ``end_date``.
    """
    tokens_raw: list[dict[str, Any]] = raw.get("tokens", [])
    return Market(
        condition_id=raw["condition_id"],
        question=raw["question"],
        tokens=[Token(token_id=t["token_id"], outcome=t["outcome"]) for t in tokens_raw],
        active=raw.get("active", False),
        closed=raw.get("closed", True),
        end_date=raw.get("end_date_iso"),
        group_id=raw.get("group_id"),
        category=raw.get("category"),
    )


# ---------------------------------------------------------------------------
# Filter helpers
# ---------------------------------------------------------------------------


def _is_btc_15m(market: Market) -> bool:
    """
    Heuristic filter for BTC 15-minute market candidates.

    A market passes if ALL of the following are true:
      1. ``active is True`` and ``closed is False``
      2. The question mentions "BTC" or "Bitcoin" (case-insensitive)
      3. The question contains a time-of-day pattern (e.g. "12:15 PM")

    Rule 3 separates short-duration (15m / hourly) BTC markets from
    long-term markets like "Will BTC reach $200k by end of 2025?".

    Limitations:
      - Cannot distinguish 15-minute from hourly resolution solely from
        a single question; true 15m identification requires cross-market
        pattern analysis or group metadata (later phase).
      - Relies on the question containing an ``H:MM AM/PM`` time string.
    """
    if not market.active or market.closed:
        return False
    q = market.question.lower()
    has_btc = any(kw in q for kw in _BTC_KEYWORDS)
    has_time = bool(_TIME_PATTERN.search(market.question))
    return has_btc and has_time
