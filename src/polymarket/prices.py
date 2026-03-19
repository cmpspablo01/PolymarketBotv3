"""
Price and orderbook fetching for Polymarket tokens.

Responsibilities:
  - Fetch mid-price for a single token via PolymarketHTTPClient
  - Fetch orderbook snapshot for a single token
  - Parse raw API responses into typed TokenPrice / Orderbook models

Does NOT:
  - Discover markets (that belongs in markets.py)
  - Contain strategy or execution logic

API assumptions (Phase 1):
  - GET /midpoint?token_id=X returns {"mid": "0.72"} (string value).
    This is the CLOB-native midpoint price.  The older /price endpoint
    requires a ``side`` parameter (buy|sell) and returns the best bid or
    ask — NOT a midpoint.  Calling /price without side returns 400.
  - GET /book?token_id=X   returns {"bids": [...], "asks": [...]}
    where each level is {"price": "0.64", "size": "100.0"}  (string values)
    May optionally include a "timestamp" field (ISO 8601 string).

Timestamp policy:
  - /midpoint does not return a timestamp; we always stamp with local UTC.
  - /book may include a "timestamp" field (ISO 8601 → datetime);
    we fall back to datetime.now(UTC) when absent.

Error handling:
  - _parse_midpoint raises ValueError on non-dict response, missing "mid" key,
    or unparseable mid value.  Callers decide how to handle the error.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from src.polymarket.http_client import PolymarketHTTPClient
from src.polymarket.models import Orderbook, OrderbookLevel, TokenPrice

log = logging.getLogger(__name__)

MIDPOINT_ENDPOINT = "/midpoint"
BOOK_ENDPOINT = "/book"


class PriceFetcher:
    """
    Fetches prices and orderbooks for Polymarket tokens.

    Wraps PolymarketHTTPClient to fetch and parse price data.

    Usage::

        fetcher = PriceFetcher(client)
        price = fetcher.fetch_price("token_abc")
        book  = fetcher.fetch_orderbook("token_abc")
    """

    def __init__(self, client: PolymarketHTTPClient) -> None:
        self._client = client

    def fetch_price(self, token_id: str) -> TokenPrice:
        """
        Fetch the CLOB midpoint price for a token via ``/midpoint``.

        Returns a TokenPrice stamped with the current UTC time
        (the /midpoint endpoint does not include a timestamp).

        Raises:
            ValueError: If the response is malformed or the mid value is missing.
        """
        raw = self._client.get(MIDPOINT_ENDPOINT, params={"token_id": token_id})
        return _parse_midpoint(token_id, raw)

    def fetch_orderbook(self, token_id: str) -> Orderbook:
        """
        Fetch the current orderbook snapshot for a token.
        """
        raw = self._client.get(BOOK_ENDPOINT, params={"token_id": token_id})
        return _parse_orderbook(token_id, raw)


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------


def _parse_midpoint(token_id: str, raw: Any) -> TokenPrice:
    """
    Parse a ``/midpoint`` response into a TokenPrice.

    Expected format: ``{"mid": "0.72"}``.

    Raises:
        ValueError: If *raw* is not a dict, the ``mid`` key is missing,
                    or the value cannot be converted to float.
    """
    if not isinstance(raw, dict):
        raise ValueError(
            f"Expected dict for midpoint response of token {token_id}, "
            f"got {type(raw).__name__}"
        )

    raw_mid = raw.get("mid")
    if raw_mid is None:
        raise ValueError(f"Missing 'mid' key in response for token {token_id}")

    try:
        price = float(raw_mid)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            f"Cannot convert mid '{raw_mid}' to float for token {token_id}"
        ) from exc

    return TokenPrice(
        token_id=token_id,
        price=price,
        timestamp=datetime.now(tz=timezone.utc),
    )


def midpoint_from_book(book: Orderbook) -> TokenPrice | None:
    """
    Derive a mid-price from the best bid/ask of an orderbook.

    Returns None if the book has no bids or no asks.
    Used as a fallback when the /price endpoint fails (e.g. neg-risk markets).
    """
    if not book.bids or not book.asks:
        return None
    best_bid = max(b.price for b in book.bids)
    best_ask = min(a.price for a in book.asks)
    mid = round((best_bid + best_ask) / 2, 6)
    return TokenPrice(
        token_id=book.token_id,
        price=mid,
        timestamp=book.timestamp,
    )


def _parse_orderbook(token_id: str, raw: Any) -> Orderbook:
    """
    Parse raw orderbook API response into an Orderbook model.

    Handles string-to-float coercion for price and size values.
    Applies the same timestamp priority as ``_parse_price``:
    API ``timestamp`` first, ``datetime.now(UTC)`` fallback.
    """
    if not isinstance(raw, dict):
        log.warning("Unexpected book response type for %s: %s", token_id, type(raw).__name__)
        return Orderbook(token_id=token_id, bids=[], asks=[])

    bids = [
        OrderbookLevel(price=float(b["price"]), size=float(b["size"]))
        for b in raw.get("bids", [])
    ]
    asks = [
        OrderbookLevel(price=float(a["price"]), size=float(a["size"]))
        for a in raw.get("asks", [])
    ]

    # Timestamp priority: API field first, local UTC fallback.
    raw_ts = raw.get("timestamp")
    if raw_ts is not None:
        timestamp = raw_ts
    else:
        timestamp = datetime.now(tz=timezone.utc)

    return Orderbook(
        token_id=token_id, bids=bids, asks=asks, timestamp=timestamp,
    )
