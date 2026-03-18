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
  - GET /price?token_id=X  returns {"price": "0.72"}  (string value)
    May optionally include a "timestamp" field (ISO 8601 string).
  - GET /book?token_id=X   returns {"bids": [...], "asks": [...]}
    where each level is {"price": "0.64", "size": "100.0"}  (string values)
    May optionally include a "timestamp" field (ISO 8601 string).

Timestamp priority (applies to both price and orderbook):
  1. Use the API-provided "timestamp" field if present (ISO 8601 → datetime).
  2. Fall back to datetime.now(UTC) only when the API omits the field.

Error handling:
  - _parse_price raises ValueError on non-dict response, missing "price" key,
    or unparseable price value.  Callers decide how to handle the error.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from src.polymarket.http_client import PolymarketHTTPClient
from src.polymarket.models import Orderbook, OrderbookLevel, TokenPrice

log = logging.getLogger(__name__)

PRICE_ENDPOINT = "/price"
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
        Fetch the current mid-price for a token.

        Returns a TokenPrice stamped with the API timestamp if available,
        otherwise with the current UTC time.

        Raises:
            ValueError: If the response is malformed or the price is missing.
        """
        raw = self._client.get(PRICE_ENDPOINT, params={"token_id": token_id})
        return _parse_price(token_id, raw)

    def fetch_orderbook(self, token_id: str) -> Orderbook:
        """
        Fetch the current orderbook snapshot for a token.
        """
        raw = self._client.get(BOOK_ENDPOINT, params={"token_id": token_id})
        return _parse_orderbook(token_id, raw)


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------


def _parse_price(token_id: str, raw: Any) -> TokenPrice:
    """
    Parse raw price API response into a TokenPrice model.

    Handles both string and numeric price values from the API.

    Raises:
        ValueError: If *raw* is not a dict, the ``price`` key is missing,
                    or the value cannot be converted to float.
    """
    if not isinstance(raw, dict):
        raise ValueError(
            f"Expected dict for price response of token {token_id}, "
            f"got {type(raw).__name__}"
        )

    raw_price = raw.get("price")
    if raw_price is None:
        raise ValueError(f"Missing 'price' key in response for token {token_id}")

    try:
        price = float(raw_price)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            f"Cannot convert price '{raw_price}' to float for token {token_id}"
        ) from exc

    # Timestamp priority: API field first, local UTC fallback.
    raw_ts = raw.get("timestamp")
    if raw_ts is not None:
        # Pydantic will coerce an ISO-8601 string to datetime.
        timestamp = raw_ts
    else:
        timestamp = datetime.now(tz=timezone.utc)

    return TokenPrice(
        token_id=token_id,
        price=price,
        timestamp=timestamp,
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
