"""
Synchronous Binance spot price fetcher for BTCUSDT.

Fetches the most recent trade from the Binance public REST API and maps
it into a :class:`BinanceSpotTick` with full timestamp provenance.

API endpoint used:
  GET https://api.binance.com/api/v3/trades?symbol=BTCUSDT&limit=1

  Returns a JSON array with one trade object::

      [
          {
              "id": 123456789,
              "price": "84273.49000000",
              "qty": "0.00100000",
              "quoteQty": "84.27349000",
              "time": 1710800000000,
              "isBuyerMaker": false,
              "isBestMatch": true
          }
      ]

  The ``time`` field is a millisecond unix timestamp from the exchange.

Design decisions:
  - Uses ``requests`` directly (no coupling to PolymarketHTTPClient).
  - No retry/backoff — callers can wrap with their own retry policy.
  - Raises ``BinanceAPIError`` on HTTP errors or malformed payloads.
  - No async, no websocket, no orderbook — raw spot price collection only.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

import requests

from src.external.models import BinanceSpotTick

log = logging.getLogger(__name__)

BINANCE_BASE_URL = "https://api.binance.com"
BTCUSDT_SYMBOL = "BTCUSDT"
DEFAULT_TIMEOUT: tuple[int, int] = (10, 30)  # (connect, read) seconds


class BinanceAPIError(Exception):
    """Raised when the Binance API returns an error or a malformed payload."""

    def __init__(self, message: str) -> None:
        super().__init__(message)


class BinanceSpotFetcher:
    """
    Minimal synchronous fetcher for the latest BTCUSDT spot trade.

    Usage::

        fetcher = BinanceSpotFetcher()
        tick = fetcher.fetch_latest_trade()
        print(tick.price, tick.exchange_timestamp)

    The fetcher captures three timestamps per tick:
      1. ``exchange_timestamp`` — from the trade's ``time`` field (exchange clock)
      2. ``local_receive_timestamp`` — our clock at HTTP response receipt
      3. ``processed_timestamp`` — our clock after parsing into the model
    """

    def __init__(
        self,
        base_url: str = BINANCE_BASE_URL,
        timeout: tuple[int, int] = DEFAULT_TIMEOUT,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout

    def fetch_latest_trade(self) -> BinanceSpotTick:
        """
        Fetch the most recent BTCUSDT trade from Binance.

        Returns:
            A :class:`BinanceSpotTick` with full timestamp provenance.

        Raises:
            BinanceAPIError: On HTTP errors or malformed/empty responses.
        """
        url = f"{self._base_url}/api/v3/trades"
        params = {"symbol": BTCUSDT_SYMBOL, "limit": "1"}

        try:
            response = requests.get(url, params=params, timeout=self._timeout)
        except requests.exceptions.RequestException as exc:
            raise BinanceAPIError(f"Network error: {exc}") from exc

        local_receive_ts = datetime.now(tz=timezone.utc)

        if response.status_code != 200:
            raise BinanceAPIError(
                f"HTTP {response.status_code}: {response.text[:200]}"
            )

        try:
            payload = response.json()
        except ValueError as exc:
            raise BinanceAPIError(f"Invalid JSON response: {exc}") from exc

        return _parse_trade(payload, local_receive_ts)


def _parse_trade(raw: Any, local_receive_ts: datetime) -> BinanceSpotTick:
    """
    Parse a Binance ``/api/v3/trades`` response into a BinanceSpotTick.

    Args:
        raw: The parsed JSON response (expected: list with one trade dict).
        local_receive_ts: UTC timestamp captured at HTTP response receipt.

    Returns:
        A validated :class:`BinanceSpotTick`.

    Raises:
        BinanceAPIError: If the payload is empty, not a list, or missing
            required fields (``price``, ``time``).
    """
    if not isinstance(raw, list) or len(raw) == 0:
        raise BinanceAPIError(
            f"Expected non-empty list of trades, got: {type(raw).__name__}"
            f" (length={len(raw) if isinstance(raw, list) else 'N/A'})"
        )

    trade = raw[0]
    if not isinstance(trade, dict):
        raise BinanceAPIError(
            f"Expected trade dict, got: {type(trade).__name__}"
        )

    # --- Extract and validate required fields ---
    if "price" not in trade:
        raise BinanceAPIError("Trade missing required field: 'price'")
    if "time" not in trade:
        raise BinanceAPIError("Trade missing required field: 'time'")

    try:
        price = float(trade["price"])
    except (ValueError, TypeError) as exc:
        raise BinanceAPIError(
            f"Cannot parse trade price '{trade['price']}': {exc}"
        ) from exc

    try:
        exchange_ts = datetime.fromtimestamp(
            int(trade["time"]) / 1000.0, tz=timezone.utc
        )
    except (ValueError, TypeError, OSError) as exc:
        raise BinanceAPIError(
            f"Cannot parse trade time '{trade['time']}': {exc}"
        ) from exc

    processed_ts = datetime.now(tz=timezone.utc)

    return BinanceSpotTick(
        symbol=BTCUSDT_SYMBOL,
        price=price,
        exchange_timestamp=exchange_ts,
        local_receive_timestamp=local_receive_ts,
        processed_timestamp=processed_ts,
        raw_payload=trade,
    )
