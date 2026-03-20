"""
Pydantic v2 models for external-source price ticks.

These models represent raw price observations from sources outside the
Polymarket ecosystem.  They are designed for Phase 2 data collection and
carry explicit timestamp provenance to support ingestion auditability
and approximate latency tracking.

Timestamp provenance (three-clock model):
  1. exchange_timestamp / source_timestamp — when the source reports the
     event occurred (exchange trade time, oracle update time, etc.).
  2. local_receive_timestamp — ``datetime.now(UTC)`` captured immediately
     after the HTTP response is received by our process.
  3. processed_timestamp — ``datetime.now(UTC)`` captured after the raw
     payload has been parsed and mapped into this model.

These three timestamps support:
  - approximate source-to-receive latency estimation
  - receive-to-processed latency tracking (our parsing overhead)
  - ingestion audit trails (when was each tick actually captured?)

Note: because our local clock and the source clock are independent,
the gap between source_timestamp and local_receive_timestamp reflects
both network latency *and* any clock difference between hosts.  It
should not be interpreted as precise clock-skew measurement.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import AwareDatetime, BaseModel, Field, model_validator


class BinanceSpotTick(BaseModel):
    """
    A single spot-price observation from Binance.

    Represents the most recent BTCUSDT trade fetched via the public
    ``GET /api/v3/trades?symbol=BTCUSDT&limit=1`` endpoint.

    Fields:
      symbol              — Trading pair (e.g. ``"BTCUSDT"``).
      price               — Last trade price in quote currency (USDT).
      exchange_timestamp   — Binance-reported trade execution time
                             (millisecond unix timestamp converted to UTC).
      local_receive_timestamp — Our local UTC clock at HTTP response receipt.
      processed_timestamp  — Our local UTC clock after parsing into this model.
      source              — Fixed identifier ``"binance_spot"``.
      raw_payload         — Optional: the raw trade dict for audit trails.
    """

    symbol: str
    price: float = Field(gt=0)
    exchange_timestamp: AwareDatetime
    local_receive_timestamp: AwareDatetime
    processed_timestamp: AwareDatetime
    source: Literal["binance_spot"] = "binance_spot"
    raw_payload: dict[str, Any] | None = None

    @model_validator(mode="after")
    def _check_local_timestamp_ordering(self) -> BinanceSpotTick:
        """Ensure local_receive_timestamp <= processed_timestamp.

        Only validates ordering between timestamps captured on the same
        host (our local clock).  exchange_timestamp comes from Binance's
        clock and is intentionally not compared — clock skew between our
        host and the exchange is expected and legitimate.
        """
        if self.local_receive_timestamp > self.processed_timestamp:
            raise ValueError(
                f"local_receive_timestamp ({self.local_receive_timestamp}) "
                f"must be <= processed_timestamp ({self.processed_timestamp})"
            )
        return self


class ReferencePriceTick(BaseModel):
    """
    A BTC/USD reference-price observation.

    Conceptually, this represents the price source used by Polymarket's
    BTC 15-minute Up/Down markets for resolution.  Those markets resolve
    based on the Chainlink BTC/USD oracle price at the window boundaries.

    In Phase 2, a true direct Chainlink integration is not yet implemented
    (it would require web3.py, an RPC provider, and the aggregator contract
    address).  Instead, this model may carry a **proxy** value — typically
    derived from Binance BTCUSDT — with ``is_proxy=True``.

    The ``is_proxy`` field is the single source of truth for whether this
    tick represents the actual reference source or a stand-in.

    Known proxy limitations (when ``is_proxy=True``):
      - USDT ≠ USD: Tether peg can deviate (typically <0.3%%, historically
        up to ~5%% under extreme stress).
      - Exchange vs oracle: Binance is a single CEX; Chainlink aggregates
        from multiple sources with its own update cadence.
      - No oracle metadata: proxy ticks lack Chainlink round IDs,
        answeredInRound, or on-chain updatedAt timestamps.

    Fields:
      pair                    — Price pair (``"BTC/USD"``).
      price                   — Reference price value.
      source_timestamp        — Upstream source's reported timestamp.
      local_receive_timestamp — Our local UTC clock at HTTP response receipt.
      processed_timestamp     — Our local UTC clock after parsing.
      source                  — Identifies the actual source used
                                (e.g. ``"binance_spot_proxy"``, or
                                ``"chainlink"`` in a future integration).
      is_proxy                — ``True`` if this is NOT the real oracle value.
      proxy_description       — Human-readable explanation of the proxy and
                                its limitations.  Expected non-None when
                                ``is_proxy=True``.
      raw_payload             — Optional raw upstream data for audit.
    """

    pair: str
    price: float = Field(gt=0)
    source_timestamp: AwareDatetime
    local_receive_timestamp: AwareDatetime
    processed_timestamp: AwareDatetime
    source: str
    is_proxy: bool
    proxy_description: str | None = None
    raw_payload: dict[str, Any] | None = None

    @model_validator(mode="after")
    def _check_proxy_semantics(self) -> ReferencePriceTick:
        """Enforce proxy-description and source consistency with is_proxy."""
        if self.is_proxy:
            if (
                self.proxy_description is None
                or not self.proxy_description.strip()
            ):
                raise ValueError(
                    "proxy_description must be a non-empty, non-whitespace "
                    "string when is_proxy is True"
                )
            if "proxy" not in self.source.lower():
                raise ValueError(
                    f"source '{self.source}' must contain 'proxy' when "
                    f"is_proxy is True"
                )
        else:
            if self.proxy_description is not None:
                raise ValueError(
                    "proxy_description must be None when is_proxy is False"
                )
            if "proxy" in self.source.lower():
                raise ValueError(
                    f"source '{self.source}' must not contain 'proxy' when "
                    f"is_proxy is False"
                )
        return self

    @model_validator(mode="after")
    def _check_local_timestamp_ordering(self) -> ReferencePriceTick:
        """Ensure local_receive_timestamp <= processed_timestamp.

        Only validates ordering between timestamps captured on the same
        host (our local clock).  source_timestamp comes from an
        independent clock and is intentionally not compared — a slightly
        fast source clock or slow local clock can legitimately produce
        source_timestamp > local_receive_timestamp.
        """
        if self.local_receive_timestamp > self.processed_timestamp:
            raise ValueError(
                f"local_receive_timestamp ({self.local_receive_timestamp}) "
                f"must be <= processed_timestamp ({self.processed_timestamp})"
            )
        return self
