"""
Boundary validation models for the Polymarket CLOB API.

These models validate raw API responses at the entry point of the system.
They carry no business logic and make no assumptions about strategy or execution.

Phase 1 model set (5 models):
  - Token          : a binary outcome token within a market
  - Market         : a discovered Polymarket market
  - TokenPrice     : a price snapshot for a single token (0.0–1.0)
  - OrderbookLevel : a single bid or ask price/size level
  - Orderbook      : a full bid/ask snapshot for a single token

TODO: Phase 1 — markets.py will parse API responses into Market / Token
TODO: Phase 1 — prices.py will parse API responses into TokenPrice / Orderbook
"""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, field_validator


class Token(BaseModel):
    """A binary outcome token belonging to a market (e.g. YES or NO)."""

    token_id: str
    outcome: str


class Market(BaseModel):
    """A Polymarket prediction market discovered via the Gamma API.

    Temporal fields:
      - ``event_start_time``: The actual start of the 15-minute trading window
        (Gamma field ``eventStartTime``). For a market titled
        "Bitcoin Up or Down - March 18, 8:45PM-9:00PM ET",
        this would be ``2026-03-19T00:45:00Z``.
      - ``end_date``: End of the 15-minute window (= event_start_time + 15 min).
      - ``start_date``: Market *listing/creation* timestamp on Polymarket.
        NOT the window start — typically ~24 h before ``event_start_time``.

    PTB (price to beat):
      Not available from Polymarket APIs.  BTC Up/Down markets resolve based
      on the Chainlink BTC/USD oracle price at ``event_start_time`` vs the
      oracle price at ``end_date``.  The opening BTC price (PTB) is a
      runtime value that must be captured from an external oracle at the
      exact moment the window opens.
    """

    condition_id: str
    question: str
    tokens: list[Token]
    active: bool
    closed: bool
    end_date: datetime | None = None
    event_start_time: datetime | None = None
    start_date: datetime | None = None
    group_id: str | None = None
    category: str | None = None
    slug: str | None = None
    market_id: str | None = None
    event_id: str | None = None
    event_slug: str | None = None
    description: str | None = None


class TokenPrice(BaseModel):
    """A price snapshot for a single token at a point in time."""

    token_id: str
    price: float
    timestamp: datetime | None = None

    @field_validator("price")
    @classmethod
    def price_in_range(cls, v: float) -> float:
        if not 0.0 <= v <= 1.0:
            raise ValueError(f"price must be between 0.0 and 1.0 inclusive, got {v}")
        return v


class OrderbookLevel(BaseModel):
    """A single price/size level on one side of an orderbook."""

    price: float
    size: float


class Orderbook(BaseModel):
    """A snapshot of the bid/ask orderbook for a single token."""

    token_id: str
    bids: list[OrderbookLevel]
    asks: list[OrderbookLevel]
    timestamp: datetime | None = None
