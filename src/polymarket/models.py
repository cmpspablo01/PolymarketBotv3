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
    """A Polymarket prediction market as returned by the CLOB API."""

    condition_id: str
    question: str
    tokens: list[Token]
    active: bool
    closed: bool
    end_date: datetime | None = None
    group_id: str | None = None
    category: str | None = None


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
