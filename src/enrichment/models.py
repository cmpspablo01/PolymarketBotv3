"""Pydantic v2 models for session-enriched multi-source data.

These models add session-awareness and temporal normalization to the
raw collected records without introducing strategy or business logic.

Timestamp layers preserved:

  - **Session/business**: ``event_start_time``, ``end_date``
    (from Polymarket event metadata)
  - **Source**: ``timestamp`` (API), ``exchange_timestamp``,
    ``source_timestamp`` (from upstream sources)
  - **Local processing**: ``local_receive_timestamp``,
    ``processed_timestamp``
  - **Storage**: ``written_at``, ``run_id``
"""

from __future__ import annotations

from typing import Any

from pydantic import AwareDatetime, BaseModel, model_validator


class SessionContext(BaseModel):
    """Identity and temporal boundaries of a single 15m BTC Up/Down session.

    Derived from Polymarket market metadata.  ``event_start_time`` is the
    actual window start; ``end_date`` is the resolution boundary.
    For 15M markets: ``end_date == event_start_time + 15 minutes``.
    """

    condition_id: str
    question: str
    market_slug: str
    event_start_time: AwareDatetime
    end_date: AwareDatetime
    event_id: str | None = None
    group_id: str | None = None

    @model_validator(mode="after")
    def _end_after_start(self) -> SessionContext:
        if self.end_date <= self.event_start_time:
            raise ValueError(
                f"end_date ({self.end_date}) must be after "
                f"event_start_time ({self.event_start_time})"
            )
        return self


class ExternalContextSnapshot(BaseModel):
    """Reduced analysis snapshot of external-source values for one cycle.

    This is intentionally **not** a full-provenance copy of the raw
    external records.  It captures only the fields most useful for
    cross-source comparison during enrichment and later analysis.
    Full raw provenance (``raw_payload``, ``local_receive_timestamp``,
    ``processed_timestamp``, etc.) is preserved in the original JSONL
    files on disk.

    All fields are optional — external sources may fail or be absent.
    When ``reference_price_is_proxy`` is True, the reference price was
    derived from a proxy source (currently Binance BTCUSDT spot).
    """

    binance_spot_price: float | None = None
    binance_spot_timestamp: AwareDatetime | None = None
    reference_price: float | None = None
    reference_price_timestamp: AwareDatetime | None = None
    reference_price_is_proxy: bool | None = None
    reference_price_source: str | None = None
    run_id: str | None = None


class EnrichedPolymarketPriceRecord(BaseModel):
    """A Polymarket price record enriched with session and external context.

    ``time_remaining_seconds`` is computed as
    ``(session.end_date - timestamp).total_seconds()``.
    Negative values indicate the record was captured after session end.
    """

    token_id: str
    outcome: str
    price: float
    timestamp: AwareDatetime
    price_source: str
    session: SessionContext
    external: ExternalContextSnapshot | None = None
    time_remaining_seconds: float | None = None
    run_id: str
    written_at: AwareDatetime


class EnrichedPolymarketOrderbookRecord(BaseModel):
    """A Polymarket orderbook record enriched with session and external context.

    ``time_remaining_seconds`` is computed as
    ``(session.end_date - timestamp).total_seconds()``.
    """

    token_id: str
    outcome: str
    bids: list[dict[str, Any]]
    asks: list[dict[str, Any]]
    timestamp: AwareDatetime
    session: SessionContext
    external: ExternalContextSnapshot | None = None
    time_remaining_seconds: float | None = None
    run_id: str
    written_at: AwareDatetime
