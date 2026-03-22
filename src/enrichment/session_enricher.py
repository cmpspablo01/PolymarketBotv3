"""Session-enrichment helpers for collected multi-source data.

Builds session-aware context objects from already-collected Polymarket
and external-source records, computes time-remaining, and attaches
external context — all without strategy or feature-engineering logic.

Assumptions
-----------
- All target markets are 15M BTC Up/Down (default window = 15 min).
- ``event_start_time`` is present in stored Polymarket records.
- ``end_date`` can be derived as ``event_start_time + 15 min`` when
  not explicitly provided.
- External context is optional and may be absent or partial.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from .models import (
    EnrichedPolymarketOrderbookRecord,
    EnrichedPolymarketPriceRecord,
    ExternalContextSnapshot,
    SessionContext,
)

DEFAULT_WINDOW_MINUTES: int = 15


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _ensure_datetime(value: str | datetime) -> datetime:
    """Parse an ISO-format string to a datetime, or return as-is.

    Python 3.11+ ``fromisoformat`` handles the ``Z`` suffix.
    """
    if isinstance(value, datetime):
        return value
    return datetime.fromisoformat(value)


# ---------------------------------------------------------------------------
# Builder functions
# ---------------------------------------------------------------------------


def build_session_context(
    record: dict[str, Any],
    *,
    end_date: datetime | str | None = None,
    window_minutes: int = DEFAULT_WINDOW_MINUTES,
) -> SessionContext:
    """Build a :class:`SessionContext` from a raw Polymarket record dict.

    ``end_date`` precedence (first match wins):

    1. Explicit *end_date* argument (caller override).
    2. ``record["end_date"]`` if present in the dict.
    3. Derived as ``event_start_time + window_minutes`` (fallback).

    Args:
        record: A dict from a stored Polymarket JSONL record.  Must
            contain ``condition_id``, ``question``, ``market_slug``,
            and ``event_start_time``.
        end_date: Explicit session end.  Takes highest precedence when
            provided.
        window_minutes: Minutes to add to ``event_start_time`` when
            neither *end_date* nor ``record["end_date"]`` is
            available.  Defaults to 15.

    Returns:
        A :class:`SessionContext` instance.

    Raises:
        KeyError: If a required field is missing from ``record``.
        ValueError: If ``event_start_time`` cannot be parsed.
    """
    est = _ensure_datetime(record["event_start_time"])

    if end_date is not None:
        resolved_end = _ensure_datetime(end_date)
    elif record.get("end_date") is not None:
        resolved_end = _ensure_datetime(record["end_date"])
    else:
        resolved_end = est + timedelta(minutes=window_minutes)

    return SessionContext(
        condition_id=record["condition_id"],
        question=record["question"],
        market_slug=record["market_slug"],
        event_start_time=est,
        end_date=resolved_end,
        event_id=record.get("event_id"),
        group_id=record.get("group_id"),
    )


def build_external_context(
    *,
    binance_tick: dict[str, Any] | None = None,
    reference_tick: dict[str, Any] | None = None,
    run_id: str | None = None,
) -> ExternalContextSnapshot:
    """Build an :class:`ExternalContextSnapshot` from raw external dicts.

    This produces a **reduced analysis snapshot** — only the fields
    most useful for cross-source comparison are extracted.  Full raw
    provenance (``raw_payload``, ``local_receive_timestamp``,
    ``processed_timestamp``, etc.) is preserved in the original JSONL
    files and is not duplicated here.

    ``run_id`` consistency is the caller's responsibility: the caller
    should pass the same ``run_id`` that appears in the source records
    to link this snapshot back to its originating cycle.

    Args:
        binance_tick: A dict from a stored Binance spot JSONL record.
        reference_tick: A dict from a stored reference price JSONL record.
        run_id: The cycle run_id linking these records.

    Returns:
        An :class:`ExternalContextSnapshot`.  Fields sourced from
        absent dicts will be ``None``.
    """
    return ExternalContextSnapshot(
        binance_spot_price=(
            binance_tick["price"] if binance_tick else None
        ),
        binance_spot_timestamp=(
            binance_tick["exchange_timestamp"] if binance_tick else None
        ),
        reference_price=(
            reference_tick["price"] if reference_tick else None
        ),
        reference_price_timestamp=(
            reference_tick["source_timestamp"] if reference_tick else None
        ),
        reference_price_is_proxy=(
            reference_tick.get("is_proxy") if reference_tick else None
        ),
        reference_price_source=(
            reference_tick.get("source") if reference_tick else None
        ),
        run_id=run_id,
    )


# ---------------------------------------------------------------------------
# Temporal helpers
# ---------------------------------------------------------------------------


def compute_time_remaining(
    record_timestamp: datetime | str,
    end_date: datetime | str,
) -> float:
    """Seconds remaining from *record_timestamp* until *end_date*.

    Returns a negative value if *record_timestamp* is past *end_date*.
    """
    ts = _ensure_datetime(record_timestamp)
    end = _ensure_datetime(end_date)
    return (end - ts).total_seconds()


# ---------------------------------------------------------------------------
# Record enrichment
# ---------------------------------------------------------------------------


def enrich_price_record(
    record: dict[str, Any],
    session: SessionContext,
    external: ExternalContextSnapshot | None = None,
) -> EnrichedPolymarketPriceRecord:
    """Enrich a raw Polymarket price record with session and external context.

    Args:
        record: A dict from a stored Polymarket price JSONL record.
        session: The session context for this record's market.
        external: Optional external-source snapshot for this cycle.

    Returns:
        An :class:`EnrichedPolymarketPriceRecord`.

    Raises:
        KeyError: If a required field is missing from ``record``.
    """
    ts = _ensure_datetime(record["timestamp"])
    time_remaining = compute_time_remaining(ts, session.end_date)

    return EnrichedPolymarketPriceRecord(
        token_id=record["token_id"],
        outcome=record["outcome"],
        price=record["price"],
        timestamp=ts,
        price_source=record.get("price_source", "unknown"),
        session=session,
        external=external,
        time_remaining_seconds=time_remaining,
        run_id=record["run_id"],
        written_at=record["written_at"],
    )


def enrich_orderbook_record(
    record: dict[str, Any],
    session: SessionContext,
    external: ExternalContextSnapshot | None = None,
) -> EnrichedPolymarketOrderbookRecord:
    """Enrich a raw Polymarket orderbook record with session and external context.

    Args:
        record: A dict from a stored Polymarket orderbook JSONL record.
        session: The session context for this record's market.
        external: Optional external-source snapshot for this cycle.

    Returns:
        An :class:`EnrichedPolymarketOrderbookRecord`.

    Raises:
        KeyError: If a required field is missing from ``record``.
    """
    ts = _ensure_datetime(record["timestamp"])
    time_remaining = compute_time_remaining(ts, session.end_date)

    return EnrichedPolymarketOrderbookRecord(
        token_id=record["token_id"],
        outcome=record["outcome"],
        bids=record["bids"],
        asks=record["asks"],
        timestamp=ts,
        session=session,
        external=external,
        time_remaining_seconds=time_remaining,
        run_id=record["run_id"],
        written_at=record["written_at"],
    )
