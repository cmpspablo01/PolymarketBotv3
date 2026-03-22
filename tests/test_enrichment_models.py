"""Tests for src/enrichment/models.py

Coverage:
  - SessionContext       : construction, required/optional fields, validator,
                           timestamp types
  - ExternalContextSnapshot : all-None, partial, proxy fields, run_id
  - EnrichedPolymarketPriceRecord : full construction, optional external,
                                    timestamp layer distinction
  - EnrichedPolymarketOrderbookRecord : full construction, optional external

All tests use synthetic data — no live API calls.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from src.enrichment.models import (
    EnrichedPolymarketOrderbookRecord,
    EnrichedPolymarketPriceRecord,
    ExternalContextSnapshot,
    SessionContext,
)

# ---------------------------------------------------------------------------
# Shared timestamps
# ---------------------------------------------------------------------------

_EST = datetime(2026, 3, 20, 18, 0, 0, tzinfo=timezone.utc)
_END = _EST + timedelta(minutes=15)
_API_TS = datetime(2026, 3, 20, 18, 5, 0, tzinfo=timezone.utc)
_WRITTEN = datetime(2026, 3, 20, 18, 5, 1, tzinfo=timezone.utc)


def _session(**overrides: object) -> SessionContext:
    defaults: dict[str, object] = dict(
        condition_id="0xabc123",
        question="Will BTC go up?",
        market_slug="btc-updown-15m-123",
        event_start_time=_EST,
        end_date=_END,
    )
    defaults.update(overrides)
    return SessionContext(**defaults)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# SessionContext
# ---------------------------------------------------------------------------


def test_session_context_construction() -> None:
    sc = _session()
    assert sc.condition_id == "0xabc123"
    assert sc.question == "Will BTC go up?"
    assert sc.market_slug == "btc-updown-15m-123"
    assert sc.event_start_time == _EST
    assert sc.end_date == _END


def test_session_context_optional_fields_default_none() -> None:
    sc = _session()
    assert sc.event_id is None
    assert sc.group_id is None


def test_session_context_with_optional_fields() -> None:
    sc = _session(event_id="ev-1", group_id="grp-1")
    assert sc.event_id == "ev-1"
    assert sc.group_id == "grp-1"


def test_session_context_end_before_start_raises() -> None:
    with pytest.raises(ValueError, match="end_date.*must be after"):
        _session(end_date=_EST - timedelta(minutes=1))


def test_session_context_end_equal_start_raises() -> None:
    with pytest.raises(ValueError, match="end_date.*must be after"):
        _session(end_date=_EST)


def test_session_context_timestamps_are_aware() -> None:
    sc = _session()
    assert sc.event_start_time.tzinfo is not None
    assert sc.end_date.tzinfo is not None


# ---------------------------------------------------------------------------
# ExternalContextSnapshot
# ---------------------------------------------------------------------------


def test_external_context_all_none() -> None:
    ec = ExternalContextSnapshot()
    assert ec.binance_spot_price is None
    assert ec.binance_spot_timestamp is None
    assert ec.reference_price is None
    assert ec.reference_price_timestamp is None
    assert ec.reference_price_is_proxy is None
    assert ec.reference_price_source is None
    assert ec.run_id is None


def test_external_context_with_binance() -> None:
    ts = datetime(2026, 3, 20, 18, 4, 0, tzinfo=timezone.utc)
    ec = ExternalContextSnapshot(
        binance_spot_price=69701.57,
        binance_spot_timestamp=ts,
    )
    assert ec.binance_spot_price == 69701.57
    assert ec.binance_spot_timestamp == ts
    assert ec.reference_price is None


def test_external_context_with_proxy_reference() -> None:
    ec = ExternalContextSnapshot(
        reference_price=69694.11,
        reference_price_is_proxy=True,
        reference_price_source="binance_spot_proxy",
    )
    assert ec.reference_price == 69694.11
    assert ec.reference_price_is_proxy is True
    assert ec.reference_price_source == "binance_spot_proxy"


def test_external_context_run_id() -> None:
    ec = ExternalContextSnapshot(run_id="cycle-xyz")
    assert ec.run_id == "cycle-xyz"


# ---------------------------------------------------------------------------
# EnrichedPolymarketPriceRecord
# ---------------------------------------------------------------------------


def test_enriched_price_construction() -> None:
    ep = EnrichedPolymarketPriceRecord(
        token_id="tok-1",
        outcome="Yes",
        price=0.55,
        timestamp=_API_TS,
        price_source="direct",
        session=_session(),
        time_remaining_seconds=600.0,
        run_id="run-1",
        written_at=_WRITTEN,
    )
    assert ep.price == 0.55
    assert ep.price_source == "direct"
    assert ep.session.condition_id == "0xabc123"
    assert ep.external is None
    assert ep.time_remaining_seconds == 600.0


def test_enriched_price_with_external() -> None:
    ext = ExternalContextSnapshot(binance_spot_price=69701.57)
    ep = EnrichedPolymarketPriceRecord(
        token_id="tok-1",
        outcome="Yes",
        price=0.55,
        timestamp=_API_TS,
        price_source="direct",
        session=_session(),
        external=ext,
        time_remaining_seconds=600.0,
        run_id="run-1",
        written_at=_WRITTEN,
    )
    assert ep.external is not None
    assert ep.external.binance_spot_price == 69701.57


def test_enriched_price_timestamps_distinct() -> None:
    """Session, source, and storage timestamps are separate layers."""
    ep = EnrichedPolymarketPriceRecord(
        token_id="tok-1",
        outcome="Yes",
        price=0.55,
        timestamp=_API_TS,
        price_source="direct",
        session=_session(),
        time_remaining_seconds=600.0,
        run_id="run-1",
        written_at=_WRITTEN,
    )
    # Source timestamp (API)
    assert ep.timestamp == _API_TS
    # Session timestamps (business)
    assert ep.session.event_start_time == _EST
    assert ep.session.end_date == _END
    # Storage timestamp
    assert ep.written_at == _WRITTEN
    # All distinct
    assert ep.timestamp != ep.session.event_start_time
    assert ep.timestamp != ep.written_at
    assert ep.session.event_start_time != ep.written_at


# ---------------------------------------------------------------------------
# EnrichedPolymarketOrderbookRecord
# ---------------------------------------------------------------------------


def test_enriched_orderbook_construction() -> None:
    eo = EnrichedPolymarketOrderbookRecord(
        token_id="tok-1",
        outcome="Yes",
        bids=[{"price": 0.50, "size": 100.0}],
        asks=[{"price": 0.55, "size": 80.0}],
        timestamp=_API_TS,
        session=_session(),
        time_remaining_seconds=600.0,
        run_id="run-1",
        written_at=_WRITTEN,
    )
    assert len(eo.bids) == 1
    assert len(eo.asks) == 1
    assert eo.external is None
    assert eo.time_remaining_seconds == 600.0


def test_enriched_orderbook_with_external() -> None:
    ext = ExternalContextSnapshot(
        reference_price=69694.11,
        reference_price_is_proxy=True,
    )
    eo = EnrichedPolymarketOrderbookRecord(
        token_id="tok-1",
        outcome="Yes",
        bids=[],
        asks=[],
        timestamp=_API_TS,
        session=_session(),
        external=ext,
        time_remaining_seconds=600.0,
        run_id="run-1",
        written_at=_WRITTEN,
    )
    assert eo.external is not None
    assert eo.external.reference_price_is_proxy is True


def test_enriched_orderbook_empty_book() -> None:
    eo = EnrichedPolymarketOrderbookRecord(
        token_id="tok-1",
        outcome="Yes",
        bids=[],
        asks=[],
        timestamp=_API_TS,
        session=_session(),
        time_remaining_seconds=600.0,
        run_id="run-1",
        written_at=_WRITTEN,
    )
    assert eo.bids == []
    assert eo.asks == []
