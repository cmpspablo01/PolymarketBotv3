"""Tests for src/enrichment/session_enricher.py

Coverage:
  - build_session_context  : correct extraction, end_date precedence
                             (explicit > record > derived fallback),
                             custom window, missing field, string parsing
  - build_external_context : from dicts, binance-only, reference-only,
                             None inputs, proxy flag preservation,
                             run_id caller responsibility
  - compute_time_remaining : positive, negative, zero, string inputs
  - enrich_price_record    : full enrichment, with external, provenance
                             preserved, missing field, negative time_remaining
  - enrich_orderbook_record: full enrichment, provenance preserved, proxy
                             flag survives round-trip

All tests use synthetic data — no live API calls.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

import pytest

from src.enrichment.session_enricher import (
    build_external_context,
    build_session_context,
    compute_time_remaining,
    enrich_orderbook_record,
    enrich_price_record,
)

# ---------------------------------------------------------------------------
# Shared test data
# ---------------------------------------------------------------------------

_EST = datetime(2026, 3, 20, 18, 0, 0, tzinfo=timezone.utc)
_END = _EST + timedelta(minutes=15)
_API_TS = datetime(2026, 3, 20, 18, 5, 0, tzinfo=timezone.utc)
_WRITTEN = datetime(2026, 3, 20, 18, 5, 1, tzinfo=timezone.utc)
_RUN_ID = "run-abc-123"


def _price_record(**overrides: Any) -> dict[str, Any]:
    defaults: dict[str, Any] = {
        "token_id": "tok-1",
        "outcome": "Yes",
        "price": 0.55,
        "timestamp": _API_TS.isoformat(),
        "price_source": "direct",
        "condition_id": "0xabc123",
        "question": "Will BTC go up?",
        "market_slug": "btc-updown-15m-123",
        "event_start_time": _EST.isoformat(),
        "event_id": "ev-1",
        "run_id": _RUN_ID,
        "written_at": _WRITTEN.isoformat(),
    }
    defaults.update(overrides)
    return defaults


def _orderbook_record(**overrides: Any) -> dict[str, Any]:
    defaults: dict[str, Any] = {
        "token_id": "tok-1",
        "outcome": "Yes",
        "bids": [{"price": 0.50, "size": 100.0}],
        "asks": [{"price": 0.55, "size": 80.0}],
        "timestamp": _API_TS.isoformat(),
        "condition_id": "0xabc123",
        "question": "Will BTC go up?",
        "market_slug": "btc-updown-15m-123",
        "event_start_time": _EST.isoformat(),
        "event_id": "ev-1",
        "run_id": _RUN_ID,
        "written_at": _WRITTEN.isoformat(),
    }
    defaults.update(overrides)
    return defaults


def _binance_tick() -> dict[str, Any]:
    return {
        "symbol": "BTCUSDT",
        "price": 69701.57,
        "source": "binance_spot",
        "exchange_timestamp": "2026-03-20T18:04:07.026000+00:00",
        "local_receive_timestamp": "2026-03-20T18:04:07.753135+00:00",
        "processed_timestamp": "2026-03-20T18:04:07.753135+00:00",
        "run_id": _RUN_ID,
        "written_at": "2026-03-20T18:04:07.755136+00:00",
    }


def _reference_tick() -> dict[str, Any]:
    return {
        "pair": "BTC/USD",
        "price": 69694.11,
        "source": "binance_spot_proxy",
        "is_proxy": True,
        "proxy_description": "Binance BTCUSDT spot proxy",
        "source_timestamp": "2026-03-20T18:04:07.938000+00:00",
        "local_receive_timestamp": "2026-03-20T18:04:08.382191+00:00",
        "processed_timestamp": "2026-03-20T18:04:08.384192+00:00",
        "run_id": _RUN_ID,
        "written_at": "2026-03-20T18:04:08.385532+00:00",
    }


# ---------------------------------------------------------------------------
# build_session_context
# ---------------------------------------------------------------------------


def test_session_context_from_record() -> None:
    sc = build_session_context(_price_record())
    assert sc.condition_id == "0xabc123"
    assert sc.question == "Will BTC go up?"
    assert sc.market_slug == "btc-updown-15m-123"
    assert sc.event_id == "ev-1"


def test_session_context_default_end_date() -> None:
    """Default end_date = event_start_time + 15 minutes."""
    sc = build_session_context(_price_record())
    assert sc.end_date == _EST + timedelta(minutes=15)


def test_session_context_custom_end_date() -> None:
    custom_end = _EST + timedelta(minutes=30)
    sc = build_session_context(_price_record(), end_date=custom_end)
    assert sc.end_date == custom_end


def test_session_context_custom_end_date_as_string() -> None:
    custom_end = _EST + timedelta(minutes=30)
    sc = build_session_context(_price_record(), end_date=custom_end.isoformat())
    assert sc.end_date == custom_end


def test_session_context_custom_window() -> None:
    sc = build_session_context(_price_record(), window_minutes=5)
    assert sc.end_date == _EST + timedelta(minutes=5)


def test_session_context_missing_field_raises() -> None:
    rec = _price_record()
    del rec["condition_id"]
    with pytest.raises(KeyError, match="condition_id"):
        build_session_context(rec)


def test_session_context_missing_event_start_time_raises() -> None:
    rec = _price_record()
    del rec["event_start_time"]
    with pytest.raises(KeyError, match="event_start_time"):
        build_session_context(rec)


def test_session_context_parses_string_timestamps() -> None:
    """ISO string timestamps in the record are parsed into datetime."""
    sc = build_session_context(_price_record())
    assert isinstance(sc.event_start_time, datetime)
    assert sc.event_start_time.tzinfo is not None


def test_session_context_group_id_none_when_absent() -> None:
    rec = _price_record()
    assert "group_id" not in rec
    sc = build_session_context(rec)
    assert sc.group_id is None


# --- end_date precedence ---------------------------------------------------


def test_session_context_uses_record_end_date() -> None:
    """When record contains end_date and no explicit arg, use record's."""
    record_end = _EST + timedelta(minutes=15)
    rec = _price_record(end_date=record_end.isoformat())
    sc = build_session_context(rec)
    assert sc.end_date == record_end


def test_session_context_explicit_overrides_record_end_date() -> None:
    """Explicit end_date argument takes precedence over record end_date."""
    record_end = _EST + timedelta(minutes=15)
    explicit_end = _EST + timedelta(minutes=30)
    rec = _price_record(end_date=record_end.isoformat())
    sc = build_session_context(rec, end_date=explicit_end)
    assert sc.end_date == explicit_end
    assert sc.end_date != record_end


def test_session_context_fallback_only_when_no_end_date() -> None:
    """Derived fallback is used only when neither arg nor record has end_date."""
    rec = _price_record()
    assert "end_date" not in rec  # confirm absent
    sc = build_session_context(rec, window_minutes=10)
    assert sc.end_date == _EST + timedelta(minutes=10)


def test_session_context_record_end_date_beats_window_fallback() -> None:
    """record['end_date'] should win over the window_minutes fallback."""
    record_end = _EST + timedelta(minutes=20)
    rec = _price_record(end_date=record_end.isoformat())
    sc = build_session_context(rec, window_minutes=5)
    # record end_date wins, not event_start_time + 5 min
    assert sc.end_date == record_end


# ---------------------------------------------------------------------------
# build_external_context
# ---------------------------------------------------------------------------


def test_external_context_from_both_dicts() -> None:
    ec = build_external_context(
        binance_tick=_binance_tick(),
        reference_tick=_reference_tick(),
        run_id=_RUN_ID,
    )
    assert ec.binance_spot_price == 69701.57
    assert ec.reference_price == 69694.11
    assert ec.reference_price_is_proxy is True
    assert ec.reference_price_source == "binance_spot_proxy"
    assert ec.run_id == _RUN_ID


def test_external_context_binance_only() -> None:
    ec = build_external_context(binance_tick=_binance_tick())
    assert ec.binance_spot_price == 69701.57
    assert ec.reference_price is None
    assert ec.reference_price_is_proxy is None


def test_external_context_reference_only() -> None:
    ec = build_external_context(reference_tick=_reference_tick())
    assert ec.binance_spot_price is None
    assert ec.reference_price == 69694.11


def test_external_context_none_inputs() -> None:
    ec = build_external_context()
    assert ec.binance_spot_price is None
    assert ec.reference_price is None
    assert ec.run_id is None


def test_external_context_proxy_flag_preserved() -> None:
    ec = build_external_context(reference_tick=_reference_tick())
    assert ec.reference_price_is_proxy is True
    assert ec.reference_price_source == "binance_spot_proxy"


def test_external_context_binance_timestamp_extracted() -> None:
    ec = build_external_context(binance_tick=_binance_tick())
    assert ec.binance_spot_timestamp is not None


def test_external_context_reference_timestamp_extracted() -> None:
    ec = build_external_context(reference_tick=_reference_tick())
    assert ec.reference_price_timestamp is not None


def test_external_context_run_id_is_caller_responsibility() -> None:
    """run_id passed to build_external_context should match source records.

    Consistency is the caller's job — the builder just stores what it gets.
    """
    tick = _binance_tick()
    ec = build_external_context(
        binance_tick=tick,
        run_id=tick["run_id"],
    )
    assert ec.run_id == tick["run_id"] == _RUN_ID


def test_external_context_run_id_mismatch_not_enforced() -> None:
    """Builder does not enforce run_id matches source records."""
    ec = build_external_context(
        binance_tick=_binance_tick(),
        run_id="different-run-id",
    )
    assert ec.run_id == "different-run-id"


# ---------------------------------------------------------------------------
# compute_time_remaining
# ---------------------------------------------------------------------------


def test_time_remaining_positive() -> None:
    result = compute_time_remaining(_API_TS, _END)
    assert result == 600.0  # 10 minutes remaining


def test_time_remaining_negative() -> None:
    past_end = _END + timedelta(minutes=5)
    result = compute_time_remaining(past_end, _END)
    assert result == -300.0


def test_time_remaining_zero() -> None:
    result = compute_time_remaining(_END, _END)
    assert result == 0.0


def test_time_remaining_accepts_strings() -> None:
    result = compute_time_remaining(_API_TS.isoformat(), _END.isoformat())
    assert result == 600.0


# ---------------------------------------------------------------------------
# enrich_price_record
# ---------------------------------------------------------------------------


def test_enrich_price_basic() -> None:
    session = build_session_context(_price_record())
    enriched = enrich_price_record(_price_record(), session)

    assert enriched.price == 0.55
    assert enriched.outcome == "Yes"
    assert enriched.price_source == "direct"
    assert enriched.time_remaining_seconds == 600.0
    assert enriched.session.condition_id == "0xabc123"
    assert enriched.external is None


def test_enrich_price_with_external() -> None:
    session = build_session_context(_price_record())
    ext = build_external_context(
        binance_tick=_binance_tick(),
        reference_tick=_reference_tick(),
    )
    enriched = enrich_price_record(_price_record(), session, external=ext)

    assert enriched.external is not None
    assert enriched.external.binance_spot_price == 69701.57
    assert enriched.external.reference_price == 69694.11


def test_enrich_price_preserves_provenance() -> None:
    """Enrichment must not overwrite any provenance timestamps."""
    session = build_session_context(_price_record())
    enriched = enrich_price_record(_price_record(), session)

    # Source timestamp preserved
    assert enriched.timestamp == _API_TS
    # Session timestamps preserved
    assert enriched.session.event_start_time == _EST
    assert enriched.session.end_date == _END
    # Storage timestamp preserved
    assert enriched.written_at == _WRITTEN
    # run_id preserved
    assert enriched.run_id == _RUN_ID


def test_enrich_price_missing_field_raises() -> None:
    session = build_session_context(_price_record())
    rec = _price_record()
    del rec["token_id"]
    with pytest.raises(KeyError, match="token_id"):
        enrich_price_record(rec, session)


def test_enrich_price_defaults_unknown_price_source() -> None:
    """If price_source is missing from record, defaults to 'unknown'."""
    session = build_session_context(_price_record())
    rec = _price_record()
    del rec["price_source"]
    enriched = enrich_price_record(rec, session)
    assert enriched.price_source == "unknown"


def test_enrich_price_negative_time_remaining() -> None:
    """Record captured after session end produces negative time_remaining."""
    past_end_ts = _END + timedelta(minutes=2)
    rec = _price_record(timestamp=past_end_ts.isoformat())
    session = build_session_context(rec)
    enriched = enrich_price_record(rec, session)

    assert enriched.time_remaining_seconds is not None
    assert enriched.time_remaining_seconds < 0
    assert enriched.time_remaining_seconds == -120.0
    # Provenance still intact
    assert enriched.timestamp == past_end_ts
    assert enriched.session.end_date == _END


# ---------------------------------------------------------------------------
# enrich_orderbook_record
# ---------------------------------------------------------------------------


def test_enrich_orderbook_basic() -> None:
    session = build_session_context(_orderbook_record())
    enriched = enrich_orderbook_record(_orderbook_record(), session)

    assert len(enriched.bids) == 1
    assert len(enriched.asks) == 1
    assert enriched.bids[0]["price"] == 0.50
    assert enriched.time_remaining_seconds == 600.0


def test_enrich_orderbook_preserves_provenance() -> None:
    session = build_session_context(_orderbook_record())
    enriched = enrich_orderbook_record(_orderbook_record(), session)

    assert enriched.timestamp == _API_TS
    assert enriched.session.event_start_time == _EST
    assert enriched.written_at == _WRITTEN
    assert enriched.run_id == _RUN_ID


def test_enrich_orderbook_with_proxy_external() -> None:
    """Proxy reference data remains explicitly marked through enrichment."""
    session = build_session_context(_orderbook_record())
    ext = build_external_context(reference_tick=_reference_tick())
    enriched = enrich_orderbook_record(_orderbook_record(), session, external=ext)

    assert enriched.external is not None
    assert enriched.external.reference_price_is_proxy is True
    assert enriched.external.reference_price_source == "binance_spot_proxy"


def test_enrich_orderbook_missing_field_raises() -> None:
    session = build_session_context(_orderbook_record())
    rec = _orderbook_record()
    del rec["bids"]
    with pytest.raises(KeyError, match="bids"):
        enrich_orderbook_record(rec, session)


def test_enrich_orderbook_empty_book() -> None:
    session = build_session_context(_orderbook_record())
    rec = _orderbook_record(bids=[], asks=[])
    enriched = enrich_orderbook_record(rec, session)
    assert enriched.bids == []
    assert enriched.asks == []
