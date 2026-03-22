"""
Tests for src/data/storage.py

Coverage:
  - save_market_snapshot      : JSON output, envelope, directory creation, empty list,
                                 enriched metadata fields
  - append_price              : JSONL creation, append behavior, timestamp preserved,
                                written_at included, directory creation, context merging
  - append_orderbook          : JSONL creation, written_at, bids/asks preserved,
                                directory creation, API timestamp for filename,
                                context merging
  - append_binance_spot_tick  : JSONL creation, provenance fields preserved,
                                written_at separate from exchange timestamps,
                                directory creation, append behavior, raw_payload
  - append_reference_price_tick: JSONL creation, provenance + proxy metadata preserved,
                                written_at separate from source timestamps,
                                directory creation, append behavior, raw_payload
  - snapshot filenames         : millisecond precision prevents collision

All tests use pytest tmp_path — no live API calls, no live filesystem.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from src.data.storage import DataStorage
from src.external.models import BinanceSpotTick, ReferencePriceTick
from src.polymarket.models import (
    Market,
    Orderbook,
    OrderbookLevel,
    Token,
    TokenPrice,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _storage(tmp_path: Path) -> DataStorage:
    """Create a DataStorage with standard subdirectories under tmp_path."""
    return DataStorage(
        markets_dir=tmp_path / "markets",
        prices_dir=tmp_path / "prices",
        orderbooks_dir=tmp_path / "orderbooks",
    )


def _make_market(**overrides: object) -> Market:
    defaults: dict = {
        "condition_id": "cond_001",
        "question": "Will BTC be above $100k at 12:00 PM ET?",
        "tokens": [Token(token_id="tok_yes", outcome="Yes")],
        "active": True,
        "closed": False,
    }
    defaults.update(overrides)
    return Market(**defaults)


def _make_price(**overrides: object) -> TokenPrice:
    defaults: dict = {
        "token_id": "tok_yes",
        "price": 0.72,
        "timestamp": datetime(2025, 3, 18, 16, 0, 0, tzinfo=timezone.utc),
    }
    defaults.update(overrides)
    return TokenPrice(**defaults)


def _make_orderbook(**overrides: object) -> Orderbook:
    defaults: dict = {
        "token_id": "tok_yes",
        "bids": [OrderbookLevel(price=0.64, size=50.0)],
        "asks": [OrderbookLevel(price=0.66, size=30.0)],
    }
    defaults.update(overrides)
    return Orderbook(**defaults)


SNAPSHOT_TS = datetime(2025, 3, 18, 16, 0, 0, tzinfo=timezone.utc)
RUN_TS = datetime(2025, 3, 18, 16, 0, 0, tzinfo=timezone.utc)
RUN_ID = "test-run-12345"


# ---------------------------------------------------------------------------
# save_market_snapshot
# ---------------------------------------------------------------------------


def test_snapshot_creates_json_file(tmp_path: Path) -> None:
    s = _storage(tmp_path)
    path = s.save_market_snapshot([_make_market()], SNAPSHOT_TS, run_id=RUN_ID)

    assert path.exists()
    assert path.suffix == ".json"
    assert "20250318T160000_000Z" in path.name


def test_snapshot_envelope_structure(tmp_path: Path) -> None:
    s = _storage(tmp_path)
    path = s.save_market_snapshot([_make_market()], SNAPSHOT_TS, run_id=RUN_ID)

    data = json.loads(path.read_text(encoding="utf-8"))
    assert "snapshot_ts" in data
    assert data["snapshot_ts"] == SNAPSHOT_TS.isoformat()
    assert "run_id" in data
    assert data["run_id"] == RUN_ID
    assert "markets" in data
    assert isinstance(data["markets"], list)
    assert len(data["markets"]) == 1


def test_snapshot_preserves_end_date(tmp_path: Path) -> None:
    end = datetime(2025, 3, 31, 23, 59, 59, tzinfo=timezone.utc)
    market = _make_market(end_date=end)
    s = _storage(tmp_path)

    path = s.save_market_snapshot([market], SNAPSHOT_TS, run_id=RUN_ID)

    data = json.loads(path.read_text(encoding="utf-8"))
    assert data["markets"][0]["end_date"] is not None
    assert "2025-03-31" in data["markets"][0]["end_date"]


def test_snapshot_preserves_enriched_metadata(tmp_path: Path) -> None:
    market = _make_market(
        slug="btc-updown-15m-123",
        event_id="99001",
        event_slug="ev-slug",
        description="Resolves Up if BTC...",
        start_date=datetime(2025, 3, 18, 15, 45, tzinfo=timezone.utc),
        event_start_time=datetime(2025, 3, 19, 16, 0, tzinfo=timezone.utc),
        market_id="55555",
    )
    s = _storage(tmp_path)
    path = s.save_market_snapshot([market], SNAPSHOT_TS, run_id=RUN_ID)

    data = json.loads(path.read_text(encoding="utf-8"))
    m = data["markets"][0]
    assert m["slug"] == "btc-updown-15m-123"
    assert m["event_id"] == "99001"
    assert m["event_slug"] == "ev-slug"
    assert m["description"] == "Resolves Up if BTC..."
    assert m["market_id"] == "55555"
    assert m["start_date"] is not None
    assert m["event_start_time"] is not None
    assert "2025-03-19" in m["event_start_time"]


def test_snapshot_empty_market_list(tmp_path: Path) -> None:
    s = _storage(tmp_path)
    path = s.save_market_snapshot([], SNAPSHOT_TS, run_id=RUN_ID)

    data = json.loads(path.read_text(encoding="utf-8"))
    assert data["markets"] == []


def test_snapshot_creates_directories(tmp_path: Path) -> None:
    nested = tmp_path / "deep" / "nested"
    s = DataStorage(
        markets_dir=nested / "markets",
        prices_dir=nested / "prices",
        orderbooks_dir=nested / "orderbooks",
    )

    path = s.save_market_snapshot([], SNAPSHOT_TS, run_id=RUN_ID)

    assert path.exists()
    assert (nested / "markets").is_dir()


def test_snapshot_includes_run_id(tmp_path: Path) -> None:
    s = _storage(tmp_path)
    path = s.save_market_snapshot([_make_market()], SNAPSHOT_TS, run_id=RUN_ID)

    data = json.loads(path.read_text(encoding="utf-8"))
    assert "run_id" in data
    assert data["run_id"] == RUN_ID


# ---------------------------------------------------------------------------
# append_price
# ---------------------------------------------------------------------------


def test_price_creates_jsonl_file(tmp_path: Path) -> None:
    s = _storage(tmp_path)
    path = s.append_price(_make_price(), RUN_TS, RUN_ID)

    assert path.exists()
    assert path.suffix == ".jsonl"
    assert "2025-03-18" in path.name


def test_price_preserves_api_timestamp(tmp_path: Path) -> None:
    api_ts = datetime(2025, 3, 18, 16, 0, 0, tzinfo=timezone.utc)
    s = _storage(tmp_path)

    path = s.append_price(_make_price(timestamp=api_ts), RUN_TS, RUN_ID)

    record = json.loads(path.read_text(encoding="utf-8").strip())
    assert "timestamp" in record
    assert "2025-03-18" in record["timestamp"]


def test_price_includes_written_at(tmp_path: Path) -> None:
    s = _storage(tmp_path)
    path = s.append_price(_make_price(), RUN_TS, RUN_ID)

    record = json.loads(path.read_text(encoding="utf-8").strip())
    assert "written_at" in record
    assert "run_id" in record
    assert record["run_id"] == RUN_ID


def test_price_written_at_differs_from_api_timestamp(
    tmp_path: Path,
) -> None:
    s = _storage(tmp_path)
    path = s.append_price(_make_price(), RUN_TS, RUN_ID)

    record = json.loads(path.read_text(encoding="utf-8").strip())
    assert "timestamp" in record
    assert "written_at" in record
    assert record["timestamp"] != record["written_at"]


def test_price_appends_to_existing_file(tmp_path: Path) -> None:
    s = _storage(tmp_path)
    s.append_price(_make_price(token_id="tok_a"), RUN_TS, RUN_ID)
    path = s.append_price(_make_price(token_id="tok_b"), RUN_TS, RUN_ID)

    lines = path.read_text(encoding="utf-8").strip().split("\n")
    assert len(lines) == 2
    assert json.loads(lines[0])["token_id"] == "tok_a"
    assert json.loads(lines[1])["token_id"] == "tok_b"


def test_price_creates_directories(tmp_path: Path) -> None:
    nested = tmp_path / "deep" / "nested"
    s = DataStorage(
        markets_dir=nested / "markets",
        prices_dir=nested / "prices",
        orderbooks_dir=nested / "orderbooks",
    )

    path = s.append_price(_make_price(), RUN_TS, RUN_ID)
    assert path.exists()


def test_price_context_merged_into_record(tmp_path: Path) -> None:
    """Context dict (traceability) is merged into the JSONL record."""
    s = _storage(tmp_path)
    ctx = {
        "condition_id": "cond_001",
        "outcome": "Up",
        "market_slug": "btc-updown-15m-123",
        "price_source": "midpoint",
    }
    path = s.append_price(_make_price(), RUN_TS, RUN_ID, context=ctx)

    record = json.loads(path.read_text(encoding="utf-8").strip())
    assert record["condition_id"] == "cond_001"
    assert record["outcome"] == "Up"
    assert record["market_slug"] == "btc-updown-15m-123"
    assert record["price_source"] == "midpoint"


# ---------------------------------------------------------------------------
# append_orderbook
# ---------------------------------------------------------------------------


def test_orderbook_creates_jsonl_file(tmp_path: Path) -> None:
    s = _storage(tmp_path)
    path = s.append_orderbook(_make_orderbook(), RUN_TS, RUN_ID)

    assert path.exists()
    assert path.suffix == ".jsonl"


def test_orderbook_includes_written_at(tmp_path: Path) -> None:
    s = _storage(tmp_path)
    path = s.append_orderbook(_make_orderbook(), RUN_TS, RUN_ID)

    record = json.loads(path.read_text(encoding="utf-8").strip())
    assert "written_at" in record
    assert "run_id" in record
    assert record["run_id"] == RUN_ID


def test_orderbook_preserves_bids_asks(tmp_path: Path) -> None:
    s = _storage(tmp_path)
    path = s.append_orderbook(_make_orderbook(), RUN_TS, RUN_ID)

    record = json.loads(path.read_text(encoding="utf-8").strip())
    assert len(record["bids"]) == 1
    assert len(record["asks"]) == 1
    assert record["bids"][0]["price"] == 0.64
    assert record["asks"][0]["size"] == 30.0


def test_orderbook_creates_directories(tmp_path: Path) -> None:
    nested = tmp_path / "deep" / "nested"
    s = DataStorage(
        markets_dir=nested / "markets",
        prices_dir=nested / "prices",
        orderbooks_dir=nested / "orderbooks",
    )

    path = s.append_orderbook(_make_orderbook(), RUN_TS, RUN_ID)
    assert path.exists()


def test_orderbook_filename_uses_run_ts(
    tmp_path: Path,
) -> None:
    """Filename is derived from run_ts, ensuring same-run grouping."""
    api_ts = datetime(2025, 3, 19, 14, 30, 0, tzinfo=timezone.utc)
    s = _storage(tmp_path)

    path = s.append_orderbook(_make_orderbook(timestamp=api_ts), RUN_TS, RUN_ID)

    # Filename uses RUN_TS (2025-03-18), not api_ts (2025-03-19)
    assert "2025-03-18" in path.name


def test_orderbook_preserves_api_timestamp_in_record(
    tmp_path: Path,
) -> None:
    api_ts = datetime(2025, 3, 18, 16, 0, 0, tzinfo=timezone.utc)
    s = _storage(tmp_path)

    path = s.append_orderbook(_make_orderbook(timestamp=api_ts), RUN_TS, RUN_ID)

    record = json.loads(path.read_text(encoding="utf-8").strip())
    assert "timestamp" in record
    assert "2025-03-18" in record["timestamp"]
    assert "written_at" in record
    assert record["timestamp"] != record["written_at"]


def test_orderbook_context_merged_into_record(tmp_path: Path) -> None:
    """Context dict (traceability) is merged into the JSONL record."""
    s = _storage(tmp_path)
    ctx = {
        "condition_id": "cond_001",
        "outcome": "Down",
        "market_slug": "btc-updown-15m-456",
        "event_id": "99001",
    }
    path = s.append_orderbook(_make_orderbook(), RUN_TS, RUN_ID, context=ctx)

    record = json.loads(path.read_text(encoding="utf-8").strip())
    assert record["condition_id"] == "cond_001"
    assert record["outcome"] == "Down"
    assert record["market_slug"] == "btc-updown-15m-456"
    assert record["event_id"] == "99001"


def test_snapshot_filenames_no_collision_rapid_saves(
    tmp_path: Path,
) -> None:
    """Two snapshots with different ms timestamps produce different files."""
    s = _storage(tmp_path)
    ts1 = datetime(2025, 3, 18, 16, 0, 0, 0, tzinfo=timezone.utc)
    ts2 = datetime(2025, 3, 18, 16, 0, 0, 1000, tzinfo=timezone.utc)

    p1 = s.save_market_snapshot([], ts1)
    p2 = s.save_market_snapshot([], ts2)

    assert p1 != p2
    assert p1.exists()
    assert p2.exists()


# ---------------------------------------------------------------------------
# External-source storage helpers
# ---------------------------------------------------------------------------

TS_EX = datetime(2026, 3, 19, 3, 0, 0, tzinfo=timezone.utc)
TS_RCV = datetime(2026, 3, 19, 3, 0, 0, 200_000, tzinfo=timezone.utc)
TS_PROC = datetime(2026, 3, 19, 3, 0, 0, 400_000, tzinfo=timezone.utc)
EXT_RUN_TS = datetime(2026, 3, 19, 3, 0, 0, tzinfo=timezone.utc)


def _ext_storage(tmp_path: Path) -> DataStorage:
    """DataStorage with external-source directories configured."""
    return DataStorage(
        markets_dir=tmp_path / "markets",
        prices_dir=tmp_path / "prices",
        orderbooks_dir=tmp_path / "orderbooks",
        binance_spot_dir=tmp_path / "binance_spot",
        reference_price_dir=tmp_path / "reference_price",
    )


def _make_binance_tick(**overrides: object) -> BinanceSpotTick:
    defaults: dict = {
        "symbol": "BTCUSDT",
        "price": 84273.49,
        "exchange_timestamp": TS_EX,
        "local_receive_timestamp": TS_RCV,
        "processed_timestamp": TS_PROC,
    }
    defaults.update(overrides)
    return BinanceSpotTick(**defaults)


def _make_reference_tick(**overrides: object) -> ReferencePriceTick:
    defaults: dict = {
        "pair": "BTC/USD",
        "price": 84273.49,
        "source_timestamp": TS_EX,
        "local_receive_timestamp": TS_RCV,
        "processed_timestamp": TS_PROC,
        "source": "binance_spot_proxy",
        "is_proxy": True,
        "proxy_description": "Binance BTCUSDT proxy for Chainlink BTC/USD",
    }
    defaults.update(overrides)
    return ReferencePriceTick(**defaults)


# ---------------------------------------------------------------------------
# append_binance_spot_tick
# ---------------------------------------------------------------------------


def test_binance_spot_creates_jsonl_file(tmp_path: Path) -> None:
    s = _ext_storage(tmp_path)
    path = s.append_binance_spot_tick(_make_binance_tick(), EXT_RUN_TS)

    assert path.exists()
    assert path.suffix == ".jsonl"
    assert "binance_spot_2026-03-19" in path.name


def test_binance_spot_preserves_provenance_fields(tmp_path: Path) -> None:
    s = _ext_storage(tmp_path)
    path = s.append_binance_spot_tick(_make_binance_tick(), EXT_RUN_TS)

    record = json.loads(path.read_text(encoding="utf-8").strip())
    assert record["symbol"] == "BTCUSDT"
    assert record["price"] == 84273.49
    assert record["source"] == "binance_spot"
    assert "exchange_timestamp" in record
    assert "local_receive_timestamp" in record
    assert "processed_timestamp" in record
    assert "2026-03-19" in record["exchange_timestamp"]


def test_binance_spot_written_at_separate_from_exchange(tmp_path: Path) -> None:
    s = _ext_storage(tmp_path)
    path = s.append_binance_spot_tick(_make_binance_tick(), EXT_RUN_TS)

    record = json.loads(path.read_text(encoding="utf-8").strip())
    assert "written_at" in record
    assert record["written_at"] != record["exchange_timestamp"]


def test_binance_spot_creates_directories(tmp_path: Path) -> None:
    nested = tmp_path / "deep" / "nested"
    s = DataStorage(
        markets_dir=nested / "markets",
        prices_dir=nested / "prices",
        orderbooks_dir=nested / "orderbooks",
        binance_spot_dir=nested / "binance_spot",
    )
    path = s.append_binance_spot_tick(_make_binance_tick(), EXT_RUN_TS)
    assert path.exists()
    assert (nested / "binance_spot").is_dir()


def test_binance_spot_appends_multiple_ticks(tmp_path: Path) -> None:
    s = _ext_storage(tmp_path)
    s.append_binance_spot_tick(_make_binance_tick(price=84000.0), EXT_RUN_TS)
    path = s.append_binance_spot_tick(_make_binance_tick(price=84100.0), EXT_RUN_TS)

    lines = path.read_text(encoding="utf-8").strip().split("\n")
    assert len(lines) == 2
    assert json.loads(lines[0])["price"] == 84000.0
    assert json.loads(lines[1])["price"] == 84100.0


def test_binance_spot_raw_payload_preserved(tmp_path: Path) -> None:
    payload = {"id": 999, "price": "84273.49", "time": 1774069200000}
    s = _ext_storage(tmp_path)
    path = s.append_binance_spot_tick(
        _make_binance_tick(raw_payload=payload), EXT_RUN_TS
    )

    record = json.loads(path.read_text(encoding="utf-8").strip())
    assert record["raw_payload"] == payload


def test_binance_spot_raw_payload_none_when_absent(tmp_path: Path) -> None:
    s = _ext_storage(tmp_path)
    path = s.append_binance_spot_tick(_make_binance_tick(), EXT_RUN_TS)

    record = json.loads(path.read_text(encoding="utf-8").strip())
    assert record["raw_payload"] is None


# ---------------------------------------------------------------------------
# append_reference_price_tick
# ---------------------------------------------------------------------------


def test_reference_price_creates_jsonl_file(tmp_path: Path) -> None:
    s = _ext_storage(tmp_path)
    path = s.append_reference_price_tick(_make_reference_tick(), EXT_RUN_TS)

    assert path.exists()
    assert path.suffix == ".jsonl"
    assert "reference_price_2026-03-19" in path.name


def test_reference_price_preserves_provenance_fields(tmp_path: Path) -> None:
    s = _ext_storage(tmp_path)
    path = s.append_reference_price_tick(_make_reference_tick(), EXT_RUN_TS)

    record = json.loads(path.read_text(encoding="utf-8").strip())
    assert record["pair"] == "BTC/USD"
    assert record["price"] == 84273.49
    assert record["source"] == "binance_spot_proxy"
    assert "source_timestamp" in record
    assert "local_receive_timestamp" in record
    assert "processed_timestamp" in record
    assert "2026-03-19" in record["source_timestamp"]


def test_reference_price_preserves_proxy_metadata(tmp_path: Path) -> None:
    s = _ext_storage(tmp_path)
    path = s.append_reference_price_tick(_make_reference_tick(), EXT_RUN_TS)

    record = json.loads(path.read_text(encoding="utf-8").strip())
    assert record["is_proxy"] is True
    assert isinstance(record["proxy_description"], str)
    assert len(record["proxy_description"]) > 0


def test_reference_price_written_at_separate_from_source(tmp_path: Path) -> None:
    s = _ext_storage(tmp_path)
    path = s.append_reference_price_tick(_make_reference_tick(), EXT_RUN_TS)

    record = json.loads(path.read_text(encoding="utf-8").strip())
    assert "written_at" in record
    assert record["written_at"] != record["source_timestamp"]


def test_reference_price_creates_directories(tmp_path: Path) -> None:
    nested = tmp_path / "deep" / "nested"
    s = DataStorage(
        markets_dir=nested / "markets",
        prices_dir=nested / "prices",
        orderbooks_dir=nested / "orderbooks",
        reference_price_dir=nested / "reference_price",
    )
    path = s.append_reference_price_tick(_make_reference_tick(), EXT_RUN_TS)
    assert path.exists()
    assert (nested / "reference_price").is_dir()


def test_reference_price_appends_multiple_ticks(tmp_path: Path) -> None:
    s = _ext_storage(tmp_path)
    s.append_reference_price_tick(_make_reference_tick(price=84000.0), EXT_RUN_TS)
    path = s.append_reference_price_tick(
        _make_reference_tick(price=84100.0), EXT_RUN_TS
    )

    lines = path.read_text(encoding="utf-8").strip().split("\n")
    assert len(lines) == 2
    assert json.loads(lines[0])["price"] == 84000.0
    assert json.loads(lines[1])["price"] == 84100.0


def test_reference_price_raw_payload_preserved(tmp_path: Path) -> None:
    payload = {"id": 999, "price": "84273.49"}
    s = _ext_storage(tmp_path)
    path = s.append_reference_price_tick(
        _make_reference_tick(raw_payload=payload), EXT_RUN_TS
    )

    record = json.loads(path.read_text(encoding="utf-8").strip())
    assert record["raw_payload"] == payload


# ---------------------------------------------------------------------------
# run_id traceability on external records
# ---------------------------------------------------------------------------


def test_binance_spot_run_id_persisted(tmp_path: Path) -> None:
    """run_id should be stored in the Binance spot JSONL record."""
    s = _ext_storage(tmp_path)
    path = s.append_binance_spot_tick(
        _make_binance_tick(), EXT_RUN_TS, run_id="cycle-abc-123"
    )

    record = json.loads(path.read_text(encoding="utf-8").strip())
    assert record["run_id"] == "cycle-abc-123"


def test_binance_spot_run_id_none_when_omitted(tmp_path: Path) -> None:
    s = _ext_storage(tmp_path)
    path = s.append_binance_spot_tick(_make_binance_tick(), EXT_RUN_TS)

    record = json.loads(path.read_text(encoding="utf-8").strip())
    assert record["run_id"] is None


def test_reference_price_run_id_persisted(tmp_path: Path) -> None:
    """run_id should be stored in the reference price JSONL record."""
    s = _ext_storage(tmp_path)
    path = s.append_reference_price_tick(
        _make_reference_tick(), EXT_RUN_TS, run_id="cycle-abc-123"
    )

    record = json.loads(path.read_text(encoding="utf-8").strip())
    assert record["run_id"] == "cycle-abc-123"


def test_reference_price_run_id_none_when_omitted(tmp_path: Path) -> None:
    s = _ext_storage(tmp_path)
    path = s.append_reference_price_tick(_make_reference_tick(), EXT_RUN_TS)

    record = json.loads(path.read_text(encoding="utf-8").strip())
    assert record["run_id"] is None


# ---------------------------------------------------------------------------
# Filename derivation uses run_ts, not source/exchange timestamps
# ---------------------------------------------------------------------------


def test_binance_spot_filename_uses_run_ts_not_exchange(tmp_path: Path) -> None:
    """Filename date should come from run_ts, not exchange_timestamp."""
    s = _ext_storage(tmp_path)
    tick = _make_binance_tick(
        exchange_timestamp=datetime(2026, 3, 18, 23, 59, 59, tzinfo=timezone.utc),
    )
    run_ts = datetime(2026, 3, 19, 0, 0, 1, tzinfo=timezone.utc)

    path = s.append_binance_spot_tick(tick, run_ts)

    assert "2026-03-19" in path.name
    assert "2026-03-18" not in path.name


def test_reference_price_filename_uses_run_ts_not_source(tmp_path: Path) -> None:
    """Filename date should come from run_ts, not source_timestamp."""
    s = _ext_storage(tmp_path)
    tick = _make_reference_tick(
        source_timestamp=datetime(2026, 3, 18, 23, 59, 59, tzinfo=timezone.utc),
    )
    run_ts = datetime(2026, 3, 19, 0, 0, 1, tzinfo=timezone.utc)

    path = s.append_reference_price_tick(tick, run_ts)

    assert "2026-03-19" in path.name
    assert "2026-03-18" not in path.name


# ---------------------------------------------------------------------------
# Missing external dir raises ValueError
# ---------------------------------------------------------------------------


def test_binance_spot_raises_when_dir_not_configured(tmp_path: Path) -> None:
    """append_binance_spot_tick must fail if binance_spot_dir was not set."""
    s = DataStorage(
        markets_dir=tmp_path / "markets",
        prices_dir=tmp_path / "prices",
        orderbooks_dir=tmp_path / "orderbooks",
        # binance_spot_dir intentionally omitted
    )
    with pytest.raises(ValueError, match="binance_spot_dir was not configured"):
        s.append_binance_spot_tick(_make_binance_tick(), EXT_RUN_TS)


def test_reference_price_raises_when_dir_not_configured(tmp_path: Path) -> None:
    """append_reference_price_tick must fail if reference_price_dir was not set."""
    s = DataStorage(
        markets_dir=tmp_path / "markets",
        prices_dir=tmp_path / "prices",
        orderbooks_dir=tmp_path / "orderbooks",
        # reference_price_dir intentionally omitted
    )
    with pytest.raises(ValueError, match="reference_price_dir was not configured"):
        s.append_reference_price_tick(_make_reference_tick(), EXT_RUN_TS)
