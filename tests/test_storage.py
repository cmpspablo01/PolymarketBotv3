"""
Tests for src/data/storage.py

Coverage:
  - save_market_snapshot : JSON output, envelope, directory creation, empty list,
                           enriched metadata fields
  - append_price        : JSONL creation, append behavior, timestamp preserved,
                          written_at included, directory creation, context merging
  - append_orderbook    : JSONL creation, written_at, bids/asks preserved,
                          directory creation, API timestamp for filename,
                          context merging
  - snapshot filenames   : millisecond precision prevents collision

All tests use pytest tmp_path — no live API calls, no live filesystem.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from src.data.storage import DataStorage
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


# ---------------------------------------------------------------------------
# save_market_snapshot
# ---------------------------------------------------------------------------


def test_snapshot_creates_json_file(tmp_path: Path) -> None:
    s = _storage(tmp_path)
    path = s.save_market_snapshot([_make_market()], SNAPSHOT_TS)

    assert path.exists()
    assert path.suffix == ".json"
    assert "20250318T160000_000Z" in path.name


def test_snapshot_envelope_structure(tmp_path: Path) -> None:
    s = _storage(tmp_path)
    path = s.save_market_snapshot([_make_market()], SNAPSHOT_TS)

    data = json.loads(path.read_text(encoding="utf-8"))
    assert "snapshot_ts" in data
    assert data["snapshot_ts"] == SNAPSHOT_TS.isoformat()
    assert "markets" in data
    assert isinstance(data["markets"], list)
    assert len(data["markets"]) == 1


def test_snapshot_preserves_end_date(tmp_path: Path) -> None:
    end = datetime(2025, 3, 31, 23, 59, 59, tzinfo=timezone.utc)
    market = _make_market(end_date=end)
    s = _storage(tmp_path)

    path = s.save_market_snapshot([market], SNAPSHOT_TS)

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
    path = s.save_market_snapshot([market], SNAPSHOT_TS)

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
    path = s.save_market_snapshot([], SNAPSHOT_TS)

    data = json.loads(path.read_text(encoding="utf-8"))
    assert data["markets"] == []


def test_snapshot_creates_directories(tmp_path: Path) -> None:
    nested = tmp_path / "deep" / "nested"
    s = DataStorage(
        markets_dir=nested / "markets",
        prices_dir=nested / "prices",
        orderbooks_dir=nested / "orderbooks",
    )

    path = s.save_market_snapshot([], SNAPSHOT_TS)

    assert path.exists()
    assert (nested / "markets").is_dir()


# ---------------------------------------------------------------------------
# append_price
# ---------------------------------------------------------------------------


def test_price_creates_jsonl_file(tmp_path: Path) -> None:
    s = _storage(tmp_path)
    path = s.append_price(_make_price())

    assert path.exists()
    assert path.suffix == ".jsonl"
    assert "2025-03-18" in path.name


def test_price_preserves_api_timestamp(tmp_path: Path) -> None:
    api_ts = datetime(2025, 3, 18, 16, 0, 0, tzinfo=timezone.utc)
    s = _storage(tmp_path)

    path = s.append_price(_make_price(timestamp=api_ts))

    record = json.loads(path.read_text(encoding="utf-8").strip())
    assert "timestamp" in record
    assert "2025-03-18" in record["timestamp"]


def test_price_includes_written_at(tmp_path: Path) -> None:
    s = _storage(tmp_path)
    path = s.append_price(_make_price())

    record = json.loads(path.read_text(encoding="utf-8").strip())
    assert "written_at" in record


def test_price_written_at_differs_from_api_timestamp(
    tmp_path: Path,
) -> None:
    s = _storage(tmp_path)
    path = s.append_price(_make_price())

    record = json.loads(path.read_text(encoding="utf-8").strip())
    assert "timestamp" in record
    assert "written_at" in record
    assert record["timestamp"] != record["written_at"]


def test_price_appends_to_existing_file(tmp_path: Path) -> None:
    s = _storage(tmp_path)
    s.append_price(_make_price(token_id="tok_a"))
    path = s.append_price(_make_price(token_id="tok_b"))

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

    path = s.append_price(_make_price())
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
    path = s.append_price(_make_price(), context=ctx)

    record = json.loads(path.read_text(encoding="utf-8").strip())
    assert record["condition_id"] == "cond_001"
    assert record["outcome"] == "Up"
    assert record["market_slug"] == "btc-updown-15m-123"
    assert record["price_source"] == "midpoint"


# ---------------------------------------------------------------------------
# append_orderbook
# ---------------------------------------------------------------------------


def test_orderbook_creates_jsonl_file(tmp_path: Path) -> None:
    api_ts = datetime(2025, 3, 18, 16, 0, 0, tzinfo=timezone.utc)
    s = _storage(tmp_path)
    path = s.append_orderbook(_make_orderbook(timestamp=api_ts))

    assert path.exists()
    assert path.suffix == ".jsonl"


def test_orderbook_includes_written_at(tmp_path: Path) -> None:
    s = _storage(tmp_path)
    path = s.append_orderbook(_make_orderbook())

    record = json.loads(path.read_text(encoding="utf-8").strip())
    assert "written_at" in record


def test_orderbook_preserves_bids_asks(tmp_path: Path) -> None:
    s = _storage(tmp_path)
    path = s.append_orderbook(_make_orderbook())

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

    path = s.append_orderbook(_make_orderbook())
    assert path.exists()


def test_orderbook_uses_api_timestamp_for_filename(
    tmp_path: Path,
) -> None:
    api_ts = datetime(2025, 3, 18, 16, 0, 0, tzinfo=timezone.utc)
    s = _storage(tmp_path)

    path = s.append_orderbook(_make_orderbook(timestamp=api_ts))

    assert "2025-03-18" in path.name


def test_orderbook_preserves_api_timestamp_in_record(
    tmp_path: Path,
) -> None:
    api_ts = datetime(2025, 3, 18, 16, 0, 0, tzinfo=timezone.utc)
    s = _storage(tmp_path)

    path = s.append_orderbook(_make_orderbook(timestamp=api_ts))

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
    path = s.append_orderbook(_make_orderbook(), context=ctx)

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
