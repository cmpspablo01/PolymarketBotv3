"""
Tests for src/data/storage.py

Coverage:
  - save_market_snapshot : JSON output, envelope, directory creation, empty list
  - append_price        : JSONL creation, append behavior, timestamp preserved,
                          written_at included, directory creation
  - append_orderbook    : JSONL creation, written_at, bids/asks preserved,
                          directory creation, API timestamp for filename
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
    storage = DataStorage(tmp_path)
    path = storage.save_market_snapshot([_make_market()], SNAPSHOT_TS)

    assert path.exists()
    assert path.suffix == ".json"
    assert "20250318T160000_000Z" in path.name


def test_snapshot_envelope_structure(tmp_path: Path) -> None:
    storage = DataStorage(tmp_path)
    path = storage.save_market_snapshot([_make_market()], SNAPSHOT_TS)

    data = json.loads(path.read_text(encoding="utf-8"))
    assert "snapshot_ts" in data
    assert data["snapshot_ts"] == SNAPSHOT_TS.isoformat()
    assert "markets" in data
    assert isinstance(data["markets"], list)
    assert len(data["markets"]) == 1


def test_snapshot_preserves_end_date(tmp_path: Path) -> None:
    end = datetime(2025, 3, 31, 23, 59, 59, tzinfo=timezone.utc)
    market = _make_market(end_date=end)
    storage = DataStorage(tmp_path)

    path = storage.save_market_snapshot([market], SNAPSHOT_TS)

    data = json.loads(path.read_text(encoding="utf-8"))
    assert data["markets"][0]["end_date"] is not None
    assert "2025-03-31" in data["markets"][0]["end_date"]


def test_snapshot_empty_market_list(tmp_path: Path) -> None:
    storage = DataStorage(tmp_path)
    path = storage.save_market_snapshot([], SNAPSHOT_TS)

    data = json.loads(path.read_text(encoding="utf-8"))
    assert data["markets"] == []


def test_snapshot_creates_directories(tmp_path: Path) -> None:
    nested = tmp_path / "deep" / "nested"
    storage = DataStorage(nested)

    path = storage.save_market_snapshot([], SNAPSHOT_TS)

    assert path.exists()
    assert (nested / "markets").is_dir()


# ---------------------------------------------------------------------------
# append_price
# ---------------------------------------------------------------------------


def test_price_creates_jsonl_file(tmp_path: Path) -> None:
    storage = DataStorage(tmp_path)
    path = storage.append_price(_make_price())

    assert path.exists()
    assert path.suffix == ".jsonl"
    assert "2025-03-18" in path.name


def test_price_preserves_api_timestamp(tmp_path: Path) -> None:
    api_ts = datetime(2025, 3, 18, 16, 0, 0, tzinfo=timezone.utc)
    storage = DataStorage(tmp_path)

    path = storage.append_price(_make_price(timestamp=api_ts))

    record = json.loads(path.read_text(encoding="utf-8").strip())
    assert "timestamp" in record
    assert "2025-03-18" in record["timestamp"]


def test_price_includes_written_at(tmp_path: Path) -> None:
    storage = DataStorage(tmp_path)
    path = storage.append_price(_make_price())

    record = json.loads(path.read_text(encoding="utf-8").strip())
    assert "written_at" in record


def test_price_written_at_differs_from_api_timestamp(
    tmp_path: Path,
) -> None:
    storage = DataStorage(tmp_path)
    path = storage.append_price(_make_price())

    record = json.loads(path.read_text(encoding="utf-8").strip())
    # Both exist and are separate keys
    assert "timestamp" in record
    assert "written_at" in record
    assert record["timestamp"] != record["written_at"]


def test_price_appends_to_existing_file(tmp_path: Path) -> None:
    storage = DataStorage(tmp_path)
    storage.append_price(_make_price(token_id="tok_a"))
    path = storage.append_price(_make_price(token_id="tok_b"))

    lines = path.read_text(encoding="utf-8").strip().split("\n")
    assert len(lines) == 2
    assert json.loads(lines[0])["token_id"] == "tok_a"
    assert json.loads(lines[1])["token_id"] == "tok_b"


def test_price_creates_directories(tmp_path: Path) -> None:
    nested = tmp_path / "deep" / "nested"
    storage = DataStorage(nested)

    path = storage.append_price(_make_price())
    assert path.exists()


# ---------------------------------------------------------------------------
# append_orderbook
# ---------------------------------------------------------------------------


def test_orderbook_creates_jsonl_file(tmp_path: Path) -> None:
    api_ts = datetime(2025, 3, 18, 16, 0, 0, tzinfo=timezone.utc)
    storage = DataStorage(tmp_path)
    path = storage.append_orderbook(_make_orderbook(timestamp=api_ts))

    assert path.exists()
    assert path.suffix == ".jsonl"


def test_orderbook_includes_written_at(tmp_path: Path) -> None:
    storage = DataStorage(tmp_path)
    path = storage.append_orderbook(_make_orderbook())

    record = json.loads(path.read_text(encoding="utf-8").strip())
    assert "written_at" in record


def test_orderbook_preserves_bids_asks(tmp_path: Path) -> None:
    storage = DataStorage(tmp_path)
    path = storage.append_orderbook(_make_orderbook())

    record = json.loads(path.read_text(encoding="utf-8").strip())
    assert len(record["bids"]) == 1
    assert len(record["asks"]) == 1
    assert record["bids"][0]["price"] == 0.64
    assert record["asks"][0]["size"] == 30.0


def test_orderbook_creates_directories(tmp_path: Path) -> None:
    nested = tmp_path / "deep" / "nested"
    storage = DataStorage(nested)

    path = storage.append_orderbook(_make_orderbook())
    assert path.exists()


def test_orderbook_uses_api_timestamp_for_filename(
    tmp_path: Path,
) -> None:
    api_ts = datetime(2025, 3, 18, 16, 0, 0, tzinfo=timezone.utc)
    storage = DataStorage(tmp_path)

    path = storage.append_orderbook(_make_orderbook(timestamp=api_ts))

    assert "2025-03-18" in path.name


def test_orderbook_preserves_api_timestamp_in_record(
    tmp_path: Path,
) -> None:
    api_ts = datetime(2025, 3, 18, 16, 0, 0, tzinfo=timezone.utc)
    storage = DataStorage(tmp_path)

    path = storage.append_orderbook(_make_orderbook(timestamp=api_ts))

    record = json.loads(path.read_text(encoding="utf-8").strip())
    assert "timestamp" in record
    assert "2025-03-18" in record["timestamp"]
    assert "written_at" in record
    assert record["timestamp"] != record["written_at"]


def test_snapshot_filenames_no_collision_rapid_saves(
    tmp_path: Path,
) -> None:
    """Two snapshots with different ms timestamps produce different files."""
    storage = DataStorage(tmp_path)
    ts1 = datetime(2025, 3, 18, 16, 0, 0, 0, tzinfo=timezone.utc)
    ts2 = datetime(2025, 3, 18, 16, 0, 0, 1000, tzinfo=timezone.utc)

    p1 = storage.save_market_snapshot([], ts1)
    p2 = storage.save_market_snapshot([], ts2)

    assert p1 != p2
    assert p1.exists()
    assert p2.exists()
