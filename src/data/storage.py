"""
Raw data persistence for Polymarket research data.

Responsibilities:
  - Write market discovery snapshots as JSON files
  - Append price records to daily JSONL files (with traceability context)
  - Append orderbook records to daily JSONL files (with traceability context)
  - Create directories as needed

File layout::

    {markets_dir}/
        markets_20250318T160000_000Z.json — one JSON per snapshot (ms precision)
    {prices_dir}/
        prices_2025-03-18.jsonl         — one line per price record, daily rolling
    {orderbooks_dir}/
        orderbooks_2025-03-18.jsonl     — one line per orderbook record, daily

Directory paths are provided explicitly by the caller (from config).

Timestamp policy:
  - API timestamps are preserved as-is in record content (source of truth).
  - A ``written_at`` field is added to JSONL records for write-time auditing.
    It is clearly labeled and never confused with API/event timestamps.
  - Market snapshot filenames use the caller-provided snapshot timestamp.
  - Price JSONL filenames use the date from the record's own API timestamp.
  - Orderbook JSONL filenames use the date from the record's API timestamp.
  - If any API timestamp is missing, a local UTC fallback is used and logged.

Traceability:
  - ``append_price`` and ``append_orderbook`` accept an optional *context*
    dict that is merged into each record.  Callers use this to inject
    market-level identifiers (condition_id, outcome, slug, event_id, etc.)
    so that every price/orderbook line can be traced back to its market
    without rebuilding the mapping externally.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.polymarket.models import Market, Orderbook, TokenPrice

log = logging.getLogger(__name__)


class DataStorage:
    """
    Persists raw Polymarket data to local files.

    Usage::

        storage = DataStorage(
            markets_dir=Path("data/markets"),
            prices_dir=Path("data/prices"),
            orderbooks_dir=Path("data/orderbooks"),
        )
        run_id = str(uuid.uuid4())
        run_ts = datetime.now(tz=timezone.utc)
        
        storage.save_market_snapshot(markets, snapshot_ts, run_id=run_id)
        storage.append_price(price, run_ts, run_id=run_id, context={...})
        storage.append_orderbook(orderbook, run_ts, run_id=run_id, context={...})
    """

    def __init__(
        self,
        markets_dir: Path,
        prices_dir: Path,
        orderbooks_dir: Path,
    ) -> None:
        self._markets_dir = markets_dir
        self._prices_dir = prices_dir
        self._orderbooks_dir = orderbooks_dir

    # ------------------------------------------------------------------
    # Market snapshots (JSON)
    # ------------------------------------------------------------------

    def save_market_snapshot(
        self,
        markets: list[Market],
        snapshot_ts: datetime,
        run_id: str | None = None,
    ) -> Path:
        """
        Save a snapshot of discovered markets to a timestamped JSON file.

        The snapshot includes the timestamp, run_id (for traceability), and
        a list of markets. Returns the path to the created file.
        """
        self._markets_dir.mkdir(parents=True, exist_ok=True)

        # Millisecond precision prevents collision on rapid successive snapshots.
        ms = snapshot_ts.strftime("%f")[:3]
        ts_str = snapshot_ts.strftime("%Y%m%dT%H%M%S") + f"_{ms}Z"
        file_path = self._markets_dir / f"markets_{ts_str}.json"

        envelope = {
            "snapshot_ts": snapshot_ts.isoformat(),
            "run_id": run_id,
            "markets": [m.model_dump(mode="json") for m in markets],
        }

        file_path.write_text(
            json.dumps(envelope, indent=2), encoding="utf-8",
        )
        log.info(
            "Saved market snapshot: %s (%d markets)",
            file_path.name,
            len(markets),
        )
        return file_path

    # ------------------------------------------------------------------
    # Price records (JSONL)
    # ------------------------------------------------------------------

    def append_price(
        self,
        price: TokenPrice,
        run_ts: datetime,
        run_id: str | None = None,
        context: dict[str, Any] | None = None,
    ) -> Path:
        """
        Append a single price record to the daily JSONL file.

        Uses ``run_ts`` (cycle start time) for daily filename derivation
        to ensure all records from the same run are grouped together.
        Adds ``run_id`` and ``written_at`` fields for traceability.

        If *context* is provided, its key/value pairs are merged into the
        record (e.g. condition_id, outcome, slug for traceability).

        Returns the path to the JSONL file.
        """
        self._prices_dir.mkdir(parents=True, exist_ok=True)

        # Use run_ts for consistent daily file naming across the run.
        date_str = run_ts.strftime("%Y-%m-%d")
        file_path = self._prices_dir / f"prices_{date_str}.jsonl"

        record = price.model_dump(mode="json")
        record["run_id"] = run_id
        record["written_at"] = datetime.now(tz=timezone.utc).isoformat()
        if context:
            record.update(context)

        with file_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(record) + "\n")

        return file_path

    # ------------------------------------------------------------------
    # Orderbook records (JSONL)
    # ------------------------------------------------------------------

    def append_orderbook(
        self,
        orderbook: Orderbook,
        run_ts: datetime,
        run_id: str | None = None,
        context: dict[str, Any] | None = None,
    ) -> Path:
        """
        Append a single orderbook record to the daily JSONL file.

        Uses ``run_ts`` (cycle start time) for daily filename derivation
        to ensure all records from the same run are grouped together.
        Adds ``run_id`` and ``written_at`` fields for traceability.

        If *context* is provided, its key/value pairs are merged into the
        record (e.g. condition_id, outcome, slug for traceability).

        Returns the path to the JSONL file.
        """
        self._orderbooks_dir.mkdir(parents=True, exist_ok=True)

        # Use run_ts for consistent daily file naming across the run.
        date_str = run_ts.strftime("%Y-%m-%d")
        file_path = self._orderbooks_dir / f"orderbooks_{date_str}.jsonl"

        record = orderbook.model_dump(mode="json")
        record["run_id"] = run_id
        record["written_at"] = datetime.now(tz=timezone.utc).isoformat()
        if context:
            record.update(context)

        with file_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(record) + "\n")

        return file_path
