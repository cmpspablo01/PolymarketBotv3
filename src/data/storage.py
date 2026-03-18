"""
Raw data persistence for Polymarket research data.

Responsibilities:
  - Write market discovery snapshots as JSON files
  - Append price records to daily JSONL files
  - Append orderbook records to daily JSONL files
  - Create directories as needed

File layout (under base_dir)::

    markets/
        markets_20250318T160000_000Z.json — one JSON per snapshot (ms precision)
    prices/
        prices_2025-03-18.jsonl         — one line per price record, daily rolling
    orderbooks/
        orderbooks_2025-03-18.jsonl     — one line per orderbook record, daily

Timestamp policy:
  - API timestamps are preserved as-is in record content (source of truth).
  - A ``written_at`` field is added to JSONL records for write-time auditing.
    It is clearly labeled and never confused with API/event timestamps.
  - Market snapshot filenames use the caller-provided snapshot timestamp.
  - Price JSONL filenames use the date from the record's own API timestamp.
  - Orderbook JSONL filenames use the date from the record's API timestamp.
  - If any API timestamp is missing, a local UTC fallback is used and logged.
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

        storage = DataStorage(Path("data"))
        storage.save_market_snapshot(markets, snapshot_ts)
        storage.append_price(price)
        storage.append_orderbook(orderbook)
    """

    def __init__(self, base_dir: Path) -> None:
        self._base_dir = base_dir

    # ------------------------------------------------------------------
    # Market snapshots (JSON)
    # ------------------------------------------------------------------

    def save_market_snapshot(
        self,
        markets: list[Market],
        snapshot_ts: datetime,
    ) -> Path:
        """
        Write a JSON file containing the full market discovery snapshot.

        The envelope contains:
          - ``snapshot_ts``: when the snapshot was taken (ISO 8601)
          - ``markets``: list of market dicts with all fields preserved

        Returns the path to the written file.
        """
        dir_path = self._base_dir / "markets"
        dir_path.mkdir(parents=True, exist_ok=True)

        # Millisecond precision prevents collision on rapid successive snapshots.
        ms = snapshot_ts.strftime("%f")[:3]
        ts_str = snapshot_ts.strftime("%Y%m%dT%H%M%S") + f"_{ms}Z"
        file_path = dir_path / f"markets_{ts_str}.json"

        payload: dict[str, Any] = {
            "snapshot_ts": snapshot_ts.isoformat(),
            "markets": [m.model_dump(mode="json") for m in markets],
        }

        file_path.write_text(
            json.dumps(payload, indent=2), encoding="utf-8",
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

    def append_price(self, price: TokenPrice) -> Path:
        """
        Append a single price record to the daily JSONL file.

        The record preserves the API timestamp from ``TokenPrice.timestamp``
        (source of truth).  A separate ``written_at`` field is added for
        write-time auditing only.

        Returns the path to the JSONL file.
        """
        dir_path = self._base_dir / "prices"
        dir_path.mkdir(parents=True, exist_ok=True)

        # Use the price's API timestamp for daily file naming.
        if price.timestamp is not None:
            ts = price.timestamp
        else:
            ts = datetime.now(tz=timezone.utc)
            log.warning(
                "Price for %s has no API timestamp; "
                "using local UTC for filename",
                price.token_id,
            )
        date_str = ts.strftime("%Y-%m-%d")
        file_path = dir_path / f"prices_{date_str}.jsonl"

        record = price.model_dump(mode="json")
        record["written_at"] = datetime.now(tz=timezone.utc).isoformat()

        with file_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(record) + "\n")

        return file_path

    # ------------------------------------------------------------------
    # Orderbook records (JSONL)
    # ------------------------------------------------------------------

    def append_orderbook(self, orderbook: Orderbook) -> Path:
        """
        Append a single orderbook record to the daily JSONL file.

        Uses the API timestamp from ``Orderbook.timestamp`` as the
        source-of-truth for daily filename derivation.  Falls back to
        local UTC only when the API timestamp is absent (and logs a
        warning).  A ``written_at`` field is added for write-time
        auditing only.

        Returns the path to the JSONL file.
        """
        dir_path = self._base_dir / "orderbooks"
        dir_path.mkdir(parents=True, exist_ok=True)

        # Use the orderbook's API timestamp for daily file naming.
        if orderbook.timestamp is not None:
            ts = orderbook.timestamp
        else:
            ts = datetime.now(tz=timezone.utc)
            log.warning(
                "Orderbook for %s has no API timestamp; "
                "using local UTC for filename",
                orderbook.token_id,
            )
        date_str = ts.strftime("%Y-%m-%d")
        file_path = dir_path / f"orderbooks_{date_str}.jsonl"

        record = orderbook.model_dump(mode="json")
        record["written_at"] = datetime.now(tz=timezone.utc).isoformat()

        with file_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(record) + "\n")

        return file_path
