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
        storage.save_market_snapshot(markets, snapshot_ts)
        storage.append_price(price, context={...})
        storage.append_orderbook(orderbook, context={...})
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
    ) -> Path:
        """
        Write a JSON file containing the full market discovery snapshot.

        The envelope contains:
          - ``snapshot_ts``: when the snapshot was taken (ISO 8601)
          - ``markets``: list of market dicts with all fields preserved

        Returns the path to the written file.
        """
        self._markets_dir.mkdir(parents=True, exist_ok=True)

        # Millisecond precision prevents collision on rapid successive snapshots.
        ms = snapshot_ts.strftime("%f")[:3]
        ts_str = snapshot_ts.strftime("%Y%m%dT%H%M%S") + f"_{ms}Z"
        file_path = self._markets_dir / f"markets_{ts_str}.json"

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

    def append_price(
        self,
        price: TokenPrice,
        context: dict[str, Any] | None = None,
    ) -> Path:
        """
        Append a single price record to the daily JSONL file.

        The record preserves the API timestamp from ``TokenPrice.timestamp``
        (source of truth).  A separate ``written_at`` field is added for
        write-time auditing only.

        If *context* is provided, its key/value pairs are merged into the
        record (e.g. condition_id, outcome, slug for traceability).

        Returns the path to the JSONL file.
        """
        self._prices_dir.mkdir(parents=True, exist_ok=True)

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
        file_path = self._prices_dir / f"prices_{date_str}.jsonl"

        record = price.model_dump(mode="json")
        if context:
            record.update(context)
        record["written_at"] = datetime.now(tz=timezone.utc).isoformat()

        with file_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(record) + "\n")

        return file_path

    # ------------------------------------------------------------------
    # Orderbook records (JSONL)
    # ------------------------------------------------------------------

    def append_orderbook(
        self,
        orderbook: Orderbook,
        context: dict[str, Any] | None = None,
    ) -> Path:
        """
        Append a single orderbook record to the daily JSONL file.

        Uses the API timestamp from ``Orderbook.timestamp`` as the
        source-of-truth for daily filename derivation.  Falls back to
        local UTC only when the API timestamp is absent (and logs a
        warning).  A ``written_at`` field is added for write-time
        auditing only.

        If *context* is provided, its key/value pairs are merged into the
        record (e.g. condition_id, outcome, slug for traceability).

        Returns the path to the JSONL file.
        """
        self._orderbooks_dir.mkdir(parents=True, exist_ok=True)

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
        file_path = self._orderbooks_dir / f"orderbooks_{date_str}.jsonl"

        record = orderbook.model_dump(mode="json")
        if context:
            record.update(context)
        record["written_at"] = datetime.now(tz=timezone.utc).isoformat()

        with file_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(record) + "\n")

        return file_path
