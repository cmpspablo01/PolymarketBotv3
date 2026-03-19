"""
Market discovery for BTC 15-minute Up/Down markets via Polymarket Gamma API.

Discovery strategy (Phase 1):
  1. Fetch open crypto events from Gamma /events endpoint (paginated to exhaustion)
  2. Filter for Bitcoin Up or Down **15-minute** events using structured tags
  3. Exclude expired markets (end_date <= now)
  4. Extract markets from matching events
  5. Parse JSON-encoded fields (clobTokenIds, outcomes)
  6. Return enriched Market objects ready for CLOB price/orderbook fetching

Granularity detection:
  Gamma tags include a structured granularity label per event:
    "5M"  → 5-minute markets
    "15M" → 15-minute markets  ← Phase 1 target
    "1H"  → hourly markets
    "4h"  → 4-hour markets
    "daily" → daily markets
  The slug prefix also encodes granularity (e.g. "btc-updown-15m-*").

Temporal semantics:
  - ``eventStartTime`` (market-level): actual start of the 15-minute window.
  - ``endDate``: end of the 15-minute window (= eventStartTime + 15 min).
  - ``startDate``: market listing/creation timestamp — NOT the window start.

Strike / price-to-beat (PTB):
  NOT available from any Polymarket API (Gamma or CLOB).  BTC Up/Down
  markets resolve based on the Chainlink BTC/USD oracle price at the
  *start* vs. *end* of the window.  The PTB is a runtime oracle value
  that must be captured from an external source at ``eventStartTime``.

API notes:
  - Discovery uses Gamma API (gamma-api.polymarket.com)
  - Prices and orderbooks use CLOB API (clob.polymarket.com)
  - Gamma returns clobTokenIds and outcomes as JSON *strings*
  - BTC Up/Down events have outcomes ["Up", "Down"]

Does NOT:
  - Fetch prices or orderbook data (that belongs in prices.py)
  - Contain strategy or execution logic
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from src.polymarket.http_client import PolymarketHTTPClient
from src.polymarket.models import Market, Token

log = logging.getLogger(__name__)

EVENTS_ENDPOINT = "/events"

# Safety limit for paginated event fetching.
# If reached, the result is logged as potentially incomplete.
_MAX_PAGES = 50

# Phase 1 target granularity — only events tagged with this are retained.
TARGET_GRANULARITY_TAG = "15M"


# ---------------------------------------------------------------------------
# Discovery statistics (for logging)
# ---------------------------------------------------------------------------


@dataclass
class DiscoveryStats:
    """Counters for granular discovery logging."""

    total_crypto_events: int = 0
    rejected_not_btc_updown: int = 0
    rejected_wrong_granularity: int = 0
    rejected_expired: int = 0
    rejected_closed: int = 0
    rejected_no_tokens: int = 0
    rejected_parse_error: int = 0
    retained: int = 0
    pagination_exhausted: bool = True
    granularity_breakdown: dict[str, int] = field(default_factory=dict)


class MarketDiscovery:
    """
    Discovers active BTC 15-minute Up/Down markets on Polymarket via Gamma API.

    Usage::

        gamma_client = PolymarketHTTPClient(base_url="https://gamma-api.polymarket.com")
        discovery = MarketDiscovery(gamma_client)
        btc_markets = discovery.discover_btc_15m()
    """

    def __init__(self, client: PolymarketHTTPClient) -> None:
        """
        Args:
            client: HTTP client configured for the **Gamma** API base URL.
        """
        self._client = client

    def discover_btc_15m(self) -> list[Market]:
        """
        Discover active BTC 15-minute Up/Down markets from Gamma events.

        Applies the following filters in order:
          1. Event title must contain "bitcoin" and "up or down"
          2. Event tags must include "15M" (rejects 5M, 1H, 4h, daily)
          3. Market end_date must be in the future
          4. Market must not be closed and must have CLOB token IDs

        Returns enriched Market objects with slug, event_id, description, etc.
        """
        now = datetime.now(tz=timezone.utc)
        stats = DiscoveryStats()

        events, stats.pagination_exhausted = self._fetch_crypto_events()
        stats.total_crypto_events = len(events)

        markets: list[Market] = []
        for ev in events:
            # --- Filter 1: Bitcoin Up or Down title ---
            if not _is_btc_updown_event(ev):
                stats.rejected_not_btc_updown += 1
                continue

            # --- Filter 2: 15M granularity tag ---
            granularity = _extract_granularity_tag(ev)
            if granularity:
                stats.granularity_breakdown[granularity] = (
                    stats.granularity_breakdown.get(granularity, 0) + 1
                )
            if granularity != TARGET_GRANULARITY_TAG:
                stats.rejected_wrong_granularity += 1
                log.debug(
                    "Rejected event (granularity=%s): %s",
                    granularity or "unknown",
                    ev.get("title", "?"),
                )
                continue

            ev_id = str(ev.get("id", ""))
            ev_slug = ev.get("slug", "")

            for raw_market in ev.get("markets", []):
                try:
                    m = _parse_gamma_market(raw_market, ev_id, ev_slug)
                except Exception:
                    slug = (
                        raw_market.get("slug", "unknown")
                        if isinstance(raw_market, dict)
                        else "unknown"
                    )
                    log.warning("Skipping unparseable market: %s", slug)
                    stats.rejected_parse_error += 1
                    continue

                # --- Filter 3: Expiry check ---
                if m.end_date is not None and m.end_date <= now:
                    stats.rejected_expired += 1
                    log.debug(
                        "Rejected expired market: %s (end_date=%s)",
                        m.slug or m.question[:60],
                        m.end_date.isoformat(),
                    )
                    continue

                # --- Filter 4: Must be open with tokens ---
                if m.closed:
                    stats.rejected_closed += 1
                    continue
                if not m.tokens:
                    stats.rejected_no_tokens += 1
                    continue

                markets.append(m)
                log.debug(
                    "Retained market: %s | tokens=%d | end=%s",
                    m.slug or m.question[:60],
                    len(m.tokens),
                    m.end_date.isoformat() if m.end_date else "?",
                )
                for tok in m.tokens:
                    log.debug(
                        "  Token: %s -> %s",
                        tok.outcome,
                        tok.token_id[:24],
                    )

        stats.retained = len(markets)
        _log_discovery_summary(stats)
        return markets

    def _fetch_crypto_events(self) -> tuple[list[dict[str, Any]], bool]:
        """
        Fetch all open crypto events from Gamma API with pagination.

        Returns:
            A tuple of (events_list, pagination_exhausted).
            pagination_exhausted is True if all pages were fetched,
            False if the safety limit was reached.
        """
        all_events: list[dict[str, Any]] = []
        offset = 0
        page_count = 0
        exhausted = True

        for _ in range(_MAX_PAGES):
            batch = self._client.get(
                EVENTS_ENDPOINT,
                params={
                    "tag_slug": "crypto",
                    "closed": "false",
                    "limit": "100",
                    "offset": str(offset),
                },
            )
            if not isinstance(batch, list) or not batch:
                break
            all_events.extend(batch)
            offset += len(batch)
            page_count += 1
            if len(batch) < 100:
                break
        else:
            # for-loop completed without break → safety limit reached
            exhausted = False
            log.warning(
                "Pagination safety limit reached (%d pages, %d events). "
                "Discovery result may be INCOMPLETE.",
                _MAX_PAGES,
                len(all_events),
            )

        log.info(
            "Fetched %d crypto events in %d page(s) from Gamma /events "
            "(exhausted=%s)",
            len(all_events),
            page_count,
            exhausted,
        )
        return all_events, exhausted


# ---------------------------------------------------------------------------
# Event / market filter
# ---------------------------------------------------------------------------


def _is_btc_updown_event(event: dict[str, Any]) -> bool:
    """
    Return True if *event* is a Bitcoin Up or Down event (any granularity).

    This is the first-pass title filter. Granularity filtering happens
    separately via ``_extract_granularity_tag``.
    """
    title = (event.get("title") or "").lower()
    return "bitcoin" in title and "up or down" in title


def _extract_granularity_tag(event: dict[str, Any]) -> str | None:
    """
    Extract the granularity tag from a Gamma event.

    Known values: "5M", "15M", "1H", "4h", "daily".
    Returns None if no recognized granularity tag is found.
    """
    known = {"5M", "15M", "1H", "4h", "daily"}
    tags = event.get("tags", [])
    for tag in tags:
        slug = tag.get("slug", "") if isinstance(tag, dict) else str(tag)
        if slug in known:
            return slug
    return None


# ---------------------------------------------------------------------------
# Gamma market parsing
# ---------------------------------------------------------------------------


def _parse_json_string(value: Any, default: list[str]) -> list[str]:
    """Parse a JSON-encoded string list, or return *default* on failure."""
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            if isinstance(parsed, list):
                return parsed
        except (json.JSONDecodeError, TypeError):
            pass
    if isinstance(value, list):
        return value
    return default


def _parse_gamma_market(
    raw: dict[str, Any],
    event_id: str = "",
    event_slug: str = "",
) -> Market:
    """
    Parse a Gamma API market dict into an enriched Market model.

    Handles Gamma-specific field naming (camelCase) and JSON-encoded
    string fields (clobTokenIds, outcomes).
    Injects event-level context (event_id, event_slug) for traceability.
    """
    clob_ids = _parse_json_string(raw.get("clobTokenIds"), [])
    outcomes = _parse_json_string(raw.get("outcomes"), [])

    tokens = [
        Token(
            token_id=tid,
            outcome=outcomes[i] if i < len(outcomes) else f"outcome_{i}",
        )
        for i, tid in enumerate(clob_ids)
    ]

    return Market(
        condition_id=raw.get("conditionId", ""),
        question=raw.get("question", ""),
        tokens=tokens,
        active=raw.get("active", False),
        closed=raw.get("closed", True),
        end_date=raw.get("endDate"),
        event_start_time=raw.get("eventStartTime"),
        start_date=raw.get("startDate"),
        group_id=raw.get("groupSlug"),
        category=None,
        slug=raw.get("slug"),
        market_id=str(raw.get("id", "")),
        event_id=event_id,
        event_slug=event_slug,
        description=raw.get("description"),
    )


# ---------------------------------------------------------------------------
# Discovery logging
# ---------------------------------------------------------------------------


def _log_discovery_summary(stats: DiscoveryStats) -> None:
    """Emit an INFO-level summary of discovery filtering results."""
    log.info(
        "Discovery summary: %d crypto events inspected, "
        "%d retained (15M BTC Up/Down)",
        stats.total_crypto_events,
        stats.retained,
    )
    log.info(
        "Rejections: not_btc_updown=%d, wrong_granularity=%d, "
        "expired=%d, closed=%d, no_tokens=%d, parse_error=%d",
        stats.rejected_not_btc_updown,
        stats.rejected_wrong_granularity,
        stats.rejected_expired,
        stats.rejected_closed,
        stats.rejected_no_tokens,
        stats.rejected_parse_error,
    )
    if stats.granularity_breakdown:
        breakdown = ", ".join(
            f"{k}={v}" for k, v in sorted(stats.granularity_breakdown.items())
        )
        log.info("BTC Up/Down granularity breakdown: %s", breakdown)
    if not stats.pagination_exhausted:
        log.warning(
            "Discovery may be INCOMPLETE — pagination safety limit reached"
        )
    log.info(
        "PTB status: not available from Polymarket APIs — "
        "requires external Chainlink BTC/USD oracle query at eventStartTime"
    )
