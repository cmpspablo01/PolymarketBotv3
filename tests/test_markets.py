"""
Tests for src/polymarket/markets.py

Coverage:
  - _is_btc_updown_event     : event title matching (Bitcoin + Up or Down)
  - _extract_granularity_tag  : tag-based granularity detection
  - _parse_json_string        : JSON string parsing, list passthrough, fallback
  - _parse_gamma_market       : camelCase fields, clobTokenIds/outcomes JSON strings,
                                token mapping, end_date, enriched metadata
  - discover_btc_15m          : integration — granularity filter, expiry filter,
                                enriched metadata, pagination
  - Granularity rejection     : 5m, 1h, 4h, daily all rejected
  - Expiry rejection          : expired markets excluded
  - Pagination safety limit   : warning when limit reached

All HTTP calls are mocked — no live network calls.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any
from unittest.mock import MagicMock, patch

from src.polymarket.http_client import PolymarketHTTPClient
from src.polymarket.markets import (
    MarketDiscovery,
    _extract_granularity_tag,
    _is_btc_updown_event,
    _parse_gamma_market,
    _parse_json_string,
)
from src.polymarket.models import Market

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

# A future date guaranteed to be after "now" in tests.
_FUTURE = "2099-12-31T23:59:59Z"
_PAST = "2020-01-01T00:00:00Z"


def _gamma_market(
    condition_id: str = "0xabc",
    question: str = "Bitcoin Up or Down - March 19, 4:00PM-4:15PM ET",
    outcomes: list[str] | None = None,
    clob_token_ids: list[str] | None = None,
    active: bool = True,
    closed: bool = False,
    end_date: str = _FUTURE,
    **extra: Any,
) -> dict[str, Any]:
    """Build a minimal raw Gamma market dict."""
    if outcomes is None:
        outcomes = ["Up", "Down"]
    if clob_token_ids is None:
        clob_token_ids = ["tok_up_001", "tok_down_001"]
    base: dict[str, Any] = {
        "id": "12345",
        "conditionId": condition_id,
        "question": question,
        "slug": "btc-updown-15m-test",
        "outcomes": json.dumps(outcomes),
        "clobTokenIds": json.dumps(clob_token_ids),
        "active": active,
        "closed": closed,
        "endDate": end_date,
        "eventStartTime": "2099-12-31T23:44:59Z",
        "startDate": "2026-03-19T15:45:00Z",
        "description": "Resolves Up if BTC price at end >= price at start.",
    }
    base.update(extra)
    return base


def _gamma_event(
    title: str = "Bitcoin Up or Down - March 19, 4:00PM-4:15PM ET",
    markets: list[dict[str, Any]] | None = None,
    tags: list[dict[str, str]] | None = None,
    **extra: Any,
) -> dict[str, Any]:
    """Build a minimal Gamma event dict with 15M tag by default."""
    if tags is None:
        tags = [
            {"slug": "crypto"},
            {"slug": "bitcoin"},
            {"slug": "up-or-down"},
            {"slug": "15M"},
        ]
    base: dict[str, Any] = {
        "id": "99001",
        "title": title,
        "slug": "btc-updown-15m-test",
        "tags": tags,
        "markets": markets if markets is not None else [_gamma_market()],
    }
    base.update(extra)
    return base


def _make_discovery(events: list[dict[str, Any]]) -> MarketDiscovery:
    """Return a MarketDiscovery with a mocked Gamma client returning *events*."""
    client = MagicMock(spec=PolymarketHTTPClient)
    # First call returns events, subsequent calls return [] (end of pagination)
    client.get.side_effect = [events, []]
    return MarketDiscovery(client)


# ---------------------------------------------------------------------------
# _is_btc_updown_event
# ---------------------------------------------------------------------------


def test_event_filter_matches_btc_updown() -> None:
    ev = {"title": "Bitcoin Up or Down - March 19, 4:00PM-4:15PM ET"}
    assert _is_btc_updown_event(ev) is True


def test_event_filter_case_insensitive() -> None:
    ev = {"title": "BITCOIN UP OR DOWN - March 19, 4PM ET"}
    assert _is_btc_updown_event(ev) is True


def test_event_filter_rejects_ethereum() -> None:
    ev = {"title": "Ethereum Up or Down - March 19, 4PM ET"}
    assert _is_btc_updown_event(ev) is False


def test_event_filter_rejects_btc_long_term() -> None:
    ev = {"title": "What price will Bitcoin hit in 2026?"}
    assert _is_btc_updown_event(ev) is False


def test_event_filter_rejects_missing_title() -> None:
    ev = {"slug": "some-slug"}
    assert _is_btc_updown_event(ev) is False


# ---------------------------------------------------------------------------
# _extract_granularity_tag
# ---------------------------------------------------------------------------


def test_granularity_tag_15m() -> None:
    ev = {"tags": [{"slug": "crypto"}, {"slug": "15M"}]}
    assert _extract_granularity_tag(ev) == "15M"


def test_granularity_tag_5m() -> None:
    ev = {"tags": [{"slug": "5M"}, {"slug": "bitcoin"}]}
    assert _extract_granularity_tag(ev) == "5M"


def test_granularity_tag_1h() -> None:
    ev = {"tags": [{"slug": "1H"}]}
    assert _extract_granularity_tag(ev) == "1H"


def test_granularity_tag_4h() -> None:
    ev = {"tags": [{"slug": "4h"}]}
    assert _extract_granularity_tag(ev) == "4h"


def test_granularity_tag_daily() -> None:
    ev = {"tags": [{"slug": "daily"}]}
    assert _extract_granularity_tag(ev) == "daily"


def test_granularity_tag_none_when_missing() -> None:
    ev = {"tags": [{"slug": "crypto"}, {"slug": "bitcoin"}]}
    assert _extract_granularity_tag(ev) is None


def test_granularity_tag_empty_tags() -> None:
    ev = {"tags": []}
    assert _extract_granularity_tag(ev) is None


# ---------------------------------------------------------------------------
# _parse_json_string
# ---------------------------------------------------------------------------


def test_parse_json_string_from_string() -> None:
    assert _parse_json_string('["Up", "Down"]', []) == ["Up", "Down"]


def test_parse_json_string_passthrough_list() -> None:
    assert _parse_json_string(["Up", "Down"], []) == ["Up", "Down"]


def test_parse_json_string_returns_default_on_none() -> None:
    assert _parse_json_string(None, ["fallback"]) == ["fallback"]


def test_parse_json_string_returns_default_on_invalid_json() -> None:
    assert _parse_json_string("not json", []) == []


def test_parse_json_string_returns_default_on_non_list_json() -> None:
    assert _parse_json_string('{"key": "val"}', []) == []


# ---------------------------------------------------------------------------
# _parse_gamma_market
# ---------------------------------------------------------------------------


def test_parse_gamma_market_basic() -> None:
    raw = _gamma_market(condition_id="0xabc", question="BTC Up or Down - 4:00PM-4:15PM ET")
    m = _parse_gamma_market(raw)

    assert isinstance(m, Market)
    assert m.condition_id == "0xabc"
    assert m.question == "BTC Up or Down - 4:00PM-4:15PM ET"
    assert len(m.tokens) == 2
    assert m.tokens[0].outcome == "Up"
    assert m.tokens[1].outcome == "Down"
    assert m.tokens[0].token_id == "tok_up_001"
    assert m.tokens[1].token_id == "tok_down_001"


def test_parse_gamma_market_enriched_metadata() -> None:
    raw = _gamma_market(
        slug="btc-updown-15m-123",
        description="Resolves Up if BTC...",
        startDate="2026-03-19T15:45:00Z",
        eventStartTime="2026-03-19T16:00:00Z",
    )
    raw["id"] = "55555"
    m = _parse_gamma_market(raw, event_id="99001", event_slug="ev-slug")

    assert m.slug == "btc-updown-15m-123"
    assert m.market_id == "55555"
    assert m.event_id == "99001"
    assert m.event_slug == "ev-slug"
    assert m.description == "Resolves Up if BTC..."
    assert m.start_date is not None
    assert m.event_start_time is not None


def test_parse_gamma_market_end_date() -> None:
    raw = _gamma_market(end_date="2026-03-19T20:00:00Z")
    m = _parse_gamma_market(raw)
    assert m.end_date is not None


def test_parse_gamma_market_no_tokens_when_missing_clob_ids() -> None:
    raw = _gamma_market()
    del raw["clobTokenIds"]
    m = _parse_gamma_market(raw)
    assert m.tokens == []


def test_parse_gamma_market_handles_list_outcomes() -> None:
    """Gamma sometimes returns outcomes as a real list, not a JSON string."""
    raw = _gamma_market()
    raw["outcomes"] = ["Up", "Down"]  # list, not string
    raw["clobTokenIds"] = ["tok_a", "tok_b"]  # list, not string
    m = _parse_gamma_market(raw)
    assert m.tokens[0].outcome == "Up"
    assert m.tokens[0].token_id == "tok_a"


def test_parse_gamma_market_group_slug() -> None:
    raw = _gamma_market(groupSlug="btc-updown-5m-123")
    m = _parse_gamma_market(raw)
    assert m.group_id == "btc-updown-5m-123"


# ---------------------------------------------------------------------------
# discover_btc_15m — integration with mocked client
# ---------------------------------------------------------------------------


def test_discover_retains_only_15m_events() -> None:
    """Only events tagged 15M are retained; 5M, 1H are rejected."""
    ev_15m = _gamma_event(
        title="Bitcoin Up or Down - March 19, 4:00PM-4:15PM ET",
        tags=[{"slug": "15M"}, {"slug": "bitcoin"}],
    )
    ev_5m = _gamma_event(
        title="Bitcoin Up or Down - March 19, 4:00PM-4:05PM ET",
        tags=[{"slug": "5M"}, {"slug": "bitcoin"}],
    )
    ev_1h = _gamma_event(
        title="Bitcoin Up or Down - March 19, 4PM ET",
        tags=[{"slug": "1H"}, {"slug": "bitcoin"}],
    )
    discovery = _make_discovery([ev_15m, ev_5m, ev_1h])

    markets = discovery.discover_btc_15m()

    assert len(markets) == 1


def test_discover_rejects_4h_events() -> None:
    ev = _gamma_event(
        title="Bitcoin Up or Down - March 19, 4:00PM-8:00PM ET",
        tags=[{"slug": "4h"}, {"slug": "bitcoin"}],
    )
    discovery = _make_discovery([ev])
    assert discovery.discover_btc_15m() == []


def test_discover_rejects_daily_events() -> None:
    ev = _gamma_event(
        title="Bitcoin Up or Down on March 19?",
        tags=[{"slug": "daily"}, {"slug": "bitcoin"}],
    )
    discovery = _make_discovery([ev])
    assert discovery.discover_btc_15m() == []


def test_discover_rejects_non_btc_events() -> None:
    ev = _gamma_event(
        title="Ethereum Up or Down - March 19, 4PM ET",
        tags=[{"slug": "15M"}, {"slug": "crypto"}],
    )
    discovery = _make_discovery([ev])
    assert discovery.discover_btc_15m() == []


def test_discover_rejects_expired_markets() -> None:
    """Markets with end_date in the past are excluded."""
    expired = _gamma_market(end_date=_PAST)
    ev = _gamma_event(markets=[expired])
    discovery = _make_discovery([ev])

    markets = discovery.discover_btc_15m()

    assert len(markets) == 0


def test_discover_retains_future_markets() -> None:
    """Markets with end_date in the future are retained."""
    future = _gamma_market(end_date=_FUTURE)
    ev = _gamma_event(markets=[future])
    discovery = _make_discovery([ev])

    markets = discovery.discover_btc_15m()

    assert len(markets) == 1


def test_discover_skips_closed_markets() -> None:
    closed_market = _gamma_market(closed=True)
    events = [_gamma_event(markets=[closed_market])]
    discovery = _make_discovery(events)

    markets = discovery.discover_btc_15m()

    assert len(markets) == 0


def test_discover_skips_markets_without_tokens() -> None:
    no_tokens = _gamma_market()
    no_tokens["clobTokenIds"] = "[]"
    events = [_gamma_event(markets=[no_tokens])]
    discovery = _make_discovery(events)

    markets = discovery.discover_btc_15m()

    assert len(markets) == 0


def test_discover_returns_empty_when_no_events() -> None:
    discovery = _make_discovery([])

    markets = discovery.discover_btc_15m()

    assert markets == []


def test_discover_paginates_events() -> None:
    """Client should be called with offset=0, then offset=N for next page."""
    client = MagicMock(spec=PolymarketHTTPClient)
    page1 = [_gamma_event()] * 100  # full page
    page2 = [_gamma_event()] * 3
    client.get.side_effect = [page1, page2, []]
    discovery = MarketDiscovery(client)

    markets = discovery.discover_btc_15m()

    # Verify pagination: at least 2 calls with increasing offsets
    calls = client.get.call_args_list
    assert len(calls) >= 2
    assert calls[0][1]["params"]["offset"] == "0"
    assert calls[1][1]["params"]["offset"] == "100"

    assert len(markets) == 103


def test_discover_logs_warning_on_pagination_limit(caplog: Any) -> None:
    """When _MAX_PAGES is reached, a warning is logged."""
    import logging
    import src.polymarket.markets as markets_mod

    original = markets_mod._MAX_PAGES
    markets_mod._MAX_PAGES = 1  # force limit to 1 page

    try:
        client = MagicMock(spec=PolymarketHTTPClient)
        # Return a full page so the loop doesn't break early
        client.get.side_effect = [[_gamma_event()] * 100]
        discovery = MarketDiscovery(client)

        with caplog.at_level(logging.WARNING, logger="src.polymarket.markets"):
            discovery.discover_btc_15m()

        assert any("safety limit" in r.message.lower() for r in caplog.records)
    finally:
        markets_mod._MAX_PAGES = original


def test_discover_enriches_market_with_event_context() -> None:
    """Retained markets carry event_id and event_slug from the parent event."""
    ev = _gamma_event(id="77777")
    ev["slug"] = "btc-updown-15m-77777"
    discovery = _make_discovery([ev])

    markets = discovery.discover_btc_15m()

    assert len(markets) == 1
    m = markets[0]
    assert m.event_id == "77777"
    assert m.event_slug == "btc-updown-15m-77777"
    assert m.slug is not None
    assert m.description is not None


def test_parse_gamma_market_event_start_time() -> None:
    """eventStartTime (15m window start) is extracted correctly."""
    raw = _gamma_market(eventStartTime="2026-03-19T16:00:00Z")
    m = _parse_gamma_market(raw)
    assert m.event_start_time is not None
    assert isinstance(m.event_start_time, datetime)


def test_parse_gamma_market_event_start_time_absent() -> None:
    """event_start_time is None when eventStartTime is missing."""
    raw = _gamma_market()
    del raw["eventStartTime"]
    m = _parse_gamma_market(raw)
    assert m.event_start_time is None


def test_start_date_is_not_window_start() -> None:
    """start_date (listing timestamp) differs from event_start_time (window start)."""
    raw = _gamma_market(
        startDate="2026-03-18T00:53:19Z",
        eventStartTime="2026-03-19T00:45:00Z",
        endDate="2026-03-19T01:00:00Z",
    )
    m = _parse_gamma_market(raw)
    assert m.start_date != m.event_start_time
    # event_start_time + 15 min == end_date
    delta = (m.end_date - m.event_start_time).total_seconds()
    assert delta == 900  # 15 minutes
