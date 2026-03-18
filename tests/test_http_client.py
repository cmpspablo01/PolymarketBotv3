"""
Tests for src/polymarket/http_client.py

Coverage:
  - 200 response          : returns parsed JSON
  - Non-retryable 4xx     : raises PolymarketAPIError immediately (no sleep)
  - Retryable 429 / 5xx   : retries up to max_retries, then raises
  - Retry-After header    : used as sleep duration on 429
  - RequestException      : retried with backoff, then raises
  - Backoff sleep         : time.sleep called on each retry
  - Context manager       : __enter__ / __exit__ close session
  - close()               : delegates to session.close()

All HTTP calls are mocked — no live network calls.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
import requests

from src.polymarket.http_client import (
    PolymarketAPIError,
    PolymarketHTTPClient,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_response(status_code: int, body: object = None, headers: dict | None = None) -> MagicMock:
    """Build a mock requests.Response."""
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = body if body is not None else {}
    resp.text = str(body)
    resp.headers = headers or {}
    return resp


def _make_client(**kwargs) -> PolymarketHTTPClient:  # type: ignore[no-untyped-def]
    """Return a client with fast backoff suitable for tests."""
    defaults = {"base_url": "https://test.example.com", "base_delay": 0.0, "max_delay": 0.0}
    defaults.update(kwargs)
    return PolymarketHTTPClient(**defaults)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


def test_get_returns_dict_on_200() -> None:
    client = _make_client()
    client._session.get = MagicMock(return_value=_make_response(200, {"key": "value"}))

    result = client.get("/markets")

    assert result == {"key": "value"}
    client._session.get.assert_called_once()


def test_get_returns_list_on_200() -> None:
    client = _make_client()
    client._session.get = MagicMock(return_value=_make_response(200, [{"id": "1"}]))

    result = client.get("/markets")

    assert result == [{"id": "1"}]


def test_get_passes_params() -> None:
    client = _make_client()
    client._session.get = MagicMock(return_value=_make_response(200, {}))

    client.get("/markets", params={"active": "true"})

    _, kwargs = client._session.get.call_args
    assert kwargs["params"] == {"active": "true"}


def test_get_builds_correct_url() -> None:
    client = _make_client(base_url="https://clob.polymarket.com")
    client._session.get = MagicMock(return_value=_make_response(200, {}))

    client.get("/markets")

    args, _ = client._session.get.call_args
    assert args[0] == "https://clob.polymarket.com/markets"


# ---------------------------------------------------------------------------
# Non-retryable errors — immediate raise, no sleep
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("status_code", [400, 401, 403, 404, 422])
def test_non_retryable_status_raises_immediately(status_code: int) -> None:
    client = _make_client()
    client._session.get = MagicMock(return_value=_make_response(status_code))

    with patch("src.polymarket.http_client.time.sleep") as mock_sleep:
        with pytest.raises(PolymarketAPIError) as exc_info:
            client.get("/markets")

    assert exc_info.value.status_code == status_code
    mock_sleep.assert_not_called()
    client._session.get.assert_called_once()


def test_polymarket_api_error_contains_status_code() -> None:
    client = _make_client()
    client._session.get = MagicMock(return_value=_make_response(404, "not found"))

    with pytest.raises(PolymarketAPIError) as exc_info:
        client.get("/missing")

    assert exc_info.value.status_code == 404
    assert "404" in str(exc_info.value)


# ---------------------------------------------------------------------------
# Retryable errors — retry and eventually raise
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("status_code", [429, 500, 502, 503, 504])
def test_retryable_status_retries_then_raises(status_code: int) -> None:
    client = _make_client(max_retries=2)
    client._session.get = MagicMock(return_value=_make_response(status_code))

    with patch("src.polymarket.http_client.time.sleep"):
        with pytest.raises(PolymarketAPIError) as exc_info:
            client.get("/markets")

    assert exc_info.value.status_code == status_code
    assert client._session.get.call_count == 3  # 1 initial + 2 retries


def test_retryable_error_succeeds_on_retry() -> None:
    client = _make_client(max_retries=2)
    client._session.get = MagicMock(
        side_effect=[
            _make_response(500),
            _make_response(500),
            _make_response(200, {"ok": True}),
        ]
    )

    with patch("src.polymarket.http_client.time.sleep"):
        result = client.get("/markets")

    assert result == {"ok": True}
    assert client._session.get.call_count == 3


def test_sleep_called_on_each_retry() -> None:
    client = _make_client(max_retries=3)
    client._session.get = MagicMock(return_value=_make_response(503))

    with patch("src.polymarket.http_client.time.sleep") as mock_sleep:
        with pytest.raises(PolymarketAPIError):
            client.get("/markets")

    assert mock_sleep.call_count == 3  # once per retry (not after final attempt)


# ---------------------------------------------------------------------------
# Retry-After header
# ---------------------------------------------------------------------------


def test_retry_after_header_used_as_sleep_duration() -> None:
    client = _make_client(max_retries=1)
    client._session.get = MagicMock(
        side_effect=[
            _make_response(429, headers={"Retry-After": "5"}),
            _make_response(200, {"ok": True}),
        ]
    )

    with patch("src.polymarket.http_client.time.sleep") as mock_sleep:
        client.get("/markets")

    mock_sleep.assert_called_once_with(5.0)


def test_retry_after_header_absent_uses_backoff() -> None:
    client = _make_client(max_retries=1, base_delay=1.0)
    client._session.get = MagicMock(
        side_effect=[
            _make_response(429, headers={}),
            _make_response(200, {}),
        ]
    )

    with patch("src.polymarket.http_client.time.sleep") as mock_sleep:
        client.get("/markets")

    # backoff was used (not Retry-After) — just confirm sleep was called once
    mock_sleep.assert_called_once()


# ---------------------------------------------------------------------------
# RequestException (network error)
# ---------------------------------------------------------------------------


def test_request_exception_retries_then_raises() -> None:
    client = _make_client(max_retries=2)
    client._session.get = MagicMock(
        side_effect=requests.exceptions.ConnectionError("connection refused")
    )

    with patch("src.polymarket.http_client.time.sleep"):
        with pytest.raises(PolymarketAPIError) as exc_info:
            client.get("/markets")

    assert exc_info.value.status_code == 0
    assert client._session.get.call_count == 3


def test_request_exception_succeeds_on_retry() -> None:
    client = _make_client(max_retries=2)
    client._session.get = MagicMock(
        side_effect=[
            requests.exceptions.Timeout("timed out"),
            _make_response(200, {"recovered": True}),
        ]
    )

    with patch("src.polymarket.http_client.time.sleep"):
        result = client.get("/markets")

    assert result == {"recovered": True}


# ---------------------------------------------------------------------------
# Backoff delay calculation
# ---------------------------------------------------------------------------


def test_backoff_delay_increases_with_attempt() -> None:
    client = _make_client(base_delay=1.0, max_delay=100.0)

    with patch("random.uniform", return_value=0.0):
        d0 = client._backoff_delay(0)
        d1 = client._backoff_delay(1)
        d2 = client._backoff_delay(2)

    assert d0 < d1 < d2


def test_backoff_delay_capped_at_max() -> None:
    client = _make_client(base_delay=10.0, max_delay=5.0)

    with patch("random.uniform", return_value=0.0):
        delay = client._backoff_delay(10)

    assert delay == 5.0


# ---------------------------------------------------------------------------
# Session management
# ---------------------------------------------------------------------------


def test_close_delegates_to_session() -> None:
    client = _make_client()
    client._session = MagicMock()

    client.close()

    client._session.close.assert_called_once()


def test_context_manager_calls_close() -> None:
    client = _make_client()
    client._session = MagicMock()
    client._session.get = MagicMock(return_value=_make_response(200, {}))

    with client:
        pass

    client._session.close.assert_called_once()


def test_custom_headers_sent() -> None:
    client = PolymarketHTTPClient(
        base_url="https://test.example.com",
        headers={"Authorization": "Bearer token123"},
    )
    assert client._session.headers.get("Authorization") == "Bearer token123"
    client.close()
