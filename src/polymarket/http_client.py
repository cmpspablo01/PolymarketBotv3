"""
Raw HTTP client for the Polymarket CLOB API.

Responsibilities:
  - Session management and connection pooling via requests.Session
  - Base URL configuration and default headers
  - Request timeout enforcement (connect=10s, read=30s)
  - Exponential backoff with jitter for retryable failures (429, 5xx)
  - Respects Retry-After header on 429 responses
  - Surfaces non-retryable errors immediately as PolymarketAPIError

Does NOT:
  - Parse market or price data (belongs in markets.py / prices.py)
  - Implement authentication signing (later phase)
  - Contain any market discovery or business logic

Retry policy:
  delay = min(base_delay * 2^attempt + uniform(0.0, 1.0), max_delay)
  Retryable status codes : 429, 500, 502, 503, 504
  Network errors         : retried with the same backoff schedule
  Max retries            : 3  (4 total attempts)
  Base delay             : 1.0s
  Max delay              : 30.0s
"""

from __future__ import annotations

import logging
import random
import time
from typing import Any

import requests
from requests import Response, Session

log = logging.getLogger(__name__)

RETRYABLE_STATUS_CODES: frozenset[int] = frozenset({429, 500, 502, 503, 504})

DEFAULT_TIMEOUT: tuple[int, int] = (10, 30)   # (connect_seconds, read_seconds)
DEFAULT_MAX_RETRIES: int = 3
DEFAULT_BASE_DELAY: float = 1.0                # seconds
DEFAULT_MAX_DELAY: float = 30.0               # seconds


class PolymarketAPIError(Exception):
    """Raised when the API returns a non-retryable error or retries are exhausted."""

    def __init__(self, status_code: int, message: str) -> None:
        super().__init__(f"HTTP {status_code}: {message}")
        self.status_code = status_code


class PolymarketHTTPClient:
    """
    Synchronous HTTP client for the Polymarket CLOB API.

    Handles session lifecycle, timeouts, and retry/backoff.
    Callers receive the raw parsed JSON; error surfaces as PolymarketAPIError.

    Usage::

        client = PolymarketHTTPClient(base_url="https://clob.polymarket.com")
        data = client.get("/markets", params={"active": "true"})
        client.close()

    Or as a context manager::

        with PolymarketHTTPClient(base_url="https://clob.polymarket.com") as client:
            data = client.get("/markets")
    """

    def __init__(
        self,
        base_url: str,
        *,
        headers: dict[str, str] | None = None,
        timeout: tuple[int, int] = DEFAULT_TIMEOUT,
        max_retries: int = DEFAULT_MAX_RETRIES,
        base_delay: float = DEFAULT_BASE_DELAY,
        max_delay: float = DEFAULT_MAX_DELAY,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout
        self._max_retries = max_retries
        self._base_delay = base_delay
        self._max_delay = max_delay
        self._session: Session = Session()
        self._session.headers.update({"Accept": "application/json"})
        if headers:
            self._session.headers.update(headers)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def get(self, path: str, params: dict[str, Any] | None = None) -> Any:
        """
        Perform a GET request with automatic retry/backoff.

        Args:
            path:   URL path relative to base_url (e.g. "/markets").
            params: Optional query string parameters.

        Returns:
            Parsed JSON response (dict or list depending on endpoint).

        Raises:
            PolymarketAPIError: On non-retryable HTTP errors or exhausted retries.
        """
        url = f"{self._base_url}/{path.lstrip('/')}"

        for attempt in range(self._max_retries + 1):
            try:
                response = self._session.get(url, params=params, timeout=self._timeout)
            except requests.exceptions.RequestException as exc:
                if attempt < self._max_retries:
                    delay = self._backoff_delay(attempt)
                    log.warning(
                        "Network error on attempt %d/%d, retrying in %.2fs: %s",
                        attempt + 1,
                        self._max_retries + 1,
                        delay,
                        exc,
                    )
                    time.sleep(delay)
                    continue
                raise PolymarketAPIError(
                    0,
                    f"Request failed after {self._max_retries + 1} attempts: {exc}",
                ) from exc

            if response.status_code == 200:
                return response.json()

            if response.status_code in RETRYABLE_STATUS_CODES:
                if attempt < self._max_retries:
                    delay = self._retry_after(response) or self._backoff_delay(attempt)
                    log.warning(
                        "HTTP %d on attempt %d/%d, retrying in %.2fs",
                        response.status_code,
                        attempt + 1,
                        self._max_retries + 1,
                        delay,
                    )
                    time.sleep(delay)
                    continue
                raise PolymarketAPIError(
                    response.status_code,
                    f"Retryable error after {self._max_retries + 1} attempts",
                )

            # Non-retryable 4xx or unexpected status — surface immediately
            raise PolymarketAPIError(response.status_code, response.text[:200])

        # Unreachable: loop always returns or raises, but satisfies type checker.
        raise PolymarketAPIError(0, "Unexpected exit from retry loop")  # pragma: no cover

    def close(self) -> None:
        """Close the underlying session and release pooled connections."""
        self._session.close()

    def __enter__(self) -> PolymarketHTTPClient:
        return self

    def __exit__(self, *args: object) -> None:
        self.close()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _backoff_delay(self, attempt: int) -> float:
        """
        Compute exponential backoff with uniform jitter.

        Formula: min(base_delay * 2^attempt + uniform(0.0, 1.0), max_delay)
        """
        delay = self._base_delay * (2**attempt) + random.uniform(0.0, 1.0)
        return min(delay, self._max_delay)

    @staticmethod
    def _retry_after(response: Response) -> float | None:
        """
        Parse the Retry-After header and return delay in seconds.

        Returns None if the header is absent or unparseable.
        """
        header = response.headers.get("Retry-After")
        if header is not None:
            try:
                return float(header)
            except ValueError:
                pass
        return None
