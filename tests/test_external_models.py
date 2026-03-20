"""
Tests for src/external/models.py

Coverage:
  - BinanceSpotTick: valid construction, price validation, optional fields,
    timestamp awareness, source literal
  - ReferencePriceTick: valid construction, is_proxy semantics,
    proxy_description, price validation, optional fields
  - Three-clock provenance: timestamps are timezone-aware and ordered
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from src.external.models import BinanceSpotTick, ReferencePriceTick

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

TS_EXCHANGE = datetime(2026, 3, 19, 3, 0, 0, tzinfo=timezone.utc)
TS_RECEIVE = datetime(2026, 3, 19, 3, 0, 0, 200_000, tzinfo=timezone.utc)
TS_PROCESSED = datetime(2026, 3, 19, 3, 0, 0, 400_000, tzinfo=timezone.utc)


def _binance_tick(**overrides: object) -> BinanceSpotTick:
    defaults: dict = {
        "symbol": "BTCUSDT",
        "price": 84273.49,
        "exchange_timestamp": TS_EXCHANGE,
        "local_receive_timestamp": TS_RECEIVE,
        "processed_timestamp": TS_PROCESSED,
    }
    defaults.update(overrides)
    return BinanceSpotTick(**defaults)


def _reference_tick(**overrides: object) -> ReferencePriceTick:
    defaults: dict = {
        "pair": "BTC/USD",
        "price": 84273.49,
        "source_timestamp": TS_EXCHANGE,
        "local_receive_timestamp": TS_RECEIVE,
        "processed_timestamp": TS_PROCESSED,
        "source": "binance_spot_proxy",
        "is_proxy": True,
        "proxy_description": "Binance BTCUSDT proxy for Chainlink BTC/USD",
    }
    defaults.update(overrides)
    return ReferencePriceTick(**defaults)


# ---------------------------------------------------------------------------
# BinanceSpotTick
# ---------------------------------------------------------------------------


class TestBinanceSpotTick:
    def test_valid_construction(self) -> None:
        tick = _binance_tick()
        assert tick.symbol == "BTCUSDT"
        assert tick.price == 84273.49
        assert tick.source == "binance_spot"
        assert tick.raw_payload is None

    def test_price_must_be_positive(self) -> None:
        with pytest.raises(ValidationError):
            _binance_tick(price=0.0)

    def test_negative_price_rejected(self) -> None:
        with pytest.raises(ValidationError):
            _binance_tick(price=-1.0)

    def test_source_is_literal_binance_spot(self) -> None:
        tick = _binance_tick()
        assert tick.source == "binance_spot"

    def test_source_rejects_wrong_value(self) -> None:
        with pytest.raises(ValidationError):
            _binance_tick(source="wrong_source")

    def test_raw_payload_default_none(self) -> None:
        tick = _binance_tick()
        assert tick.raw_payload is None

    def test_raw_payload_set(self) -> None:
        payload = {"id": 123, "price": "84273.49"}
        tick = _binance_tick(raw_payload=payload)
        assert tick.raw_payload == payload

    def test_timestamps_are_timezone_aware(self) -> None:
        tick = _binance_tick()
        assert tick.exchange_timestamp.tzinfo is not None
        assert tick.local_receive_timestamp.tzinfo is not None
        assert tick.processed_timestamp.tzinfo is not None

    def test_three_clock_ordering_in_test_data(self) -> None:
        """In our test data, exchange <= receive <= processed (not enforced across hosts)."""
        tick = _binance_tick()
        assert tick.exchange_timestamp <= tick.local_receive_timestamp
        assert tick.local_receive_timestamp <= tick.processed_timestamp

    def test_missing_symbol_raises(self) -> None:
        with pytest.raises(ValidationError):
            BinanceSpotTick(
                price=100.0,
                exchange_timestamp=TS_EXCHANGE,
                local_receive_timestamp=TS_RECEIVE,
                processed_timestamp=TS_PROCESSED,
            )

    def test_missing_price_raises(self) -> None:
        with pytest.raises(ValidationError):
            BinanceSpotTick(
                symbol="BTCUSDT",
                exchange_timestamp=TS_EXCHANGE,
                local_receive_timestamp=TS_RECEIVE,
                processed_timestamp=TS_PROCESSED,
            )

    def test_model_dump_includes_all_fields(self) -> None:
        tick = _binance_tick()
        data = tick.model_dump(mode="json")
        assert "symbol" in data
        assert "price" in data
        assert "exchange_timestamp" in data
        assert "local_receive_timestamp" in data
        assert "processed_timestamp" in data
        assert "source" in data

    # -----------------------------------------------------------------------
    # Same-host timestamp ordering enforcement
    # -----------------------------------------------------------------------

    def test_receive_after_processed_rejected(self) -> None:
        """local_receive_timestamp > processed_timestamp must fail (same host)."""
        late_receive = datetime(2026, 3, 19, 4, 0, 0, tzinfo=timezone.utc)
        early_processed = datetime(2026, 3, 19, 3, 0, 0, tzinfo=timezone.utc)
        with pytest.raises(ValidationError, match="local_receive_timestamp"):
            _binance_tick(
                local_receive_timestamp=late_receive,
                processed_timestamp=early_processed,
            )

    def test_exchange_ahead_of_local_accepted(self) -> None:
        """exchange_timestamp slightly ahead of local_receive is valid (clock skew)."""
        ahead_exchange = datetime(2026, 3, 19, 3, 0, 1, tzinfo=timezone.utc)
        behind_receive = datetime(2026, 3, 19, 3, 0, 0, tzinfo=timezone.utc)
        tick = _binance_tick(
            exchange_timestamp=ahead_exchange,
            local_receive_timestamp=behind_receive,
            processed_timestamp=datetime(2026, 3, 19, 3, 0, 2, tzinfo=timezone.utc),
        )
        assert tick.exchange_timestamp > tick.local_receive_timestamp

    # -----------------------------------------------------------------------
    # Timezone awareness enforcement
    # -----------------------------------------------------------------------

    def test_naive_exchange_timestamp_rejected(self) -> None:
        """Naive (no tzinfo) exchange_timestamp must fail."""
        with pytest.raises(ValidationError):
            _binance_tick(exchange_timestamp=datetime(2026, 3, 19, 3, 0, 0))

    def test_naive_local_receive_timestamp_rejected(self) -> None:
        """Naive local_receive_timestamp must fail."""
        with pytest.raises(ValidationError):
            _binance_tick(local_receive_timestamp=datetime(2026, 3, 19, 3, 0, 0))

    def test_naive_processed_timestamp_rejected(self) -> None:
        """Naive processed_timestamp must fail."""
        with pytest.raises(ValidationError):
            _binance_tick(processed_timestamp=datetime(2026, 3, 19, 3, 0, 0))


# ---------------------------------------------------------------------------
# ReferencePriceTick
# ---------------------------------------------------------------------------


class TestReferencePriceTick:
    def test_valid_proxy_construction(self) -> None:
        tick = _reference_tick()
        assert tick.pair == "BTC/USD"
        assert tick.price == 84273.49
        assert tick.source == "binance_spot_proxy"
        assert tick.is_proxy is True
        assert tick.proxy_description is not None

    def test_valid_direct_construction(self) -> None:
        """is_proxy=False represents a true oracle source."""
        tick = _reference_tick(
            source="chainlink",
            is_proxy=False,
            proxy_description=None,
        )
        assert tick.is_proxy is False
        assert tick.proxy_description is None
        assert tick.source == "chainlink"

    def test_price_must_be_positive(self) -> None:
        with pytest.raises(ValidationError):
            _reference_tick(price=0.0)

    def test_negative_price_rejected(self) -> None:
        with pytest.raises(ValidationError):
            _reference_tick(price=-1.0)

    def test_is_proxy_required(self) -> None:
        with pytest.raises(ValidationError):
            ReferencePriceTick(
                pair="BTC/USD",
                price=84000.0,
                source_timestamp=TS_EXCHANGE,
                local_receive_timestamp=TS_RECEIVE,
                processed_timestamp=TS_PROCESSED,
                source="test",
                # is_proxy omitted
            )

    def test_raw_payload_default_none(self) -> None:
        tick = _reference_tick()
        assert tick.raw_payload is None

    def test_raw_payload_set(self) -> None:
        payload = {"id": 123, "price": "84273.49"}
        tick = _reference_tick(raw_payload=payload)
        assert tick.raw_payload == payload

    def test_proxy_description_none_for_direct_source(self) -> None:
        """is_proxy=False allows proxy_description=None (the only valid option)."""
        tick = _reference_tick(
            source="chainlink",
            is_proxy=False,
            proxy_description=None,
        )
        assert tick.proxy_description is None

    def test_timestamps_are_timezone_aware(self) -> None:
        tick = _reference_tick()
        assert tick.source_timestamp.tzinfo is not None
        assert tick.local_receive_timestamp.tzinfo is not None
        assert tick.processed_timestamp.tzinfo is not None

    def test_three_clock_ordering(self) -> None:
        """source <= receive <= processed."""
        tick = _reference_tick()
        assert tick.source_timestamp <= tick.local_receive_timestamp
        assert tick.local_receive_timestamp <= tick.processed_timestamp

    def test_missing_pair_raises(self) -> None:
        with pytest.raises(ValidationError):
            ReferencePriceTick(
                price=84000.0,
                source_timestamp=TS_EXCHANGE,
                local_receive_timestamp=TS_RECEIVE,
                processed_timestamp=TS_PROCESSED,
                source="test",
                is_proxy=True,
            )

    def test_missing_source_raises(self) -> None:
        with pytest.raises(ValidationError):
            ReferencePriceTick(
                pair="BTC/USD",
                price=84000.0,
                source_timestamp=TS_EXCHANGE,
                local_receive_timestamp=TS_RECEIVE,
                processed_timestamp=TS_PROCESSED,
                is_proxy=True,
            )

    def test_model_dump_includes_all_fields(self) -> None:
        tick = _reference_tick()
        data = tick.model_dump(mode="json")
        assert "pair" in data
        assert "price" in data
        assert "source_timestamp" in data
        assert "local_receive_timestamp" in data
        assert "processed_timestamp" in data
        assert "source" in data
        assert "is_proxy" in data
        assert "proxy_description" in data

    # -----------------------------------------------------------------------
    # Proxy semantics enforcement
    # -----------------------------------------------------------------------

    def test_proxy_true_without_description_rejected(self) -> None:
        """is_proxy=True with proxy_description=None must fail."""
        with pytest.raises(ValidationError, match="proxy_description"):
            _reference_tick(is_proxy=True, proxy_description=None)

    def test_proxy_true_with_empty_description_rejected(self) -> None:
        """is_proxy=True with proxy_description='' must fail."""
        with pytest.raises(ValidationError, match="proxy_description"):
            _reference_tick(is_proxy=True, proxy_description="")

    def test_proxy_false_with_description_rejected(self) -> None:
        """is_proxy=False with a proxy_description set must fail."""
        with pytest.raises(ValidationError, match="proxy_description"):
            _reference_tick(
                source="chainlink",
                is_proxy=False,
                proxy_description="should not be here",
            )

    # -----------------------------------------------------------------------
    # Timestamp ordering enforcement
    # -----------------------------------------------------------------------

    def test_source_ahead_of_local_accepted(self) -> None:
        """source_timestamp slightly ahead of local_receive is valid (clock skew)."""
        ahead_source = datetime(2026, 3, 19, 3, 0, 1, tzinfo=timezone.utc)
        behind_receive = datetime(2026, 3, 19, 3, 0, 0, tzinfo=timezone.utc)
        tick = _reference_tick(
            source_timestamp=ahead_source,
            local_receive_timestamp=behind_receive,
            processed_timestamp=datetime(2026, 3, 19, 3, 0, 2, tzinfo=timezone.utc),
        )
        assert tick.source_timestamp > tick.local_receive_timestamp

    def test_receive_after_processed_rejected(self) -> None:
        """local_receive_timestamp > processed_timestamp must fail (same host)."""
        late_receive = datetime(2026, 3, 19, 4, 0, 0, tzinfo=timezone.utc)
        early_processed = datetime(2026, 3, 19, 3, 0, 0, tzinfo=timezone.utc)
        with pytest.raises(ValidationError, match="local_receive_timestamp"):
            _reference_tick(
                source_timestamp=TS_EXCHANGE,
                local_receive_timestamp=late_receive,
                processed_timestamp=early_processed,
            )

    # -----------------------------------------------------------------------
    # Source / is_proxy coupling
    # -----------------------------------------------------------------------

    def test_proxy_true_with_non_proxy_source_rejected(self) -> None:
        """source='chainlink' + is_proxy=True is nonsense."""
        with pytest.raises(ValidationError, match="must contain 'proxy'"):
            _reference_tick(
                source="chainlink",
                is_proxy=True,
                proxy_description="some proxy",
            )

    def test_proxy_false_with_proxy_source_rejected(self) -> None:
        """source='binance_spot_proxy' + is_proxy=False is nonsense."""
        with pytest.raises(ValidationError, match="must not contain 'proxy'"):
            _reference_tick(
                source="binance_spot_proxy",
                is_proxy=False,
                proxy_description=None,
            )

    # -----------------------------------------------------------------------
    # Whitespace-only proxy_description
    # -----------------------------------------------------------------------

    def test_proxy_true_with_whitespace_description_rejected(self) -> None:
        """is_proxy=True with proxy_description='   ' must fail."""
        with pytest.raises(ValidationError, match="proxy_description"):
            _reference_tick(is_proxy=True, proxy_description="   ")

    # -----------------------------------------------------------------------
    # Timezone awareness enforcement
    # -----------------------------------------------------------------------

    def test_naive_source_timestamp_rejected(self) -> None:
        """Naive (no tzinfo) source_timestamp must fail."""
        with pytest.raises(ValidationError):
            _reference_tick(source_timestamp=datetime(2026, 3, 19, 3, 0, 0))

    def test_naive_local_receive_timestamp_rejected(self) -> None:
        """Naive local_receive_timestamp must fail."""
        with pytest.raises(ValidationError):
            _reference_tick(local_receive_timestamp=datetime(2026, 3, 19, 3, 0, 0))

    def test_naive_processed_timestamp_rejected(self) -> None:
        """Naive processed_timestamp must fail."""
        with pytest.raises(ValidationError):
            _reference_tick(processed_timestamp=datetime(2026, 3, 19, 3, 0, 0))
