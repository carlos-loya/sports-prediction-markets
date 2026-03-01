"""Tests for real-time event serialization and deserialization."""

from __future__ import annotations

from sports_pipeline.realtime.events import (
    BookLevel,
    BookSnapshotEvent,
    EdgeEvent,
    EventType,
    FillEvent,
    LifecycleEvent,
    OrderAction,
    OrderRequestEvent,
    OrderSide,
    RiskAlertEvent,
    RiskLevel,
    SystemEvent,
    TickEvent,
    TradeEvent,
    deserialize_event,
)


class TestTickEvent:
    def test_serialize_roundtrip(self):
        event = TickEvent(
            ticker="KXNBA-YES",
            yes_price=0.65,
            no_price=0.35,
            yes_bid=0.64,
            yes_ask=0.66,
            volume=1000,
            open_interest=500,
        )
        data = event.to_json()
        restored = deserialize_event(data)
        assert isinstance(restored, TickEvent)
        assert restored.ticker == "KXNBA-YES"
        assert restored.yes_price == 0.65
        assert restored.no_price == 0.35
        assert restored.event_type == EventType.TICK

    def test_defaults(self):
        event = TickEvent(ticker="T1", yes_price=0.5, no_price=0.5)
        assert event.yes_bid == 0.0
        assert event.yes_ask == 0.0
        assert event.volume == 0


class TestTradeEvent:
    def test_serialize_roundtrip(self):
        event = TradeEvent(
            ticker="KXNBA-YES",
            price=0.65,
            count=10,
            taker_side="yes",
            trade_id="t123",
        )
        data = event.to_json()
        restored = deserialize_event(data)
        assert isinstance(restored, TradeEvent)
        assert restored.price == 0.65
        assert restored.count == 10
        assert restored.taker_side == "yes"


class TestBookSnapshotEvent:
    def test_serialize_with_levels(self):
        event = BookSnapshotEvent(
            ticker="T1",
            yes_bids=[BookLevel(price=0.64, quantity=100)],
            yes_asks=[BookLevel(price=0.66, quantity=50)],
            seq=42,
        )
        data = event.to_json()
        restored = deserialize_event(data)
        assert isinstance(restored, BookSnapshotEvent)
        assert len(restored.yes_bids) == 1
        assert restored.yes_bids[0].price == 0.64
        assert restored.seq == 42


class TestFillEvent:
    def test_serialize_roundtrip(self):
        event = FillEvent(
            order_id="ord-123",
            ticker="T1",
            side="yes",
            action="buy",
            price=0.65,
            count=5,
            remaining_count=0,
        )
        data = event.to_json()
        restored = deserialize_event(data)
        assert isinstance(restored, FillEvent)
        assert restored.order_id == "ord-123"
        assert restored.remaining_count == 0


class TestLifecycleEvent:
    def test_serialize_roundtrip(self):
        event = LifecycleEvent(
            ticker="T1",
            status="settled",
            result="yes",
        )
        data = event.to_json()
        restored = deserialize_event(data)
        assert isinstance(restored, LifecycleEvent)
        assert restored.status == "settled"
        assert restored.result == "yes"


class TestEdgeEvent:
    def test_serialize_roundtrip(self):
        event = EdgeEvent(
            ticker="T1",
            model_prob=0.72,
            market_prob=0.65,
            raw_edge=0.07,
            tradable_edge=0.04,
            kelly_fraction=0.15,
            suggested_side="yes",
            confidence="high",
            model_name="ensemble",
        )
        data = event.to_json()
        restored = deserialize_event(data)
        assert isinstance(restored, EdgeEvent)
        assert restored.raw_edge == 0.07
        assert restored.rejected is False

    def test_rejected_edge(self):
        event = EdgeEvent(
            ticker="T1",
            model_prob=0.52,
            market_prob=0.50,
            raw_edge=0.02,
            tradable_edge=-0.01,
            kelly_fraction=0.0,
            suggested_side="yes",
            confidence="low",
            rejected=True,
            reject_reason="below_min_edge",
        )
        assert event.rejected is True
        assert event.reject_reason == "below_min_edge"


class TestOrderRequestEvent:
    def test_serialize_roundtrip(self):
        event = OrderRequestEvent(
            ticker="T1",
            side=OrderSide.YES,
            action=OrderAction.BUY,
            price=65.0,
            count=10,
            source="edge_processor",
        )
        data = event.to_json()
        restored = deserialize_event(data)
        assert isinstance(restored, OrderRequestEvent)
        assert restored.side == OrderSide.YES
        assert restored.action == OrderAction.BUY


class TestRiskAlertEvent:
    def test_serialize_roundtrip(self):
        event = RiskAlertEvent(
            level=RiskLevel.CRITICAL,
            reason="VPIN > 0.6",
            ticker="T1",
            action="cancel_ticker",
        )
        data = event.to_json()
        restored = deserialize_event(data)
        assert isinstance(restored, RiskAlertEvent)
        assert restored.level == RiskLevel.CRITICAL


class TestSystemEvent:
    def test_serialize_roundtrip(self):
        event = SystemEvent(action="startup", detail="Phase 1")
        data = event.to_json()
        restored = deserialize_event(data)
        assert isinstance(restored, SystemEvent)
        assert restored.action == "startup"


class TestDeserializeEvent:
    def test_unknown_event_type_raises(self):
        import pytest

        with pytest.raises(ValueError):
            deserialize_event(b'{"event_type": "unknown"}')

    def test_all_event_types_registered(self):
        from sports_pipeline.realtime.events import EVENT_REGISTRY

        assert len(EVENT_REGISTRY) == len(EventType)
