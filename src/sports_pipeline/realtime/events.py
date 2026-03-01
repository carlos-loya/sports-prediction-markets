"""Event dataclasses for real-time Kafka message passing.

All events are serializable to/from JSON for Kafka transport.
"""

from __future__ import annotations

import json
from datetime import datetime
from enum import StrEnum

from pydantic import BaseModel, Field


class EventType(StrEnum):
    """Types of events flowing through the real-time system."""

    TICK = "tick"
    TRADE = "trade"
    BOOK_SNAPSHOT = "book_snapshot"
    FILL = "fill"
    LIFECYCLE = "lifecycle"
    EDGE = "edge"
    ORDER_REQUEST = "order_request"
    RISK_ALERT = "risk_alert"
    SYSTEM = "system"


class BaseEvent(BaseModel):
    """Base event with common fields."""

    event_type: EventType
    timestamp: datetime = Field(default_factory=datetime.utcnow)

    def to_json(self) -> bytes:
        return self.model_dump_json().encode("utf-8")

    @classmethod
    def from_json(cls, data: bytes) -> BaseEvent:
        return cls.model_validate_json(data)


class TickEvent(BaseEvent):
    """Price update for a market."""

    event_type: EventType = EventType.TICK
    ticker: str
    yes_price: float
    no_price: float
    yes_bid: float = 0.0
    yes_ask: float = 0.0
    volume: int = 0
    open_interest: int = 0


class TradeEvent(BaseEvent):
    """A trade execution on a market."""

    event_type: EventType = EventType.TRADE
    ticker: str
    price: float
    count: int  # number of contracts
    taker_side: str  # "yes" or "no"
    trade_id: str = ""


class BookLevel(BaseModel):
    """A single price level in the order book."""

    price: float
    quantity: int


class BookSnapshotEvent(BaseEvent):
    """Full order book state for a market."""

    event_type: EventType = EventType.BOOK_SNAPSHOT
    ticker: str
    yes_bids: list[BookLevel] = []
    yes_asks: list[BookLevel] = []
    seq: int = 0  # sequence number for gap detection


class FillEvent(BaseEvent):
    """A fill on one of our orders."""

    event_type: EventType = EventType.FILL
    order_id: str
    ticker: str
    side: str  # "yes" or "no"
    action: str  # "buy" or "sell"
    price: float
    count: int
    remaining_count: int = 0


class LifecycleEvent(BaseEvent):
    """Market lifecycle change (open, close, settle)."""

    event_type: EventType = EventType.LIFECYCLE
    ticker: str
    status: str  # "open", "closed", "settled"
    result: str = ""  # "yes", "no", "" if not settled


class EdgeEvent(BaseEvent):
    """A detected edge between model and market price."""

    event_type: EventType = EventType.EDGE
    ticker: str
    market_title: str = ""
    model_prob: float
    market_prob: float
    raw_edge: float
    tradable_edge: float
    kelly_fraction: float
    suggested_side: str  # "yes" or "no"
    confidence: str  # "high", "medium", "low"
    model_name: str = ""
    rejected: bool = False
    reject_reason: str = ""


class OrderSide(StrEnum):
    YES = "yes"
    NO = "no"


class OrderAction(StrEnum):
    BUY = "buy"
    SELL = "sell"


class OrderRequestEvent(BaseEvent):
    """Request to place an order."""

    event_type: EventType = EventType.ORDER_REQUEST
    ticker: str
    side: OrderSide
    action: OrderAction
    price: float  # limit price in cents (1-99)
    count: int  # number of contracts
    expiration_ts: datetime | None = None  # GTD expiry
    source: str = ""  # "edge_processor", "market_maker"


class RiskLevel(StrEnum):
    NORMAL = "normal"
    ELEVATED = "elevated"
    CRITICAL = "critical"
    EMERGENCY = "emergency"


class RiskAlertEvent(BaseEvent):
    """Risk management alert."""

    event_type: EventType = EventType.RISK_ALERT
    level: RiskLevel
    reason: str
    ticker: str = ""  # empty = global
    action: str = ""  # "cancel_ticker", "cancel_all", "shutdown"


class SystemEvent(BaseEvent):
    """System-level events (health, shutdown)."""

    event_type: EventType = EventType.SYSTEM
    action: str  # "startup", "shutdown", "health"
    detail: str = ""


# Registry for deserialization
EVENT_REGISTRY: dict[EventType, type[BaseEvent]] = {
    EventType.TICK: TickEvent,
    EventType.TRADE: TradeEvent,
    EventType.BOOK_SNAPSHOT: BookSnapshotEvent,
    EventType.FILL: FillEvent,
    EventType.LIFECYCLE: LifecycleEvent,
    EventType.EDGE: EdgeEvent,
    EventType.ORDER_REQUEST: OrderRequestEvent,
    EventType.RISK_ALERT: RiskAlertEvent,
    EventType.SYSTEM: SystemEvent,
}


def deserialize_event(data: bytes) -> BaseEvent:
    """Deserialize a JSON event into the correct typed event class."""
    parsed = json.loads(data)
    event_type = EventType(parsed["event_type"])
    cls = EVENT_REGISTRY[event_type]
    return cls.model_validate(parsed)
