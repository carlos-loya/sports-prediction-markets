"""Pydantic models for Kalshi WebSocket message types.

Each channel has its own message shape. We parse incoming JSON into
typed models, then convert to internal events for Kafka.
"""

from __future__ import annotations

from datetime import datetime  # noqa: TC003

from pydantic import BaseModel, Field

# --- Incoming message wrapper ---


class WSMessage(BaseModel):
    """Top-level WebSocket message from Kalshi."""

    type: str  # "ticker", "trade", "orderbook_delta", "fill", etc.
    sid: int = 0  # subscription id
    seq: int = 0  # sequence number
    msg: dict = Field(default_factory=dict)


# --- Channel-specific message payloads ---


class TickerMsg(BaseModel):
    """Ticker channel: price and volume updates."""

    market_ticker: str
    yes_price: float = 0
    no_price: float = 0
    yes_bid: float = 0
    yes_ask: float = 0
    volume: int = 0
    open_interest: int = 0
    ts: int = 0  # unix ms


class TradeMsg(BaseModel):
    """Trade channel: individual trades."""

    market_ticker: str
    yes_price: float = 0
    no_price: float = 0
    count: int = 0
    taker_side: str = ""  # "yes" or "no"
    trade_id: str = ""
    ts: int = 0


class OrderBookDeltaLevel(BaseModel):
    """Single level in an orderbook delta."""

    price: float
    delta: int  # positive = add, negative = remove


class OrderBookDeltaMsg(BaseModel):
    """Orderbook delta channel: incremental book updates."""

    market_ticker: str
    yes: list[OrderBookDeltaLevel] = []
    no: list[OrderBookDeltaLevel] = []
    ts: int = 0


class FillMsg(BaseModel):
    """Fill channel: our order fills."""

    order_id: str
    market_ticker: str
    side: str = ""  # "yes" or "no"
    action: str = ""  # "buy" or "sell"
    yes_price: float = 0
    no_price: float = 0
    count: int = 0
    remaining_count: int = 0
    ts: int = 0


class MarketLifecycleMsg(BaseModel):
    """Market lifecycle channel: status changes."""

    market_ticker: str
    status: str = ""  # "open", "closed", "settled"
    result: str = ""  # "yes", "no", ""
    ts: int = 0


def parse_channel_message(channel: str, data: dict) -> BaseModel:
    """Parse a channel message payload into the appropriate Pydantic model."""
    parsers: dict[str, type[BaseModel]] = {
        "ticker": TickerMsg,
        "trade": TradeMsg,
        "orderbook_delta": OrderBookDeltaMsg,
        "fill": FillMsg,
        "market_lifecycle_v2": MarketLifecycleMsg,
    }
    parser = parsers.get(channel)
    if parser is None:
        raise ValueError(f"Unknown channel: {channel}")
    return parser.model_validate(data)


def ts_to_datetime(ts_ms: int) -> datetime:
    """Convert a millisecond timestamp to a datetime."""
    if ts_ms == 0:
        return datetime.utcnow()
    return datetime.utcfromtimestamp(ts_ms / 1000)
