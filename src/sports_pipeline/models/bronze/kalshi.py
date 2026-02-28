"""Bronze-layer Pydantic models for Kalshi market data."""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel


class BronzeKalshiMarket(BaseModel):
    """Raw market data from Kalshi API."""
    snapshot_timestamp: datetime
    ticker: str
    event_ticker: str
    title: str
    category: str
    sub_category: str | None = None
    status: str
    yes_price: float
    no_price: float
    yes_bid: float | None = None
    yes_ask: float | None = None
    no_bid: float | None = None
    no_ask: float | None = None
    volume: int = 0
    open_interest: int = 0
    close_time: datetime | None = None
    result: str | None = None


class BronzeKalshiOrderBook(BaseModel):
    """Raw order book snapshot from Kalshi API."""
    snapshot_timestamp: datetime
    ticker: str
    yes_bids: list[OrderBookLevel] = []
    yes_asks: list[OrderBookLevel] = []
    no_bids: list[OrderBookLevel] = []
    no_asks: list[OrderBookLevel] = []


class OrderBookLevel(BaseModel):
    """Single level in an order book."""
    price: float
    quantity: int


# Fix forward reference
BronzeKalshiOrderBook.model_rebuild()


class BronzeKalshiCandlestick(BaseModel):
    """Raw candlestick data for a market."""
    ticker: str
    timestamp: datetime
    open_price: float
    high_price: float
    low_price: float
    close_price: float
    volume: int
