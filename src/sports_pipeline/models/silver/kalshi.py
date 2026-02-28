"""Silver-layer Pydantic models for cleaned Kalshi market data."""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel


class SilverKalshiMarket(BaseModel):
    """Cleaned Kalshi market with derived fields."""
    snapshot_timestamp: datetime
    ticker: str
    event_ticker: str
    title: str
    sport: str | None = None
    market_type: str | None = None  # game_outcome, total, player_prop, future
    matched_entity_id: str | None = None  # Matched team/player/game ID
    matched_entity_name: str | None = None
    status: str
    yes_price: float
    no_price: float
    implied_probability: float  # = yes_price (for YES side)
    mid_price: float | None = None  # Midpoint of bid/ask
    spread: float | None = None  # Ask - Bid
    volume: int = 0
    open_interest: int = 0
    close_time: datetime | None = None
