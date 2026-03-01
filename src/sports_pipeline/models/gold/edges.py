"""Gold-layer Pydantic models for edge detection."""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel


class EdgeSignal(BaseModel):
    """A detected edge between model probability and market price."""
    signal_id: str
    timestamp: datetime
    kalshi_ticker: str
    market_title: str
    sport: str
    market_type: str
    kalshi_implied_prob: float
    model_prob: float
    edge: float  # model_prob - kalshi_implied_prob
    edge_pct: float  # edge / kalshi_implied_prob
    confidence: str  # "high", "medium", "low"
    model_name: str
    kelly_fraction: float
    suggested_side: str  # "YES" or "NO"
    reasoning: str


class EdgeReport(BaseModel):
    """Summary report of detected edges."""
    timestamp: datetime
    total_markets_scanned: int
    total_edges_found: int
    high_confidence_edges: int
    signals: list[EdgeSignal] = []


class TradeLogEntry(BaseModel):
    """A single trade log entry for real-time edge evaluation tracking."""
    log_id: str
    timestamp: datetime
    ticker: str
    model_prob: float
    market_prob: float
    raw_edge: float
    tradable_edge: float
    kelly_fraction: float
    suggested_side: str
    confidence: str
    model_name: str
    rejected: bool
    reject_reason: str = ""
