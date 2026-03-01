"""Fee and slippage model for Kalshi trading.

Adjusts raw edge to account for taker fees and expected slippage,
producing a tradable edge that determines whether an order is worth placing.

tradable_edge = raw_edge - taker_fee - slippage
"""

from __future__ import annotations

from sports_pipeline.realtime.config import FeeConfig


def compute_tradable_edge(
    raw_edge: float,
    config: FeeConfig,
) -> float:
    """Compute tradable edge after fees and slippage.

    Args:
        raw_edge: Model probability - market implied probability.
        config: Fee configuration.

    Returns:
        Tradable edge (can be negative if fees exceed the raw edge).
    """
    fee_pct = config.taker_fee_cents / 100.0
    slippage_pct = config.slippage_cents / 100.0
    return raw_edge - fee_pct - slippage_pct


def is_tradable(raw_edge: float, config: FeeConfig) -> bool:
    """Check if the edge is large enough to be tradable after fees."""
    return compute_tradable_edge(raw_edge, config) >= config.min_tradable_edge
