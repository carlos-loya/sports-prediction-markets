"""Information entropy filter for market selection.

Only process markets with sufficient uncertainty (high entropy).
Markets near 0 or 1 have little informational value for edge detection.

H(p) = -p·log₂(p) - (1-p)·log₂(1-p)

Maximum entropy occurs at p=0.50. We filter to markets with YES price
in [min_price, max_price] (default [0.30, 0.70]).
"""

from __future__ import annotations

import math

from sports_pipeline.realtime.config import EntropyConfig


def binary_entropy(p: float) -> float:
    """Calculate binary entropy H(p) in bits.

    Args:
        p: Probability in (0, 1). Returns 0.0 for p <= 0 or p >= 1.
    """
    if p <= 0.0 or p >= 1.0:
        return 0.0
    return -(p * math.log2(p) + (1 - p) * math.log2(1 - p))


def passes_entropy_filter(yes_price: float, config: EntropyConfig) -> bool:
    """Check if a market's YES price falls within the entropy filter range.

    Args:
        yes_price: Current YES price (implied probability).
        config: Entropy filter configuration.

    Returns:
        True if the market has sufficient uncertainty to be worth processing.
    """
    return config.min_price <= yes_price <= config.max_price


class EntropyFilter:
    """Stateful entropy filter that tracks which markets pass the filter."""

    def __init__(self, config: EntropyConfig) -> None:
        self._config = config
        self._active_tickers: set[str] = set()

    @property
    def active_tickers(self) -> set[str]:
        return self._active_tickers.copy()

    def evaluate(self, ticker: str, yes_price: float) -> bool:
        """Evaluate whether a market passes the entropy filter.

        Updates internal tracking of active tickers.

        Returns:
            True if the market should be processed.
        """
        passes = passes_entropy_filter(yes_price, self._config)
        if passes:
            self._active_tickers.add(ticker)
        else:
            self._active_tickers.discard(ticker)
        return passes

    def remove(self, ticker: str) -> None:
        self._active_tickers.discard(ticker)
