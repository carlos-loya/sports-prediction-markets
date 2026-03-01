"""Spread widening monitor as an information signal.

Widening spreads can indicate informed trading activity or
reduced market maker confidence. Used as an additional signal
alongside VPIN for risk management.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field

from sports_pipeline.utils.logging import get_logger

log = get_logger(__name__)


@dataclass
class SpreadMonitor:
    """Monitors bid-ask spread for anomalous widening."""

    ticker: str
    window_size: int = 50
    _spreads: deque[float] = field(init=False)

    def __post_init__(self) -> None:
        self._spreads = deque(maxlen=self.window_size)

    @property
    def current_spread(self) -> float | None:
        if not self._spreads:
            return None
        return self._spreads[-1]

    @property
    def avg_spread(self) -> float | None:
        if not self._spreads:
            return None
        return sum(self._spreads) / len(self._spreads)

    @property
    def is_widening(self) -> bool:
        """True if current spread is >2x the rolling average."""
        avg = self.avg_spread
        current = self.current_spread
        if avg is None or current is None or avg == 0:
            return False
        return current > 2.0 * avg

    def on_book_update(self, best_bid: float, best_ask: float) -> None:
        """Record a new spread observation."""
        spread = best_ask - best_bid
        if spread >= 0:
            self._spreads.append(spread)

    def reset(self) -> None:
        self._spreads.clear()


class SpreadMonitorManager:
    """Manages spread monitors for multiple markets."""

    def __init__(self, window_size: int = 50) -> None:
        self._window_size = window_size
        self._monitors: dict[str, SpreadMonitor] = {}

    def get(self, ticker: str) -> SpreadMonitor:
        if ticker not in self._monitors:
            self._monitors[ticker] = SpreadMonitor(
                ticker=ticker, window_size=self._window_size
            )
        return self._monitors[ticker]

    def on_book_update(
        self, ticker: str, best_bid: float, best_ask: float
    ) -> bool:
        """Update and return whether spread is widening."""
        monitor = self.get(ticker)
        monitor.on_book_update(best_bid, best_ask)
        return monitor.is_widening

    def remove(self, ticker: str) -> None:
        self._monitors.pop(ticker, None)
