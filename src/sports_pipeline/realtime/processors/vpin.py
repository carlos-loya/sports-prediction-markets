"""VPIN (Volume-Synchronized Probability of Informed Trading) calculator.

Measures order flow toxicity from a trade stream using volume buckets.
High VPIN indicates informed trading and should trigger spread widening
or position reduction.

VPIN = Σ|buy_vol_i - sell_vol_i| / Σ(buy_vol_i + sell_vol_i)

Trade classification uses the tick rule: if price > previous price,
the trade is buyer-initiated; if price < previous, seller-initiated.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field

from sports_pipeline.realtime.config import VPINConfig
from sports_pipeline.utils.logging import get_logger

log = get_logger(__name__)


@dataclass
class VPINBucket:
    """A single volume bucket accumulating buy/sell volume."""

    buy_volume: int = 0
    sell_volume: int = 0

    @property
    def total_volume(self) -> int:
        return self.buy_volume + self.sell_volume

    @property
    def imbalance(self) -> int:
        return abs(self.buy_volume - self.sell_volume)


@dataclass
class VPINCalculator:
    """Calculates rolling VPIN from a trade stream for a single market.

    Args:
        config: VPIN configuration (bucket_size, n_buckets, thresholds).
    """

    config: VPINConfig
    _buckets: deque[VPINBucket] = field(init=False)
    _current_bucket: VPINBucket = field(init=False)
    _last_price: float = field(init=False, default=0.0)
    _trade_count: int = field(init=False, default=0)

    def __post_init__(self) -> None:
        self._buckets = deque(maxlen=self.config.n_buckets)
        self._current_bucket = VPINBucket()

    @property
    def vpin(self) -> float | None:
        """Current VPIN value, or None if not enough data."""
        if len(self._buckets) < self.config.n_buckets:
            return None
        total_imbalance = sum(b.imbalance for b in self._buckets)
        total_volume = sum(b.total_volume for b in self._buckets)
        if total_volume == 0:
            return None
        return total_imbalance / total_volume

    @property
    def is_elevated(self) -> bool:
        v = self.vpin
        return v is not None and v >= self.config.threshold_elevated

    @property
    def is_critical(self) -> bool:
        v = self.vpin
        return v is not None and v >= self.config.threshold_critical

    @property
    def bucket_count(self) -> int:
        return len(self._buckets)

    def on_trade(self, price: float, volume: int, taker_side: str = "") -> float | None:
        """Process a trade and return updated VPIN (or None if insufficient data).

        Args:
            price: Trade price.
            volume: Number of contracts.
            taker_side: Explicit taker side ("yes"/"no"). If empty, uses tick rule.

        Returns:
            Current VPIN value or None.
        """
        # Classify trade direction
        if taker_side in ("yes", "buy"):
            is_buy = True
        elif taker_side in ("no", "sell"):
            is_buy = False
        else:
            # Tick rule: compare to last trade price
            is_buy = price >= self._last_price

        self._last_price = price
        self._trade_count += 1

        # Add volume to current bucket
        remaining = volume
        while remaining > 0:
            space = self.config.bucket_size - self._current_bucket.total_volume
            fill = min(remaining, space)
            if is_buy:
                self._current_bucket.buy_volume += fill
            else:
                self._current_bucket.sell_volume += fill
            remaining -= fill

            if self._current_bucket.total_volume >= self.config.bucket_size:
                self._buckets.append(self._current_bucket)
                self._current_bucket = VPINBucket()

        return self.vpin

    def reset(self) -> None:
        """Reset all state."""
        self._buckets.clear()
        self._current_bucket = VPINBucket()
        self._last_price = 0.0
        self._trade_count = 0


class VPINManager:
    """Manages VPIN calculators for multiple markets."""

    def __init__(self, config: VPINConfig) -> None:
        self._config = config
        self._calculators: dict[str, VPINCalculator] = {}

    def get(self, ticker: str) -> VPINCalculator:
        if ticker not in self._calculators:
            self._calculators[ticker] = VPINCalculator(config=self._config)
        return self._calculators[ticker]

    def on_trade(
        self, ticker: str, price: float, volume: int, taker_side: str = ""
    ) -> float | None:
        return self.get(ticker).on_trade(price, volume, taker_side)

    def remove(self, ticker: str) -> None:
        self._calculators.pop(ticker, None)
