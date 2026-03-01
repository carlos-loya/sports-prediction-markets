"""Avellaneda-Stoikov market maker with log-odds transformation.

Maps binary probabilities [0,1] → ℝ via logit for AS model, then
maps quotes back to [0,1] for Kalshi limit orders.

Reservation price: r = s - q·γ·σ²·(T-t)
Optimal spread:    δ = γ·σ²·(T-t) + (2/γ)·ln(1 + γ/κ)

Where:
  s = mid price in log-odds
  q = inventory (positive = long YES)
  γ = risk aversion parameter
  σ = volatility in log-odds space
  T-t = time to expiry (normalized to 1)
  κ = order arrival intensity
"""

from __future__ import annotations

import math
from collections import deque
from dataclasses import dataclass, field

from sports_pipeline.realtime.config import MarketMakerConfig
from sports_pipeline.utils.logging import get_logger

log = get_logger(__name__)


def logit(p: float) -> float:
    """Map probability [0,1] → ℝ. Clips to avoid infinities."""
    p = max(0.01, min(0.99, p))
    return math.log(p / (1 - p))


def inv_logit(x: float) -> float:
    """Map ℝ → probability [0,1] (sigmoid)."""
    return 1.0 / (1.0 + math.exp(-x))


@dataclass
class MMQuote:
    """A pair of bid/ask quotes from the market maker."""

    bid_price: float  # YES bid price (probability)
    ask_price: float  # YES ask price (probability)
    bid_size: int
    ask_size: int
    reservation_price: float  # mid in probability space
    spread: float  # in probability space


@dataclass
class MarketMakerState:
    """Per-market state for the market maker."""

    ticker: str
    inventory: int = 0  # positive = long YES
    mid_price: float = 0.5
    volatility: float = 0.0
    time_to_expiry: float = 1.0  # normalized [0, 1]
    _price_history: deque[float] = field(
        default_factory=lambda: deque(maxlen=200)
    )
    last_quote: MMQuote | None = None

    def update_price(self, yes_price: float) -> None:
        """Record a new price observation and update volatility estimate."""
        self.mid_price = yes_price
        logit_price = logit(yes_price)
        self._price_history.append(logit_price)

    def estimate_volatility(self, window: int = 100) -> float:
        """Estimate volatility as std dev of log-odds prices."""
        if len(self._price_history) < 2:
            return 0.1  # default
        prices = list(self._price_history)[-window:]
        if len(prices) < 2:
            return 0.1
        mean = sum(prices) / len(prices)
        variance = sum((p - mean) ** 2 for p in prices) / (len(prices) - 1)
        self.volatility = max(math.sqrt(variance), 0.01)
        return self.volatility


class AvellanedaStoikov:
    """Avellaneda-Stoikov market making engine."""

    def __init__(self, config: MarketMakerConfig) -> None:
        self._config = config
        self._states: dict[str, MarketMakerState] = {}

    @property
    def enabled(self) -> bool:
        return self._config.enabled

    def get_state(self, ticker: str) -> MarketMakerState:
        if ticker not in self._states:
            self._states[ticker] = MarketMakerState(ticker=ticker)
        return self._states[ticker]

    def on_tick(self, ticker: str, yes_price: float) -> None:
        """Update state on new price tick."""
        state = self.get_state(ticker)
        state.update_price(yes_price)

    def on_fill(self, ticker: str, side: str, count: int) -> None:
        """Update inventory on fill."""
        state = self.get_state(ticker)
        if side == "yes":
            state.inventory += count
        else:
            state.inventory -= count

    def compute_quotes(
        self,
        ticker: str,
        time_to_expiry: float = 1.0,
    ) -> MMQuote | None:
        """Compute optimal bid/ask quotes for a market.

        Returns None if market maker is disabled or insufficient data.
        """
        if not self.enabled:
            return None

        state = self.get_state(ticker)
        state.time_to_expiry = time_to_expiry

        sigma = state.estimate_volatility(self._config.sigma_window)
        gamma = self._config.gamma
        kappa = self._config.kappa

        # Work in log-odds space
        s = logit(state.mid_price)
        q = state.inventory
        tau = max(state.time_to_expiry, 0.001)

        # Reservation price (log-odds)
        r = s - q * gamma * (sigma**2) * tau

        # Optimal spread (log-odds)
        delta = gamma * (sigma**2) * tau + (2.0 / gamma) * math.log(
            1.0 + gamma / kappa
        )

        # Half spread
        half_delta = delta / 2.0

        # Convert back to probability space
        reservation_prob = inv_logit(r)
        bid_prob = inv_logit(r - half_delta)
        ask_prob = inv_logit(r + half_delta)

        # Enforce minimum spread
        min_spread = self._config.min_spread_cents / 100.0
        if ask_prob - bid_prob < min_spread:
            mid = (bid_prob + ask_prob) / 2
            bid_prob = mid - min_spread / 2
            ask_prob = mid + min_spread / 2

        # Clip to valid price range [0.01, 0.99]
        bid_prob = max(0.01, min(0.99, bid_prob))
        ask_prob = max(0.01, min(0.99, ask_prob))

        # Position-dependent sizing
        max_pos = self._config.max_position
        bid_size = max(1, max_pos - max(0, state.inventory))
        ask_size = max(1, max_pos + min(0, state.inventory))

        quote = MMQuote(
            bid_price=bid_prob,
            ask_price=ask_prob,
            bid_size=bid_size,
            ask_size=ask_size,
            reservation_price=reservation_prob,
            spread=ask_prob - bid_prob,
        )
        state.last_quote = quote
        return quote

    def remove(self, ticker: str) -> None:
        self._states.pop(ticker, None)
