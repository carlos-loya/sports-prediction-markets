"""Kelly criterion for position sizing."""

from __future__ import annotations

from sports_pipeline.config import get_settings
from sports_pipeline.utils.logging import get_logger

log = get_logger(__name__)


class KellyCriterion:
    """Calculate optimal bet size using Kelly criterion."""

    def __init__(self, fraction: float | None = None) -> None:
        """Initialize with a fractional Kelly multiplier.

        Args:
            fraction: Fraction of full Kelly to use (0.25 = quarter Kelly).
                     If None, uses config value.
        """
        settings = get_settings()
        self.fraction = fraction or settings.edge_detection.kelly_fraction

    def calculate(self, model_prob: float, market_price: float) -> float:
        """Calculate Kelly fraction for a bet.

        Kelly formula: f* = (p * b - q) / b
        Where:
            p = model probability of winning
            b = decimal odds - 1 (net payout per unit bet)
            q = 1 - p

        Args:
            model_prob: Model's estimated probability
            market_price: Kalshi YES price (cost of contract)

        Returns:
            Recommended bet fraction (0 to 1), with fractional Kelly applied.
        """
        if market_price <= 0 or market_price >= 1:
            return 0.0

        # Decimal odds = payout / cost = 1 / market_price
        decimal_odds = 1.0 / market_price
        b = decimal_odds - 1.0  # Net payout

        if b <= 0:
            return 0.0

        p = model_prob
        q = 1.0 - p

        kelly = (p * b - q) / b

        # Only bet when Kelly is positive (edge exists)
        if kelly <= 0:
            return 0.0

        # Apply fractional Kelly and cap at reasonable maximum
        fractional_kelly = kelly * self.fraction
        capped = min(fractional_kelly, 0.20)  # Never bet more than 20% of bankroll

        return round(capped, 4)

    def calculate_no_side(self, model_prob: float, market_price: float) -> float:
        """Calculate Kelly for betting NO on a market.

        Args:
            model_prob: Model's probability the event does NOT happen
            market_price: Kalshi NO price (= 1 - yes_price)

        Returns:
            Recommended bet fraction.
        """
        return self.calculate(model_prob=model_prob, market_price=market_price)
