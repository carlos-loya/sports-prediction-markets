"""Live Bayesian probability updater.

Updates model probabilities in real-time as new evidence arrives
(trades, score changes, etc.) using Bayesian inference.
"""

from __future__ import annotations

import math
from dataclasses import dataclass


@dataclass
class BayesianUpdater:
    """Updates probabilities using Bayesian inference with Beta prior.

    Uses a Beta(alpha, beta) distribution as the conjugate prior for
    Bernoulli observations. The posterior after observing k successes
    out of n trials is Beta(alpha + k, beta + n - k).
    """

    alpha: float = 1.0  # prior successes (starts uniform)
    beta: float = 1.0  # prior failures

    @property
    def mean(self) -> float:
        """Posterior mean probability."""
        return self.alpha / (self.alpha + self.beta)

    @property
    def variance(self) -> float:
        """Posterior variance."""
        a, b = self.alpha, self.beta
        return (a * b) / ((a + b) ** 2 * (a + b + 1))

    @property
    def std(self) -> float:
        """Posterior standard deviation."""
        return math.sqrt(self.variance)

    def update(self, observation: bool, weight: float = 1.0) -> float:
        """Update the posterior with a new observation.

        Args:
            observation: True for YES outcome evidence, False for NO.
            weight: How much to weight this observation (default 1.0).

        Returns:
            Updated posterior mean.
        """
        if observation:
            self.alpha += weight
        else:
            self.beta += weight
        return self.mean

    def update_with_price(
        self,
        market_price: float,
        confidence: float = 0.5,
    ) -> float:
        """Update using market price as a noisy signal.

        Treats market price as evidence with given confidence weight.

        Args:
            market_price: Current YES price as implied probability.
            confidence: How much to trust the market signal (0-1).

        Returns:
            Updated posterior mean.
        """
        weight = confidence
        self.alpha += market_price * weight
        self.beta += (1.0 - market_price) * weight
        return self.mean

    def reset(self, prior_alpha: float = 1.0, prior_beta: float = 1.0) -> None:
        """Reset to a new prior."""
        self.alpha = prior_alpha
        self.beta = prior_beta


class BayesianUpdaterManager:
    """Manages Bayesian updaters for multiple markets."""

    def __init__(self) -> None:
        self._updaters: dict[str, BayesianUpdater] = {}

    def get(self, ticker: str) -> BayesianUpdater:
        if ticker not in self._updaters:
            self._updaters[ticker] = BayesianUpdater()
        return self._updaters[ticker]

    def update(
        self, ticker: str, observation: bool, weight: float = 1.0
    ) -> float:
        return self.get(ticker).update(observation, weight)

    def remove(self, ticker: str) -> None:
        self._updaters.pop(ticker, None)
