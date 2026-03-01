"""Empirical Kelly criterion via Monte Carlo simulation.

Instead of using the closed-form Kelly formula (which assumes known edge),
we sample from the uncertainty distribution of our model probability
and compute Kelly for each sample, then take a conservative estimate.

f_empirical = mean(positive_kellys) × (1 - CV_edge) × fraction
"""

from __future__ import annotations

import numpy as np

from sports_pipeline.realtime.config import KellyConfig


def empirical_kelly(
    model_prob: float,
    market_price: float,
    model_uncertainty: float,
    config: KellyConfig,
) -> float:
    """Compute position size using Monte Carlo Kelly.

    Args:
        model_prob: Our estimated probability of YES.
        market_price: Market implied probability (YES price).
        model_uncertainty: Std dev of model probability estimate
            (e.g., from ensemble disagreement).
        config: Kelly configuration.

    Returns:
        Fraction of bankroll to wager (0 to max_bankroll_pct).
    """
    if model_uncertainty <= 0:
        # No uncertainty: fall back to standard fractional Kelly
        return _standard_kelly(model_prob, market_price, config.fraction, config.max_bankroll_pct)

    rng = np.random.default_rng()

    # Sample model probabilities from uncertainty distribution
    samples = rng.normal(model_prob, model_uncertainty, config.n_simulations)
    # Clip to valid probability range
    samples = np.clip(samples, 0.01, 0.99)

    # Compute Kelly fraction for each sample
    edges = samples - market_price
    # Kelly = edge / odds, where odds = 1/market_price - 1 for binary
    odds = (1.0 / market_price) - 1.0
    kellys = edges / (odds * market_price) if odds > 0 else np.zeros_like(edges)

    # Only consider positive Kelly values
    positive = kellys[kellys > 0]
    if len(positive) == 0:
        return 0.0

    mean_kelly = float(np.mean(positive))

    # Coefficient of variation of the edge samples penalizes uncertainty
    edge_samples = samples - market_price
    positive_edges = edge_samples[edge_samples > 0]
    if len(positive_edges) < 2:
        cv_penalty = 0.5
    else:
        cv = float(np.std(positive_edges) / np.mean(positive_edges))
        cv_penalty = max(0.0, 1.0 - cv)

    f = mean_kelly * cv_penalty * config.fraction
    return min(f, config.max_bankroll_pct)


def _standard_kelly(
    model_prob: float,
    market_price: float,
    fraction: float,
    max_pct: float,
) -> float:
    """Standard fractional Kelly for known edge."""
    if market_price <= 0 or market_price >= 1:
        return 0.0
    edge = model_prob - market_price
    if edge <= 0:
        return 0.0
    odds = (1.0 / market_price) - 1.0
    kelly = edge / (odds * market_price) if odds > 0 else 0.0
    return min(kelly * fraction, max_pct)
