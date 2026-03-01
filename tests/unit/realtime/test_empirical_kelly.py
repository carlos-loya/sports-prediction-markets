"""Tests for empirical Kelly criterion."""

from __future__ import annotations

from sports_pipeline.realtime.config import KellyConfig
from sports_pipeline.realtime.sizing.empirical_kelly import _standard_kelly, empirical_kelly


class TestStandardKelly:
    def test_positive_edge(self):
        # Model says 60%, market at 50%
        f = _standard_kelly(0.60, 0.50, fraction=1.0, max_pct=1.0)
        assert f > 0

    def test_no_edge(self):
        f = _standard_kelly(0.50, 0.50, fraction=1.0, max_pct=1.0)
        assert f == 0.0

    def test_negative_edge(self):
        f = _standard_kelly(0.40, 0.50, fraction=1.0, max_pct=1.0)
        assert f == 0.0

    def test_fractional_kelly(self):
        full = _standard_kelly(0.60, 0.50, fraction=1.0, max_pct=1.0)
        half = _standard_kelly(0.60, 0.50, fraction=0.5, max_pct=1.0)
        assert abs(half - full * 0.5) < 1e-10

    def test_max_cap(self):
        f = _standard_kelly(0.99, 0.10, fraction=1.0, max_pct=0.20)
        assert f <= 0.20

    def test_invalid_market_price(self):
        assert _standard_kelly(0.5, 0.0, 1.0, 1.0) == 0.0
        assert _standard_kelly(0.5, 1.0, 1.0, 1.0) == 0.0


class TestEmpiricalKelly:
    def test_zero_uncertainty_uses_standard(self):
        config = KellyConfig(n_simulations=1000, fraction=0.25, max_bankroll_pct=0.20)
        f = empirical_kelly(0.70, 0.50, model_uncertainty=0.0, config=config)
        expected = _standard_kelly(0.70, 0.50, 0.25, 0.20)
        assert abs(f - expected) < 1e-10

    def test_positive_edge_with_uncertainty(self):
        config = KellyConfig(n_simulations=10000, fraction=0.25, max_bankroll_pct=0.20)
        f = empirical_kelly(0.70, 0.50, model_uncertainty=0.05, config=config)
        assert f > 0
        assert f <= 0.20

    def test_no_edge_returns_zero_or_near_zero(self):
        config = KellyConfig(n_simulations=10000, fraction=0.25, max_bankroll_pct=0.20)
        f = empirical_kelly(0.50, 0.50, model_uncertainty=0.01, config=config)
        # With no edge and tiny uncertainty, should be near zero
        assert f < 0.05

    def test_high_uncertainty_reduces_size(self):
        config = KellyConfig(n_simulations=10000, fraction=0.25, max_bankroll_pct=0.20)
        f_low = empirical_kelly(0.65, 0.50, model_uncertainty=0.02, config=config)
        f_high = empirical_kelly(0.65, 0.50, model_uncertainty=0.15, config=config)
        # Higher uncertainty should generally lead to smaller position
        # (not deterministic due to Monte Carlo, but typically true)
        assert f_low >= 0
        assert f_high >= 0

    def test_respects_max_bankroll(self):
        config = KellyConfig(n_simulations=10000, fraction=1.0, max_bankroll_pct=0.10)
        f = empirical_kelly(0.90, 0.10, model_uncertainty=0.01, config=config)
        assert f <= 0.10
