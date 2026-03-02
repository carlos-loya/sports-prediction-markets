"""Tests for calibration analysis."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from sports_pipeline.backtesting.calibration import (
    _compute_vpin,
    edge_calibration,
    generate_calibration_report,
    model_uncertainty,
    optimal_thresholds,
    vpin_effectiveness,
)


@pytest.fixture
def sample_results():
    """Sample replay results DataFrame."""
    np.random.seed(42)
    n = 100
    edges = np.random.uniform(0.02, 0.15, n)
    won = np.random.random(n) < (0.5 + edges)  # Higher edge → higher win prob
    pnl = np.where(won, edges * 100, -50)

    bankroll = 10000.0
    bankrolls = []
    for p in pnl:
        bankroll += p
        bankrolls.append(bankroll)

    return pd.DataFrame({
        "ticker": [f"KXNBA-{i}" for i in range(n)],
        "timestamp": pd.date_range("2024-01-01", periods=n, freq="h"),
        "model_prob": np.random.uniform(0.4, 0.7, n),
        "market_prob": np.random.uniform(0.35, 0.65, n),
        "raw_edge": edges,
        "tradable_edge": edges - 0.03,
        "kelly_fraction": np.random.uniform(0.01, 0.05, n),
        "bet_amount": np.random.uniform(50, 200, n),
        "pnl": pnl,
        "bankroll": bankrolls,
        "won": won,
        "result": np.where(won, "yes", "no"),
        "suggested_side": "yes",
        "confidence": "medium",
        "model_name": "elo_basketball",
        "sport": "basketball",
        "title": "test",
    })


class TestEdgeCalibration:
    def test_produces_bins(self, sample_results):
        cal = edge_calibration(sample_results)
        assert not cal.empty
        assert "edge_bin" in cal.columns
        assert "win_rate" in cal.columns
        assert "count" in cal.columns
        assert "avg_pnl" in cal.columns

    def test_custom_bins(self, sample_results):
        cal = edge_calibration(sample_results, n_bins=5)
        assert len(cal) <= 5

    def test_empty_input(self):
        cal = edge_calibration(pd.DataFrame())
        assert cal.empty

    def test_win_rate_in_valid_range(self, sample_results):
        cal = edge_calibration(sample_results)
        assert (cal["win_rate"] >= 0).all()
        assert (cal["win_rate"] <= 1).all()


class TestModelUncertainty:
    def test_computes_error_metrics(self, sample_results):
        unc = model_uncertainty(sample_results)
        assert "mean_error" in unc
        assert "std_error" in unc
        assert "mae" in unc
        assert "rmse" in unc
        assert unc["mae"] >= 0
        assert unc["rmse"] >= 0

    def test_empty_input(self):
        unc = model_uncertainty(pd.DataFrame())
        assert unc["mean_error"] == 0.0
        assert unc["rmse"] == 0.0

    def test_perfect_model(self):
        """A model that predicts 1.0 for 'yes' outcomes should have low error."""
        df = pd.DataFrame({
            "ticker": ["A", "B"],
            "model_prob": [1.0, 0.0],
            "result": ["yes", "no"],
        })
        unc = model_uncertainty(df)
        assert unc["mae"] == 0.0
        assert unc["rmse"] == 0.0


class TestOptimalThresholds:
    def test_finds_threshold(self, sample_results):
        thresholds = optimal_thresholds(sample_results)
        assert "best_min_edge" in thresholds
        assert "best_sharpe" in thresholds
        assert "best_hit_rate" in thresholds
        assert "best_pnl" in thresholds
        assert thresholds["best_min_edge"] >= 0.01

    def test_empty_input(self):
        thresholds = optimal_thresholds(pd.DataFrame())
        assert thresholds["best_min_edge"] == 0.0

    def test_all_results_have_small_edge(self):
        """When all edges are tiny, should still find something."""
        df = pd.DataFrame({
            "raw_edge": [0.005] * 20,
            "pnl": [1.0] * 20,
            "won": [True] * 20,
            "bankroll": list(range(10000, 10020)),
        })
        thresholds = optimal_thresholds(df)
        # All edges below 0.01 threshold, so no valid subset
        assert thresholds["best_min_edge"] == 0.0


class TestVPINEffectiveness:
    def test_computes_vpin_for_markets(self):
        trades = pd.DataFrame({
            "ticker": ["KXNBA-1"] * 200,
            "count": [10] * 200,
            "taker_side": ["yes", "no"] * 100,
            "yes_price": [0.50] * 200,
            "created_time": pd.date_range("2024-01-01", periods=200, freq="min"),
        })
        markets = pd.DataFrame({
            "ticker": ["KXNBA-1"],
            "result": ["yes"],
        })

        result = vpin_effectiveness(trades, markets, bucket_size=10, n_buckets=5)
        assert isinstance(result, pd.DataFrame)
        if not result.empty:
            assert "max_vpin" in result.columns
            assert "vpin_predicted" in result.columns

    def test_empty_inputs(self):
        result = vpin_effectiveness(pd.DataFrame(), pd.DataFrame())
        assert result.empty

    def test_too_few_trades(self):
        """Markets with fewer trades than 2*bucket_size are skipped."""
        trades = pd.DataFrame({
            "ticker": ["KXNBA-1"] * 5,
            "count": [1] * 5,
            "taker_side": ["yes"] * 5,
            "yes_price": [0.50] * 5,
            "created_time": pd.date_range("2024-01-01", periods=5, freq="min"),
        })
        markets = pd.DataFrame({
            "ticker": ["KXNBA-1"],
            "result": ["yes"],
        })
        result = vpin_effectiveness(trades, markets, bucket_size=50, n_buckets=50)
        assert result.empty


class TestComputeVpin:
    def test_balanced_trading(self):
        """Balanced buy/sell within each bucket should produce low VPIN."""
        # 200 trades of count=1, alternating yes/no → each bucket of 10
        # gets ~5 yes + 5 no → low imbalance
        trades = pd.DataFrame({
            "count": [1] * 200,
            "taker_side": ["yes", "no"] * 100,
        })
        vpin_values = _compute_vpin(trades, bucket_size=10, n_buckets=5)
        if vpin_values:
            assert all(v <= 1.0 for v in vpin_values)
            # Balanced trading → low VPIN
            assert np.mean(vpin_values) < 0.5

    def test_one_sided_trading(self):
        """All-buy trading should produce high VPIN."""
        trades = pd.DataFrame({
            "count": [10] * 100,
            "taker_side": ["yes"] * 100,
        })
        vpin_values = _compute_vpin(trades, bucket_size=10, n_buckets=5)
        if vpin_values:
            # One-sided → high VPIN
            assert np.mean(vpin_values) > 0.5


class TestGenerateReport:
    def test_report_contains_sections(self, sample_results):
        edge_cal = edge_calibration(sample_results)
        unc = model_uncertainty(sample_results)
        thresh = optimal_thresholds(sample_results)

        report = generate_calibration_report(sample_results, edge_cal, unc, thresh)

        assert "CALIBRATION REPORT" in report
        assert "Model Uncertainty" in report
        assert "Optimal Thresholds" in report
        assert "Edge Calibration" in report

    def test_report_with_empty_results(self):
        report = generate_calibration_report(
            pd.DataFrame(),
            pd.DataFrame(columns=["edge_bin", "count", "win_rate", "avg_pnl", "avg_edge"]),
            {"mean_error": 0, "std_error": 0, "mae": 0, "rmse": 0},
            {"best_min_edge": 0, "best_sharpe": 0, "best_hit_rate": 0, "best_pnl": 0},
        )
        assert "CALIBRATION REPORT" in report
