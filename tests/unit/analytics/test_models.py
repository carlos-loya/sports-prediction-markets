"""Tests for probability models."""

from __future__ import annotations

import pytest

from sports_pipeline.analytics.calibration import brier_score, calibration_error
from sports_pipeline.analytics.elo import EloModel
from sports_pipeline.analytics.poisson import PoissonModel
from sports_pipeline.edge_detection.kelly import KellyCriterion


class TestEloModel:
    def test_initial_ratings(self):
        elo = EloModel(sport="soccer")
        assert elo.get_rating("Arsenal") == 1500

    def test_predict_probabilities_sum_to_one(self):
        elo = EloModel(sport="soccer")
        preds = elo.predict("Arsenal", "Chelsea")
        total = sum(preds.values())
        assert total == pytest.approx(1.0, abs=0.01)

    def test_predict_nba_no_draws(self):
        elo = EloModel(sport="basketball")
        preds = elo.predict("Lakers", "Celtics")
        assert "draw" not in preds
        assert preds["home_win"] + preds["away_win"] == pytest.approx(1.0, abs=0.01)

    def test_update_changes_ratings(self):
        elo = EloModel(sport="soccer")
        elo.update("Arsenal", "Chelsea", home_goals=3, away_goals=0)
        assert elo.get_rating("Arsenal") > 1500
        assert elo.get_rating("Chelsea") < 1500

    def test_home_advantage(self):
        elo = EloModel(sport="soccer")
        preds = elo.predict("TeamA", "TeamB")
        # With equal ratings, home team should have advantage
        assert preds["home_win"] > preds["away_win"]

    def test_bulk_update(self):
        elo = EloModel(sport="soccer")
        matches = [
            {"home_team": "A", "away_team": "B", "home_goals": 2, "away_goals": 0},
            {"home_team": "B", "away_team": "A", "home_goals": 1, "away_goals": 1},
            {"home_team": "A", "away_team": "B", "home_goals": 3, "away_goals": 1},
        ]
        elo.bulk_update(matches)
        assert elo.get_rating("A") > elo.get_rating("B")


class TestPoissonModel:
    def test_predict_sums_to_one(self):
        model = PoissonModel()
        model.attack_strength["Home"] = 1.2
        model.defense_strength["Home"] = 0.9
        model.attack_strength["Away"] = 1.0
        model.defense_strength["Away"] = 1.1

        preds = model.predict("Home", "Away")
        total = sum(preds.values())
        assert total == pytest.approx(1.0, abs=0.01)

    def test_predict_over_under(self):
        model = PoissonModel()
        model.attack_strength["Home"] = 1.3
        model.defense_strength["Home"] = 0.8
        model.attack_strength["Away"] = 0.9
        model.defense_strength["Away"] = 1.2

        ou = model.predict_over_under("Home", "Away", line=2.5)
        assert ou["over"] + ou["under"] == pytest.approx(1.0, abs=0.01)
        assert 0 < ou["over"] < 1
        assert 0 < ou["under"] < 1

    def test_fit_from_matches(self):
        model = PoissonModel()
        matches = [
            {"home_team": "A", "away_team": "B", "home_goals": 2, "away_goals": 0},
            {"home_team": "B", "away_team": "A", "home_goals": 1, "away_goals": 3},
            {"home_team": "A", "away_team": "C", "home_goals": 1, "away_goals": 1},
        ]
        model.fit(matches)
        assert "A" in model.attack_strength
        assert model.attack_strength["A"] > model.attack_strength["B"]


class TestCalibration:
    def test_perfect_brier_score(self):
        probs = [1.0, 0.0, 1.0]
        outcomes = [1, 0, 1]
        assert brier_score(probs, outcomes) == pytest.approx(0.0)

    def test_worst_brier_score(self):
        probs = [0.0, 1.0]
        outcomes = [1, 0]
        assert brier_score(probs, outcomes) == pytest.approx(1.0)

    def test_calibration_error_range(self):
        probs = [0.3, 0.5, 0.7, 0.9]
        outcomes = [0, 1, 1, 1]
        ece = calibration_error(probs, outcomes)
        assert 0 <= ece <= 1


class TestKellyCriterion:
    def test_positive_edge(self):
        kelly = KellyCriterion(fraction=1.0)  # Full Kelly
        f = kelly.calculate(model_prob=0.60, market_price=0.50)
        assert f > 0

    def test_no_edge(self):
        kelly = KellyCriterion(fraction=1.0)
        f = kelly.calculate(model_prob=0.50, market_price=0.50)
        assert f == 0.0

    def test_negative_edge(self):
        kelly = KellyCriterion(fraction=1.0)
        f = kelly.calculate(model_prob=0.30, market_price=0.50)
        assert f == 0.0

    def test_fractional_kelly(self):
        full = KellyCriterion(fraction=1.0)
        quarter = KellyCriterion(fraction=0.25)
        f_full = full.calculate(model_prob=0.65, market_price=0.50)
        f_quarter = quarter.calculate(model_prob=0.65, market_price=0.50)
        assert f_quarter < f_full
        # Full Kelly may be capped at 0.20, so just verify quarter is smaller
        assert f_quarter > 0

    def test_cap_at_20_percent(self):
        kelly = KellyCriterion(fraction=1.0)
        f = kelly.calculate(model_prob=0.95, market_price=0.10)
        assert f <= 0.20
