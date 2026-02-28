"""Pace-adjusted ratings model for NBA predictions."""

from __future__ import annotations

from sports_pipeline.analytics.base import BaseProbabilityModel
from sports_pipeline.config import get_settings


class PaceAdjustedModel(BaseProbabilityModel):
    """NBA pace-adjusted efficiency model.

    Uses offensive/defensive efficiency ratings and pace to predict
    total points and margin.
    """

    def __init__(self) -> None:
        super().__init__()
        settings = get_settings()
        self.league_avg_pace = settings.models.pace_adjusted.league_avg_pace
        self.team_stats: dict[str, dict] = {}

    @property
    def name(self) -> str:
        return "pace_adjusted_nba"

    def set_team_stats(
        self,
        team: str,
        off_rating: float,
        def_rating: float,
        pace: float,
    ) -> None:
        """Set team efficiency stats."""
        self.team_stats[team] = {
            "off_rating": off_rating,
            "def_rating": def_rating,
            "pace": pace,
        }

    def predict_score(self, home_team: str, away_team: str) -> tuple[float, float]:
        """Predict expected score for each team.

        Uses the matchup of offensive vs defensive efficiency,
        adjusted for pace.

        Returns:
            (home_score, away_score) predictions
        """
        home = self.team_stats.get(home_team, {
            "off_rating": 110.0, "def_rating": 110.0, "pace": self.league_avg_pace
        })
        away = self.team_stats.get(away_team, {
            "off_rating": 110.0, "def_rating": 110.0, "pace": self.league_avg_pace
        })

        # Expected pace of the game
        game_pace = (home["pace"] + away["pace"]) / 2

        # Home offensive efficiency vs away defensive efficiency
        home_eff = (home["off_rating"] + away["def_rating"]) / 2
        away_eff = (away["off_rating"] + home["def_rating"]) / 2

        # Convert to points: efficiency * possessions / 100
        possessions = game_pace
        home_score = home_eff * possessions / 100
        away_score = away_eff * possessions / 100

        # Home court advantage (~3 points)
        home_score += 1.5
        away_score -= 1.5

        return round(home_score, 1), round(away_score, 1)

    def predict(self, home_team: str, away_team: str) -> dict[str, float]:
        """Predict win probabilities based on expected scores.

        Uses a simple logistic mapping from expected margin to win probability.
        """
        home_score, away_score = self.predict_score(home_team, away_team)
        margin = home_score - away_score

        # Convert margin to win probability
        # Empirically, each point of margin ≈ 3% win probability
        import math
        home_win_prob = 1.0 / (1.0 + math.exp(-0.15 * margin))

        return {
            "home_win": round(home_win_prob, 4),
            "away_win": round(1 - home_win_prob, 4),
            "predicted_total": round(home_score + away_score, 1),
            "predicted_margin": round(margin, 1),
        }

    def predict_total(self, home_team: str, away_team: str, line: float) -> dict[str, float]:
        """Predict over/under for total points."""
        home_score, away_score = self.predict_score(home_team, away_team)
        predicted_total = home_score + away_score

        # Use normal approximation; NBA game totals have std dev ~15
        from scipy.stats import norm

        std_dev = 15.0
        z = (line - predicted_total) / std_dev
        under_prob = norm.cdf(z)

        return {
            "over": round(1 - under_prob, 4),
            "under": round(under_prob, 4),
            "predicted_total": round(predicted_total, 1),
        }
