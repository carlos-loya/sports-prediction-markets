"""Elo rating system for both soccer and basketball."""

from __future__ import annotations

from sports_pipeline.analytics.base import BaseProbabilityModel
from sports_pipeline.config import get_settings


class EloModel(BaseProbabilityModel):
    """Elo rating system for win probability estimation.

    Maintains ratings per team and computes win probabilities from rating differences.
    """

    def __init__(self, sport: str = "soccer") -> None:
        super().__init__()
        self.sport = sport
        settings = get_settings()

        if sport == "soccer":
            self.k_factor = settings.models.elo.k_factor_soccer
            self.home_advantage = settings.models.elo.home_advantage_soccer
        else:
            self.k_factor = settings.models.elo.k_factor_nba
            self.home_advantage = settings.models.elo.home_advantage_nba

        self.initial_rating = settings.models.elo.initial_rating
        self.ratings: dict[str, float] = {}

    @property
    def name(self) -> str:
        return f"elo_{self.sport}"

    def get_rating(self, team: str) -> float:
        """Get current Elo rating for a team."""
        return self.ratings.get(team, self.initial_rating)

    def set_rating(self, team: str, rating: float) -> None:
        """Set Elo rating for a team."""
        self.ratings[team] = rating

    def expected_score(self, rating_a: float, rating_b: float) -> float:
        """Calculate expected score for team A against team B.

        E_A = 1 / (1 + 10^((R_B - R_A) / 400))
        """
        return 1.0 / (1.0 + 10 ** ((rating_b - rating_a) / 400.0))

    def predict(self, home_team: str, away_team: str) -> dict[str, float]:
        """Predict match outcome probabilities.

        Args:
            home_team: Home team name
            away_team: Away team name

        Returns:
            Dict with "home_win", "draw" (soccer only), "away_win" probabilities.
        """
        home_rating = self.get_rating(home_team) + self.home_advantage
        away_rating = self.get_rating(away_team)

        home_expected = self.expected_score(home_rating, away_rating)

        if self.sport == "soccer":
            # For soccer, estimate draw probability
            # Simple heuristic: draw probability peaks when teams are evenly matched
            draw_base = 0.26
            rating_diff = abs(home_rating - away_rating)
            draw_adj = max(0, draw_base - rating_diff / 2000)
            draw_prob = draw_adj

            home_win = home_expected * (1 - draw_prob)
            away_win = (1 - home_expected) * (1 - draw_prob)

            return {
                "home_win": round(home_win, 4),
                "draw": round(draw_prob, 4),
                "away_win": round(away_win, 4),
            }
        else:
            # Basketball: no draws
            return {
                "home_win": round(home_expected, 4),
                "away_win": round(1 - home_expected, 4),
            }

    def update(
        self,
        home_team: str,
        away_team: str,
        home_goals: int,
        away_goals: int,
    ) -> None:
        """Update Elo ratings after a match result.

        Args:
            home_team: Home team name
            away_team: Away team name
            home_goals: Home team score
            away_goals: Away team score
        """
        home_rating = self.get_rating(home_team) + self.home_advantage
        away_rating = self.get_rating(away_team)

        home_expected = self.expected_score(home_rating, away_rating)

        # Actual score: 1 for win, 0.5 for draw, 0 for loss
        if home_goals > away_goals:
            home_actual = 1.0
        elif home_goals < away_goals:
            home_actual = 0.0
        else:
            home_actual = 0.5

        # Goal difference multiplier (rewards larger margins)
        goal_diff = abs(home_goals - away_goals)
        if goal_diff <= 1:
            multiplier = 1.0
        elif goal_diff == 2:
            multiplier = 1.5
        else:
            multiplier = (11.0 + goal_diff) / 8.0

        # Update ratings
        delta = self.k_factor * multiplier * (home_actual - home_expected)

        self.ratings[home_team] = self.get_rating(home_team) + delta
        self.ratings[away_team] = self.get_rating(away_team) - delta

        self.log.debug(
            "elo_updated",
            home=home_team,
            away=away_team,
            delta=round(delta, 2),
            home_new=round(self.ratings[home_team], 1),
            away_new=round(self.ratings[away_team], 1),
        )

    def bulk_update(self, matches: list[dict]) -> None:
        """Process a list of matches chronologically to build ratings.

        Each dict needs: home_team, away_team, home_goals, away_goals
        """
        for match in matches:
            self.update(
                home_team=match["home_team"],
                away_team=match["away_team"],
                home_goals=match["home_goals"],
                away_goals=match["away_goals"],
            )
