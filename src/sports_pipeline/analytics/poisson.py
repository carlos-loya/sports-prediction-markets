"""Poisson model for soccer goal prediction."""

from __future__ import annotations

from itertools import product

from scipy.stats import poisson

from sports_pipeline.analytics.base import BaseProbabilityModel
from sports_pipeline.config import get_settings


class PoissonModel(BaseProbabilityModel):
    """Poisson model for soccer match outcome prediction.

    Models goals scored as a Poisson process using team attack/defense strengths.
    """

    def __init__(self) -> None:
        super().__init__()
        settings = get_settings()
        self.league_avg_goals = settings.models.poisson.league_avg_goals
        self.attack_strength: dict[str, float] = {}
        self.defense_strength: dict[str, float] = {}

    @property
    def name(self) -> str:
        return "poisson_soccer"

    def fit(self, matches: list[dict]) -> None:
        """Fit attack/defense strengths from historical match data.

        Each dict needs: home_team, away_team, home_goals, away_goals
        """
        from collections import defaultdict

        home_scored: dict[str, list] = defaultdict(list)
        home_conceded: dict[str, list] = defaultdict(list)
        away_scored: dict[str, list] = defaultdict(list)
        away_conceded: dict[str, list] = defaultdict(list)

        for m in matches:
            home_scored[m["home_team"]].append(m["home_goals"])
            home_conceded[m["home_team"]].append(m["away_goals"])
            away_scored[m["away_team"]].append(m["away_goals"])
            away_conceded[m["away_team"]].append(m["home_goals"])

        all_teams = set(home_scored.keys()) | set(away_scored.keys())

        for team in all_teams:
            hs = home_scored.get(team, [])
            as_ = away_scored.get(team, [])
            hc = home_conceded.get(team, [])
            ac = away_conceded.get(team, [])

            total_scored = sum(hs) + sum(as_)
            total_conceded = sum(hc) + sum(ac)
            total_games = len(hs) + len(as_)

            if total_games > 0 and self.league_avg_goals > 0:
                avg_scored = total_scored / total_games
                avg_conceded = total_conceded / total_games
                self.attack_strength[team] = avg_scored / self.league_avg_goals
                self.defense_strength[team] = avg_conceded / self.league_avg_goals
            else:
                self.attack_strength[team] = 1.0
                self.defense_strength[team] = 1.0

        self.log.info("poisson_fitted", teams=len(all_teams))

    def predict_goals(self, home_team: str, away_team: str) -> tuple[float, float]:
        """Predict expected goals for each team.

        Returns:
            (home_lambda, away_lambda) - expected goals for each team
        """
        home_attack = self.attack_strength.get(home_team, 1.0)
        away_defense = self.defense_strength.get(away_team, 1.0)
        away_attack = self.attack_strength.get(away_team, 1.0)
        home_defense = self.defense_strength.get(home_team, 1.0)

        # Home advantage factor (~1.1-1.2x for home team)
        home_advantage = 1.15

        home_lambda = home_attack * away_defense * self.league_avg_goals * home_advantage
        away_lambda = away_attack * home_defense * self.league_avg_goals

        return home_lambda, away_lambda

    def predict(self, home_team: str, away_team: str, max_goals: int = 7) -> dict[str, float]:
        """Predict match outcome probabilities.

        Returns:
            Dict with "home_win", "draw", "away_win" probabilities.
        """
        home_lambda, away_lambda = self.predict_goals(home_team, away_team)

        home_win = 0.0
        draw = 0.0
        away_win = 0.0

        for h, a in product(range(max_goals + 1), repeat=2):
            prob = poisson.pmf(h, home_lambda) * poisson.pmf(a, away_lambda)
            if h > a:
                home_win += prob
            elif h == a:
                draw += prob
            else:
                away_win += prob

        return {
            "home_win": round(home_win, 4),
            "draw": round(draw, 4),
            "away_win": round(away_win, 4),
        }

    def predict_over_under(self, home_team: str, away_team: str, line: float) -> dict[str, float]:
        """Predict over/under probabilities for a given line.

        Args:
            home_team: Home team
            away_team: Away team
            line: Total goals line (e.g., 2.5)

        Returns:
            Dict with "over" and "under" probabilities.
        """
        home_lambda, away_lambda = self.predict_goals(home_team, away_team)
        total_lambda = home_lambda + away_lambda

        # P(total <= floor(line))
        under_prob = poisson.cdf(int(line), total_lambda)

        return {
            "over": round(1 - under_prob, 4),
            "under": round(under_prob, 4),
        }
