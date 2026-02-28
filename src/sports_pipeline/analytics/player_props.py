"""Player prop probability models."""

from __future__ import annotations

import numpy as np
from scipy.stats import norm

from sports_pipeline.analytics.base import BaseProbabilityModel


class PlayerPropModel(BaseProbabilityModel):
    """Model player stat distributions for prop market predictions."""

    def __init__(self) -> None:
        super().__init__()
        self.player_stats: dict[str, dict] = {}

    @property
    def name(self) -> str:
        return "player_props"

    def set_player_stats(
        self,
        player_name: str,
        stat_type: str,
        mean: float,
        std: float,
        games: int,
    ) -> None:
        """Set player stat distribution parameters."""
        key = f"{player_name}_{stat_type}"
        self.player_stats[key] = {
            "mean": mean,
            "std": std,
            "games": games,
        }

    def fit_from_game_logs(
        self,
        player_name: str,
        stat_type: str,
        values: list[float],
    ) -> None:
        """Fit distribution from historical game log values."""
        if len(values) < 5:
            self.log.warning("insufficient_data", player=player_name, stat=stat_type, n=len(values))
            return

        arr = np.array(values)
        self.set_player_stats(
            player_name=player_name,
            stat_type=stat_type,
            mean=float(np.mean(arr)),
            std=float(np.std(arr, ddof=1)),
            games=len(values),
        )

    def predict(self, player_name: str, stat_type: str, line: float) -> dict[str, float]:
        """Predict over/under probability for a player prop.

        Args:
            player_name: Player name
            stat_type: "points", "rebounds", "assists", etc.
            line: The prop line (e.g., 24.5 points)

        Returns:
            Dict with "over" and "under" probabilities.
        """
        key = f"{player_name}_{stat_type}"
        stats = self.player_stats.get(key)

        if not stats:
            self.log.warning("no_player_stats", player=player_name, stat=stat_type)
            return {"over": 0.5, "under": 0.5}

        mean = stats["mean"]
        std = stats["std"]

        if std <= 0:
            std = 1.0  # Prevent division by zero

        # P(X > line) using normal distribution
        z = (line - mean) / std
        under_prob = float(norm.cdf(z))

        return {
            "over": round(1 - under_prob, 4),
            "under": round(under_prob, 4),
            "mean": round(mean, 1),
            "std": round(std, 1),
        }

    def predict_with_matchup_adj(
        self,
        player_name: str,
        stat_type: str,
        line: float,
        opp_def_rating: float,
        league_avg_def_rating: float = 110.0,
    ) -> dict[str, float]:
        """Predict with opponent defensive rating adjustment."""
        key = f"{player_name}_{stat_type}"
        stats = self.player_stats.get(key)

        if not stats:
            return {"over": 0.5, "under": 0.5}

        # Adjust mean based on opponent defense
        adj_factor = opp_def_rating / league_avg_def_rating
        adj_mean = stats["mean"] * adj_factor
        std = stats["std"]

        if std <= 0:
            std = 1.0

        z = (line - adj_mean) / std
        under_prob = float(norm.cdf(z))

        return {
            "over": round(1 - under_prob, 4),
            "under": round(under_prob, 4),
            "adj_mean": round(adj_mean, 1),
        }
