"""Logistic regression model for win probability."""

from __future__ import annotations

import numpy as np
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler

from sports_pipeline.analytics.base import BaseProbabilityModel


class LogisticModel(BaseProbabilityModel):
    """Logistic regression for match outcome prediction.

    Features: Elo diff, recent form, H2H record, xG difference, home advantage.
    """

    def __init__(self, sport: str = "soccer") -> None:
        super().__init__()
        self.sport = sport
        self.model: LogisticRegression | None = None
        self.scaler = StandardScaler()
        self._is_fitted = False

    @property
    def name(self) -> str:
        return f"logistic_{self.sport}"

    def fit(self, features: np.ndarray, labels: np.ndarray) -> None:
        """Fit the logistic regression model.

        Args:
            features: Feature matrix (n_samples, n_features)
            labels: Target labels (0=away_win, 1=draw, 2=home_win for soccer;
                    0=away_win, 1=home_win for basketball)
        """
        self.log.info("fitting_logistic", samples=len(labels), sport=self.sport)

        scaled = self.scaler.fit_transform(features)

        if self.sport == "soccer":
            self.model = LogisticRegression(
                multi_class="multinomial", max_iter=1000, C=1.0
            )
        else:
            self.model = LogisticRegression(max_iter=1000, C=1.0)

        self.model.fit(scaled, labels)
        self._is_fitted = True
        self.log.info("logistic_fitted", accuracy=round(self.model.score(scaled, labels), 3))

    def predict(self, features: np.ndarray | list[float]) -> dict[str, float]:
        """Predict outcome probabilities for a single matchup.

        Args:
            features: Feature vector for the match

        Returns:
            Dict with outcome probabilities.
        """
        if not self._is_fitted or self.model is None:
            raise RuntimeError("Model not fitted. Call fit() first.")

        arr = np.array(features).reshape(1, -1)
        scaled = self.scaler.transform(arr)
        probs = self.model.predict_proba(scaled)[0]

        if self.sport == "soccer":
            return {
                "away_win": round(float(probs[0]), 4),
                "draw": round(float(probs[1]), 4),
                "home_win": round(float(probs[2]), 4),
            }
        else:
            return {
                "away_win": round(float(probs[0]), 4),
                "home_win": round(float(probs[1]), 4),
            }

    @staticmethod
    def build_features(
        home_elo: float,
        away_elo: float,
        home_form_pts: float,
        away_form_pts: float,
        home_xg_diff: float = 0.0,
        h2h_home_win_pct: float = 0.5,
        is_home: float = 1.0,
    ) -> list[float]:
        """Build feature vector for a match.

        Returns:
            List of feature values.
        """
        return [
            home_elo - away_elo,  # Elo difference
            home_form_pts - away_form_pts,  # Form difference
            home_xg_diff,  # xG difference
            h2h_home_win_pct,  # H2H home win %
            is_home,  # Home indicator
        ]
