"""Model calibration: Platt scaling, isotonic regression, Brier scores."""

from __future__ import annotations

import numpy as np
from sklearn.isotonic import IsotonicRegression

from sports_pipeline.utils.logging import get_logger

log = get_logger(__name__)


def brier_score(probabilities: list[float], outcomes: list[int]) -> float:
    """Calculate Brier score for a set of predictions.

    Lower is better. Perfect = 0, worst = 1.

    Args:
        probabilities: Predicted probabilities (0-1)
        outcomes: Actual outcomes (0 or 1)

    Returns:
        Brier score.
    """
    probs = np.array(probabilities)
    actual = np.array(outcomes)
    return float(np.mean((probs - actual) ** 2))


def log_loss_score(probabilities: list[float], outcomes: list[int], eps: float = 1e-15) -> float:
    """Calculate log loss for a set of predictions.

    Args:
        probabilities: Predicted probabilities (0-1)
        outcomes: Actual outcomes (0 or 1)
        eps: Small value to prevent log(0)

    Returns:
        Log loss score.
    """
    probs = np.clip(np.array(probabilities), eps, 1 - eps)
    actual = np.array(outcomes)
    return float(-np.mean(actual * np.log(probs) + (1 - actual) * np.log(1 - probs)))


def calibration_error(
    probabilities: list[float],
    outcomes: list[int],
    n_bins: int = 10,
) -> float:
    """Calculate Expected Calibration Error (ECE).

    Args:
        probabilities: Predicted probabilities
        outcomes: Actual outcomes (0 or 1)
        n_bins: Number of bins for calibration

    Returns:
        ECE value.
    """
    probs = np.array(probabilities)
    actual = np.array(outcomes)
    bin_edges = np.linspace(0, 1, n_bins + 1)

    ece = 0.0
    total = len(probs)

    for i in range(n_bins):
        mask = (probs >= bin_edges[i]) & (probs < bin_edges[i + 1])
        if mask.sum() == 0:
            continue
        bin_acc = actual[mask].mean()
        bin_conf = probs[mask].mean()
        bin_size = mask.sum()
        ece += (bin_size / total) * abs(bin_acc - bin_conf)

    return float(ece)


class IsotonicCalibrator:
    """Calibrate probabilities using isotonic regression."""

    def __init__(self) -> None:
        self._calibrator = IsotonicRegression(
            y_min=0.0, y_max=1.0, out_of_bounds="clip"
        )
        self._is_fitted = False

    def fit(self, probabilities: list[float], outcomes: list[int]) -> None:
        """Fit calibrator on historical predictions and outcomes."""
        self._calibrator.fit(probabilities, outcomes)
        self._is_fitted = True
        log.info("isotonic_calibrator_fitted", n_samples=len(probabilities))

    def calibrate(self, probability: float) -> float:
        """Calibrate a single probability."""
        if not self._is_fitted:
            return probability
        return float(self._calibrator.predict([probability])[0])

    def calibrate_batch(self, probabilities: list[float]) -> list[float]:
        """Calibrate a list of probabilities."""
        if not self._is_fitted:
            return probabilities
        return [float(p) for p in self._calibrator.predict(probabilities)]
