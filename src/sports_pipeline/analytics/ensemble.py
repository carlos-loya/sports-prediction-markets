"""Ensemble model combining multiple probability models."""

from __future__ import annotations

from sports_pipeline.analytics.base import BaseProbabilityModel


class EnsembleModel(BaseProbabilityModel):
    """Weighted ensemble of multiple probability models."""

    def __init__(self) -> None:
        super().__init__()
        self.models: list[BaseProbabilityModel] = []
        self.weights: list[float] = []

    @property
    def name(self) -> str:
        return "ensemble"

    def add_model(self, model: BaseProbabilityModel, weight: float = 1.0) -> None:
        """Add a model to the ensemble with a given weight."""
        self.models.append(model)
        self.weights.append(weight)

    def predict(self, **kwargs) -> dict[str, float]:
        """Generate weighted average probability prediction.

        All sub-models are called with the same kwargs.
        """
        if not self.models:
            raise RuntimeError("No models in ensemble. Call add_model() first.")

        total_weight = sum(self.weights)
        combined: dict[str, float] = {}

        for model, weight in zip(self.models, self.weights):
            preds = model.predict(**kwargs)
            norm_weight = weight / total_weight

            for outcome, prob in preds.items():
                if outcome not in combined:
                    combined[outcome] = 0.0
                combined[outcome] += prob * norm_weight

        # Normalize to ensure probabilities sum to 1
        prob_sum = sum(combined.values())
        if prob_sum > 0:
            combined = {k: round(v / prob_sum, 4) for k, v in combined.items()}

        return combined

    def update_weights_from_brier(self, brier_scores: dict[str, float]) -> None:
        """Update model weights based on Brier scores (lower = better).

        Args:
            brier_scores: Dict mapping model name to Brier score
        """
        for i, model in enumerate(self.models):
            score = brier_scores.get(model.name)
            if score is not None and score > 0:
                # Inverse Brier: lower score = higher weight
                self.weights[i] = 1.0 / score

        self.log.info(
            "weights_updated",
            weights={m.name: round(w, 3) for m, w in zip(self.models, self.weights)},
        )
