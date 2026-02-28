"""Abstract base interface for probability models."""

from __future__ import annotations

from abc import ABC, abstractmethod

from sports_pipeline.utils.logging import get_logger


class BaseProbabilityModel(ABC):
    """Base class for all probability estimation models."""

    def __init__(self) -> None:
        self.log = get_logger(self.__class__.__name__)

    @abstractmethod
    def predict(self, **kwargs) -> dict[str, float]:
        """Generate probability predictions.

        Returns:
            Dict mapping outcome labels to probabilities (must sum to ~1.0).
        """
        ...

    @property
    @abstractmethod
    def name(self) -> str:
        """Model name for logging and tracking."""
        ...
