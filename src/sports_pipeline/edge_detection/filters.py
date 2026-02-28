"""Edge quality filters: liquidity, spread, time-to-close, confidence."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from sports_pipeline.config import get_settings
from sports_pipeline.utils.logging import get_logger

log = get_logger(__name__)


class EdgeFilter:
    """Filter detected edges for actionability."""

    def __init__(self) -> None:
        self.settings = get_settings().edge_detection

    def apply(self, edges: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Apply all filters to a list of edge signals.

        Returns only edges that pass all quality checks.
        """
        before = len(edges)
        filtered = []

        for edge in edges:
            if not self._passes_min_edge(edge):
                continue
            if not self._passes_volume(edge):
                continue
            if not self._passes_time_to_close(edge):
                continue
            if not self._passes_spread(edge):
                continue
            filtered.append(edge)

        log.info("edges_filtered", before=before, after=len(filtered))
        return filtered

    def _passes_min_edge(self, edge: dict[str, Any]) -> bool:
        """Check minimum edge threshold."""
        return abs(edge.get("edge", 0)) >= self.settings.min_edge_pct

    def _passes_volume(self, edge: dict[str, Any]) -> bool:
        """Check minimum volume requirement."""
        volume = edge.get("volume", 0)
        return volume >= self.settings.min_volume

    def _passes_time_to_close(self, edge: dict[str, Any]) -> bool:
        """Check that market has enough time before closing."""
        close_time = edge.get("close_time")
        if close_time is None:
            return True  # No close time means it's open-ended

        if isinstance(close_time, str):
            close_time = datetime.fromisoformat(close_time)

        min_hours = self.settings.min_time_to_close_hours
        min_time = datetime.utcnow() + timedelta(hours=min_hours)
        return close_time > min_time

    def _passes_spread(self, edge: dict[str, Any]) -> bool:
        """Check that edge exceeds the bid-ask spread."""
        spread = edge.get("spread")
        if spread is None:
            return True  # No spread data, allow through

        abs_edge = abs(edge.get("edge", 0))
        return abs_edge > spread
