"""Core edge detection: compare model probabilities vs Kalshi market prices."""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

import pandas as pd

from sports_pipeline.config import get_settings
from sports_pipeline.constants import (
    CONFIDENCE_HIGH,
    CONFIDENCE_LOW,
    CONFIDENCE_MEDIUM,
    SIDE_NO,
    SIDE_YES,
)
from sports_pipeline.loaders.duckdb_loader import DuckDBLoader
from sports_pipeline.utils.logging import get_logger

log = get_logger(__name__)


class EdgeDetector:
    """Detect edges between model probabilities and Kalshi market prices."""

    def __init__(self, loader: DuckDBLoader | None = None) -> None:
        self.loader = loader or DuckDBLoader()
        self.settings = get_settings()

    def detect(
        self,
        kalshi_implied_prob: float,
        model_prob: float,
        kalshi_ticker: str = "",
        market_title: str = "",
        sport: str = "",
        market_type: str = "",
        model_name: str = "",
    ) -> dict[str, Any] | None:
        """Detect edge for a single market.

        Args:
            kalshi_implied_prob: Kalshi YES price (implied probability)
            model_prob: Model's estimated probability
            kalshi_ticker: Market ticker
            market_title: Market title
            sport: Sport category
            market_type: Market type
            model_name: Name of the model

        Returns:
            Edge signal dict if edge exceeds threshold, else None.
        """
        edge = model_prob - kalshi_implied_prob
        abs_edge = abs(edge)

        if abs_edge < self.settings.edge_detection.min_edge_pct:
            return None

        # Determine edge direction and confidence
        if edge > 0:
            suggested_side = SIDE_YES
            edge_pct = edge / max(kalshi_implied_prob, 0.01)
        else:
            suggested_side = SIDE_NO
            edge_pct = abs(edge) / max(1 - kalshi_implied_prob, 0.01)

        confidence = self._classify_confidence(abs_edge)

        reasoning = (
            f"Model ({model_name}) estimates {model_prob:.1%} vs market {kalshi_implied_prob:.1%}. "
            f"Edge: {edge:+.1%} on {suggested_side} side."
        )

        return {
            "signal_id": str(uuid.uuid4())[:12],
            "timestamp": datetime.utcnow(),
            "kalshi_ticker": kalshi_ticker,
            "market_title": market_title,
            "sport": sport,
            "market_type": market_type,
            "kalshi_implied_prob": kalshi_implied_prob,
            "model_prob": model_prob,
            "edge": edge,
            "edge_pct": edge_pct,
            "confidence": confidence,
            "model_name": model_name,
            "kelly_fraction": 0.0,  # Filled later by Kelly calculator
            "suggested_side": suggested_side,
            "reasoning": reasoning,
        }

    def _classify_confidence(self, abs_edge: float) -> str:
        """Classify edge confidence based on magnitude."""
        levels = self.settings.edge_detection.confidence_levels
        if abs_edge >= levels.get("high", 0.10):
            return CONFIDENCE_HIGH
        elif abs_edge >= levels.get("medium", 0.07):
            return CONFIDENCE_MEDIUM
        return CONFIDENCE_LOW

    def detect_all(self) -> list[dict[str, Any]]:
        """Run edge detection across all active markets with model predictions.

        This queries gold layer for latest market snapshots and model predictions,
        then detects edges.
        """
        log.info("running_full_edge_detection")

        try:
            markets = self.loader.query("""
                SELECT DISTINCT ON (ticker)
                    ticker, title, sport, market_type,
                    matched_entity_id, yes_price, volume, close_time
                FROM gold.kalshi_market_snapshots
                WHERE status != 'closed'
                ORDER BY ticker, snapshot_timestamp DESC
            """)
        except Exception:
            log.warning("no_market_data_available")
            return []

        edges = []
        for _, market in markets.iterrows():
            # For each market, get model prediction
            # This is a simplified version; real implementation would
            # route to the appropriate model based on market type
            model_prob = self._get_model_prediction(market)
            if model_prob is None:
                continue

            edge = self.detect(
                kalshi_implied_prob=market["yes_price"],
                model_prob=model_prob,
                kalshi_ticker=market["ticker"],
                market_title=market.get("title", ""),
                sport=market.get("sport", ""),
                market_type=market.get("market_type", ""),
                model_name="ensemble",
            )
            if edge:
                edges.append(edge)

        log.info("edge_detection_complete", total_edges=len(edges))
        return edges

    def _get_model_prediction(self, market: pd.Series) -> float | None:
        """Get model prediction for a market. Returns None if no prediction available."""
        # This would be connected to the actual model pipeline
        # For now, returns None (no prediction available)
        return None

    def save_edges(self, edges: list[dict[str, Any]]) -> int:
        """Save detected edges to DuckDB."""
        if not edges:
            return 0

        df = pd.DataFrame(edges)
        return self.loader.load_dataframe(df, "edge_signals")
