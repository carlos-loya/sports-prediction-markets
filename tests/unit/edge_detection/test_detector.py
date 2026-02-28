"""Tests for edge detection."""

from __future__ import annotations

import pytest

from sports_pipeline.edge_detection.detector import EdgeDetector
from sports_pipeline.edge_detection.filters import EdgeFilter


class TestEdgeDetector:
    def test_detect_positive_edge(self, tmp_duckdb):
        detector = EdgeDetector(loader=tmp_duckdb)
        edge = detector.detect(
            kalshi_implied_prob=0.40,
            model_prob=0.55,
            kalshi_ticker="TEST-TICKER",
            market_title="Test Market",
            sport="basketball",
            market_type="game_outcome",
            model_name="elo",
        )

        assert edge is not None
        assert edge["edge"] == pytest.approx(0.15, abs=0.01)
        assert edge["suggested_side"] == "YES"
        assert edge["confidence"] == "high"

    def test_detect_negative_edge(self, tmp_duckdb):
        detector = EdgeDetector(loader=tmp_duckdb)
        edge = detector.detect(
            kalshi_implied_prob=0.60,
            model_prob=0.45,
            kalshi_ticker="TEST-TICKER",
            model_name="elo",
            sport="basketball",
            market_type="game_outcome",
        )

        assert edge is not None
        assert edge["edge"] < 0
        assert edge["suggested_side"] == "NO"

    def test_no_edge_below_threshold(self, tmp_duckdb):
        detector = EdgeDetector(loader=tmp_duckdb)
        edge = detector.detect(
            kalshi_implied_prob=0.50,
            model_prob=0.52,  # Only 2% edge, below 5% threshold
            kalshi_ticker="TEST-TICKER",
            model_name="elo",
            sport="basketball",
            market_type="game_outcome",
        )
        assert edge is None


class TestEdgeFilter:
    def test_filter_by_volume(self):
        edge_filter = EdgeFilter()
        edges = [
            {"edge": 0.10, "volume": 5, "spread": None, "close_time": None},
            {"edge": 0.10, "volume": 500, "spread": None, "close_time": None},
        ]
        filtered = edge_filter.apply(edges)
        assert len(filtered) == 1
        assert filtered[0]["volume"] == 500
