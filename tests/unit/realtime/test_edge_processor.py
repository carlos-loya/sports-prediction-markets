"""Tests for real-time edge processor."""

from __future__ import annotations

from sports_pipeline.realtime.config import RealtimeConfig
from sports_pipeline.realtime.events import TickEvent
from sports_pipeline.realtime.processors.edge_processor import (
    EdgeProcessor,
    ModelCache,
    ModelCacheEntry,
)


def _make_tick(ticker: str = "T1", yes_price: float = 0.50) -> TickEvent:
    return TickEvent(ticker=ticker, yes_price=yes_price, no_price=1.0 - yes_price)


class TestModelCache:
    def test_put_and_get(self):
        cache = ModelCache()
        entry = ModelCacheEntry(
            ticker="T1", model_prob=0.65, model_uncertainty=0.03, model_name="elo"
        )
        cache.put(entry)
        assert cache.get("T1") is entry
        assert cache.size == 1

    def test_get_missing(self):
        cache = ModelCache()
        assert cache.get("T1") is None

    def test_needs_refresh(self):
        cache = ModelCache(refresh_interval=0)
        assert cache.needs_refresh() is True
        cache.mark_refreshed()
        # With interval=0, immediately needs refresh again
        assert cache.needs_refresh() is True

    def test_refresh_from_dict(self):
        cache = ModelCache()
        entries = {
            "T1": ModelCacheEntry(
                ticker="T1", model_prob=0.6, model_uncertainty=0.02, model_name="elo"
            ),
            "T2": ModelCacheEntry(
                ticker="T2", model_prob=0.7, model_uncertainty=0.05, model_name="poisson"
            ),
        }
        cache.refresh_from_dict(entries)
        assert cache.size == 2
        assert cache.get("T1").model_prob == 0.6


class TestEdgeProcessor:
    def _make_processor(self, cache_entries=None) -> EdgeProcessor:
        config = RealtimeConfig()
        cache = ModelCache()
        if cache_entries:
            for entry in cache_entries:
                cache.put(entry)
        return EdgeProcessor(config, cache)

    def test_reject_no_model(self):
        proc = self._make_processor()
        result = proc.evaluate(_make_tick("T1", 0.50))
        assert result.rejected is True
        assert result.reject_reason == "no_model"

    def test_reject_entropy_filter(self):
        entry = ModelCacheEntry(
            ticker="T1", model_prob=0.90, model_uncertainty=0.02, model_name="elo"
        )
        proc = self._make_processor([entry])
        # Price at 0.90 is outside default entropy range [0.30, 0.70]
        result = proc.evaluate(_make_tick("T1", 0.90))
        assert result.rejected is True
        assert result.reject_reason == "entropy_filter"

    def test_reject_below_min_edge(self):
        entry = ModelCacheEntry(
            ticker="T1", model_prob=0.52, model_uncertainty=0.02, model_name="elo"
        )
        proc = self._make_processor([entry])
        # Edge = 0.02, after fees (0.07 + 0.01) = -0.06 < 0.03 threshold
        result = proc.evaluate(_make_tick("T1", 0.50))
        assert result.rejected is True
        assert result.reject_reason == "below_min_edge"

    def test_tradable_edge(self):
        entry = ModelCacheEntry(
            ticker="T1", model_prob=0.70, model_uncertainty=0.03, model_name="ensemble"
        )
        proc = self._make_processor([entry])
        result = proc.evaluate(_make_tick("T1", 0.50))
        assert result.rejected is False
        assert result.raw_edge > 0
        assert result.tradable_edge > 0
        assert result.kelly_fraction > 0
        assert result.suggested_side == "yes"

    def test_stats_tracking(self):
        entry = ModelCacheEntry(
            ticker="T1", model_prob=0.70, model_uncertainty=0.03, model_name="elo"
        )
        proc = self._make_processor([entry])
        proc.evaluate(_make_tick("T1", 0.50))  # traded
        proc.evaluate(_make_tick("T2", 0.50))  # rejected: no model
        stats = proc.stats
        assert stats["evaluated"] == 2
        assert stats["traded"] == 1
        assert stats["rejected"] == 1

    def test_confidence_classification(self):
        entry = ModelCacheEntry(
            ticker="T1", model_prob=0.65, model_uncertainty=0.01, model_name="elo"
        )
        proc = self._make_processor([entry])
        # Edge = 0.15, should be "high"
        result = proc.evaluate(_make_tick("T1", 0.50))
        assert result.confidence == "high"
