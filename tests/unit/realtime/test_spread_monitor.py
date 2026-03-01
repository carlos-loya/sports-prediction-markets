"""Tests for spread monitor."""

from __future__ import annotations

from sports_pipeline.realtime.processors.spread_monitor import (
    SpreadMonitor,
    SpreadMonitorManager,
)


class TestSpreadMonitor:
    def test_empty(self):
        m = SpreadMonitor(ticker="T1")
        assert m.current_spread is None
        assert m.avg_spread is None
        assert m.is_widening is False

    def test_record_spreads(self):
        m = SpreadMonitor(ticker="T1")
        m.on_book_update(0.48, 0.52)
        assert abs(m.current_spread - 0.04) < 1e-10

    def test_is_widening(self):
        m = SpreadMonitor(ticker="T1", window_size=10)
        # Normal spreads
        for _ in range(10):
            m.on_book_update(0.49, 0.51)  # spread = 0.02
        assert m.is_widening is False
        # Sudden widening
        m.on_book_update(0.45, 0.55)  # spread = 0.10, > 2x avg
        assert m.is_widening is True

    def test_reset(self):
        m = SpreadMonitor(ticker="T1")
        m.on_book_update(0.48, 0.52)
        m.reset()
        assert m.current_spread is None


class TestSpreadMonitorManager:
    def test_manages_multiple(self):
        mgr = SpreadMonitorManager()
        mgr.on_book_update("T1", 0.49, 0.51)
        mgr.on_book_update("T2", 0.48, 0.52)
        assert mgr.get("T1").current_spread is not None
        assert mgr.get("T2").current_spread is not None

    def test_returns_widening_status(self):
        mgr = SpreadMonitorManager(window_size=5)
        for _ in range(5):
            mgr.on_book_update("T1", 0.49, 0.51)
        assert mgr.on_book_update("T1", 0.40, 0.60) is True
