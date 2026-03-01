"""Tests for trade logger."""

from __future__ import annotations

from sports_pipeline.realtime.events import EdgeEvent
from sports_pipeline.realtime.logging.trade_logger import TradeLogger


def _make_edge(ticker: str = "T1", rejected: bool = False) -> EdgeEvent:
    return EdgeEvent(
        ticker=ticker,
        model_prob=0.65,
        market_prob=0.50,
        raw_edge=0.15,
        tradable_edge=0.07,
        kelly_fraction=0.05,
        suggested_side="yes",
        confidence="high",
        model_name="elo",
        rejected=rejected,
        reject_reason="below_min_edge" if rejected else "",
    )


class TestTradeLogger:
    def test_log_increments_count(self):
        logger = TradeLogger(buffer_size=10)
        logger.log(_make_edge())
        assert logger.pending == 1
        assert logger.total_logged == 1

    def test_should_flush(self):
        logger = TradeLogger(buffer_size=3)
        logger.log(_make_edge())
        logger.log(_make_edge())
        assert logger.should_flush() is False
        logger.log(_make_edge())
        assert logger.should_flush() is True

    def test_flush_returns_tuples(self):
        logger = TradeLogger(buffer_size=10)
        logger.log(_make_edge("T1"))
        logger.log(_make_edge("T2", rejected=True))
        tuples = logger.flush()
        assert len(tuples) == 2
        assert logger.pending == 0
        # Each tuple should have 13 fields
        assert len(tuples[0]) == 13
        assert len(tuples[1]) == 13

    def test_flush_empty_returns_empty(self):
        logger = TradeLogger()
        assert logger.flush() == []

    def test_logs_both_traded_and_rejected(self):
        logger = TradeLogger(buffer_size=10)
        logger.log(_make_edge("T1", rejected=False))
        logger.log(_make_edge("T2", rejected=True))
        tuples = logger.flush()
        # Index 11 is rejected field
        assert tuples[0][11] is False
        assert tuples[1][11] is True
