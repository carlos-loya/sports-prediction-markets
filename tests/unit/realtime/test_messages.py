"""Tests for WebSocket message parsing."""

from __future__ import annotations

import pytest

from sports_pipeline.realtime.websocket.messages import (
    FillMsg,
    MarketLifecycleMsg,
    OrderBookDeltaMsg,
    TickerMsg,
    TradeMsg,
    parse_channel_message,
    ts_to_datetime,
)


class TestTickerMsg:
    def test_parse_full(self):
        data = {
            "market_ticker": "KXNBA-24FEB28-BOS-YES",
            "yes_price": 0.65,
            "no_price": 0.35,
            "yes_bid": 0.64,
            "yes_ask": 0.66,
            "volume": 1000,
            "open_interest": 500,
            "ts": 1709136000000,
        }
        msg = TickerMsg.model_validate(data)
        assert msg.market_ticker == "KXNBA-24FEB28-BOS-YES"
        assert msg.yes_price == 0.65
        assert msg.volume == 1000

    def test_parse_minimal(self):
        data = {"market_ticker": "T1"}
        msg = TickerMsg.model_validate(data)
        assert msg.yes_price == 0
        assert msg.volume == 0


class TestTradeMsg:
    def test_parse(self):
        data = {
            "market_ticker": "T1",
            "yes_price": 0.70,
            "no_price": 0.30,
            "count": 5,
            "taker_side": "yes",
            "trade_id": "trade-abc",
            "ts": 1709136000000,
        }
        msg = TradeMsg.model_validate(data)
        assert msg.count == 5
        assert msg.taker_side == "yes"


class TestOrderBookDeltaMsg:
    def test_parse_with_levels(self):
        data = {
            "market_ticker": "T1",
            "yes": [{"price": 0.64, "delta": 100}, {"price": 0.63, "delta": -50}],
            "no": [{"price": 0.36, "delta": 75}],
            "ts": 1709136000000,
        }
        msg = OrderBookDeltaMsg.model_validate(data)
        assert len(msg.yes) == 2
        assert msg.yes[0].price == 0.64
        assert msg.yes[1].delta == -50
        assert len(msg.no) == 1


class TestFillMsg:
    def test_parse(self):
        data = {
            "order_id": "ord-123",
            "market_ticker": "T1",
            "side": "yes",
            "action": "buy",
            "yes_price": 0.65,
            "count": 10,
            "remaining_count": 0,
            "ts": 1709136000000,
        }
        msg = FillMsg.model_validate(data)
        assert msg.order_id == "ord-123"
        assert msg.count == 10


class TestMarketLifecycleMsg:
    def test_parse_settled(self):
        data = {
            "market_ticker": "T1",
            "status": "settled",
            "result": "yes",
            "ts": 1709136000000,
        }
        msg = MarketLifecycleMsg.model_validate(data)
        assert msg.status == "settled"
        assert msg.result == "yes"


class TestParseChannelMessage:
    def test_ticker_channel(self):
        msg = parse_channel_message("ticker", {"market_ticker": "T1", "yes_price": 0.5})
        assert isinstance(msg, TickerMsg)

    def test_trade_channel(self):
        msg = parse_channel_message("trade", {"market_ticker": "T1", "count": 1})
        assert isinstance(msg, TradeMsg)

    def test_orderbook_delta_channel(self):
        msg = parse_channel_message("orderbook_delta", {"market_ticker": "T1"})
        assert isinstance(msg, OrderBookDeltaMsg)

    def test_fill_channel(self):
        msg = parse_channel_message(
            "fill", {"order_id": "o1", "market_ticker": "T1"}
        )
        assert isinstance(msg, FillMsg)

    def test_lifecycle_channel(self):
        msg = parse_channel_message(
            "market_lifecycle_v2", {"market_ticker": "T1", "status": "open"}
        )
        assert isinstance(msg, MarketLifecycleMsg)

    def test_unknown_channel_raises(self):
        with pytest.raises(ValueError, match="Unknown channel"):
            parse_channel_message("nonexistent", {})


class TestTsToDatetime:
    def test_valid_timestamp(self):
        dt = ts_to_datetime(1709136000000)
        assert dt.year == 2024
        assert dt.month == 2

    def test_zero_returns_now(self):
        dt = ts_to_datetime(0)
        # Should be close to now
        from datetime import datetime

        assert (datetime.utcnow() - dt).total_seconds() < 5
