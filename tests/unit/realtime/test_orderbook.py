"""Tests for local order book synchronization."""

from __future__ import annotations

from sports_pipeline.realtime.websocket.orderbook_sync import (
    LocalOrderBook,
    OrderBookManager,
)


class TestLocalOrderBook:
    def test_empty_book(self):
        book = LocalOrderBook(ticker="T1")
        assert book.best_bid is None
        assert book.best_ask is None
        assert book.mid_price is None
        assert book.spread is None

    def test_apply_bid_delta(self):
        book = LocalOrderBook(ticker="T1")
        book.apply_delta(0.64, 100, "bid")
        book.apply_delta(0.63, 50, "bid")
        assert book.best_bid == 0.64
        assert book.yes_bids[0.64] == 100

    def test_apply_ask_delta(self):
        book = LocalOrderBook(ticker="T1")
        book.apply_delta(0.66, 100, "ask")
        book.apply_delta(0.67, 50, "ask")
        assert book.best_ask == 0.66

    def test_mid_price(self):
        book = LocalOrderBook(ticker="T1")
        book.apply_delta(0.64, 100, "bid")
        book.apply_delta(0.66, 100, "ask")
        assert book.mid_price == 0.65

    def test_spread(self):
        book = LocalOrderBook(ticker="T1")
        book.apply_delta(0.64, 100, "bid")
        book.apply_delta(0.66, 100, "ask")
        assert abs(book.spread - 0.02) < 1e-10

    def test_remove_level_on_negative_delta(self):
        book = LocalOrderBook(ticker="T1")
        book.apply_delta(0.64, 100, "bid")
        book.apply_delta(0.64, -100, "bid")
        assert 0.64 not in book.yes_bids
        assert book.best_bid is None

    def test_reset(self):
        book = LocalOrderBook(ticker="T1")
        book.apply_delta(0.64, 100, "bid")
        book.last_seq = 42
        book.reset()
        assert len(book.yes_bids) == 0
        assert book.last_seq == 0


class TestOrderBookManager:
    def test_get_creates_book(self):
        mgr = OrderBookManager()
        book = mgr.get_book("T1")
        assert book.ticker == "T1"

    def test_get_returns_same_book(self):
        mgr = OrderBookManager()
        b1 = mgr.get_book("T1")
        b2 = mgr.get_book("T1")
        assert b1 is b2

    def test_remove_book(self):
        mgr = OrderBookManager()
        mgr.get_book("T1")
        mgr.remove_book("T1")
        # Getting again creates a fresh one
        b = mgr.get_book("T1")
        assert len(b.yes_bids) == 0
