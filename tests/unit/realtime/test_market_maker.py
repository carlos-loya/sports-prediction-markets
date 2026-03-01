"""Tests for Avellaneda-Stoikov market maker."""

from __future__ import annotations

import math

from sports_pipeline.realtime.config import MarketMakerConfig
from sports_pipeline.realtime.processors.market_maker import (
    AvellanedaStoikov,
    MarketMakerState,
    inv_logit,
    logit,
)


class TestLogitTransform:
    def test_roundtrip(self):
        for p in [0.1, 0.25, 0.5, 0.75, 0.9]:
            assert abs(inv_logit(logit(p)) - p) < 1e-10

    def test_logit_half(self):
        assert abs(logit(0.5)) < 1e-10

    def test_inv_logit_zero(self):
        assert abs(inv_logit(0.0) - 0.5) < 1e-10

    def test_clips_extremes(self):
        # Should not produce infinity
        assert math.isfinite(logit(0.001))
        assert math.isfinite(logit(0.999))


class TestMarketMakerState:
    def test_update_price(self):
        state = MarketMakerState(ticker="T1")
        state.update_price(0.60)
        assert state.mid_price == 0.60

    def test_volatility_default(self):
        state = MarketMakerState(ticker="T1")
        assert state.estimate_volatility() == 0.1  # default with no data

    def test_volatility_from_prices(self):
        state = MarketMakerState(ticker="T1")
        # Add enough prices to estimate volatility
        for i in range(20):
            state.update_price(0.50 + 0.01 * (i % 5))
        vol = state.estimate_volatility()
        assert vol > 0


class TestAvellanedaStoikov:
    def test_disabled_returns_none(self):
        config = MarketMakerConfig(enabled=False)
        mm = AvellanedaStoikov(config)
        assert mm.compute_quotes("T1") is None

    def test_basic_quotes(self):
        config = MarketMakerConfig(enabled=True, gamma=0.1, kappa=1.5)
        mm = AvellanedaStoikov(config)
        # Feed some prices
        for i in range(20):
            mm.on_tick("T1", 0.50 + 0.005 * (i % 3 - 1))
        quote = mm.compute_quotes("T1")
        assert quote is not None
        assert quote.bid_price < quote.ask_price
        assert 0.01 <= quote.bid_price <= 0.99
        assert 0.01 <= quote.ask_price <= 0.99

    def test_inventory_shifts_quotes(self):
        config = MarketMakerConfig(enabled=True, gamma=0.5, kappa=1.5)
        mm = AvellanedaStoikov(config)
        for i in range(20):
            mm.on_tick("T1", 0.50)
        # No inventory
        q0 = mm.compute_quotes("T1")
        # Long inventory: should shift quotes down (want to sell)
        mm.on_fill("T1", "yes", 20)
        q_long = mm.compute_quotes("T1")
        assert q_long.reservation_price < q0.reservation_price

    def test_min_spread_enforced(self):
        config = MarketMakerConfig(
            enabled=True, gamma=0.001, kappa=100, min_spread_cents=5
        )
        mm = AvellanedaStoikov(config)
        for _ in range(20):
            mm.on_tick("T1", 0.50)
        quote = mm.compute_quotes("T1")
        assert quote.spread >= 0.05  # min_spread_cents / 100

    def test_position_dependent_sizing(self):
        config = MarketMakerConfig(enabled=True, max_position=50)
        mm = AvellanedaStoikov(config)
        for _ in range(10):
            mm.on_tick("T1", 0.50)
        # Long position reduces bid size
        mm.on_fill("T1", "yes", 40)
        quote = mm.compute_quotes("T1")
        assert quote.bid_size < 50  # should be 50 - 40 = 10
