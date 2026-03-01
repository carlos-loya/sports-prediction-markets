"""Tests for fee model."""

from __future__ import annotations

from sports_pipeline.realtime.config import FeeConfig
from sports_pipeline.realtime.sizing.fee_model import compute_tradable_edge, is_tradable


class TestComputeTradableEdge:
    def test_basic(self):
        config = FeeConfig(taker_fee_cents=7.0, slippage_cents=1.0)
        # 0.15 - 0.07 - 0.01 = 0.07
        result = compute_tradable_edge(0.15, config)
        assert abs(result - 0.07) < 1e-10

    def test_negative_edge(self):
        config = FeeConfig(taker_fee_cents=7.0, slippage_cents=1.0)
        # 0.05 - 0.07 - 0.01 = -0.03
        result = compute_tradable_edge(0.05, config)
        assert result < 0

    def test_zero_fees(self):
        config = FeeConfig(taker_fee_cents=0.0, slippage_cents=0.0)
        result = compute_tradable_edge(0.10, config)
        assert abs(result - 0.10) < 1e-10


class TestIsTradable:
    def test_tradable(self):
        config = FeeConfig(
            taker_fee_cents=7.0,
            slippage_cents=1.0,
            min_tradable_edge=0.03,
        )
        # tradable_edge = 0.15 - 0.08 = 0.07 >= 0.03
        assert is_tradable(0.15, config) is True

    def test_not_tradable(self):
        config = FeeConfig(
            taker_fee_cents=7.0,
            slippage_cents=1.0,
            min_tradable_edge=0.03,
        )
        # tradable_edge = 0.09 - 0.08 = 0.01 < 0.03
        assert is_tradable(0.09, config) is False

    def test_edge_case_above_threshold(self):
        config = FeeConfig(
            taker_fee_cents=7.0,
            slippage_cents=1.0,
            min_tradable_edge=0.02,
        )
        # tradable_edge = 0.11 - 0.08 = 0.03 > 0.02
        assert is_tradable(0.11, config) is True
