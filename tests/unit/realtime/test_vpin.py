"""Tests for VPIN calculator."""

from __future__ import annotations

from sports_pipeline.realtime.config import VPINConfig
from sports_pipeline.realtime.processors.vpin import VPINBucket, VPINCalculator, VPINManager


class TestVPINBucket:
    def test_empty_bucket(self):
        b = VPINBucket()
        assert b.total_volume == 0
        assert b.imbalance == 0

    def test_buy_only(self):
        b = VPINBucket(buy_volume=100, sell_volume=0)
        assert b.total_volume == 100
        assert b.imbalance == 100

    def test_balanced(self):
        b = VPINBucket(buy_volume=50, sell_volume=50)
        assert b.imbalance == 0


class TestVPINCalculator:
    def _make_config(self, bucket_size=10, n_buckets=5) -> VPINConfig:
        return VPINConfig(bucket_size=bucket_size, n_buckets=n_buckets)

    def test_none_until_enough_buckets(self):
        calc = VPINCalculator(config=self._make_config(bucket_size=10, n_buckets=5))
        # Add trades that fill 4 buckets (40 contracts)
        for i in range(40):
            result = calc.on_trade(0.5, 1, "yes")
        assert result is None
        assert calc.bucket_count == 4

    def test_vpin_after_enough_buckets(self):
        calc = VPINCalculator(config=self._make_config(bucket_size=10, n_buckets=5))
        # Fill 5 buckets with all buys
        for _ in range(50):
            calc.on_trade(0.5, 1, "yes")
        assert calc.vpin is not None
        assert calc.vpin == 1.0  # all buys = max VPIN

    def test_balanced_flow_low_vpin(self):
        calc = VPINCalculator(config=self._make_config(bucket_size=10, n_buckets=5))
        # Alternate buy and sell in each bucket
        for _ in range(5):
            for _ in range(5):
                calc.on_trade(0.5, 1, "yes")
            for _ in range(5):
                calc.on_trade(0.5, 1, "no")
        assert calc.vpin is not None
        assert calc.vpin == 0.0  # perfectly balanced

    def test_tick_rule_classification(self):
        calc = VPINCalculator(config=self._make_config(bucket_size=10, n_buckets=5))
        # Rising prices = buy-initiated
        for i in range(50):
            price = 0.50 + i * 0.01
            calc.on_trade(price, 1)
        assert calc.vpin is not None
        assert calc.vpin == 1.0  # all classified as buys

    def test_large_trade_spans_buckets(self):
        calc = VPINCalculator(config=self._make_config(bucket_size=10, n_buckets=5))
        # Single trade of 50 fills 5 buckets
        calc.on_trade(0.5, 50, "yes")
        assert calc.bucket_count == 5
        assert calc.vpin == 1.0

    def test_is_elevated(self):
        config = self._make_config(bucket_size=10, n_buckets=5)
        config.threshold_elevated = 0.3
        calc = VPINCalculator(config=config)
        # Not enough data
        assert calc.is_elevated is False
        # Fill with all buys
        calc.on_trade(0.5, 50, "yes")
        assert calc.is_elevated is True

    def test_is_critical(self):
        config = self._make_config(bucket_size=10, n_buckets=5)
        config.threshold_critical = 0.6
        calc = VPINCalculator(config=config)
        calc.on_trade(0.5, 50, "yes")
        assert calc.is_critical is True

    def test_rolling_window(self):
        calc = VPINCalculator(config=self._make_config(bucket_size=10, n_buckets=3))
        # Fill 3 buckets with buys
        calc.on_trade(0.5, 30, "yes")
        assert calc.vpin == 1.0
        # Fill 3 more with sells (old buckets roll off)
        calc.on_trade(0.5, 30, "no")
        assert calc.vpin == 1.0  # still 1.0 because all-sell is also max imbalance

    def test_reset(self):
        calc = VPINCalculator(config=self._make_config(bucket_size=10, n_buckets=5))
        calc.on_trade(0.5, 50, "yes")
        assert calc.vpin is not None
        calc.reset()
        assert calc.vpin is None
        assert calc.bucket_count == 0


class TestVPINManager:
    def test_manages_multiple_tickers(self):
        mgr = VPINManager(VPINConfig(bucket_size=10, n_buckets=3))
        mgr.on_trade("T1", 0.5, 30, "yes")
        mgr.on_trade("T2", 0.5, 30, "no")

        assert mgr.get("T1").vpin == 1.0
        assert mgr.get("T2").vpin == 1.0

    def test_remove_ticker(self):
        mgr = VPINManager(VPINConfig(bucket_size=10, n_buckets=3))
        mgr.on_trade("T1", 0.5, 30, "yes")
        mgr.remove("T1")
        # New calculator for T1
        assert mgr.get("T1").vpin is None
