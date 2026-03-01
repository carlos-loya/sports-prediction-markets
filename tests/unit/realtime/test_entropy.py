"""Tests for entropy filter."""

from __future__ import annotations

import math

from sports_pipeline.realtime.config import EntropyConfig
from sports_pipeline.realtime.processors.entropy_filter import (
    EntropyFilter,
    binary_entropy,
    passes_entropy_filter,
)


class TestBinaryEntropy:
    def test_max_at_half(self):
        assert abs(binary_entropy(0.5) - 1.0) < 1e-10

    def test_zero_at_extremes(self):
        assert binary_entropy(0.0) == 0.0
        assert binary_entropy(1.0) == 0.0

    def test_symmetric(self):
        assert abs(binary_entropy(0.3) - binary_entropy(0.7)) < 1e-10

    def test_monotonic_increase_to_half(self):
        for p in [0.1, 0.2, 0.3, 0.4]:
            assert binary_entropy(p) < binary_entropy(p + 0.1)

    def test_known_value(self):
        # H(0.25) = -0.25*log2(0.25) - 0.75*log2(0.75) ≈ 0.8113
        expected = -(0.25 * math.log2(0.25) + 0.75 * math.log2(0.75))
        assert abs(binary_entropy(0.25) - expected) < 1e-10


class TestPassesEntropyFilter:
    def test_in_range(self):
        config = EntropyConfig(min_price=0.30, max_price=0.70)
        assert passes_entropy_filter(0.50, config) is True
        assert passes_entropy_filter(0.30, config) is True
        assert passes_entropy_filter(0.70, config) is True

    def test_out_of_range(self):
        config = EntropyConfig(min_price=0.30, max_price=0.70)
        assert passes_entropy_filter(0.10, config) is False
        assert passes_entropy_filter(0.90, config) is False
        assert passes_entropy_filter(0.29, config) is False
        assert passes_entropy_filter(0.71, config) is False


class TestEntropyFilter:
    def test_evaluate_adds_to_active(self):
        f = EntropyFilter(EntropyConfig())
        assert f.evaluate("T1", 0.50) is True
        assert "T1" in f.active_tickers

    def test_evaluate_removes_from_active(self):
        f = EntropyFilter(EntropyConfig())
        f.evaluate("T1", 0.50)
        assert "T1" in f.active_tickers
        f.evaluate("T1", 0.10)  # drifted out of range
        assert "T1" not in f.active_tickers

    def test_active_tickers_is_copy(self):
        f = EntropyFilter(EntropyConfig())
        f.evaluate("T1", 0.50)
        tickers = f.active_tickers
        tickers.add("T2")
        assert "T2" not in f.active_tickers

    def test_remove(self):
        f = EntropyFilter(EntropyConfig())
        f.evaluate("T1", 0.50)
        f.remove("T1")
        assert "T1" not in f.active_tickers
